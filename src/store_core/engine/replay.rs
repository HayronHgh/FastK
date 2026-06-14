use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::marker::PhantomData;
use std::path::PathBuf;

use crate::chunk::header::ChunkHeader;
use crate::error::{FastKError, Result};
use crate::storage::path;
use crate::types::{ChunkMeta, ChunkState, FixedRecord, SeriesMeta};

/// Options used to create a sealed-chunk replay cursor.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReplayOptions {
    pub start_ts: i64,
    pub end_ts: Option<i64>,
    pub batch_hint: Option<usize>,
}

/// Deterministic replay cursor over sealed FastK chunks.
///
/// The cursor reads only sealed chunks. It does not follow hot segments, WAL, or live appends.
#[derive(Debug)]
pub struct ReplayCursor<R> {
    chunks: Vec<ReplayChunkRef>,
    current_chunk_index: usize,
    current_record_index: u64,
    end_ts: Option<i64>,
    exhausted: bool,
    _record: PhantomData<R>,
}

#[derive(Debug, Clone)]
struct ReplayChunkRef {
    series_dir: PathBuf,
    chunk: ChunkMeta,
    series_id: u64,
}

impl<R: FixedRecord> ReplayCursor<R> {
    pub(crate) fn new(
        series_dir: PathBuf,
        meta: &SeriesMeta,
        options: ReplayOptions,
    ) -> Result<Self> {
        if let Some(end_ts) = options.end_ts {
            if options.start_ts > end_ts {
                return Err(FastKError::InvalidInput(format!(
                    "replay start_ts {} is after end_ts {}",
                    options.start_ts, end_ts
                )));
            }
        }
        validate_meta::<R>(meta)?;

        let mut chunks: Vec<_> = meta
            .chunks
            .iter()
            .filter(|chunk| chunk.state == ChunkState::Sealed)
            .filter(|chunk| chunk.end_ts >= options.start_ts)
            .filter(|chunk| {
                options
                    .end_ts
                    .map(|end_ts| chunk.start_ts <= end_ts)
                    .unwrap_or(true)
            })
            .cloned()
            .map(|chunk| ReplayChunkRef {
                series_dir: series_dir.clone(),
                chunk,
                series_id: meta.series_id,
            })
            .collect();
        chunks.sort_by(|left, right| {
            (
                left.chunk.start_ts,
                &left.chunk.month_key,
                left.chunk.generation,
                left.chunk.chunk_id,
            )
                .cmp(&(
                    right.chunk.start_ts,
                    &right.chunk.month_key,
                    right.chunk.generation,
                    right.chunk.chunk_id,
                ))
        });

        let mut cursor = Self {
            chunks,
            current_chunk_index: 0,
            current_record_index: 0,
            end_ts: options.end_ts,
            exhausted: false,
            _record: PhantomData,
        };
        cursor.seek_to_start(options.start_ts)?;
        Ok(cursor)
    }

    /// Reads up to `max_records` in deterministic chunk/file order.
    pub fn next_batch(&mut self, max_records: usize) -> Result<Vec<R>> {
        if max_records == 0 {
            return Err(FastKError::InvalidInput(
                "replay max_records must be greater than zero".to_string(),
            ));
        }
        if self.exhausted {
            return Ok(Vec::new());
        }

        let mut out = Vec::with_capacity(max_records);
        while out.len() < max_records && !self.exhausted {
            let Some(chunk_ref) = self.chunks.get(self.current_chunk_index) else {
                self.exhausted = true;
                break;
            };
            let remaining = max_records - out.len();
            let (mut rows, next_index) = read_batch_from_chunk::<R>(
                chunk_ref,
                self.current_record_index,
                self.end_ts,
                remaining,
            )?;
            out.append(&mut rows);
            self.current_record_index = next_index;

            if self.current_record_index >= chunk_ref.chunk.count
                || self
                    .end_ts
                    .map(|end_ts| chunk_ref.chunk.end_ts > end_ts && out.len() < max_records)
                    .unwrap_or(false)
            {
                self.advance_chunk()?;
            }

            if out.len() == max_records {
                break;
            }
        }
        Ok(out)
    }

    pub fn is_exhausted(&self) -> bool {
        self.exhausted
    }

    fn seek_to_start(&mut self, start_ts: i64) -> Result<()> {
        while let Some(chunk_ref) = self.chunks.get(self.current_chunk_index) {
            if chunk_ref.chunk.end_ts < start_ts || chunk_ref.chunk.count == 0 {
                self.current_chunk_index += 1;
                continue;
            }
            let index = lower_bound_in_chunk::<R>(chunk_ref, start_ts)?;
            if index >= chunk_ref.chunk.count {
                self.current_chunk_index += 1;
                continue;
            }
            self.current_record_index = index;
            self.exhausted = false;
            return Ok(());
        }
        self.exhausted = true;
        Ok(())
    }

    fn advance_chunk(&mut self) -> Result<()> {
        self.current_chunk_index += 1;
        self.current_record_index = 0;
        while let Some(chunk_ref) = self.chunks.get(self.current_chunk_index) {
            if chunk_ref.chunk.count == 0 {
                self.current_chunk_index += 1;
                continue;
            }
            self.current_record_index = 0;
            self.exhausted = false;
            return Ok(());
        }
        self.exhausted = true;
        Ok(())
    }
}

fn validate_meta<R: FixedRecord>(meta: &SeriesMeta) -> Result<()> {
    if meta.record_type != R::RECORD_TYPE {
        return Err(FastKError::InvalidData(format!(
            "replay record type mismatch: meta={:?} requested={:?}",
            meta.record_type,
            R::RECORD_TYPE
        )));
    }
    if meta.schema_id != R::SCHEMA_ID {
        return Err(FastKError::InvalidData(format!(
            "replay schema mismatch: meta={} requested={}",
            meta.schema_id,
            R::SCHEMA_ID
        )));
    }
    if meta.record_size as usize != R::BYTE_SIZE {
        return Err(FastKError::InvalidData(format!(
            "replay record size mismatch: meta={} requested={}",
            meta.record_size,
            R::BYTE_SIZE
        )));
    }
    Ok(())
}

fn lower_bound_in_chunk<R: FixedRecord>(chunk_ref: &ReplayChunkRef, target_ts: i64) -> Result<u64> {
    let mut file = open_chunk_file(chunk_ref)?;
    let header = read_and_validate_header::<R>(&mut file, chunk_ref)?;
    let mut left = 0u64;
    let mut right = header.count;
    while left < right {
        let mid = left + (right - left) / 2;
        let ts = read_timestamp_at(&mut file, &header, mid)?;
        if ts < target_ts {
            left = mid + 1;
        } else {
            right = mid;
        }
    }
    Ok(left)
}

fn read_batch_from_chunk<R: FixedRecord>(
    chunk_ref: &ReplayChunkRef,
    start_index: u64,
    end_ts: Option<i64>,
    max_records: usize,
) -> Result<(Vec<R>, u64)> {
    if max_records == 0 || start_index >= chunk_ref.chunk.count {
        return Ok((Vec::new(), start_index));
    }
    let mut file = open_chunk_file(chunk_ref)?;
    let header = read_and_validate_header::<R>(&mut file, chunk_ref)?;
    let mut out = Vec::with_capacity(max_records.min((header.count - start_index) as usize));
    let mut index = start_index;
    while index < header.count && out.len() < max_records {
        let record = read_record_at::<R>(&mut file, &header, index)?;
        if end_ts.map(|end_ts| record.ts() > end_ts).unwrap_or(false) {
            return Ok((out, header.count));
        }
        out.push(record);
        index += 1;
    }
    Ok((out, index))
}

fn open_chunk_file(chunk_ref: &ReplayChunkRef) -> Result<File> {
    let chunk_path =
        path::resolve_relative_path(&chunk_ref.series_dir, &chunk_ref.chunk.relative_path);
    Ok(File::open(chunk_path)?)
}

fn read_and_validate_header<R: FixedRecord>(
    file: &mut File,
    chunk_ref: &ReplayChunkRef,
) -> Result<ChunkHeader> {
    file.seek(SeekFrom::Start(0))?;
    let header = ChunkHeader::read_from(file)?;
    if header.schema_id != R::SCHEMA_ID {
        return Err(FastKError::InvalidData(format!(
            "replay expected schema {}, got {}",
            R::SCHEMA_ID,
            header.schema_id
        )));
    }
    if header.record_size as usize != R::BYTE_SIZE {
        return Err(FastKError::InvalidData(format!(
            "replay unexpected record size: {}",
            header.record_size
        )));
    }
    if header.series_id != chunk_ref.series_id {
        return Err(FastKError::InvalidData(format!(
            "replay series_id mismatch: header={} expected={}",
            header.series_id, chunk_ref.series_id
        )));
    }
    Ok(header)
}

fn read_timestamp_at(file: &mut File, header: &ChunkHeader, index: u64) -> Result<i64> {
    let offset = header.header_size as u64 + index * header.record_size as u64;
    file.seek(SeekFrom::Start(offset))?;
    let mut bytes = [0u8; 8];
    file.read_exact(&mut bytes)?;
    Ok(i64::from_le_bytes(bytes))
}

fn read_record_at<R: FixedRecord>(file: &mut File, header: &ChunkHeader, index: u64) -> Result<R> {
    let offset = header.header_size as u64 + index * header.record_size as u64;
    file.seek(SeekFrom::Start(offset))?;
    let mut bytes = vec![0u8; R::BYTE_SIZE];
    file.read_exact(&mut bytes)?;
    R::decode_le(&bytes)
}
