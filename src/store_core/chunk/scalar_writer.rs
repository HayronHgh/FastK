use std::io::Write;
use std::path::Path;

use crate::chunk::header::{
    ChunkHeader, CHUNK_FLAG_HAS_SPARSE_INDEX, CHUNK_MAGIC, CHUNK_VERSION_CURRENT, SCALAR_SCHEMA_ID,
};
use crate::chunk::sparse_index::{self, DEFAULT_SPARSE_INDEX_EVERY};
use crate::error::{FastKError, Result};
use crate::storage::{fs, path};
use crate::types::{ChunkMeta, ChunkState, ScalarRecord, SeriesMeta};

/// Input describing the target identity and layout of a newly written scalar chunk.
#[derive(Debug, Clone)]
pub struct WriteScalarChunkOptions {
    pub chunk_id: u64,
    pub generation: u32,
    pub state: ChunkState,
    pub relative_path: String,
    pub sparse_index_every: u32,
}

impl Default for WriteScalarChunkOptions {
    fn default() -> Self {
        Self {
            chunk_id: 0,
            generation: 1,
            state: ChunkState::Sealed,
            relative_path: String::new(),
            sparse_index_every: DEFAULT_SPARSE_INDEX_EVERY,
        }
    }
}

/// Validates and writes a scalar chunk as `[header][records][sparse_index]`.
pub fn write_chunk(
    chunk_path: &Path,
    meta: &SeriesMeta,
    records: &[ScalarRecord],
    options: &WriteScalarChunkOptions,
) -> Result<ChunkMeta> {
    ScalarRecord::validate_strict_order(records)?;
    let first = records
        .first()
        .ok_or_else(|| FastKError::InvalidInput("records must not be empty".to_string()))?;
    let month_key = path::month_key(first.ts)?;

    for record in records.iter().skip(1) {
        let record_month = path::month_key(record.ts)?;
        if record_month != month_key {
            return Err(FastKError::InvalidInput(format!(
                "records span multiple months: {month_key} vs {record_month}"
            )));
        }
    }

    let sparse_index_every = options.sparse_index_every.max(1);
    let sparse_index = sparse_index::build_for_scalar(records, sparse_index_every)?;
    let index_bytes = sparse_index::encode_entries(&sparse_index);
    let record_bytes_len = records
        .len()
        .checked_mul(ScalarRecord::BYTE_SIZE)
        .ok_or_else(|| FastKError::InvalidInput("scalar chunk byte length overflow".to_string()))?;
    let header_size = ChunkHeader::BYTE_SIZE_V2;

    let header = ChunkHeader {
        magic: CHUNK_MAGIC,
        version: CHUNK_VERSION_CURRENT,
        header_size: header_size as u32,
        record_size: ScalarRecord::BYTE_SIZE as u32,
        schema_id: SCALAR_SCHEMA_ID,
        series_id: meta.series_id,
        chunk_id: options.chunk_id,
        generation: options.generation as u64,
        timeframe_ms: meta.timeframe_ms,
        start_ts: records[0].ts,
        end_ts: records[records.len() - 1].ts,
        count: records.len() as u64,
        flags: CHUNK_FLAG_HAS_SPARSE_INDEX,
        index_offset: (header_size + record_bytes_len) as u64,
        index_len: sparse_index.len() as u64,
        sparse_index_every: sparse_index_every as u64,
    };

    let mut bytes = Vec::with_capacity(header_size + record_bytes_len + index_bytes.len());
    bytes.extend_from_slice(&header.to_le_bytes());
    for record in records {
        bytes.extend_from_slice(&record.to_le_bytes());
    }
    bytes.extend_from_slice(&index_bytes);
    let chunk_checksum = fs::checksum64(&bytes);

    fs::atomic_write_new(chunk_path, |writer| {
        writer.write_all(&bytes)?;
        Ok(())
    })?;

    Ok(ChunkMeta {
        chunk_id: options.chunk_id,
        month_key,
        start_ts: header.start_ts,
        end_ts: header.end_ts,
        count: header.count,
        state: options.state,
        layout_version: header.version,
        header_len: header.header_size,
        sparse_index_every,
        sparse_index_offset: header.index_offset,
        sparse_index_len: header.index_len as u32,
        chunk_checksum,
        generation: options.generation,
        relative_path: options.relative_path.clone(),
        sidecars: Vec::new(),
    })
}
