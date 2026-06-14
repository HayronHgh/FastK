use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::Path;

use crate::chunk::cache::{CachedChunkLayout, ChunkRuntime};
use crate::chunk::header::ChunkHeader;
use crate::chunk::sparse_index::{self, SparseIndexEntry};
use crate::error::{FastKError, Result};
use crate::metrics::StoreMetrics;
use crate::types::{ChunkMeta, FixedRecord};

pub fn append_range_in_chunk<R: FixedRecord>(
    runtime: &ChunkRuntime,
    series_dir: &Path,
    series_id: u64,
    chunk: &ChunkMeta,
    start_ts: i64,
    end_ts: i64,
    out: &mut Vec<R>,
) -> Result<()> {
    let layout = runtime.get_layout(chunk, series_id, series_dir)?;
    validate_header::<R>(&layout.header)?;
    let mut file = runtime.open_file(chunk, series_id, series_dir)?;
    let metrics = runtime.metrics();
    append_range_in_file(
        &mut file,
        &layout,
        start_ts,
        end_ts,
        out,
        Some(metrics.as_ref()),
    )
}

pub fn read_tail_in_chunk<R: FixedRecord>(
    runtime: &ChunkRuntime,
    series_dir: &Path,
    series_id: u64,
    chunk: &ChunkMeta,
    n: usize,
) -> Result<Vec<R>> {
    if n == 0 {
        return Ok(Vec::new());
    }
    let layout = runtime.get_layout(chunk, series_id, series_dir)?;
    validate_header::<R>(&layout.header)?;
    let mut file = runtime.open_file(chunk, series_id, series_dir)?;
    let metrics = runtime.metrics();
    let count = layout.header.count as usize;
    let start = count.saturating_sub(n) as u64;
    read_record_span(
        &mut file,
        &layout.header,
        start,
        layout.header.count - start,
        Some(metrics.as_ref()),
    )
}

fn append_range_in_file<R: FixedRecord>(
    file: &mut File,
    layout: &CachedChunkLayout,
    start_ts: i64,
    end_ts: i64,
    out: &mut Vec<R>,
    metrics: Option<&StoreMetrics>,
) -> Result<()> {
    if start_ts > end_ts
        || end_ts < layout.header.start_ts
        || start_ts > layout.header.end_ts
        || layout.header.count == 0
    {
        return Ok(());
    }

    let start_idx = lower_bound_via_sparse::<R>(file, layout, start_ts, metrics)?;
    if start_idx >= layout.header.count {
        return Ok(());
    }
    let end_exclusive = upper_bound_via_sparse::<R>(file, layout, end_ts, metrics)?;
    if end_exclusive <= start_idx {
        return Ok(());
    }
    let mut rows = read_record_span::<R>(
        file,
        &layout.header,
        start_idx,
        end_exclusive - start_idx,
        metrics,
    )?;
    out.append(&mut rows);
    Ok(())
}

fn validate_header<R: FixedRecord>(header: &ChunkHeader) -> Result<()> {
    if header.schema_id != R::SCHEMA_ID {
        return Err(FastKError::InvalidData(format!(
            "expected schema {}, got {}",
            R::SCHEMA_ID,
            header.schema_id
        )));
    }
    if header.record_size as usize != R::BYTE_SIZE {
        return Err(FastKError::InvalidData(format!(
            "unexpected record size: {}",
            header.record_size
        )));
    }
    Ok(())
}

fn lower_bound_via_sparse<R: FixedRecord>(
    file: &mut File,
    layout: &CachedChunkLayout,
    target_ts: i64,
    metrics: Option<&StoreMetrics>,
) -> Result<u64> {
    if layout.sparse_index.is_empty() {
        return lower_bound(file, &layout.header, target_ts, metrics);
    }
    let (start_idx, end_idx) = lookup_window(&layout.header, &layout.sparse_index, target_ts)?;
    let records = read_record_span::<R>(
        file,
        &layout.header,
        start_idx,
        end_idx - start_idx,
        metrics,
    )?;
    Ok(start_idx + lower_bound_in_records(&records, target_ts) as u64)
}

fn upper_bound_via_sparse<R: FixedRecord>(
    file: &mut File,
    layout: &CachedChunkLayout,
    target_ts: i64,
    metrics: Option<&StoreMetrics>,
) -> Result<u64> {
    if layout.sparse_index.is_empty() {
        return upper_bound(file, &layout.header, target_ts, metrics);
    }
    let (start_idx, end_idx) = lookup_window(&layout.header, &layout.sparse_index, target_ts)?;
    let records = read_record_span::<R>(
        file,
        &layout.header,
        start_idx,
        end_idx - start_idx,
        metrics,
    )?;
    Ok(start_idx + upper_bound_in_records(&records, target_ts) as u64)
}

fn lower_bound(
    file: &mut File,
    header: &ChunkHeader,
    target_ts: i64,
    metrics: Option<&StoreMetrics>,
) -> Result<u64> {
    let mut left = 0u64;
    let mut right = header.count;
    while left < right {
        let mid = left + (right - left) / 2;
        let ts = read_timestamp_at(file, header, mid, metrics)?;
        if ts < target_ts {
            left = mid + 1;
        } else {
            right = mid;
        }
    }
    Ok(left)
}

fn upper_bound(
    file: &mut File,
    header: &ChunkHeader,
    target_ts: i64,
    metrics: Option<&StoreMetrics>,
) -> Result<u64> {
    let mut left = 0u64;
    let mut right = header.count;
    while left < right {
        let mid = left + (right - left) / 2;
        let ts = read_timestamp_at(file, header, mid, metrics)?;
        if ts <= target_ts {
            left = mid + 1;
        } else {
            right = mid;
        }
    }
    Ok(left)
}

fn lookup_window(
    header: &ChunkHeader,
    entries: &[SparseIndexEntry],
    target_ts: i64,
) -> Result<(u64, u64)> {
    if header.count == 0 {
        return Ok((0, 0));
    }
    if entries.is_empty() {
        return Ok((0, header.count));
    }
    if let Some(anchor) = sparse_index::anchor_for_ts(entries, target_ts) {
        let anchor_pos = entries
            .iter()
            .position(|entry| *entry == anchor)
            .ok_or_else(|| {
                FastKError::InvalidData("anchor lookup produced missing entry".to_string())
            })?;
        let end_idx = entries
            .get(anchor_pos + 1)
            .map(|entry| entry.record_idx)
            .unwrap_or(header.count);
        return Ok((anchor.record_idx, end_idx));
    }
    let first_end = entries
        .get(1)
        .map(|entry| entry.record_idx)
        .unwrap_or(header.count);
    Ok((0, first_end))
}

fn read_timestamp_at(
    file: &mut File,
    header: &ChunkHeader,
    index: u64,
    metrics: Option<&StoreMetrics>,
) -> Result<i64> {
    let offset = header.header_size as u64 + index * header.record_size as u64;
    file.seek(SeekFrom::Start(offset))?;
    let mut bytes = [0u8; 8];
    file.read_exact(&mut bytes)?;
    if let Some(metrics) = metrics {
        metrics.record_bytes_read(8);
    }
    Ok(i64::from_le_bytes(bytes))
}

fn read_record_span<R: FixedRecord>(
    file: &mut File,
    header: &ChunkHeader,
    start_index: u64,
    count: u64,
    metrics: Option<&StoreMetrics>,
) -> Result<Vec<R>> {
    if count == 0 {
        return Ok(Vec::new());
    }
    let end_index = start_index
        .checked_add(count)
        .ok_or_else(|| FastKError::InvalidData("fixed span end overflow".to_string()))?;
    if end_index > header.count {
        return Err(FastKError::InvalidData(format!(
            "fixed span [{}..{}) exceeds chunk row count {}",
            start_index, end_index, header.count
        )));
    }
    let byte_len = (count as usize)
        .checked_mul(R::BYTE_SIZE)
        .ok_or_else(|| FastKError::InvalidData("fixed span byte length overflow".to_string()))?;
    let offset = header.header_size as u64 + start_index * header.record_size as u64;
    file.seek(SeekFrom::Start(offset))?;
    let mut bytes = vec![0u8; byte_len];
    file.read_exact(&mut bytes)?;
    if let Some(metrics) = metrics {
        metrics.record_bytes_read(byte_len);
    }
    bytes
        .chunks_exact(R::BYTE_SIZE)
        .map(R::decode_le)
        .collect::<Result<Vec<_>>>()
}

fn lower_bound_in_records<R: FixedRecord>(records: &[R], target_ts: i64) -> usize {
    let mut left = 0usize;
    let mut right = records.len();
    while left < right {
        let mid = left + (right - left) / 2;
        if records[mid].ts() < target_ts {
            left = mid + 1;
        } else {
            right = mid;
        }
    }
    left
}

fn upper_bound_in_records<R: FixedRecord>(records: &[R], target_ts: i64) -> usize {
    let mut left = 0usize;
    let mut right = records.len();
    while left < right {
        let mid = left + (right - left) / 2;
        if records[mid].ts() <= target_ts {
            left = mid + 1;
        } else {
            right = mid;
        }
    }
    left
}
