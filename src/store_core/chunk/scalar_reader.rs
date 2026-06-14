use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::mem;
use std::ops::RangeInclusive;
use std::path::Path;
use std::slice;
use std::sync::Arc;
use std::time::Instant;

use crate::chunk::cache::{CachedChunkLayout, ChunkRuntime};
use crate::chunk::header::{ChunkHeader, SCALAR_SCHEMA_ID};
use crate::chunk::sparse_index::{self, SparseIndexEntry};
use crate::error::{FastKError, Result};
use crate::metrics::StoreMetrics;
use crate::types::{ChunkMeta, ScalarRecord};

const SHORT_RANGE_MAX_RECORDS: usize = 2_048;

/// Internal specialization used by scalar read paths.
#[allow(dead_code)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScalarReadPath {
    PointLookup,
    ShortRange,
    RawSpan,
}

/// Finds an exact scalar record inside a cached chunk.
pub fn find_in_chunk(
    runtime: &ChunkRuntime,
    series_dir: &Path,
    series_id: u64,
    chunk: &ChunkMeta,
    ts: i64,
) -> Result<Option<ScalarRecord>> {
    let layout = runtime.get_layout(chunk, series_id, series_dir)?;
    validate_header(&layout.header)?;
    let mut file = runtime.open_file(chunk, series_id, series_dir)?;
    let metrics = runtime.metrics();
    find_by_ts_in_file(&mut file, &layout, ts, Some(metrics.as_ref()))
}

/// Appends a timestamp range from a cached scalar chunk into an output buffer.
pub fn append_range_in_chunk(
    runtime: &ChunkRuntime,
    series_dir: &Path,
    series_id: u64,
    chunk: &ChunkMeta,
    start_ts: i64,
    end_ts: i64,
    out: &mut Vec<ScalarRecord>,
) -> Result<ScalarReadPath> {
    let layout = runtime.get_layout(chunk, series_id, series_dir)?;
    validate_header(&layout.header)?;
    let mut file = runtime.open_file(chunk, series_id, series_dir)?;
    let metrics = runtime.metrics();
    append_range_in_file_with_metrics(
        &mut file,
        &layout,
        start_ts,
        end_ts,
        out,
        Some(metrics.as_ref()),
    )
}

pub fn read_row_ranges_with_metrics(
    path: &Path,
    ranges: &[RangeInclusive<u32>],
    metrics: &StoreMetrics,
) -> Result<Vec<ScalarRecord>> {
    let mut file = File::open(path)?;
    let layout = load_layout_from_file(&mut file)?;
    validate_header(&layout.header)?;
    read_row_ranges_from_file(&mut file, &layout.header, ranges, Some(metrics))
}

fn load_layout_from_file(file: &mut File) -> Result<CachedChunkLayout> {
    file.seek(SeekFrom::Start(0))?;
    let header = ChunkHeader::read_from(file)?;
    validate_header(&header)?;
    let sparse_index = Arc::new(sparse_index::read_from_file(file, &header)?);
    Ok(CachedChunkLayout {
        header,
        sparse_index,
    })
}

fn find_by_ts_in_file(
    file: &mut File,
    layout: &CachedChunkLayout,
    ts: i64,
    metrics: Option<&StoreMetrics>,
) -> Result<Option<ScalarRecord>> {
    if ts < layout.header.start_ts || ts > layout.header.end_ts {
        return Ok(None);
    }

    if layout.sparse_index.is_empty() {
        let idx = lower_bound(file, &layout.header, ts, metrics)?;
        if idx >= layout.header.count {
            return Ok(None);
        }
        let read_started = Instant::now();
        let record = read_record_at(file, &layout.header, idx, metrics)?;
        if let Some(metrics) = metrics {
            metrics.record_point_record_read_ns(read_started.elapsed().as_nanos() as u64);
        }
        return Ok((record.ts == ts).then_some(record));
    }

    let (start_idx, end_idx) = lookup_window(&layout.header, &layout.sparse_index, ts)?;
    let read_started = Instant::now();
    let records = read_record_span(
        file,
        &layout.header,
        start_idx,
        end_idx - start_idx,
        metrics,
    )?;
    if let Some(metrics) = metrics {
        metrics.record_point_record_read_ns(read_started.elapsed().as_nanos() as u64);
    }
    let search_started = Instant::now();
    let local = lower_bound_in_records(&records, ts);
    if let Some(metrics) = metrics {
        metrics.record_point_local_search_ns(search_started.elapsed().as_nanos() as u64);
    }
    if local < records.len() && records[local].ts == ts {
        let decode_started = Instant::now();
        let record = records[local];
        if let Some(metrics) = metrics {
            metrics.record_point_decode_ns(decode_started.elapsed().as_nanos() as u64);
        }
        Ok(Some(record))
    } else {
        Ok(None)
    }
}

fn append_range_in_file_with_metrics(
    file: &mut File,
    layout: &CachedChunkLayout,
    start_ts: i64,
    end_ts: i64,
    out: &mut Vec<ScalarRecord>,
    metrics: Option<&StoreMetrics>,
) -> Result<ScalarReadPath> {
    if start_ts > end_ts
        || end_ts < layout.header.start_ts
        || start_ts > layout.header.end_ts
        || layout.header.count == 0
    {
        return Ok(ScalarReadPath::ShortRange);
    }

    if start_ts <= layout.header.start_ts && end_ts >= layout.header.end_ts {
        append_record_span(file, &layout.header, 0, layout.header.count, out, metrics)?;
        return Ok(ScalarReadPath::RawSpan);
    }

    if is_short_range(&layout.header, start_ts, end_ts) {
        let start_idx = lower_bound_via_sparse(file, layout, start_ts, metrics)?;
        if start_idx >= layout.header.count {
            return Ok(ScalarReadPath::ShortRange);
        }
        let end_exclusive = upper_bound_via_sparse(file, layout, end_ts, metrics)?;
        if end_exclusive <= start_idx {
            return Ok(ScalarReadPath::ShortRange);
        }
        out.reserve((end_exclusive - start_idx) as usize);
        append_record_span(
            file,
            &layout.header,
            start_idx,
            end_exclusive - start_idx,
            out,
            metrics,
        )?;
        return Ok(ScalarReadPath::ShortRange);
    }

    let start_idx = lower_bound(file, &layout.header, start_ts, metrics)?;
    if start_idx >= layout.header.count {
        return Ok(ScalarReadPath::RawSpan);
    }
    let end_exclusive = upper_bound(file, &layout.header, end_ts, metrics)?;
    if end_exclusive <= start_idx {
        return Ok(ScalarReadPath::RawSpan);
    }
    append_record_span(
        file,
        &layout.header,
        start_idx,
        end_exclusive - start_idx,
        out,
        metrics,
    )?;
    Ok(ScalarReadPath::RawSpan)
}

fn read_row_ranges_from_file(
    file: &mut File,
    header: &ChunkHeader,
    ranges: &[RangeInclusive<u32>],
    metrics: Option<&StoreMetrics>,
) -> Result<Vec<ScalarRecord>> {
    if ranges.is_empty() {
        return Ok(Vec::new());
    }

    let total_rows: usize = ranges
        .iter()
        .map(|range| (*range.end() as usize).saturating_sub(*range.start() as usize) + 1)
        .sum();
    let mut out = Vec::with_capacity(total_rows);
    for range in ranges {
        let start = *range.start() as u64;
        let end = *range.end() as u64;
        let count = end
            .checked_sub(start)
            .and_then(|span| span.checked_add(1))
            .ok_or_else(|| FastKError::InvalidData("scalar row span overflow".to_string()))?;
        let mut rows = read_record_span(file, header, start, count, metrics)?;
        out.append(&mut rows);
    }
    Ok(out)
}

fn validate_header(header: &ChunkHeader) -> Result<()> {
    if header.schema_id != SCALAR_SCHEMA_ID {
        return Err(FastKError::InvalidData(format!(
            "expected scalar schema {}, got {}",
            SCALAR_SCHEMA_ID, header.schema_id
        )));
    }
    if header.record_size as usize != ScalarRecord::BYTE_SIZE {
        return Err(FastKError::InvalidData(format!(
            "unexpected scalar record size: {}",
            header.record_size
        )));
    }
    Ok(())
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

fn lower_bound_via_sparse(
    file: &mut File,
    layout: &CachedChunkLayout,
    target_ts: i64,
    metrics: Option<&StoreMetrics>,
) -> Result<u64> {
    if layout.sparse_index.is_empty() {
        return lower_bound(file, &layout.header, target_ts, metrics);
    }
    let (start_idx, end_idx) = lookup_window(&layout.header, &layout.sparse_index, target_ts)?;
    let records = read_record_span(
        file,
        &layout.header,
        start_idx,
        end_idx - start_idx,
        metrics,
    )?;
    Ok(start_idx + lower_bound_in_records(&records, target_ts) as u64)
}

fn upper_bound_via_sparse(
    file: &mut File,
    layout: &CachedChunkLayout,
    target_ts: i64,
    metrics: Option<&StoreMetrics>,
) -> Result<u64> {
    if layout.sparse_index.is_empty() {
        return upper_bound(file, &layout.header, target_ts, metrics);
    }
    let (start_idx, end_idx) = lookup_window(&layout.header, &layout.sparse_index, target_ts)?;
    let records = read_record_span(
        file,
        &layout.header,
        start_idx,
        end_idx - start_idx,
        metrics,
    )?;
    Ok(start_idx + upper_bound_in_records(&records, target_ts) as u64)
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

fn is_short_range(header: &ChunkHeader, start_ts: i64, end_ts: i64) -> bool {
    if header.timeframe_ms <= 0 {
        return false;
    }
    let span = end_ts.saturating_sub(start_ts);
    let estimated = (span / header.timeframe_ms).saturating_add(1);
    estimated as usize <= SHORT_RANGE_MAX_RECORDS
}

fn read_record_at(
    file: &mut File,
    header: &ChunkHeader,
    index: u64,
    metrics: Option<&StoreMetrics>,
) -> Result<ScalarRecord> {
    let offset = header.header_size as u64 + index * header.record_size as u64;
    file.seek(SeekFrom::Start(offset))?;
    let mut bytes = [0u8; ScalarRecord::BYTE_SIZE];
    file.read_exact(&mut bytes)?;
    if let Some(metrics) = metrics {
        metrics.record_bytes_read(ScalarRecord::BYTE_SIZE);
    }
    Ok(ScalarRecord::from_le_bytes(bytes))
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

fn read_record_span(
    file: &mut File,
    header: &ChunkHeader,
    start_index: u64,
    count: u64,
    metrics: Option<&StoreMetrics>,
) -> Result<Vec<ScalarRecord>> {
    if count == 0 {
        return Ok(Vec::new());
    }

    let end_index = start_index
        .checked_add(count)
        .ok_or_else(|| FastKError::InvalidData("scalar span end overflow".to_string()))?;
    if end_index > header.count {
        return Err(FastKError::InvalidData(format!(
            "scalar span [{}..{}) exceeds chunk row count {}",
            start_index, end_index, header.count
        )));
    }

    let byte_len = (count as usize)
        .checked_mul(ScalarRecord::BYTE_SIZE)
        .ok_or_else(|| FastKError::InvalidData("scalar span byte length overflow".to_string()))?;
    let offset = header.header_size as u64 + start_index * header.record_size as u64;
    file.seek(SeekFrom::Start(offset))?;
    read_record_buffer(file, byte_len, count as usize, metrics)
}

fn append_record_span(
    file: &mut File,
    header: &ChunkHeader,
    start_index: u64,
    count: u64,
    out: &mut Vec<ScalarRecord>,
    metrics: Option<&StoreMetrics>,
) -> Result<()> {
    if count == 0 {
        return Ok(());
    }

    let end_index = start_index
        .checked_add(count)
        .ok_or_else(|| FastKError::InvalidData("scalar append span overflow".to_string()))?;
    if end_index > header.count {
        return Err(FastKError::InvalidData(format!(
            "scalar append span [{}..{}) exceeds chunk row count {}",
            start_index, end_index, header.count
        )));
    }

    let byte_len = (count as usize)
        .checked_mul(ScalarRecord::BYTE_SIZE)
        .ok_or_else(|| FastKError::InvalidData("scalar append span byte overflow".to_string()))?;
    let offset = header.header_size as u64 + start_index * header.record_size as u64;
    file.seek(SeekFrom::Start(offset))?;
    append_record_buffer(file, out, byte_len, count as usize, metrics)
}

fn lower_bound_in_records(records: &[ScalarRecord], target_ts: i64) -> usize {
    let mut left = 0usize;
    let mut right = records.len();
    while left < right {
        let mid = left + (right - left) / 2;
        if records[mid].ts < target_ts {
            left = mid + 1;
        } else {
            right = mid;
        }
    }
    left
}

fn upper_bound_in_records(records: &[ScalarRecord], target_ts: i64) -> usize {
    let mut left = 0usize;
    let mut right = records.len();
    while left < right {
        let mid = left + (right - left) / 2;
        if records[mid].ts <= target_ts {
            left = mid + 1;
        } else {
            right = mid;
        }
    }
    left
}

#[cfg(target_endian = "little")]
fn read_record_buffer(
    file: &mut File,
    byte_len: usize,
    record_count: usize,
    metrics: Option<&StoreMetrics>,
) -> Result<Vec<ScalarRecord>> {
    debug_assert_eq!(mem::size_of::<ScalarRecord>(), ScalarRecord::BYTE_SIZE);

    let mut records = Vec::<ScalarRecord>::with_capacity(record_count);
    let buffer = unsafe { slice::from_raw_parts_mut(records.as_mut_ptr() as *mut u8, byte_len) };
    file.read_exact(buffer)?;
    if let Some(metrics) = metrics {
        metrics.record_bytes_read(byte_len);
    }
    unsafe {
        records.set_len(record_count);
    }
    Ok(records)
}

#[cfg(target_endian = "little")]
fn append_record_buffer(
    file: &mut File,
    out: &mut Vec<ScalarRecord>,
    byte_len: usize,
    record_count: usize,
    metrics: Option<&StoreMetrics>,
) -> Result<()> {
    debug_assert_eq!(mem::size_of::<ScalarRecord>(), ScalarRecord::BYTE_SIZE);

    let initial_len = out.len();
    out.reserve(record_count);
    let buffer = unsafe {
        slice::from_raw_parts_mut(out.as_mut_ptr().add(initial_len) as *mut u8, byte_len)
    };
    file.read_exact(buffer)?;
    if let Some(metrics) = metrics {
        metrics.record_bytes_read(byte_len);
    }
    unsafe {
        out.set_len(initial_len + record_count);
    }
    Ok(())
}

#[cfg(not(target_endian = "little"))]
fn read_record_buffer(
    file: &mut File,
    byte_len: usize,
    record_count: usize,
    metrics: Option<&StoreMetrics>,
) -> Result<Vec<ScalarRecord>> {
    let mut bytes = vec![0u8; byte_len];
    file.read_exact(&mut bytes)?;
    if let Some(metrics) = metrics {
        metrics.record_bytes_read(byte_len);
    }
    let mut records = Vec::with_capacity(record_count);
    for chunk in bytes.chunks_exact(ScalarRecord::BYTE_SIZE) {
        let mut raw = [0u8; ScalarRecord::BYTE_SIZE];
        raw.copy_from_slice(chunk);
        records.push(ScalarRecord::from_le_bytes(raw));
    }
    Ok(records)
}

#[cfg(not(target_endian = "little"))]
fn append_record_buffer(
    file: &mut File,
    out: &mut Vec<ScalarRecord>,
    byte_len: usize,
    record_count: usize,
    metrics: Option<&StoreMetrics>,
) -> Result<()> {
    let mut records = read_record_buffer(file, byte_len, record_count, metrics)?;
    out.append(&mut records);
    Ok(())
}
