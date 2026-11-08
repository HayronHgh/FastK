use crate::chunk::file::{ChunkFile as File, ReadSpan};
use std::io::{Read, Seek, SeekFrom};
use std::mem::{self, MaybeUninit};
use std::path::Path;
use std::slice;
use std::time::Instant;

use crate::chunk::cache::{CachedChunkLayout, ChunkRuntime};
use crate::chunk::header::ChunkHeader;
use crate::chunk::sparse_index::{self, SparseIndexEntry};
use crate::error::{FastKError, Result};
use crate::metrics::StoreMetrics;
use crate::types::{ChunkMeta, FixedRecord, KlineFlowRecord, KlineRecord};

const SHORT_RANGE_MAX_RECORDS: usize = 2_048;
const MAX_RECORD_BYTES: usize = 128;

/// Kline row layouts sharing this reader. Implementations are packed little-endian
/// `i64` words with `ts` first, so every bit pattern is a valid value. Functions are
/// monomorphized per layout; the OHLCV path is unchanged by the flow layout.
pub(crate) trait KlineRow: FixedRecord + Default + Send + bytemuck::NoUninit {}

impl KlineRow for KlineRecord {}
impl KlineRow for KlineFlowRecord {}

/// Internal specialization used by the reader path.
#[allow(dead_code)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KlineReadPath {
    FullScan,
    TailRead,
    PointLookup,
    ShortRange,
}

/// Reads all records in a chunk.
#[cfg(test)]
pub fn read_all(path: &Path) -> Result<Vec<KlineRecord>> {
    let mut file = File::open(path)?;
    let layout = load_layout_from_file::<KlineRecord>(&mut file)?;
    read_record_span::<KlineRecord>(&mut file, &layout.header, 0, layout.header.count, None)
}

/// Finds an exact kline inside a cached chunk.
pub fn find_in_chunk<R: KlineRow>(
    runtime: &ChunkRuntime,
    series_dir: &Path,
    series_id: u64,
    chunk: &ChunkMeta,
    ts: i64,
) -> Result<Option<R>> {
    let layout = runtime.get_layout(chunk, series_id, series_dir)?;
    validate_header::<R>(&layout.header)?;
    let mut file = runtime.open_file(chunk, series_id, series_dir)?;
    let metrics = runtime.metrics();
    find_by_ts_in_file(&mut file, &layout, ts, Some(metrics.as_ref()))
}

/// Appends a timestamp range from a cached chunk into an output buffer.
pub fn append_range_in_chunk<R: KlineRow>(
    runtime: &ChunkRuntime,
    series_dir: &Path,
    series_id: u64,
    chunk: &ChunkMeta,
    start_ts: i64,
    end_ts: i64,
    out: &mut Vec<R>,
) -> Result<KlineReadPath> {
    let layout = runtime.get_layout(chunk, series_id, series_dir)?;
    validate_header::<R>(&layout.header)?;
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

#[cfg(test)]
fn load_layout_from_file<R: KlineRow>(file: &mut File) -> Result<CachedChunkLayout> {
    file.seek(SeekFrom::Start(0))?;
    let header = ChunkHeader::read_from(file)?;
    validate_header::<R>(&header)?;
    let sparse_index = std::sync::Arc::new(sparse_index::read_from_file(file, &header)?);
    Ok(CachedChunkLayout {
        header,
        sparse_index,
    })
}

fn find_by_ts_in_file<R: KlineRow>(
    file: &mut File,
    layout: &CachedChunkLayout,
    ts: i64,
    metrics: Option<&StoreMetrics>,
) -> Result<Option<R>> {
    if ts < layout.header.start_ts || ts > layout.header.end_ts {
        return Ok(None);
    }

    if layout.sparse_index.is_empty() {
        let idx = lower_bound(file, &layout.header, ts, metrics)?;
        if idx >= layout.header.count {
            return Ok(None);
        }
        let read_started = Instant::now();
        let record: R = read_record_at(file, &layout.header, idx, metrics)?;
        if let Some(metrics) = metrics {
            metrics.record_point_record_read_ns(read_started.elapsed().as_nanos() as u64);
        }
        return Ok((record.ts() == ts).then_some(record));
    }

    let (start_idx, end_idx) = lookup_window(&layout.header, &layout.sparse_index, ts)?;
    let read_started = Instant::now();
    let records = read_record_span::<R>(
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
    if local < records.len() && records[local].ts() == ts {
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

fn append_range_in_file_with_metrics<R: KlineRow>(
    file: &mut File,
    layout: &CachedChunkLayout,
    start_ts: i64,
    end_ts: i64,
    out: &mut Vec<R>,
    metrics: Option<&StoreMetrics>,
) -> Result<KlineReadPath> {
    if start_ts > end_ts
        || end_ts < layout.header.start_ts
        || start_ts > layout.header.end_ts
        || layout.header.count == 0
    {
        return Ok(KlineReadPath::ShortRange);
    }

    if start_ts <= layout.header.start_ts && end_ts >= layout.header.end_ts {
        append_record_span(file, &layout.header, 0, layout.header.count, out, metrics)?;
        return Ok(KlineReadPath::FullScan);
    }

    if is_short_range(&layout.header, start_ts, end_ts) {
        if !layout.sparse_index.is_empty() {
            let first = lookup_window(&layout.header, &layout.sparse_index, start_ts)?;
            let last = lookup_window(&layout.header, &layout.sparse_index, end_ts)?;
            if last.0 <= first.1 {
                // Read adjacent sparse blocks into the output, preserving earlier chunks.
                let initial_len = out.len();
                let count = last.1.checked_sub(first.0).ok_or_else(|| {
                    FastKError::InvalidData("sparse windows are out of order".into())
                })?;
                append_record_span(file, &layout.header, first.0, count, out, metrics)?;
                let rows = &out[initial_len..];
                let start = lower_bound_in_records(rows, start_ts);
                let end = upper_bound_in_records(rows, end_ts);
                out.copy_within(initial_len + start..initial_len + end, initial_len);
                out.truncate(initial_len + end - start);
                return Ok(KlineReadPath::ShortRange);
            }
        }
        let start_idx = lower_bound_via_sparse::<R>(file, layout, start_ts, metrics)?;
        if start_idx >= layout.header.count {
            return Ok(KlineReadPath::ShortRange);
        }

        let end_exclusive = upper_bound_via_sparse::<R>(file, layout, end_ts, metrics)?;
        if end_exclusive <= start_idx {
            return Ok(KlineReadPath::ShortRange);
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
        return Ok(KlineReadPath::ShortRange);
    }

    let start_idx = lower_bound(file, &layout.header, start_ts, metrics)?;
    if start_idx >= layout.header.count {
        return Ok(KlineReadPath::FullScan);
    }

    let end_exclusive = upper_bound(file, &layout.header, end_ts, metrics)?;
    if end_exclusive <= start_idx {
        return Ok(KlineReadPath::FullScan);
    }

    append_record_span(
        file,
        &layout.header,
        start_idx,
        end_exclusive - start_idx,
        out,
        metrics,
    )?;
    Ok(KlineReadPath::FullScan)
}

fn validate_header<R: KlineRow>(header: &ChunkHeader) -> Result<()> {
    if header.schema_id != R::SCHEMA_ID {
        return Err(FastKError::InvalidData(format!(
            "expected kline schema {}, got {}",
            R::SCHEMA_ID,
            header.schema_id
        )));
    }
    if header.record_size as usize != R::BYTE_SIZE {
        return Err(FastKError::InvalidData(format!(
            "unexpected kline record size: {}",
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

fn lower_bound_via_sparse<R: KlineRow>(
    file: &mut File,
    layout: &CachedChunkLayout,
    target_ts: i64,
    metrics: Option<&StoreMetrics>,
) -> Result<u64> {
    if layout.header.split_flow() || layout.sparse_index.is_empty() {
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

fn upper_bound_via_sparse<R: KlineRow>(
    file: &mut File,
    layout: &CachedChunkLayout,
    target_ts: i64,
    metrics: Option<&StoreMetrics>,
) -> Result<u64> {
    if layout.header.split_flow() || layout.sparse_index.is_empty() {
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

fn lookup_window(
    header: &ChunkHeader,
    entries: &[SparseIndexEntry],
    target_ts: i64,
) -> Result<(u64, u64)> {
    sparse_index::lookup_window(header, entries, target_ts)
}

fn is_short_range(header: &ChunkHeader, start_ts: i64, end_ts: i64) -> bool {
    if header.timeframe_ms <= 0 {
        return false;
    }

    let span = end_ts.saturating_sub(start_ts);
    let estimated = (span / header.timeframe_ms).saturating_add(1);
    estimated as usize <= SHORT_RANGE_MAX_RECORDS
}

fn read_record_at<R: KlineRow>(
    file: &mut File,
    header: &ChunkHeader,
    index: u64,
    metrics: Option<&StoreMetrics>,
) -> Result<R> {
    const { assert!(R::BYTE_SIZE <= MAX_RECORD_BYTES) };
    let offset = header.header_size as u64 + index * header.timestamp_stride();
    if header.split_flow() {
        let mut result = None;
        read_split(file, offset, header.count, 1, 8..80, metrics, |row| {
            result = Some(R::decode_le(row)?);
            Ok(())
        })?;
        return result.ok_or_else(|| FastKError::InvalidData("missing split flow row".into()));
    }
    file.seek(SeekFrom::Start(offset))?;
    let mut buffer = [0u8; MAX_RECORD_BYTES];
    let bytes = &mut buffer[..R::BYTE_SIZE];
    file.read_exact(bytes)?;
    if let Some(metrics) = metrics {
        metrics.record_bytes_read(R::BYTE_SIZE);
    }
    R::decode_le(bytes)
}

fn read_timestamp_at(
    file: &mut File,
    header: &ChunkHeader,
    index: u64,
    metrics: Option<&StoreMetrics>,
) -> Result<i64> {
    let offset = header.header_size as u64 + index * header.timestamp_stride();
    file.seek(SeekFrom::Start(offset))?;
    let mut bytes = [0u8; 8];
    file.read_exact(&mut bytes)?;
    if let Some(metrics) = metrics {
        metrics.record_bytes_read(8);
    }
    Ok(i64::from_le_bytes(bytes))
}

fn read_record_span<R: KlineRow>(
    file: &mut File,
    header: &ChunkHeader,
    start_index: u64,
    count: u64,
    metrics: Option<&StoreMetrics>,
) -> Result<Vec<R>> {
    if count == 0 {
        return Ok(Vec::new());
    }
    let end_index = match start_index.checked_add(count) {
        Some(end_index) => end_index,
        None => {
            return Err(FastKError::InvalidData(
                "requested record span overflows u64".to_string(),
            ))
        }
    };
    if end_index > header.count {
        return Err(FastKError::InvalidData(format!(
            "requested record span [{}..{}) exceeds chunk row count {}",
            start_index, end_index, header.count
        )));
    }

    if header.split_flow() {
        let mut out = Vec::new();
        append_record_span(file, header, start_index, count, &mut out, metrics)?;
        return Ok(out);
    }

    let byte_len = (count as usize)
        .checked_mul(R::BYTE_SIZE)
        .ok_or_else(|| FastKError::InvalidData("record span byte length overflow".to_string()))?;
    let offset = header.header_size as u64 + start_index * header.timestamp_stride();

    file.seek(SeekFrom::Start(offset))?;
    read_record_buffer(file, byte_len, count as usize, metrics)
}

fn append_record_span<R: KlineRow>(
    file: &mut File,
    header: &ChunkHeader,
    start_index: u64,
    count: u64,
    out: &mut Vec<R>,
    metrics: Option<&StoreMetrics>,
) -> Result<()> {
    if count == 0 {
        return Ok(());
    }

    let end_index = match start_index.checked_add(count) {
        Some(end_index) => end_index,
        None => {
            return Err(FastKError::InvalidData(
                "requested append span overflows u64".to_string(),
            ))
        }
    };
    if end_index > header.count {
        return Err(FastKError::InvalidData(format!(
            "requested append span [{}..{}) exceeds chunk row count {}",
            start_index, end_index, header.count
        )));
    }

    if header.split_flow() {
        let count = usize::try_from(count)
            .map_err(|_| FastKError::InvalidData("record count exceeds address space".into()))?;
        return read_split(
            file,
            header.header_size as u64 + start_index * 8,
            header.count,
            count,
            8..80,
            metrics,
            |row| {
                out.push(R::decode_le(row)?);
                Ok(())
            },
        );
    }

    let byte_len = (count as usize)
        .checked_mul(R::BYTE_SIZE)
        .ok_or_else(|| FastKError::InvalidData("append span byte length overflow".to_string()))?;
    let offset = header.header_size as u64 + start_index * header.timestamp_stride();

    file.seek(SeekFrom::Start(offset))?;
    append_record_buffer(file, out, byte_len, count as usize, metrics)
}

fn lower_bound_in_records<R: KlineRow>(records: &[R], target_ts: i64) -> usize {
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

fn upper_bound_in_records<R: KlineRow>(records: &[R], target_ts: i64) -> usize {
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

#[cfg(target_endian = "little")]
fn read_record_buffer<R: KlineRow>(
    file: &mut File,
    byte_len: usize,
    record_count: usize,
    metrics: Option<&StoreMetrics>,
) -> Result<Vec<R>> {
    debug_assert_eq!(mem::size_of::<R>(), R::BYTE_SIZE);

    let mut records = vec![R::default(); record_count];
    let buffer = unsafe { slice::from_raw_parts_mut(records.as_mut_ptr() as *mut u8, byte_len) };
    file.read_exact(buffer)?;
    if let Some(metrics) = metrics {
        metrics.record_bytes_read(byte_len);
    }
    Ok(records)
}

#[cfg(target_endian = "little")]
fn append_record_buffer<R: KlineRow>(
    file: &mut File,
    out: &mut Vec<R>,
    byte_len: usize,
    record_count: usize,
    metrics: Option<&StoreMetrics>,
) -> Result<()> {
    debug_assert_eq!(mem::size_of::<R>(), R::BYTE_SIZE);

    let initial_len = out.len();
    out.resize(initial_len + record_count, R::default());
    let buffer = unsafe {
        slice::from_raw_parts_mut(out.as_mut_ptr().add(initial_len) as *mut u8, byte_len)
    };
    if let Err(error) = file.read_exact(buffer) {
        out.truncate(initial_len);
        return Err(error.into());
    }
    if let Some(metrics) = metrics {
        metrics.record_bytes_read(byte_len);
    }
    Ok(())
}

#[cfg(not(target_endian = "little"))]
fn read_record_buffer<R: KlineRow>(
    file: &mut File,
    byte_len: usize,
    record_count: usize,
    metrics: Option<&StoreMetrics>,
) -> Result<Vec<R>> {
    let mut bytes = vec![0u8; byte_len];
    file.read_exact(&mut bytes)?;
    if let Some(metrics) = metrics {
        metrics.record_bytes_read(byte_len);
    }

    let mut records = Vec::with_capacity(record_count);
    for chunk in bytes.chunks_exact(R::BYTE_SIZE) {
        records.push(R::decode_le(chunk)?);
    }
    Ok(records)
}

#[cfg(not(target_endian = "little"))]
fn append_record_buffer<R: KlineRow>(
    file: &mut File,
    out: &mut Vec<R>,
    byte_len: usize,
    record_count: usize,
    metrics: Option<&StoreMetrics>,
) -> Result<()> {
    let mut records = read_record_buffer(file, byte_len, record_count, metrics)?;
    out.append(&mut records);
    Ok(())
}

pub(crate) fn plan_range<R: KlineRow>(
    runtime: &ChunkRuntime,
    series_dir: &Path,
    series_id: u64,
    chunk: &ChunkMeta,
    start: i64,
    end: i64,
) -> Result<ReadSpan> {
    let layout = runtime.get_layout(chunk, series_id, series_dir)?;
    validate_header::<R>(&layout.header)?;
    let mut file = runtime.open_file(chunk, series_id, series_dir)?;
    let first = if start <= layout.header.start_ts {
        0
    } else {
        lower_bound_via_sparse::<R>(&mut file, &layout, start, Some(&runtime.metrics()))?
    };
    let last = if end >= layout.header.end_ts {
        layout.header.count
    } else {
        upper_bound_via_sparse::<R>(&mut file, &layout, end, Some(&runtime.metrics()))?
    };
    let offset = first
        .checked_mul(layout.header.timestamp_stride())
        .and_then(|offset| offset.checked_add(layout.header.header_size as u64))
        .ok_or_else(|| FastKError::InvalidData("record offset overflow".into()))?;
    let rows = usize::try_from(last.saturating_sub(first))
        .map_err(|_| FastKError::InvalidData("record count exceeds address space".into()))?;
    Ok(ReadSpan {
        file,
        offset,
        rows,
        split_flow_count: layout.header.split_flow().then_some(layout.header.count),
    })
}

pub(crate) fn read_planned<R: KlineRow>(
    span: &ReadSpan,
    out: &mut [MaybeUninit<R>],
    metrics: &StoreMetrics,
) -> Result<()> {
    if out.len() != span.rows {
        return Err(FastKError::InvalidData(
            "read destination size mismatch".into(),
        ));
    }
    if let Some(count) = span.split_flow_count {
        let mut slots = out.iter_mut();
        return read_split(
            &mut span.file.clone(),
            span.offset,
            count,
            span.rows,
            8..80,
            Some(metrics),
            |row| {
                slots
                    .next()
                    .expect("one destination per row")
                    .write(R::decode_le(row)?);
                Ok(())
            },
        );
    }
    let mut file = span.file.clone();
    file.seek(SeekFrom::Start(span.offset))?;
    let length = out
        .len()
        .checked_mul(R::BYTE_SIZE)
        .ok_or_else(|| FastKError::InvalidData("read byte length overflow".into()))?;
    #[cfg(target_endian = "little")]
    {
        const {
            assert!(mem::size_of::<R>() == R::BYTE_SIZE);
        }
        // The destination is MaybeUninit, has no padding, and every i64 bit pattern
        // is valid. No record reference is formed until the exact read succeeds.
        let bytes = unsafe {
            slice::from_raw_parts_mut(out.as_mut_ptr().cast::<MaybeUninit<u8>>(), length)
        };
        file.read_exact_uninit(bytes)?;
    }
    #[cfg(not(target_endian = "little"))]
    {
        let mut bytes = vec![0u8; length];
        file.read_exact(&mut bytes)?;
        for (record, bytes) in out.iter_mut().zip(bytes.chunks_exact(R::BYTE_SIZE)) {
            record.write(R::decode_le(bytes)?);
        }
    }
    metrics.record_bytes_read(length);
    Ok(())
}

/// Fills OHLCV rows from a span planned over a kline flow chunk.
///
/// Flow rows begin with the OHLCV words, so each destination row is the first
/// 48 bytes of an 80-byte source row. Spans up to 64 KiB are staged in one read
/// into an uninitialized stack buffer (no allocation); larger spans use one
/// bounded heap buffer reused across 4,096-row batches.
pub(crate) fn read_planned_ohlcv_from_flow(
    span: &ReadSpan,
    out: &mut [MaybeUninit<KlineRecord>],
    metrics: &StoreMetrics,
) -> Result<()> {
    if let Some(count) = span.split_flow_count {
        let mut slots = out.iter_mut();
        return read_split(
            &mut span.file.clone(),
            span.offset,
            count,
            span.rows,
            8..48,
            Some(metrics),
            |row| {
                slots
                    .next()
                    .expect("one destination per row")
                    .write(KlineRecord::decode_le(&row[..48])?);
                Ok(())
            },
        );
    }
    const STACK_BYTES: usize = 64 * 1024;
    const BATCH_ROWS: usize = 4_096;
    if out.len() != span.rows {
        return Err(FastKError::InvalidData(
            "read destination size mismatch".into(),
        ));
    }
    let length = out
        .len()
        .checked_mul(KlineFlowRecord::BYTE_SIZE)
        .ok_or_else(|| FastKError::InvalidData("read byte length overflow".into()))?;
    let mut file = span.file.clone();
    file.seek(SeekFrom::Start(span.offset))?;
    if length <= STACK_BYTES {
        let mut stack = [MaybeUninit::<u8>::uninit(); STACK_BYTES];
        let staged = &mut stack[..length];
        file.read_exact_uninit(staged)?;
        // SAFETY: a successful exact read initialized every byte of `staged`.
        let bytes = unsafe { slice::from_raw_parts(staged.as_ptr().cast::<u8>(), length) };
        project_flow_rows(out, bytes);
    } else {
        let mut staging = vec![0u8; out.len().min(BATCH_ROWS) * KlineFlowRecord::BYTE_SIZE];
        for destination in out.chunks_mut(BATCH_ROWS) {
            let bytes = &mut staging[..destination.len() * KlineFlowRecord::BYTE_SIZE];
            file.read_exact(bytes)?;
            project_flow_rows(destination, bytes);
        }
    }
    metrics.record_bytes_read(length);
    Ok(())
}

fn project_flow_rows(out: &mut [MaybeUninit<KlineRecord>], bytes: &[u8]) {
    for (record, row) in out
        .iter_mut()
        .zip(bytes.chunks_exact(KlineFlowRecord::BYTE_SIZE))
    {
        let prefix: [u8; KlineRecord::BYTE_SIZE] = row[..KlineRecord::BYTE_SIZE]
            .try_into()
            .expect("flow rows contain the OHLCV prefix");
        record.write(KlineRecord::from_le_bytes(prefix));
    }
}

pub(crate) fn plan_tail<R: KlineRow>(
    runtime: &ChunkRuntime,
    series_dir: &Path,
    series_id: u64,
    chunk: &ChunkMeta,
    take: usize,
) -> Result<ReadSpan> {
    let layout = runtime.get_layout(chunk, series_id, series_dir)?;
    validate_header::<R>(&layout.header)?;
    let rows = take.min(layout.header.count as usize);
    let first = layout.header.count - rows as u64;
    let offset = first
        .checked_mul(layout.header.timestamp_stride())
        .and_then(|offset| offset.checked_add(layout.header.header_size as u64))
        .ok_or_else(|| FastKError::InvalidData("tail offset overflow".into()))?;
    Ok(ReadSpan {
        file: runtime.open_file(chunk, series_id, series_dir)?,
        offset,
        rows,
        split_flow_count: layout.header.split_flow().then_some(layout.header.count),
    })
}

/// Reassembles the requested words from timestamp, price and activity sections.
/// Only selected bytes are read. Batches bound scratch space for bulk reads.
pub(crate) fn read_split(
    file: &mut (impl Read + Seek),
    timestamp_offset: u64,
    chunk_rows: u64,
    rows: usize,
    fields: std::ops::Range<usize>,
    metrics: Option<&StoreMetrics>,
    mut emit: impl FnMut(&[u8; 80]) -> Result<()>,
) -> Result<()> {
    const BATCH_ROWS: usize = 1024;
    let prices = fields.start < 40;
    let activity_words = fields.end.saturating_sub(40);
    let mut times = vec![0; rows.min(BATCH_ROWS) * 8];
    let mut price_bytes = vec![0; rows.min(BATCH_ROWS) * if prices { 32 } else { 0 }];
    // A full activity section can be read contiguously. OHLCV only needs
    // volume but still reads that section, where volume is interleaved.
    let mut activity = vec![0; rows.min(BATCH_ROWS) * if activity_words > 0 { 40 } else { 0 }];
    let header = ChunkHeader::BYTE_SIZE_V2 as u64;
    let first = (timestamp_offset - header) / 8;
    for batch in (0..rows).step_by(BATCH_ROWS) {
        let n = (rows - batch).min(BATCH_ROWS);
        let index = first + batch as u64;
        let mut read = |offset, bytes: &mut [u8]| -> Result<()> {
            file.seek(SeekFrom::Start(offset))?;
            file.read_exact(bytes)?;
            if let Some(metrics) = metrics {
                metrics.record_bytes_read(bytes.len());
            }
            Ok(())
        };
        read(header + index * 8, &mut times[..n * 8])?;
        if prices {
            read(
                header + chunk_rows * 8 + index * 32,
                &mut price_bytes[..n * 32],
            )?;
        }
        if activity_words > 0 {
            read(
                header + chunk_rows * 40 + index * 40,
                &mut activity[..n * 40],
            )?;
        }
        for i in 0..n {
            let mut row = [0; 80];
            row[..8].copy_from_slice(&times[i * 8..(i + 1) * 8]);
            if prices {
                row[8..40].copy_from_slice(&price_bytes[i * 32..(i + 1) * 32]);
            }
            if activity_words > 0 {
                row[40..40 + activity_words]
                    .copy_from_slice(&activity[i * 40..i * 40 + activity_words]);
            }
            emit(&row)?;
        }
    }
    Ok(())
}

pub(crate) fn read_projection<R>(
    span: &ReadSpan,
    out: &mut [MaybeUninit<R>],
    metrics: &StoreMetrics,
    flow: bool,
    fields: std::ops::Range<usize>,
    project: fn(KlineFlowRecord) -> R,
) -> Result<()> {
    let mut file = span.file.clone();
    if let Some(count) = span.split_flow_count {
        let mut slots = out.iter_mut();
        return read_split(
            &mut file,
            span.offset,
            count,
            span.rows,
            fields,
            Some(metrics),
            |row| {
                slots
                    .next()
                    .expect("one destination per row")
                    .write(project(KlineFlowRecord::from_le_bytes(*row)));
                Ok(())
            },
        );
    }

    file.seek(SeekFrom::Start(span.offset))?;
    let stride = if flow {
        KlineFlowRecord::BYTE_SIZE
    } else {
        KlineRecord::BYTE_SIZE
    };
    let mut staging = vec![0; span.rows.min(1024) * stride];
    for batch in out.chunks_mut(1024) {
        let bytes = &mut staging[..batch.len() * stride];
        file.read_exact(bytes)?;
        metrics.record_bytes_read(bytes.len());
        for (slot, row) in batch.iter_mut().zip(bytes.chunks_exact(stride)) {
            let row = if flow {
                KlineFlowRecord::decode_le(row)?
            } else {
                KlineFlowRecord::from_ohlcv(KlineRecord::decode_le(row)?)
            };
            slot.write(project(row));
        }
    }
    Ok(())
}
