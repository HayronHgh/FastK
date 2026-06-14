use std::fs;
use std::ops::RangeInclusive;
use std::path::Path;

use crate::error::{FastKError, Result};
use crate::types::{CompareOp, ScalarPredicate, ScalarPredicateExpr, ScalarRecord};

/// Single zone-map summary entry for a contiguous row block.
#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ZoneMapEntry {
    pub start_row: u32,
    pub end_row: u32,
    pub start_ts: i64,
    pub end_ts: i64,
    pub min_value: i64,
    pub max_value: i64,
}

impl ZoneMapEntry {
    pub const BYTE_SIZE: usize = 40;

    /// Encodes an entry to fixed-width bytes.
    pub fn to_le_bytes(&self) -> [u8; Self::BYTE_SIZE] {
        let mut buf = [0u8; Self::BYTE_SIZE];
        buf[..4].copy_from_slice(&self.start_row.to_le_bytes());
        buf[4..8].copy_from_slice(&self.end_row.to_le_bytes());
        buf[8..16].copy_from_slice(&self.start_ts.to_le_bytes());
        buf[16..24].copy_from_slice(&self.end_ts.to_le_bytes());
        buf[24..32].copy_from_slice(&self.min_value.to_le_bytes());
        buf[32..40].copy_from_slice(&self.max_value.to_le_bytes());
        buf
    }

    /// Decodes an entry from fixed-width bytes.
    pub fn from_le_bytes(bytes: [u8; Self::BYTE_SIZE]) -> Self {
        let mut u32_buf = [0u8; 4];
        let mut i64_buf = [0u8; 8];

        u32_buf.copy_from_slice(&bytes[..4]);
        let start_row = u32::from_le_bytes(u32_buf);
        u32_buf.copy_from_slice(&bytes[4..8]);
        let end_row = u32::from_le_bytes(u32_buf);

        i64_buf.copy_from_slice(&bytes[8..16]);
        let start_ts = i64::from_le_bytes(i64_buf);
        i64_buf.copy_from_slice(&bytes[16..24]);
        let end_ts = i64::from_le_bytes(i64_buf);
        i64_buf.copy_from_slice(&bytes[24..32]);
        let min_value = i64::from_le_bytes(i64_buf);
        i64_buf.copy_from_slice(&bytes[32..40]);
        let max_value = i64::from_le_bytes(i64_buf);

        Self {
            start_row,
            end_row,
            start_ts,
            end_ts,
            min_value,
            max_value,
        }
    }
}

/// Builds zone-map summaries from timestamp-ordered scalar records.
pub fn build_entries(records: &[ScalarRecord], block_size: usize) -> Result<Vec<ZoneMapEntry>> {
    if block_size == 0 {
        return Err(FastKError::InvalidInput(
            "zone-map block_size must be greater than zero".to_string(),
        ));
    }
    if records.is_empty() {
        return Ok(Vec::new());
    }

    ScalarRecord::validate_strict_order(records)?;
    if records.len() > u32::MAX as usize {
        return Err(FastKError::InvalidInput(
            "zone-map supports at most u32::MAX rows per record slice".to_string(),
        ));
    }

    let mut entries = Vec::new();
    for (block_idx, block) in records.chunks(block_size).enumerate() {
        let start_row = (block_idx * block_size) as u32;
        let end_row = start_row + block.len() as u32 - 1;
        let mut min_value = i64::MAX;
        let mut max_value = i64::MIN;

        for record in block {
            min_value = min_value.min(record.value);
            max_value = max_value.max(record.value);
        }

        entries.push(ZoneMapEntry {
            start_row,
            end_row,
            start_ts: block[0].ts,
            end_ts: block[block.len() - 1].ts,
            min_value,
            max_value,
        });
    }

    Ok(entries)
}

/// Persists a `.zmap` file.
pub fn write_entries(path: &Path, entries: &[ZoneMapEntry]) -> Result<()> {
    let mut bytes = Vec::with_capacity(entries.len() * ZoneMapEntry::BYTE_SIZE);
    for entry in entries {
        bytes.extend_from_slice(&entry.to_le_bytes());
    }
    fs::write(path, bytes)?;
    Ok(())
}

/// Reads a `.zmap` file.
pub fn read_entries(path: &Path) -> Result<Vec<ZoneMapEntry>> {
    let bytes = fs::read(path)?;
    if bytes.len() % ZoneMapEntry::BYTE_SIZE != 0 {
        return Err(FastKError::InvalidData(format!(
            "invalid zmap byte length {}",
            bytes.len()
        )));
    }

    let mut entries = Vec::with_capacity(bytes.len() / ZoneMapEntry::BYTE_SIZE);
    for chunk in bytes.chunks_exact(ZoneMapEntry::BYTE_SIZE) {
        let mut raw = [0u8; ZoneMapEntry::BYTE_SIZE];
        raw.copy_from_slice(chunk);
        entries.push(ZoneMapEntry::from_le_bytes(raw));
    }
    Ok(entries)
}

/// Returns timestamps that match the predicate and time window using zone-map pruning.
pub fn find_timestamps(
    entries: &[ZoneMapEntry],
    records: &[ScalarRecord],
    predicate: &ScalarPredicate,
    start_ts: i64,
    end_ts: i64,
) -> Result<Vec<i64>> {
    if start_ts > end_ts || entries.is_empty() || records.is_empty() {
        return Ok(Vec::new());
    }

    let row_ranges = candidate_row_ranges(entries, predicate, start_ts, end_ts)?;
    let mut timestamps = Vec::new();

    for range in row_ranges {
        let start = *range.start() as usize;
        let end = *range.end() as usize;
        if end >= records.len() {
            return Err(FastKError::InvalidData(format!(
                "zone-map row {} exceeds record length {}",
                end,
                records.len()
            )));
        }

        for record in &records[start..=end] {
            if record.ts >= start_ts && record.ts <= end_ts && predicate.matches(record.value)? {
                timestamps.push(record.ts);
            }
        }
    }

    timestamps.sort_unstable();
    timestamps.dedup();
    Ok(timestamps)
}

pub fn candidate_row_ranges(
    entries: &[ZoneMapEntry],
    predicate: &ScalarPredicate,
    start_ts: i64,
    end_ts: i64,
) -> Result<Vec<RangeInclusive<u32>>> {
    let mut ranges = Vec::new();

    for entry in entries {
        if entry.end_ts < start_ts || entry.start_ts > end_ts {
            continue;
        }
        if entry_may_match(entry, predicate)? {
            ranges.push(entry.start_row..=entry.end_row);
        }
    }

    Ok(ranges)
}

pub fn candidate_row_ranges_for_expr(
    entries: &[ZoneMapEntry],
    predicate: &ScalarPredicateExpr,
    start_ts: i64,
    end_ts: i64,
) -> Result<Vec<RangeInclusive<u32>>> {
    predicate.validate()?;
    let mut ranges = Vec::new();

    for entry in entries {
        if entry.end_ts < start_ts || entry.start_ts > end_ts {
            continue;
        }
        if predicate.may_match_value_range(entry.min_value, entry.max_value) {
            ranges.push(entry.start_row..=entry.end_row);
        }
    }

    Ok(ranges)
}

fn entry_may_match(entry: &ZoneMapEntry, predicate: &ScalarPredicate) -> Result<bool> {
    Ok(match predicate.op {
        CompareOp::Gt => entry.max_value > predicate.value,
        CompareOp::Gte => entry.max_value >= predicate.value,
        CompareOp::Lt => entry.min_value < predicate.value,
        CompareOp::Lte => entry.min_value <= predicate.value,
        CompareOp::Eq => entry.min_value <= predicate.value && entry.max_value >= predicate.value,
        CompareOp::Between => {
            let (lower, upper) = predicate.bounds()?;
            entry.max_value >= lower && entry.min_value <= upper
        }
    })
}
