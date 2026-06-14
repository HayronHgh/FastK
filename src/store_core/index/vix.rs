use std::fs;
use std::path::Path;

use crate::error::{FastKError, Result};
use crate::types::{CompareOp, ScalarPredicate, ScalarPredicateExpr, ScalarRecord};

/// Single value-index entry sorted by `(value, ts)`.
#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ValueIndexEntry {
    pub value: i64,
    pub ts: i64,
}

impl ValueIndexEntry {
    pub const BYTE_SIZE: usize = 16;

    /// Encodes an entry to fixed-width bytes.
    pub fn to_le_bytes(&self) -> [u8; Self::BYTE_SIZE] {
        let mut buf = [0u8; Self::BYTE_SIZE];
        buf[..8].copy_from_slice(&self.value.to_le_bytes());
        buf[8..16].copy_from_slice(&self.ts.to_le_bytes());
        buf
    }

    /// Decodes an entry from fixed-width bytes.
    pub fn from_le_bytes(bytes: [u8; Self::BYTE_SIZE]) -> Self {
        let mut value_buf = [0u8; 8];
        let mut ts_buf = [0u8; 8];
        value_buf.copy_from_slice(&bytes[..8]);
        ts_buf.copy_from_slice(&bytes[8..16]);
        Self {
            value: i64::from_le_bytes(value_buf),
            ts: i64::from_le_bytes(ts_buf),
        }
    }
}

/// Builds a sorted value index from scalar records.
pub fn build_entries(records: &[ScalarRecord]) -> Vec<ValueIndexEntry> {
    let mut entries: Vec<_> = records
        .iter()
        .map(|record| ValueIndexEntry {
            value: record.value,
            ts: record.ts,
        })
        .collect();

    entries.sort_by_key(|entry| (entry.value, entry.ts));
    entries
}

/// Persists a `.vix` file.
pub fn write_entries(path: &Path, entries: &[ValueIndexEntry]) -> Result<()> {
    let mut bytes = Vec::with_capacity(entries.len() * ValueIndexEntry::BYTE_SIZE);
    for entry in entries {
        bytes.extend_from_slice(&entry.to_le_bytes());
    }
    fs::write(path, bytes)?;
    Ok(())
}

/// Reads a `.vix` file.
pub fn read_entries(path: &Path) -> Result<Vec<ValueIndexEntry>> {
    let bytes = fs::read(path)?;
    if bytes.len() % ValueIndexEntry::BYTE_SIZE != 0 {
        return Err(FastKError::InvalidData(format!(
            "invalid vix byte length {}",
            bytes.len()
        )));
    }

    let mut entries = Vec::with_capacity(bytes.len() / ValueIndexEntry::BYTE_SIZE);
    for chunk in bytes.chunks_exact(ValueIndexEntry::BYTE_SIZE) {
        let mut raw = [0u8; ValueIndexEntry::BYTE_SIZE];
        raw.copy_from_slice(chunk);
        entries.push(ValueIndexEntry::from_le_bytes(raw));
    }
    Ok(entries)
}

/// Returns timestamps that match the predicate and time window using value-sorted entries.
pub fn find_timestamps(
    entries: &[ValueIndexEntry],
    predicate: &ScalarPredicate,
    start_ts: i64,
    end_ts: i64,
) -> Result<Vec<i64>> {
    if start_ts > end_ts || entries.is_empty() {
        return Ok(Vec::new());
    }

    let range = matching_slice(entries, predicate)?;
    let mut timestamps = Vec::new();

    for entry in &entries[range] {
        if entry.ts >= start_ts && entry.ts <= end_ts && predicate.matches(entry.value)? {
            timestamps.push(entry.ts);
        }
    }

    timestamps.sort_unstable();
    timestamps.dedup();
    Ok(timestamps)
}

pub fn matching_slices_for_expr(
    entries: &[ValueIndexEntry],
    predicate: &ScalarPredicateExpr,
) -> Result<Vec<std::ops::Range<usize>>> {
    predicate.validate()?;
    let len = entries.len();
    if len == 0 {
        return Ok(Vec::new());
    }

    let ranges = match predicate {
        ScalarPredicateExpr::Eq(value) => {
            vec![lower_bound(entries, *value)..upper_bound(entries, *value)]
        }
        ScalarPredicateExpr::InSet(values) => {
            let mut values = values.clone();
            values.sort_unstable();
            values.dedup();
            values
                .into_iter()
                .map(|value| lower_bound(entries, value)..upper_bound(entries, value))
                .filter(|range| !range.is_empty())
                .collect()
        }
        ScalarPredicateExpr::Gt(value) => vec![upper_bound(entries, *value)..len],
        ScalarPredicateExpr::Gte(value) => vec![lower_bound(entries, *value)..len],
        ScalarPredicateExpr::Lt(value) => vec![0..lower_bound(entries, *value)],
        ScalarPredicateExpr::Lte(value) => vec![0..upper_bound(entries, *value)],
        ScalarPredicateExpr::Between {
            min,
            max,
            inclusive,
        } => {
            let start = if *inclusive {
                lower_bound(entries, *min)
            } else {
                upper_bound(entries, *min)
            };
            let end = if *inclusive {
                upper_bound(entries, *max)
            } else {
                lower_bound(entries, *max)
            };
            vec![start..end]
        }
        ScalarPredicateExpr::Ne(_) | ScalarPredicateExpr::NotInSet(_) => vec![0..len],
    };

    Ok(ranges
        .into_iter()
        .filter(|range| !range.is_empty())
        .collect())
}

fn matching_slice(
    entries: &[ValueIndexEntry],
    predicate: &ScalarPredicate,
) -> Result<std::ops::Range<usize>> {
    let len = entries.len();

    let range = match predicate.op {
        CompareOp::Gt => upper_bound(entries, predicate.value)..len,
        CompareOp::Gte => lower_bound(entries, predicate.value)..len,
        CompareOp::Lt => 0..lower_bound(entries, predicate.value),
        CompareOp::Lte => 0..upper_bound(entries, predicate.value),
        CompareOp::Eq => {
            let start = lower_bound(entries, predicate.value);
            let end = upper_bound(entries, predicate.value);
            start..end
        }
        CompareOp::Between => {
            let (lower, upper) = predicate.bounds()?;
            lower_bound(entries, lower)..upper_bound(entries, upper)
        }
    };

    Ok(range)
}

fn lower_bound(entries: &[ValueIndexEntry], target: i64) -> usize {
    let mut left = 0usize;
    let mut right = entries.len();

    while left < right {
        let mid = left + (right - left) / 2;
        if entries[mid].value < target {
            left = mid + 1;
        } else {
            right = mid;
        }
    }

    left
}

fn upper_bound(entries: &[ValueIndexEntry], target: i64) -> usize {
    let mut left = 0usize;
    let mut right = entries.len();

    while left < right {
        let mid = left + (right - left) / 2;
        if entries[mid].value <= target {
            left = mid + 1;
        } else {
            right = mid;
        }
    }

    left
}
