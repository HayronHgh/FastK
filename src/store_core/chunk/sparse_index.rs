use std::fs::File;
use std::io::{Read, Seek, SeekFrom};

use crate::chunk::header::{ChunkHeader, CHUNK_FLAG_HAS_SPARSE_INDEX};
use crate::error::{FastKError, Result};
use crate::types::{FixedRecord, KlineRecord, ScalarRecord};

pub const DEFAULT_SPARSE_INDEX_EVERY: u32 = 128;

/// Single sparse timestamp anchor inside a chunk.
#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SparseIndexEntry {
    pub ts: i64,
    pub record_idx: u64,
}

impl SparseIndexEntry {
    pub const BYTE_SIZE: usize = 16;

    pub fn to_le_bytes(&self) -> [u8; Self::BYTE_SIZE] {
        let mut buf = [0u8; Self::BYTE_SIZE];
        buf[..8].copy_from_slice(&self.ts.to_le_bytes());
        buf[8..16].copy_from_slice(&self.record_idx.to_le_bytes());
        buf
    }

    pub fn from_le_bytes(bytes: [u8; Self::BYTE_SIZE]) -> Self {
        let mut ts_bytes = [0u8; 8];
        let mut idx_bytes = [0u8; 8];
        ts_bytes.copy_from_slice(&bytes[..8]);
        idx_bytes.copy_from_slice(&bytes[8..16]);
        Self {
            ts: i64::from_le_bytes(ts_bytes),
            record_idx: u64::from_le_bytes(idx_bytes),
        }
    }
}

pub fn build_for_kline(records: &[KlineRecord], every: u32) -> Result<Vec<SparseIndexEntry>> {
    if every == 0 {
        return Err(FastKError::InvalidInput(
            "sparse index frequency must be greater than zero".to_string(),
        ));
    }
    KlineRecord::validate_strict_order(records)?;
    Ok(records
        .iter()
        .enumerate()
        .step_by(every as usize)
        .map(|(record_idx, record)| SparseIndexEntry {
            ts: record.ts,
            record_idx: record_idx as u64,
        })
        .collect())
}

pub fn build_for_scalar(records: &[ScalarRecord], every: u32) -> Result<Vec<SparseIndexEntry>> {
    if every == 0 {
        return Err(FastKError::InvalidInput(
            "sparse index frequency must be greater than zero".to_string(),
        ));
    }
    ScalarRecord::validate_strict_order(records)?;
    Ok(records
        .iter()
        .enumerate()
        .step_by(every as usize)
        .map(|(record_idx, record)| SparseIndexEntry {
            ts: record.ts,
            record_idx: record_idx as u64,
        })
        .collect())
}

pub fn build_for_fixed<R: FixedRecord>(records: &[R], every: u32) -> Result<Vec<SparseIndexEntry>> {
    if every == 0 {
        return Err(FastKError::InvalidInput(
            "sparse index frequency must be greater than zero".to_string(),
        ));
    }
    R::validate_strict_order(records)?;
    Ok(records
        .iter()
        .enumerate()
        .step_by(every as usize)
        .map(|(record_idx, record)| SparseIndexEntry {
            ts: record.ts(),
            record_idx: record_idx as u64,
        })
        .collect())
}

pub fn encode_entries(entries: &[SparseIndexEntry]) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(entries.len() * SparseIndexEntry::BYTE_SIZE);
    for entry in entries {
        bytes.extend_from_slice(&entry.to_le_bytes());
    }
    bytes
}

pub fn read_from_file(file: &mut File, header: &ChunkHeader) -> Result<Vec<SparseIndexEntry>> {
    if header.version < crate::chunk::header::CHUNK_VERSION_V2
        || (header.flags & CHUNK_FLAG_HAS_SPARSE_INDEX) == 0
        || header.index_len == 0
    {
        return Ok(Vec::new());
    }

    let byte_len = (header.index_len as usize)
        .checked_mul(SparseIndexEntry::BYTE_SIZE)
        .ok_or_else(|| FastKError::InvalidData("sparse index byte length overflow".to_string()))?;

    let mut bytes = vec![0u8; byte_len];
    file.seek(SeekFrom::Start(header.index_offset))?;
    file.read_exact(&mut bytes)?;
    decode_entries(&bytes)
}

pub fn decode_entries(bytes: &[u8]) -> Result<Vec<SparseIndexEntry>> {
    if bytes.len() % SparseIndexEntry::BYTE_SIZE != 0 {
        return Err(FastKError::InvalidData(format!(
            "invalid sparse index byte length {}",
            bytes.len()
        )));
    }

    let mut entries = Vec::with_capacity(bytes.len() / SparseIndexEntry::BYTE_SIZE);
    for chunk in bytes.chunks_exact(SparseIndexEntry::BYTE_SIZE) {
        let mut raw = [0u8; SparseIndexEntry::BYTE_SIZE];
        raw.copy_from_slice(chunk);
        entries.push(SparseIndexEntry::from_le_bytes(raw));
    }
    Ok(entries)
}

pub fn anchor_for_ts(entries: &[SparseIndexEntry], ts: i64) -> Option<SparseIndexEntry> {
    if entries.is_empty() {
        return None;
    }

    let mut left = 0usize;
    let mut right = entries.len();

    while left < right {
        let mid = left + (right - left) / 2;
        if entries[mid].ts <= ts {
            left = mid + 1;
        } else {
            right = mid;
        }
    }

    left.checked_sub(1).map(|index| entries[index])
}

#[cfg(test)]
mod tests {
    use crate::chunk::sparse_index::{anchor_for_ts, build_for_kline, SparseIndexEntry};
    use crate::types::KlineRecord;

    #[test]
    fn anchor_lookup_handles_boundaries() {
        let entries = vec![
            SparseIndexEntry {
                ts: 100,
                record_idx: 0,
            },
            SparseIndexEntry {
                ts: 228,
                record_idx: 128,
            },
            SparseIndexEntry {
                ts: 356,
                record_idx: 256,
            },
        ];

        assert_eq!(anchor_for_ts(&entries, 99), None);
        assert_eq!(
            anchor_for_ts(&entries, 100).map(|entry| entry.record_idx),
            Some(0)
        );
        assert_eq!(
            anchor_for_ts(&entries, 229).map(|entry| entry.record_idx),
            Some(128)
        );
        assert_eq!(
            anchor_for_ts(&entries, 356).map(|entry| entry.record_idx),
            Some(256)
        );
    }

    #[test]
    fn build_for_kline_emits_expected_anchors() {
        let records: Vec<_> = (0..260)
            .map(|idx| KlineRecord {
                ts: idx * 60_000,
                open: idx,
                high: idx,
                low: idx,
                close: idx,
                volume: idx,
            })
            .collect();

        let entries = build_for_kline(&records, 128).expect("build should succeed");
        assert_eq!(
            entries
                .iter()
                .map(|entry| entry.record_idx)
                .collect::<Vec<_>>(),
            vec![0, 128, 256]
        );
    }
}
