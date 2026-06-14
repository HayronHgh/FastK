use std::ops::Range;

use serde::{Deserialize, Serialize};

use crate::error::{FastKError, Result};

/// Logical record family carried by a series.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[repr(u32)]
pub enum RecordType {
    Kline = 1,
    Scalar = 2,
    Trade = 10,
    Bbo = 11,
    BookDelta = 12,
}

impl RecordType {
    pub fn from_u32(value: u32) -> Result<Self> {
        match value {
            1 => Ok(Self::Kline),
            2 => Ok(Self::Scalar),
            10 => Ok(Self::Trade),
            11 => Ok(Self::Bbo),
            12 => Ok(Self::BookDelta),
            other => Err(FastKError::InvalidData(format!(
                "unsupported record type: {other}",
            ))),
        }
    }

    pub fn as_u32(self) -> u32 {
        self as u32
    }
}

/// Logical partition unit used to derive chunk keys.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[repr(u8)]
pub enum PartitionUnit {
    Month = 1,
    Day = 2,
    Hour = 3,
    Rows = 4,
    Bytes = 5,
}

impl PartitionUnit {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Month => "month",
            Self::Day => "day",
            Self::Hour => "hour",
            Self::Rows => "rows",
            Self::Bytes => "bytes",
        }
    }

    pub fn from_str(value: &str) -> Result<Self> {
        match value {
            "month" => Ok(Self::Month),
            "day" => Ok(Self::Day),
            "hour" => Ok(Self::Hour),
            "rows" => Ok(Self::Rows),
            "bytes" => Ok(Self::Bytes),
            other => Err(FastKError::InvalidData(format!(
                "unsupported partition unit: {other}",
            ))),
        }
    }

    pub fn is_time_based(self) -> bool {
        matches!(self, Self::Month | Self::Day | Self::Hour)
    }
}

/// Chunk partition policy for a series.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PartitionPolicy {
    pub unit: PartitionUnit,
    pub max_rows: Option<u64>,
    pub max_bytes: Option<u64>,
    pub max_duration_ms: Option<i64>,
}

impl PartitionPolicy {
    pub fn month() -> Self {
        Self::new(PartitionUnit::Month)
    }

    pub fn day() -> Self {
        Self::new(PartitionUnit::Day)
    }

    pub fn hour() -> Self {
        Self::new(PartitionUnit::Hour)
    }

    pub fn new(unit: PartitionUnit) -> Self {
        Self {
            unit,
            max_rows: None,
            max_bytes: None,
            max_duration_ms: None,
        }
    }
}

/// Lifecycle state for a chunk entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[repr(u8)]
pub enum ChunkState {
    Active = 1,
    Sealed = 2,
    Merging = 3,
    Replaced = 4,
}

impl ChunkState {
    pub fn from_u8(value: u8) -> Result<Self> {
        match value {
            1 => Ok(Self::Active),
            2 => Ok(Self::Sealed),
            3 => Ok(Self::Merging),
            4 => Ok(Self::Replaced),
            other => Err(FastKError::InvalidData(format!(
                "unsupported chunk state: {other}",
            ))),
        }
    }

    pub fn as_u8(self) -> u8 {
        self as u8
    }

    pub fn is_readable(self) -> bool {
        matches!(self, Self::Active | Self::Sealed | Self::Merging)
    }
}

/// Sidecar metadata attached to a chunk.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SidecarMeta {
    pub kind: String,
    pub relative_path: String,
    pub generation: u32,
    pub checksum: u64,
    pub block_size: u32,
    pub record_count: u64,
}

/// Per-chunk metadata stored inside the series manifest.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChunkMeta {
    pub chunk_id: u64,
    pub month_key: String,
    pub start_ts: i64,
    pub end_ts: i64,
    pub count: u64,
    pub state: ChunkState,
    pub layout_version: u32,
    pub header_len: u32,
    pub sparse_index_every: u32,
    pub sparse_index_offset: u64,
    pub sparse_index_len: u32,
    pub chunk_checksum: u64,
    pub generation: u32,
    pub relative_path: String,
    pub sidecars: Vec<SidecarMeta>,
}

impl ChunkMeta {
    pub fn validate(&self) -> Result<()> {
        if self.count > 0 && self.start_ts > self.end_ts {
            return Err(FastKError::InvalidData(format!(
                "chunk {} has start_ts {} after end_ts {}",
                self.relative_path, self.start_ts, self.end_ts
            )));
        }
        if self.relative_path.trim().is_empty() {
            return Err(FastKError::InvalidData(
                "chunk relative_path must not be empty".to_string(),
            ));
        }
        if self.sparse_index_every == 0 {
            return Err(FastKError::InvalidData(format!(
                "chunk {} has sparse_index_every=0",
                self.relative_path
            )));
        }
        Ok(())
    }
}

/// Series-level metadata cached at runtime and persisted via a binary manifest.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SeriesMeta {
    pub symbol: String,
    pub category: String,
    pub name: String,
    pub timeframe_ms: i64,
    pub record_type: RecordType,
    pub record_size: u32,
    pub schema_id: u32,
    pub price_scale: i64,
    pub volume_scale: i64,
    pub chunk_unit: String,
    pub series_id: u64,
    pub manifest_seq: u64,
    pub created_at: i64,
    pub updated_at: i64,
    pub flags: u32,
    pub active_chunk_id: Option<u64>,
    pub chunks: Vec<ChunkMeta>,
}

impl SeriesMeta {
    pub fn validate(&self) -> Result<()> {
        if self.symbol.trim().is_empty()
            || self.category.trim().is_empty()
            || self.name.trim().is_empty()
        {
            return Err(FastKError::InvalidData(
                "series identity fields must not be empty".to_string(),
            ));
        }
        if self.record_size == 0 {
            return Err(FastKError::InvalidData(
                "record_size must be greater than zero".to_string(),
            ));
        }
        let partition_unit = PartitionUnit::from_str(&self.chunk_unit)?;
        if !partition_unit.is_time_based() {
            return Err(FastKError::InvalidData(format!(
                "partition unit is declared but not implemented yet: {}",
                self.chunk_unit
            )));
        }

        let mut prev_end = None;
        for chunk in &self.chunks {
            chunk.validate()?;
            if let Some(end_ts) = prev_end {
                if chunk.start_ts <= end_ts {
                    return Err(FastKError::InvalidData(format!(
                        "series {}:{} has overlapping or unsorted chunks around {}",
                        self.symbol, self.name, chunk.relative_path
                    )));
                }
            }
            prev_end = Some(chunk.end_ts);
        }

        Ok(())
    }

    pub fn find_chunk_for_ts(&self, ts: i64) -> Option<usize> {
        if self.chunks.is_empty() {
            return None;
        }

        let mut left = 0usize;
        let mut right = self.chunks.len();

        while left < right {
            let mid = left + (right - left) / 2;
            let chunk = &self.chunks[mid];
            if chunk.start_ts <= ts {
                left = mid + 1;
            } else {
                right = mid;
            }
        }

        if left == 0 {
            return None;
        }

        let index = left - 1;
        let chunk = &self.chunks[index];
        if chunk.start_ts <= ts && chunk.end_ts >= ts {
            Some(index)
        } else {
            None
        }
    }

    pub fn find_chunks_for_range(&self, start_ts: i64, end_ts: i64) -> Range<usize> {
        if start_ts > end_ts || self.chunks.is_empty() {
            return 0..0;
        }

        let start_index = self.first_chunk_with_end_ge(start_ts);
        if start_index >= self.chunks.len() {
            return 0..0;
        }

        let end_exclusive = self.first_chunk_with_start_gt(end_ts);
        if start_index >= end_exclusive {
            return 0..0;
        }

        start_index..end_exclusive
    }

    pub fn latest_chunk_index(&self) -> Option<usize> {
        self.chunks.len().checked_sub(1)
    }

    pub fn chunk_indices_for_month(&self, month_key: &str) -> Vec<usize> {
        self.chunk_indices_for_partition(month_key)
    }

    pub fn chunk_indices_for_partition(&self, partition_key: &str) -> Vec<usize> {
        self.chunks
            .iter()
            .enumerate()
            .filter_map(|(index, chunk)| (chunk.month_key == partition_key).then_some(index))
            .collect()
    }

    fn first_chunk_with_end_ge(&self, ts: i64) -> usize {
        let mut left = 0usize;
        let mut right = self.chunks.len();

        while left < right {
            let mid = left + (right - left) / 2;
            if self.chunks[mid].end_ts < ts {
                left = mid + 1;
            } else {
                right = mid;
            }
        }

        left
    }

    fn first_chunk_with_start_gt(&self, ts: i64) -> usize {
        let mut left = 0usize;
        let mut right = self.chunks.len();

        while left < right {
            let mid = left + (right - left) / 2;
            if self.chunks[mid].start_ts <= ts {
                left = mid + 1;
            } else {
                right = mid;
            }
        }

        left
    }
}

#[cfg(test)]
mod tests {
    use crate::types::{ChunkMeta, ChunkState, RecordType, SeriesMeta};

    #[test]
    fn find_chunk_for_ts_returns_expected_index() {
        let meta = sample_meta();

        assert_eq!(meta.find_chunk_for_ts(120), Some(1));
        assert_eq!(meta.find_chunk_for_ts(199), Some(1));
        assert_eq!(meta.find_chunk_for_ts(90), None);
        assert_eq!(meta.find_chunk_for_ts(401), None);
    }

    #[test]
    fn find_chunks_for_range_returns_expected_span() {
        let meta = sample_meta();

        assert_eq!(meta.find_chunks_for_range(90, 99), 0..0);
        assert_eq!(meta.find_chunks_for_range(95, 205), 0..2);
        assert_eq!(meta.find_chunks_for_range(220, 260), 2..3);
        assert_eq!(meta.find_chunks_for_range(301, 399), 3..4);
        assert_eq!(meta.find_chunks_for_range(401, 450), 0..0);
    }

    #[test]
    fn validate_rejects_overlapping_chunks() {
        let mut meta = sample_meta();
        meta.chunks[2].start_ts = 150;

        let err = meta.validate().expect_err("overlap should be rejected");
        assert!(matches!(err, crate::FastKError::InvalidData(_)));
    }

    fn sample_meta() -> SeriesMeta {
        SeriesMeta {
            symbol: "BTCUSDT".to_string(),
            category: "kline".to_string(),
            name: "1m".to_string(),
            timeframe_ms: 60_000,
            record_type: RecordType::Kline,
            record_size: 48,
            schema_id: 1,
            price_scale: 100_000,
            volume_scale: 100_000,
            chunk_unit: "month".to_string(),
            series_id: 1,
            manifest_seq: 1,
            created_at: 1,
            updated_at: 1,
            flags: 0,
            active_chunk_id: None,
            chunks: vec![
                sample_chunk(10, "2024-01", 100, 109, "chunks/2024-01.chunk"),
                sample_chunk(11, "2024-02", 120, 199, "chunks/2024-02.chunk"),
                sample_chunk(12, "2024-03", 220, 299, "chunks/2024-03.chunk"),
                sample_chunk(13, "2024-04", 301, 400, "chunks/2024-04.chunk"),
            ],
        }
    }

    fn sample_chunk(
        chunk_id: u64,
        month_key: &str,
        start_ts: i64,
        end_ts: i64,
        relative_path: &str,
    ) -> ChunkMeta {
        ChunkMeta {
            chunk_id,
            month_key: month_key.to_string(),
            start_ts,
            end_ts,
            count: 10,
            state: ChunkState::Sealed,
            layout_version: 2,
            header_len: 128,
            sparse_index_every: 128,
            sparse_index_offset: 608,
            sparse_index_len: 1,
            chunk_checksum: 42,
            generation: 1,
            relative_path: relative_path.to_string(),
            sidecars: Vec::new(),
        }
    }
}
