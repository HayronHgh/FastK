use std::path::{Path, PathBuf};

use chrono::{Datelike, Timelike, Utc};

use crate::error::{FastKError, Result};
use crate::types::PartitionUnit;

pub const SERIES_DIR: &str = "series";
pub const KLINE_CATEGORY: &str = "kline";
pub const TRADE_CATEGORY: &str = "trade";
pub const BBO_CATEGORY: &str = "bbo";
pub const BOOK_DELTA_CATEGORY: &str = "book_delta";
pub const CHUNKS_DIR: &str = "chunks";
pub const MANIFEST_FILE: &str = "series.meta";

/// Returns the root directory for all series.
pub fn series_root(root: &Path) -> PathBuf {
    root.join(SERIES_DIR)
}

/// Returns the generic directory for a specific series.
pub fn series_dir(root: &Path, symbol: &str, category: &str, name: &str) -> PathBuf {
    series_root(root).join(symbol).join(category).join(name)
}

/// Returns the directory for a specific kline series.
pub fn kline_series_dir(root: &Path, symbol: &str, timeframe: &str) -> PathBuf {
    series_dir(root, symbol, KLINE_CATEGORY, timeframe)
}

/// Returns the directory for a scalar series.
pub fn scalar_series_dir(root: &Path, symbol: &str, category: &str, name: &str) -> PathBuf {
    series_dir(root, symbol, category, name)
}

/// Returns the chunk directory for a series directory.
pub fn chunks_dir(series_dir: &Path) -> PathBuf {
    series_dir.join(CHUNKS_DIR)
}

/// Returns the metadata path for a series directory.
pub fn series_meta_path(series_dir: &Path) -> PathBuf {
    series_dir.join(MANIFEST_FILE)
}

/// Builds a relative path under a series directory for a chunk filename.
pub fn chunk_relative_path(file_name: &str) -> String {
    format!("{CHUNKS_DIR}/{file_name}")
}

/// Returns the data path for a chunk filename.
#[cfg(test)]
pub fn chunk_path(series_dir: &Path, file_name: &str) -> PathBuf {
    resolve_relative_path(series_dir, &chunk_relative_path(file_name))
}

/// Resolves a relative series path.
pub fn resolve_relative_path(series_dir: &Path, relative_path: &str) -> PathBuf {
    let normalized = relative_path.replace('\\', "/");
    normalized
        .split('/')
        .filter(|part| !part.is_empty())
        .fold(series_dir.to_path_buf(), |acc, part| acc.join(part))
}

/// Builds the canonical chunk file name for the first sealed chunk in a UTC month.
pub fn month_chunk_file_name(ts: i64) -> Result<String> {
    Ok(partition_chunk_file_name(&month_key(ts)?))
}

/// Builds the append delta chunk file name for a UTC month.
pub fn month_delta_chunk_file_name(month_key: &str, generation: u32) -> String {
    partition_delta_chunk_file_name(month_key, generation)
}

/// Builds the merge replacement chunk file name for a UTC month.
pub fn month_merged_chunk_file_name(month_key: &str, generation: u32) -> String {
    partition_merged_chunk_file_name(month_key, generation)
}

/// Builds the canonical chunk file name for the first sealed chunk in a partition.
pub fn partition_chunk_file_name(partition_key: &str) -> String {
    format!("{partition_key}.chunk")
}

/// Builds an append delta chunk file name for a partition.
pub fn partition_delta_chunk_file_name(partition_key: &str, generation: u32) -> String {
    format!("{partition_key}.delta.g{generation:08}.chunk")
}

/// Builds a merge replacement chunk file name for a partition.
pub fn partition_merged_chunk_file_name(partition_key: &str, generation: u32) -> String {
    format!("{partition_key}.g{generation:08}.chunk")
}

/// Builds the sidecar relative path from a chunk relative path and extension.
pub fn sidecar_relative_path(chunk_relative_path: &str, extension: &str) -> String {
    format!("{chunk_relative_path}.{extension}")
}

/// Builds the `YYYY-MM` key for a timestamp, grouped by UTC month.
pub fn month_key(ts: i64) -> Result<String> {
    let dt = chrono::DateTime::<Utc>::from_timestamp_millis(ts)
        .ok_or_else(|| FastKError::InvalidInput(format!("timestamp out of range: {ts}")))?;
    Ok(format!("{:04}-{:02}", dt.year(), dt.month()))
}

/// Builds a partition key for a timestamp using a supported time-based unit.
pub fn partition_key(ts: i64, unit: PartitionUnit) -> Result<String> {
    let dt = chrono::DateTime::<Utc>::from_timestamp_millis(ts)
        .ok_or_else(|| FastKError::InvalidInput(format!("timestamp out of range: {ts}")))?;
    match unit {
        PartitionUnit::Month => Ok(format!("{:04}-{:02}", dt.year(), dt.month())),
        PartitionUnit::Day => Ok(format!(
            "{:04}-{:02}-{:02}",
            dt.year(),
            dt.month(),
            dt.day()
        )),
        PartitionUnit::Hour => Ok(format!(
            "{:04}-{:02}-{:02}T{:02}",
            dt.year(),
            dt.month(),
            dt.day(),
            dt.hour()
        )),
        PartitionUnit::Rows | PartitionUnit::Bytes => Err(FastKError::InvalidInput(format!(
            "partition unit {} is not implemented for timestamp partitioning",
            unit.as_str()
        ))),
    }
}
