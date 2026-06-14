use std::path::PathBuf;
use std::sync::Arc;

use crate::chunk::header::{KLINE_SCHEMA_ID, SCALAR_SCHEMA_ID};
use crate::error::Result;
use crate::storage::{fs, manifest, path};
use crate::types::{FixedRecord, PartitionPolicy, RecordType, ScalarSeriesKey, SeriesMeta};

/// Small catalog wrapper for locating and persisting series metadata.
#[derive(Debug, Clone)]
pub struct Catalog {
    root: PathBuf,
}

impl Catalog {
    /// Creates a catalog rooted at a FastK directory.
    pub fn new(root: PathBuf) -> Self {
        Self { root }
    }

    /// Ensures the base directory exists.
    pub fn init(&self) -> Result<()> {
        fs::ensure_dir(&path::series_root(&self.root))
    }

    /// Loads kline series metadata from disk.
    pub fn load_kline_meta(&self, symbol: &str, timeframe: &str) -> Result<SeriesMeta> {
        Ok((*self.load_kline_meta_shared(symbol, timeframe)?).clone())
    }

    /// Loads shared kline series metadata from disk or process cache.
    pub fn load_kline_meta_shared(&self, symbol: &str, timeframe: &str) -> Result<Arc<SeriesMeta>> {
        Ok(self.load_kline_meta_report(symbol, timeframe)?.meta)
    }

    /// Returns the shared in-process kline manifest cache entry without revalidating the file.
    pub fn load_kline_meta_cached_report(
        &self,
        symbol: &str,
        timeframe: &str,
    ) -> Option<manifest::ManifestLoadReport> {
        manifest::load_series_meta_shared_cached(&path::kline_series_dir(
            &self.root, symbol, timeframe,
        ))
    }

    /// Loads shared kline metadata together with manifest bootstrap stats.
    pub fn load_kline_meta_report(
        &self,
        symbol: &str,
        timeframe: &str,
    ) -> Result<manifest::ManifestLoadReport> {
        manifest::load_series_meta_shared(&path::kline_series_dir(&self.root, symbol, timeframe))
    }

    /// Saves kline series metadata to disk.
    pub fn save_kline_meta(&self, meta: &SeriesMeta) -> Result<()> {
        let series_dir = path::kline_series_dir(&self.root, &meta.symbol, &meta.name);
        manifest::save_series_meta(&series_dir, meta)
    }

    /// Loads scalar series metadata from disk.
    pub fn load_scalar_meta(&self, key: &ScalarSeriesKey) -> Result<SeriesMeta> {
        Ok((*self.load_scalar_meta_shared(key)?).clone())
    }

    /// Loads shared scalar series metadata from disk or process cache.
    pub fn load_scalar_meta_shared(&self, key: &ScalarSeriesKey) -> Result<Arc<SeriesMeta>> {
        Ok(self.load_scalar_meta_report(key)?.meta)
    }

    /// Returns the shared in-process scalar manifest cache entry without revalidating the file.
    pub fn load_scalar_meta_cached_report(
        &self,
        key: &ScalarSeriesKey,
    ) -> Option<manifest::ManifestLoadReport> {
        manifest::load_series_meta_shared_cached(&path::scalar_series_dir(
            &self.root,
            &key.symbol,
            &key.category,
            &key.name,
        ))
    }

    /// Loads shared scalar metadata together with manifest bootstrap stats.
    pub fn load_scalar_meta_report(
        &self,
        key: &ScalarSeriesKey,
    ) -> Result<manifest::ManifestLoadReport> {
        manifest::load_series_meta_shared(&path::scalar_series_dir(
            &self.root,
            &key.symbol,
            &key.category,
            &key.name,
        ))
    }

    /// Saves scalar series metadata to disk.
    pub fn save_scalar_meta(&self, meta: &SeriesMeta) -> Result<()> {
        let series_dir =
            path::scalar_series_dir(&self.root, &meta.symbol, &meta.category, &meta.name);
        manifest::save_series_meta(&series_dir, meta)
    }

    /// Returns a cached generic manifest report without revalidating the file.
    pub fn load_series_meta_cached_report(
        &self,
        symbol: &str,
        category: &str,
        name: &str,
    ) -> Option<manifest::ManifestLoadReport> {
        manifest::load_series_meta_shared_cached(&path::series_dir(
            &self.root, symbol, category, name,
        ))
    }

    /// Loads generic metadata together with manifest bootstrap stats.
    pub fn load_series_meta_report(
        &self,
        symbol: &str,
        category: &str,
        name: &str,
    ) -> Result<manifest::ManifestLoadReport> {
        manifest::load_series_meta_shared(&path::series_dir(&self.root, symbol, category, name))
    }

    /// Saves generic series metadata to disk.
    pub fn save_series_meta(&self, meta: &SeriesMeta) -> Result<()> {
        let series_dir = path::series_dir(&self.root, &meta.symbol, &meta.category, &meta.name);
        manifest::save_series_meta(&series_dir, meta)
    }

    /// Builds metadata for a new kline series.
    pub fn build_kline_meta(
        symbol: &str,
        timeframe: &str,
        timeframe_ms: i64,
        price_scale: i64,
        volume_scale: i64,
    ) -> SeriesMeta {
        let now = now_timestamp_ms();
        SeriesMeta {
            symbol: symbol.to_string(),
            category: path::KLINE_CATEGORY.to_string(),
            name: timeframe.to_string(),
            timeframe_ms,
            record_type: RecordType::Kline,
            record_size: crate::types::KlineRecord::BYTE_SIZE as u32,
            schema_id: KLINE_SCHEMA_ID,
            price_scale,
            volume_scale,
            chunk_unit: "month".to_string(),
            series_id: Self::series_id_for(symbol, path::KLINE_CATEGORY, timeframe),
            manifest_seq: 1,
            created_at: now,
            updated_at: now,
            flags: 0,
            active_chunk_id: None,
            chunks: Vec::new(),
        }
    }

    /// Builds metadata for a new scalar series.
    pub fn build_scalar_meta(key: &ScalarSeriesKey, timeframe_ms: i64) -> SeriesMeta {
        let now = now_timestamp_ms();
        SeriesMeta {
            symbol: key.symbol.clone(),
            category: key.category.clone(),
            name: key.name.clone(),
            timeframe_ms,
            record_type: RecordType::Scalar,
            record_size: crate::types::ScalarRecord::BYTE_SIZE as u32,
            schema_id: SCALAR_SCHEMA_ID,
            price_scale: 1,
            volume_scale: 1,
            chunk_unit: "month".to_string(),
            series_id: Self::series_id_for(&key.symbol, &key.category, &key.name),
            manifest_seq: 1,
            created_at: now,
            updated_at: now,
            flags: 0,
            active_chunk_id: None,
            chunks: Vec::new(),
        }
    }

    /// Builds metadata for a fixed-record non-kline/non-scalar series.
    pub fn build_fixed_meta<R: FixedRecord>(
        symbol: &str,
        category: &str,
        name: &str,
        timeframe_ms: i64,
        partition_policy: &PartitionPolicy,
    ) -> SeriesMeta {
        let now = now_timestamp_ms();
        SeriesMeta {
            symbol: symbol.to_string(),
            category: category.to_string(),
            name: name.to_string(),
            timeframe_ms,
            record_type: R::RECORD_TYPE,
            record_size: R::BYTE_SIZE as u32,
            schema_id: R::SCHEMA_ID,
            price_scale: 1,
            volume_scale: 1,
            chunk_unit: partition_policy.unit.as_str().to_string(),
            series_id: Self::series_id_for(symbol, category, name),
            manifest_seq: 1,
            created_at: now,
            updated_at: now,
            flags: 0,
            active_chunk_id: None,
            chunks: Vec::new(),
        }
    }

    /// Returns the deterministic series identifier used in chunk headers.
    pub fn series_id_for(symbol: &str, category: &str, name: &str) -> u64 {
        const FNV_OFFSET: u64 = 0xcbf29ce484222325;
        const FNV_PRIME: u64 = 0x100000001b3;

        let mut hash = FNV_OFFSET;
        for byte in format!("{symbol}:{category}:{name}").as_bytes() {
            hash ^= *byte as u64;
            hash = hash.wrapping_mul(FNV_PRIME);
        }
        hash
    }
}

fn now_timestamp_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_millis() as i64)
        .unwrap_or(0)
}
