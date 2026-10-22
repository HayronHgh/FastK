use std::collections::HashMap;
use std::ops::RangeInclusive;
use std::path::Path;
use std::path::PathBuf;
use std::sync::{Arc, RwLock};
use std::time::Instant;

use crate::chunk::cache::ChunkRuntime;
use crate::chunk::{fixed_reader, fixed_writer, kline_reader, kline_writer};
use crate::chunk::{scalar_reader, scalar_writer};
use crate::engine::catalog::Catalog;
use crate::engine::compaction::{
    can_transition, merge_output_state, CompactionDecision, CompactionPolicy,
};
use crate::engine::derived::{decode_scoped_scalar_key, indicator_series_key, INDICATOR_CATEGORY};
use crate::engine::replay::{ReplayCursor, ReplayOptions};
use crate::engine::sequence::{
    observe_sequence, SequenceObservation, SequenceScanReport, SequencedRecord,
};
use crate::error::{FastKError, Result};
use crate::index::cache::{ScalarSidecarRuntime, SidecarCacheKey};
use crate::index::{vix, zmap, PredicateQueryEngine};
use crate::metrics::{MetricsLevel, StoreMetrics, StoreMetricsSnapshot};
use crate::storage::{
    lock::{StoreReadLock, StoreWriteLock},
    manifest, path, recovery,
};
use crate::types::{
    BboRecord, BookDeltaRecord, ChunkState, FixedRecord, KlineRecord, PartitionPolicy,
    PartitionUnit, ScalarIndexKind, ScalarPredicate, ScalarPredicateExpr, ScalarPredicateMatch,
    ScalarPredicateQuery, ScalarPredicateQueryResult, ScalarPredicateQueryStats, ScalarRecord,
    ScalarSeriesKey, SeriesMeta, SidecarMeta, TradeRecord,
};

const SHORT_RANGE_MAX_RECORDS: i64 = 2_048;
#[path = "parallel.rs"]
mod parallel;
#[path = "store_writes.rs"]
mod writes;
use writes::validate_month_records;
pub use writes::RangeWriteSummary;
pub const DEFAULT_SCALAR_ZMAP_BLOCK_SIZE: usize = 256;

/// Lightweight series inventory entry exposed to integration callers.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct SeriesInventoryEntry {
    pub symbol: String,
    pub category: String,
    pub name: String,
    pub record_type: crate::types::RecordType,
    pub timeframe_ms: i64,
    pub chunk_count: usize,
    pub record_count: u64,
    pub sidecar_count: usize,
    pub active_delta_count: usize,
}

/// Store-wide counts used by backtest integrations and validation tools.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct StoreStats {
    pub series_count: usize,
    pub kline_series_count: usize,
    pub scalar_series_count: usize,
    pub chunk_count: usize,
    pub record_count: u64,
    pub sidecar_count: usize,
    pub active_delta_count: usize,
}

/// Lightweight store health view for backtest integrations and admin tooling.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct StoreHealthSummary {
    pub series_count: usize,
    pub clean_series_count: usize,
    pub issue_series_count: usize,
    pub orphan_count: usize,
    pub overlap_group_count: usize,
    pub pending_recovery: bool,
    pub platform_fsync_guarantee: &'static str,
}

/// Per-month inventory summary exposed to integration callers.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct MonthInventoryEntry {
    pub month_key: String,
    pub chunk_count: usize,
    pub record_count: u64,
    pub sealed_chunk_count: usize,
    pub active_delta_count: usize,
    pub sidecar_count: usize,
}

/// Read-session reset scope used by long-running backtest processes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
pub enum SessionResetMode {
    QueryOnly,
    Logical,
    FullDetach,
}

/// Lightweight cache and attachment summary for session-level observability.
#[derive(Debug, Clone, PartialEq, serde::Serialize)]
pub struct SessionCacheSummary {
    pub attached_kline_count: usize,
    pub attached_scalar_count: usize,
    pub metrics_level: MetricsLevel,
    pub metrics: StoreMetricsSnapshot,
}

/// Query capabilities attached to a scalar series.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct ScalarQueryCapabilities {
    pub raw_scan: bool,
    pub has_zmap: bool,
    pub has_vix: bool,
}

impl ScalarQueryCapabilities {
    fn from_meta(meta: &SeriesMeta) -> Self {
        let has_zmap = meta
            .chunks
            .iter()
            .flat_map(|chunk| chunk.sidecars.iter())
            .any(|sidecar| sidecar.kind == "zmap");
        let has_vix = meta
            .chunks
            .iter()
            .flat_map(|chunk| chunk.sidecars.iter())
            .any(|sidecar| sidecar.kind == "vix");

        Self {
            raw_scan: true,
            has_zmap,
            has_vix,
        }
    }
}

/// Inventory summary for indicator series exposed through the scalar lifecycle.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct IndicatorInventoryEntry {
    pub symbol: String,
    pub timeframe: String,
    pub indicator_name: String,
    pub chunk_count: usize,
    pub record_count: u64,
    pub sidecar_count: usize,
    pub active_delta_count: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ScalarQueryStrategy {
    Zmap,
    Vix,
    Raw,
}

/// Lightweight read facade intended for long-running backtest processes.
#[derive(Debug)]
pub struct FastKReadSession<'a> {
    store: &'a FastKStore,
    attached_kline: Vec<AttachedKlineSeries>,
    attached_scalar: Vec<AttachedScalarSeries>,
}

#[derive(Debug, Clone)]
struct AttachedKlineSeries {
    symbol: String,
    timeframe: String,
    series_dir: PathBuf,
    meta: Arc<SeriesMeta>,
}

#[derive(Debug, Clone)]
struct AttachedScalarSeries {
    key: ScalarSeriesKey,
    series_dir: PathBuf,
    meta: Arc<SeriesMeta>,
    capabilities: Option<ScalarQueryCapabilities>,
}

/// Kline-focused binary store for FastK.
#[derive(Debug)]
pub struct FastKStore {
    root: PathBuf,
    catalog: Catalog,
    manifest_cache: RwLock<HashMap<String, Arc<SeriesMeta>>>,
    chunk_runtime: ChunkRuntime,
    scalar_sidecars: ScalarSidecarRuntime,
    metrics: Arc<StoreMetrics>,
    compaction_policy: CompactionPolicy,
    read_only: bool,
    read_pool: Option<rayon::ThreadPool>,
    _read_lock: Option<StoreReadLock>,
}

impl FastKStore {
    /// Opens a store rooted at `root`.
    pub fn open(root: impl Into<PathBuf>) -> Result<Self> {
        Self::open_mode(root.into(), false)
    }

    /// Opens without recovery, migration or filesystem writes. Holds a shared OS lock
    /// until dropped; writers fail fast while this handle is alive.
    pub fn open_read_only(root: impl Into<PathBuf>) -> Result<Self> {
        Self::open_mode(root.into(), true)
    }

    fn open_mode(root: PathBuf, read_only: bool) -> Result<Self> {
        let read_lock = if read_only {
            StoreReadLock::acquire(&root)?
        } else {
            None
        };
        if read_only && recovery::has_pending_recovery(&root)? {
            return Err(FastKError::InvalidData(
                "store requires explicit writable recovery".into(),
            ));
        }
        if read_only {
            manifest::invalidate_root(&root);
        }
        let open_started = Instant::now();
        if !read_only && recovery::has_pending_recovery(&root)? {
            let _ = recovery::startup_recover(&root)?;
        }
        let chunk_runtime = ChunkRuntime::default();
        let metrics = chunk_runtime.metrics();
        let store = Self {
            catalog: if read_only {
                Catalog::read_only(root.clone())
            } else {
                Catalog::new(root.clone())
            },
            root,
            manifest_cache: RwLock::new(HashMap::new()),
            scalar_sidecars: ScalarSidecarRuntime::new(metrics.clone()),
            chunk_runtime,
            metrics,
            compaction_policy: CompactionPolicy::default(),
            read_only,
            read_pool: None,
            _read_lock: read_lock,
        };
        store
            .metrics
            .record_store_bootstrap_ns(open_started.elapsed().as_nanos() as u64);
        Ok(store)
    }

    pub fn kline_metadata(&self, symbol: &str, timeframe: &str) -> Result<Arc<SeriesMeta>> {
        self.load_kline_meta(symbol, timeframe)
    }

    pub fn scalar_metadata(&self, key: &ScalarSeriesKey) -> Result<Arc<SeriesMeta>> {
        self.load_scalar_meta(key)
    }

    /// Creates the base directory structure and removes leftover temp artifacts.
    pub fn init(&mut self) -> Result<()> {
        let _lock = self.begin_mutation()?;
        self.init_locked()
    }

    fn init_locked(&mut self) -> Result<()> {
        self.catalog.init()?;
        let _ = recovery::startup_recover_locked(&self.root, recovery::RecoveryOptions::default())?;
        Ok(())
    }

    /// Registers a kline series and persists its metadata.
    pub fn register_kline_series(
        &mut self,
        symbol: &str,
        timeframe: &str,
        timeframe_ms: i64,
        price_scale: i64,
        volume_scale: i64,
    ) -> Result<()> {
        writes::validate_configuration(timeframe_ms, price_scale, volume_scale)?;
        let _lock = self.begin_mutation()?;
        self.register_kline_series_locked(
            symbol,
            timeframe,
            timeframe_ms,
            price_scale,
            volume_scale,
        )
    }

    fn register_kline_series_locked(
        &mut self,
        symbol: &str,
        timeframe: &str,
        timeframe_ms: i64,
        price_scale: i64,
        volume_scale: i64,
    ) -> Result<()> {
        let series_dir = path::kline_series_dir(&self.root, symbol, timeframe);
        let meta_path = path::series_meta_path(&series_dir);
        if meta_path.exists() {
            let existing = self.load_kline_meta(symbol, timeframe)?;
            validate_existing_series(&existing, timeframe_ms, price_scale, volume_scale)?;
            return Ok(());
        }

        let marker = recovery::RecoveryMarkerGuard::arm(&self.root)?;
        crate::storage::fs::ensure_dir(&path::chunks_dir(&series_dir))?;
        let meta =
            Catalog::build_kline_meta(symbol, timeframe, timeframe_ms, price_scale, volume_scale);
        manifest::save_series_meta(&series_dir, &meta)?;
        self.store_meta_in_cache(meta);
        marker.commit()?;
        Ok(())
    }

    /// Writes a kline chunk or append delta using temp-file + atomic rename.
    pub fn put_kline_chunk(
        &mut self,
        symbol: &str,
        timeframe: &str,
        records: &[KlineRecord],
    ) -> Result<()> {
        validate_month_records(records)?;
        let _lock = self.begin_mutation()?;
        self.put_kline_chunk_locked(symbol, timeframe, records)
    }

    fn put_kline_chunk_locked(
        &mut self,
        symbol: &str,
        timeframe: &str,
        records: &[KlineRecord],
    ) -> Result<()> {
        let mut meta = (*self.load_kline_meta(symbol, timeframe)?).clone();
        let series_dir = path::kline_series_dir(&self.root, symbol, timeframe);

        let first = records
            .first()
            .ok_or_else(|| FastKError::InvalidInput("records must not be empty".to_string()))?;
        let month_key = path::month_key(first.ts)?;
        let month_indices = meta.chunk_indices_for_month(&month_key);
        let chunk_id = next_chunk_id(&meta);
        let generation = month_indices
            .iter()
            .map(|index| meta.chunks[*index].generation)
            .max()
            .unwrap_or(0)
            .saturating_add(1);

        let (file_name, state) = if month_indices.is_empty() {
            (path::month_chunk_file_name(first.ts)?, ChunkState::Sealed)
        } else {
            let last_chunk = &meta.chunks[*month_indices.last().expect("month_indices not empty")];
            if records[0].ts <= last_chunk.end_ts {
                return Err(FastKError::InvalidInput(format!(
                    "append overlaps existing month chunk: {} <= {}",
                    records[0].ts, last_chunk.end_ts
                )));
            }
            (
                path::month_delta_chunk_file_name(&month_key, generation),
                ChunkState::Active,
            )
        };

        let relative_path = path::chunk_relative_path(&file_name);
        let chunk_path = path::resolve_relative_path(&series_dir, &relative_path);
        let marker = recovery::RecoveryMarkerGuard::arm(&self.root)?;
        let chunk_meta = kline_writer::write_chunk(
            &chunk_path,
            &meta,
            records,
            &kline_writer::WriteKlineChunkOptions {
                chunk_id,
                generation,
                state,
                relative_path,
                sparse_index_every: crate::chunk::sparse_index::DEFAULT_SPARSE_INDEX_EVERY,
            },
        )?;

        crate::storage::fs::fault_point("kline_chunk");
        manifest::upsert_chunk_meta(&mut meta, chunk_meta.clone())?;
        if chunk_meta.state == ChunkState::Active {
            meta.active_chunk_id = Some(chunk_meta.chunk_id);
        }
        self.catalog.save_kline_meta(&meta)?;
        crate::storage::fs::fault_point("kline_manifest");
        record_file_bytes_written(self.metrics.as_ref(), &chunk_path);
        record_file_bytes_written(self.metrics.as_ref(), &path::series_meta_path(&series_dir));
        self.store_meta_in_cache(meta.clone());

        if let Some(decision) = self.compaction_policy.evaluate_month(&meta, &month_key) {
            if self.compaction_policy.auto_merge && decision.should_merge {
                self.merge_kline_month_locked(symbol, timeframe, &month_key, &mut meta)?;
            }
        }
        marker.commit()?;
        Ok(())
    }

    /// Merges all chunks belonging to `month_key` into a new replacement chunk.
    pub fn merge_kline_month(
        &mut self,
        symbol: &str,
        timeframe: &str,
        month_key: &str,
    ) -> Result<()> {
        let _lock = self.begin_mutation()?;
        let marker = recovery::RecoveryMarkerGuard::arm(&self.root)?;
        let mut meta = (*self.load_kline_meta(symbol, timeframe)?).clone();
        let result = self.merge_kline_month_locked(symbol, timeframe, month_key, &mut meta);
        if result.is_ok() {
            marker.commit()?;
        }
        result
    }

    /// Returns compaction decisions for every month of the kline series.
    pub fn kline_compaction_advice(
        &self,
        symbol: &str,
        timeframe: &str,
    ) -> Result<Vec<CompactionDecision>> {
        let meta = self.load_kline_meta(symbol, timeframe)?;
        Ok(self.compaction_policy.evaluate_all(&meta))
    }

    /// Returns the exact kline at `ts`, if it exists.
    pub fn get_kline_at(
        &self,
        symbol: &str,
        timeframe: &str,
        ts: i64,
    ) -> Result<Option<KlineRecord>> {
        let meta = self.load_kline_meta(symbol, timeframe)?;
        let series_dir = path::kline_series_dir(&self.root, symbol, timeframe);
        self.get_kline_at_prepared(&series_dir, meta.as_ref(), ts)
    }

    /// Returns klines inside the inclusive timestamp range.
    pub fn get_kline_range(
        &self,
        symbol: &str,
        timeframe: &str,
        start_ts: i64,
        end_ts: i64,
    ) -> Result<Vec<KlineRecord>> {
        if start_ts > end_ts {
            return Ok(Vec::new());
        }

        let meta = self.load_kline_meta(symbol, timeframe)?;
        let series_dir = path::kline_series_dir(&self.root, symbol, timeframe);
        self.get_kline_range_prepared(&series_dir, meta.as_ref(), start_ts, end_ts)
    }

    /// Returns the latest `n` klines in ascending timestamp order.
    pub fn get_kline_latest_n(
        &self,
        symbol: &str,
        timeframe: &str,
        n: usize,
    ) -> Result<Vec<KlineRecord>> {
        if n == 0 {
            return Ok(Vec::new());
        }

        let meta = self.load_kline_meta(symbol, timeframe)?;
        let series_dir = path::kline_series_dir(&self.root, symbol, timeframe);
        self.get_kline_latest_n_prepared(&series_dir, meta.as_ref(), n)
    }

    /// Creates a deterministic sealed-chunk replay cursor for a kline series.
    pub fn replay_kline(
        &self,
        symbol: &str,
        timeframe: &str,
        start_ts: i64,
        end_ts: Option<i64>,
    ) -> Result<ReplayCursor<KlineRecord>> {
        let meta = self.load_kline_meta(symbol, timeframe)?;
        ReplayCursor::new(
            path::kline_series_dir(&self.root, symbol, timeframe),
            meta.as_ref(),
            ReplayOptions {
                start_ts,
                end_ts,
                batch_hint: None,
            },
        )
    }

    /// Validates manifests and chunk checksums reachable from the store root.
    pub fn validate_store(&self) -> Result<()> {
        recovery::validate_store(&self.root)
    }

    /// Returns manifest-vs-filesystem validation details for every discovered series.
    pub fn validate_manifest_vs_fs(&self) -> Result<Vec<recovery::ManifestValidation>> {
        recovery::validate_manifest_vs_fs(&self.root)
    }

    /// Returns verbose manifest-vs-filesystem validation details with checksum revalidation.
    pub fn validate_manifest_vs_fs_verbose(&self) -> Result<Vec<recovery::ManifestValidation>> {
        recovery::validate_manifest_vs_fs_with_options(
            &self.root,
            recovery::ValidationOptions {
                verbose: true,
                revalidate_checksums: true,
            },
        )
    }

    /// Lists chunk and sidecar artifacts that are not currently tracked by manifests.
    pub fn list_orphans(&self) -> Result<Vec<recovery::OrphanArtifact>> {
        recovery::scan_orphan_artifacts(&self.root)
    }

    /// Performs a deeper scrub pass without mutating on-disk state.
    pub fn scrub_store(&self, verbose: bool) -> Result<Vec<recovery::ScrubSeriesReport>> {
        recovery::scrub_store(
            &self.root,
            recovery::ValidationOptions {
                verbose,
                revalidate_checksums: true,
            },
        )
    }

    /// Runs startup recovery logic explicitly and returns what changed.
    pub fn repair_store(&mut self) -> Result<recovery::RecoveryReport> {
        let _lock = self.acquire_write_ownership(false)?;
        let report =
            recovery::startup_recover_locked(&self.root, recovery::RecoveryOptions::default())?;
        self.manifest_cache
            .write()
            .expect("manifest cache poisoned")
            .clear();
        self.chunk_runtime.clear();
        self.scalar_sidecars.clear();
        Ok(report)
    }

    /// Returns what startup recovery would do without mutating on-disk state.
    pub fn repair_store_dry_run(&self) -> Result<recovery::RecoveryReport> {
        recovery::startup_recover_with_options(
            &self.root,
            recovery::RecoveryOptions { dry_run: true },
        )
    }

    /// Returns a snapshot of runtime counters collected since the last reset.
    pub fn metrics_snapshot(&self) -> StoreMetricsSnapshot {
        self.metrics.snapshot()
    }

    /// Returns the currently active metrics verbosity level.
    pub fn metrics_level(&self) -> MetricsLevel {
        self.metrics.level()
    }

    /// Updates runtime metrics verbosity without recreating the store.
    pub fn set_metrics_level(&self, level: MetricsLevel) {
        self.metrics.set_level(level);
    }

    /// Clears runtime counters so benchmark callers can isolate workloads.
    pub fn reset_metrics(&self) {
        self.metrics.reset();
    }

    /// Drops read-path caches so benchmark callers can emulate colder repeated queries.
    pub fn clear_read_caches(&self) {
        self.manifest_cache
            .write()
            .expect("manifest cache poisoned")
            .clear();
        self.chunk_runtime.clear();
        self.scalar_sidecars.clear();
    }

    /// Drops runtime query caches while keeping manifest cache and open file handles.
    ///
    /// This is intended for long-lived read sessions that want to approximate colder page-cache
    /// behavior without also paying full process/bootstrap costs on every query.
    pub fn clear_runtime_caches(&self) {
        self.chunk_runtime.clear_layouts();
        self.scalar_sidecars.clear();
    }

    /// Drops scalar sidecar query caches while retaining chunk layouts and manifest snapshots.
    pub fn clear_scalar_query_caches(&self) {
        self.scalar_sidecars.clear();
    }

    /// Returns the current compaction policy.
    pub fn compaction_policy(&self) -> &CompactionPolicy {
        &self.compaction_policy
    }

    /// Replaces the compaction policy used for append-driven months.
    pub fn set_compaction_policy(&mut self, policy: CompactionPolicy) {
        self.compaction_policy = policy;
    }

    /// Creates a lightweight read session that can attach multiple kline/scalar series.
    pub fn read_session(&self) -> FastKReadSession<'_> {
        FastKReadSession {
            store: self,
            attached_kline: Vec::new(),
            attached_scalar: Vec::new(),
        }
    }

    /// Creates a backtest-oriented read facade over a fresh read session.
    /// Returns a storage-level read facade intended for upper-layer backtest adapters.
    ///
    /// The returned view does not execute backtests or strategy logic; it only wraps
    /// [`FastKReadSession`] for repeated storage reads.
    pub fn backtest_view(&self) -> crate::engine::backtest::BacktestStoreView<'_> {
        crate::engine::backtest::BacktestStoreView::new(self)
    }

    /// Lists every registered series under the store root.
    pub fn list_series(&self) -> Result<Vec<SeriesInventoryEntry>> {
        let mut entries = recovery::discover_series_dirs(&path::series_root(&self.root))?
            .into_iter()
            .map(|series_dir| self.load_series_inventory_entry(&series_dir))
            .collect::<Result<Vec<_>>>()?;
        entries.sort_by(|left, right| {
            (&left.symbol, &left.category, &left.name).cmp(&(
                &right.symbol,
                &right.category,
                &right.name,
            ))
        });
        Ok(entries)
    }

    /// Lists only kline series.
    pub fn list_kline_series(&self) -> Result<Vec<SeriesInventoryEntry>> {
        Ok(self
            .list_series()?
            .into_iter()
            .filter(|entry| entry.category == path::KLINE_CATEGORY)
            .collect())
    }

    /// Lists only scalar or derived series.
    pub fn list_scalar_series(&self) -> Result<Vec<SeriesInventoryEntry>> {
        Ok(self
            .list_series()?
            .into_iter()
            .filter(|entry| entry.category != path::KLINE_CATEGORY)
            .collect())
    }

    /// Returns the current chunk inventory for a kline series.
    pub fn kline_chunk_inventory(
        &self,
        symbol: &str,
        timeframe: &str,
    ) -> Result<Vec<crate::types::ChunkMeta>> {
        Ok(self.load_kline_meta(symbol, timeframe)?.chunks.clone())
    }

    /// Returns the current chunk inventory for a scalar series.
    pub fn scalar_chunk_inventory(
        &self,
        series_key: &ScalarSeriesKey,
    ) -> Result<Vec<crate::types::ChunkMeta>> {
        Ok(self.load_scalar_meta(series_key)?.chunks.clone())
    }

    /// Returns grouped month inventory for a kline series.
    pub fn kline_month_inventory(
        &self,
        symbol: &str,
        timeframe: &str,
    ) -> Result<Vec<MonthInventoryEntry>> {
        Ok(month_inventory(
            self.load_kline_meta(symbol, timeframe)?.as_ref(),
        ))
    }

    /// Returns grouped month inventory for a scalar series.
    pub fn scalar_month_inventory(
        &self,
        series_key: &ScalarSeriesKey,
    ) -> Result<Vec<MonthInventoryEntry>> {
        Ok(month_inventory(self.load_scalar_meta(series_key)?.as_ref()))
    }

    /// Lists indicator names registered for one `(symbol, timeframe)` pair.
    pub fn list_indicators(&self, symbol: &str, timeframe: &str) -> Result<Vec<String>> {
        let mut indicators: Vec<_> = self
            .list_scalar_series()?
            .into_iter()
            .filter_map(|entry| {
                if entry.symbol != symbol || entry.category != INDICATOR_CATEGORY {
                    return None;
                }
                let key = ScalarSeriesKey {
                    symbol: entry.symbol,
                    category: entry.category,
                    name: entry.name,
                };
                let binding = decode_scoped_scalar_key(&key)?;
                (binding.timeframe == timeframe).then_some(binding.name)
            })
            .collect();
        indicators.sort();
        indicators.dedup();
        Ok(indicators)
    }

    /// Returns indicator inventory entries scoped to one `(symbol, timeframe)` pair.
    pub fn indicator_inventory(
        &self,
        symbol: &str,
        timeframe: &str,
    ) -> Result<Vec<IndicatorInventoryEntry>> {
        let mut entries: Vec<_> = self
            .list_scalar_series()?
            .into_iter()
            .filter_map(|entry| {
                if entry.symbol != symbol || entry.category != INDICATOR_CATEGORY {
                    return None;
                }
                let key = ScalarSeriesKey {
                    symbol: entry.symbol.clone(),
                    category: entry.category.clone(),
                    name: entry.name.clone(),
                };
                let binding = decode_scoped_scalar_key(&key)?;
                (binding.timeframe == timeframe).then_some(IndicatorInventoryEntry {
                    symbol: entry.symbol,
                    timeframe: binding.timeframe,
                    indicator_name: binding.name,
                    chunk_count: entry.chunk_count,
                    record_count: entry.record_count,
                    sidecar_count: entry.sidecar_count,
                    active_delta_count: entry.active_delta_count,
                })
            })
            .collect();
        entries.sort_by(|left, right| left.indicator_name.cmp(&right.indicator_name));
        Ok(entries)
    }

    /// Summarizes the whole store for backtest tooling and validation UIs.
    pub fn store_stats(&self) -> Result<StoreStats> {
        let entries = self.list_series()?;
        let mut stats = StoreStats {
            series_count: entries.len(),
            ..StoreStats {
                series_count: entries.len(),
                kline_series_count: 0,
                scalar_series_count: 0,
                chunk_count: 0,
                record_count: 0,
                sidecar_count: 0,
                active_delta_count: 0,
            }
        };
        for entry in entries {
            if entry.category == path::KLINE_CATEGORY {
                stats.kline_series_count += 1;
            } else {
                stats.scalar_series_count += 1;
            }
            stats.chunk_count += entry.chunk_count;
            stats.record_count = stats.record_count.saturating_add(entry.record_count);
            stats.sidecar_count += entry.sidecar_count;
            stats.active_delta_count += entry.active_delta_count;
        }
        Ok(stats)
    }

    /// Returns a high-level health summary suitable for backtest bootstrap checks.
    pub fn health_summary(&self) -> Result<StoreHealthSummary> {
        let validations = self.validate_manifest_vs_fs()?;
        let orphans = self.list_orphans()?;
        let clean_series_count = validations
            .iter()
            .filter(|report| report.is_clean())
            .count();
        let overlap_group_count = validations
            .iter()
            .map(|report| report.overlap_groups.len())
            .sum();

        Ok(StoreHealthSummary {
            series_count: validations.len(),
            clean_series_count,
            issue_series_count: validations.len().saturating_sub(clean_series_count),
            orphan_count: orphans.len(),
            overlap_group_count,
            pending_recovery: recovery::has_pending_recovery(&self.root)?,
            platform_fsync_guarantee: if cfg!(windows) {
                "best-effort-parent-dir-fsync"
            } else {
                "fsync-parent-dir"
            },
        })
    }

    /// Registers a scalar series that will store fixed-width indicator records.
    pub fn register_scalar_series(
        &mut self,
        series_key: &ScalarSeriesKey,
        timeframe_ms: i64,
    ) -> Result<()> {
        if timeframe_ms <= 0 {
            return Err(FastKError::InvalidInput(
                "timeframe_ms must be positive".into(),
            ));
        }
        let _lock = self.begin_mutation()?;
        self.register_scalar_series_locked(series_key, timeframe_ms)
    }

    fn register_scalar_series_locked(
        &mut self,
        series_key: &ScalarSeriesKey,
        timeframe_ms: i64,
    ) -> Result<()> {
        let series_dir = path::scalar_series_dir(
            &self.root,
            &series_key.symbol,
            &series_key.category,
            &series_key.name,
        );
        let meta_path = path::series_meta_path(&series_dir);
        if meta_path.exists() {
            let existing = self.load_scalar_meta(series_key)?;
            if existing.timeframe_ms != timeframe_ms {
                return Err(FastKError::InvalidInput(format!(
                    "scalar series already exists with timeframe_ms={}",
                    existing.timeframe_ms
                )));
            }
            return Ok(());
        }

        let marker = recovery::RecoveryMarkerGuard::arm(&self.root)?;
        crate::storage::fs::ensure_dir(&path::chunks_dir(&series_dir))?;
        let meta = Catalog::build_scalar_meta(series_key, timeframe_ms);
        manifest::save_series_meta(&series_dir, &meta)?;
        self.store_series_in_cache(meta);
        marker.commit()?;
        Ok(())
    }

    /// Registers an indicator series as a scoped scalar series under `category="indicator"`.
    ///
    /// FastK does not calculate the indicator. It only reserves storage and lifecycle metadata.
    pub fn register_indicator_series(
        &mut self,
        symbol: &str,
        timeframe: &str,
        indicator_name: &str,
    ) -> Result<()> {
        let timeframe_ms = self.load_kline_meta(symbol, timeframe)?.timeframe_ms;
        let key = indicator_series_key(symbol, timeframe, indicator_name);
        self.register_scalar_series(&key, timeframe_ms)
    }

    /// Writes or replaces a scalar month chunk and rebuilds `.zmap` / `.vix` sidecars.
    pub fn put_scalar_chunk(
        &mut self,
        series_key: &ScalarSeriesKey,
        timeframe_ms: i64,
        records: &[ScalarRecord],
        zmap_block_size: usize,
    ) -> Result<()> {
        if timeframe_ms <= 0 || zmap_block_size == 0 {
            return Err(FastKError::InvalidInput(
                "timeframe and zone-map block size must be positive".into(),
            ));
        }
        validate_month_records(records)?;
        let _lock = self.begin_mutation()?;
        self.put_scalar_chunk_locked(series_key, timeframe_ms, records, zmap_block_size)
    }

    fn put_scalar_chunk_locked(
        &mut self,
        series_key: &ScalarSeriesKey,
        timeframe_ms: i64,
        records: &[ScalarRecord],
        zmap_block_size: usize,
    ) -> Result<()> {
        let series_dir = path::scalar_series_dir(
            &self.root,
            &series_key.symbol,
            &series_key.category,
            &series_key.name,
        );

        let mut meta = if path::series_meta_path(&series_dir).exists() {
            let meta = (*self.load_scalar_meta(series_key)?).clone();
            if meta.timeframe_ms != timeframe_ms {
                return Err(FastKError::InvalidInput(format!(
                    "scalar series already exists with timeframe_ms={}",
                    meta.timeframe_ms
                )));
            }
            meta
        } else {
            Catalog::build_scalar_meta(series_key, timeframe_ms)
        };

        let first = records
            .first()
            .ok_or_else(|| FastKError::InvalidInput("records must not be empty".to_string()))?;
        let month_key = path::month_key(first.ts)?;
        let month_indices = meta.chunk_indices_for_month(&month_key);
        let chunk_id = next_chunk_id(&meta);
        let generation = month_indices
            .iter()
            .map(|index| meta.chunks[*index].generation)
            .max()
            .unwrap_or(0)
            .saturating_add(1);
        let file_name = if month_indices.is_empty() {
            path::month_chunk_file_name(first.ts)?
        } else {
            path::month_merged_chunk_file_name(&month_key, generation)
        };
        let relative_path = path::chunk_relative_path(&file_name);
        let chunk_path = path::resolve_relative_path(&series_dir, &relative_path);
        let marker = recovery::RecoveryMarkerGuard::arm(&self.root)?;
        let mut chunk_meta = scalar_writer::write_chunk(
            &chunk_path,
            &meta,
            records,
            &scalar_writer::WriteScalarChunkOptions {
                chunk_id,
                generation,
                state: ChunkState::Sealed,
                relative_path,
                sparse_index_every: crate::chunk::sparse_index::DEFAULT_SPARSE_INDEX_EVERY,
            },
        )?;
        crate::storage::fs::fault_point("scalar_chunk");
        chunk_meta.sidecars =
            build_scalar_sidecars(&series_dir, &chunk_meta, records, zmap_block_size)?;

        crate::storage::fs::fault_point("scalar_sidecars");
        let written_sidecars = chunk_meta.sidecars.clone();
        let mut obsolete = Vec::new();
        if month_indices.is_empty() {
            manifest::upsert_chunk_meta(&mut meta, chunk_meta)?;
        } else {
            let removed = manifest::replace_month_chunks(&mut meta, &month_key, chunk_meta)?;
            for chunk in &removed {
                self.scalar_sidecars.invalidate(&chunk.relative_path);
            }
            obsolete = removed;
        }
        self.catalog.save_scalar_meta(&meta)?;
        crate::storage::fs::fault_point("scalar_manifest");
        remove_sidecars_for_chunks(&series_dir, &obsolete);
        record_file_bytes_written(self.metrics.as_ref(), &chunk_path);
        for sidecar in &written_sidecars {
            let sidecar_path = path::resolve_relative_path(&series_dir, &sidecar.relative_path);
            record_file_bytes_written(self.metrics.as_ref(), &sidecar_path);
        }
        record_file_bytes_written(self.metrics.as_ref(), &path::series_meta_path(&series_dir));
        self.store_series_in_cache(meta);
        marker.commit()?;
        Ok(())
    }

    /// Writes indicator rows using the scalar-series lifecycle and default sidecar settings.
    pub fn put_indicator_chunk(
        &mut self,
        symbol: &str,
        timeframe: &str,
        indicator_name: &str,
        records: &[ScalarRecord],
    ) -> Result<()> {
        let timeframe_ms = self.load_kline_meta(symbol, timeframe)?.timeframe_ms;
        let key = indicator_series_key(symbol, timeframe, indicator_name);
        self.put_scalar_chunk(&key, timeframe_ms, records, DEFAULT_SCALAR_ZMAP_BLOCK_SIZE)
    }

    /// Returns the exact scalar record at `ts`, if present.
    pub fn get_scalar_at(
        &self,
        series_key: &ScalarSeriesKey,
        ts: i64,
    ) -> Result<Option<ScalarRecord>> {
        let meta = self.load_scalar_meta(series_key)?;
        let series_dir = path::scalar_series_dir(
            &self.root,
            &series_key.symbol,
            &series_key.category,
            &series_key.name,
        );
        self.get_scalar_at_prepared(&series_dir, meta.as_ref(), ts)
    }

    /// Returns the exact indicator record at `ts`, if present.
    pub fn get_indicator_at(
        &self,
        symbol: &str,
        timeframe: &str,
        indicator_name: &str,
        ts: i64,
    ) -> Result<Option<ScalarRecord>> {
        let key = indicator_series_key(symbol, timeframe, indicator_name);
        self.get_scalar_at(&key, ts)
    }

    /// Returns scalar records inside the inclusive timestamp range.
    pub fn get_scalar_range(
        &self,
        series_key: &ScalarSeriesKey,
        start_ts: i64,
        end_ts: i64,
    ) -> Result<Vec<ScalarRecord>> {
        if start_ts > end_ts {
            return Ok(Vec::new());
        }

        let meta = self.load_scalar_meta(series_key)?;
        let series_dir = path::scalar_series_dir(
            &self.root,
            &series_key.symbol,
            &series_key.category,
            &series_key.name,
        );
        self.get_scalar_range_prepared(&series_dir, meta.as_ref(), start_ts, end_ts)
    }

    /// Creates a deterministic sealed-chunk replay cursor for a scalar series.
    pub fn replay_scalar(
        &self,
        series_key: &ScalarSeriesKey,
        start_ts: i64,
        end_ts: Option<i64>,
    ) -> Result<ReplayCursor<ScalarRecord>> {
        let meta = self.load_scalar_meta(series_key)?;
        ReplayCursor::new(
            path::scalar_series_dir(
                &self.root,
                &series_key.symbol,
                &series_key.category,
                &series_key.name,
            ),
            meta.as_ref(),
            ReplayOptions {
                start_ts,
                end_ts,
                batch_hint: None,
            },
        )
    }

    /// Returns indicator records inside the inclusive timestamp range.
    pub fn get_indicator_range(
        &self,
        symbol: &str,
        timeframe: &str,
        indicator_name: &str,
        start_ts: i64,
        end_ts: i64,
    ) -> Result<Vec<ScalarRecord>> {
        let key = indicator_series_key(symbol, timeframe, indicator_name);
        self.get_scalar_range(&key, start_ts, end_ts)
    }

    /// Finds scalar timestamps through zone-map pruning.
    pub fn find_scalar_timestamps_via_zmap(
        &self,
        series_key: &ScalarSeriesKey,
        predicate: &ScalarPredicate,
        start_ts: i64,
        end_ts: i64,
    ) -> Result<Vec<i64>> {
        self.find_scalar_timestamps(
            series_key,
            predicate,
            start_ts,
            end_ts,
            ScalarQueryStrategy::Zmap,
        )
    }

    /// Finds indicator timestamps through zone-map pruning.
    pub fn find_indicator_timestamps_via_zmap(
        &self,
        symbol: &str,
        timeframe: &str,
        indicator_name: &str,
        predicate: &ScalarPredicate,
        start_ts: i64,
        end_ts: i64,
    ) -> Result<Vec<i64>> {
        let key = indicator_series_key(symbol, timeframe, indicator_name);
        self.find_scalar_timestamps_via_zmap(&key, predicate, start_ts, end_ts)
    }

    /// Finds scalar timestamps through value-index lookup.
    pub fn find_scalar_timestamps_via_vix(
        &self,
        series_key: &ScalarSeriesKey,
        predicate: &ScalarPredicate,
        start_ts: i64,
        end_ts: i64,
    ) -> Result<Vec<i64>> {
        self.find_scalar_timestamps(
            series_key,
            predicate,
            start_ts,
            end_ts,
            ScalarQueryStrategy::Vix,
        )
    }

    /// Finds indicator timestamps through value-index lookup.
    pub fn find_indicator_timestamps_via_vix(
        &self,
        symbol: &str,
        timeframe: &str,
        indicator_name: &str,
        predicate: &ScalarPredicate,
        start_ts: i64,
        end_ts: i64,
    ) -> Result<Vec<i64>> {
        let key = indicator_series_key(symbol, timeframe, indicator_name);
        self.find_scalar_timestamps_via_vix(&key, predicate, start_ts, end_ts)
    }

    /// Finds scalar timestamps by scanning raw chunk records without sidecars.
    pub fn find_scalar_timestamps_raw(
        &self,
        series_key: &ScalarSeriesKey,
        predicate: &ScalarPredicate,
        start_ts: i64,
        end_ts: i64,
    ) -> Result<Vec<i64>> {
        self.find_scalar_timestamps(
            series_key,
            predicate,
            start_ts,
            end_ts,
            ScalarQueryStrategy::Raw,
        )
    }

    /// Runs a storage-level predicate query over `ScalarRecord.value`.
    ///
    /// FastK only compares stored integer values. It does not interpret feature,
    /// factor, signal, risk, metric, strategy, or market-data semantics.
    pub fn query_scalar_predicate(
        &self,
        query: ScalarPredicateQuery,
    ) -> Result<ScalarPredicateQueryResult> {
        validate_scalar_predicate_query(&query)?;
        if query.predicate.is_impossible() {
            return Ok(ScalarPredicateQueryResult {
                matches: Vec::new(),
                stats: ScalarPredicateQueryStats::default(),
            });
        }

        let meta = self.load_scalar_meta(&query.key)?;
        let series_dir = path::scalar_series_dir(
            &self.root,
            &query.key.symbol,
            &query.key.category,
            &query.key.name,
        );
        self.query_scalar_predicate_prepared(&series_dir, meta.as_ref(), &query)
    }

    /// Finds matching timestamps using the best available storage-level scalar index.
    pub fn find_scalar_timestamps_by_predicate(
        &self,
        key: &ScalarSeriesKey,
        start_ts: i64,
        end_ts: i64,
        predicate: ScalarPredicateExpr,
    ) -> Result<Vec<i64>> {
        let result = self.query_scalar_predicate(ScalarPredicateQuery {
            key: key.clone(),
            start_ts,
            end_ts,
            predicate,
            return_values: false,
        })?;
        Ok(result.matches.into_iter().map(|entry| entry.ts).collect())
    }

    /// Finds matching scalar records using the best available storage-level scalar index.
    pub fn find_scalar_points_by_predicate(
        &self,
        key: &ScalarSeriesKey,
        start_ts: i64,
        end_ts: i64,
        predicate: ScalarPredicateExpr,
    ) -> Result<Vec<ScalarRecord>> {
        let result = self.query_scalar_predicate(ScalarPredicateQuery {
            key: key.clone(),
            start_ts,
            end_ts,
            predicate,
            return_values: true,
        })?;
        Ok(result
            .matches
            .into_iter()
            .filter_map(|entry| {
                entry.value.map(|value| ScalarRecord {
                    ts: entry.ts,
                    value,
                })
            })
            .collect())
    }

    /// Reports whether the scalar series currently has query sidecars attached.
    pub fn scalar_query_capabilities(
        &self,
        series_key: &ScalarSeriesKey,
    ) -> Result<ScalarQueryCapabilities> {
        let meta = self.load_scalar_meta(series_key)?;
        let has_zmap = meta
            .chunks
            .iter()
            .flat_map(|chunk| chunk.sidecars.iter())
            .any(|sidecar| sidecar.kind == "zmap");
        let has_vix = meta
            .chunks
            .iter()
            .flat_map(|chunk| chunk.sidecars.iter())
            .any(|sidecar| sidecar.kind == "vix");

        Ok(ScalarQueryCapabilities {
            raw_scan: true,
            has_zmap,
            has_vix,
        })
    }

    /// Returns query capabilities for one indicator series.
    pub fn indicator_capabilities(
        &self,
        symbol: &str,
        timeframe: &str,
        indicator_name: &str,
    ) -> Result<ScalarQueryCapabilities> {
        let key = indicator_series_key(symbol, timeframe, indicator_name);
        self.scalar_query_capabilities(&key)
    }

    /// Registers a trade tick series. Trade series default to hourly partitions.
    pub fn register_trade_series(
        &mut self,
        symbol: &str,
        channel: &str,
        timeframe_ms: i64,
    ) -> Result<()> {
        self.register_trade_series_with_partition(
            symbol,
            channel,
            timeframe_ms,
            PartitionPolicy::hour(),
        )
    }

    /// Registers a trade tick series with an explicit time-based partition policy.
    pub fn register_trade_series_with_partition(
        &mut self,
        symbol: &str,
        channel: &str,
        timeframe_ms: i64,
        partition_policy: PartitionPolicy,
    ) -> Result<()> {
        self.register_fixed_series::<TradeRecord>(
            symbol,
            path::TRADE_CATEGORY,
            channel,
            timeframe_ms,
            partition_policy,
        )
    }

    /// Writes sorted trade records, splitting them across the series partition policy.
    pub fn put_trade_chunk(
        &mut self,
        symbol: &str,
        channel: &str,
        records: &[TradeRecord],
    ) -> Result<()> {
        self.put_fixed_chunk::<TradeRecord>(symbol, path::TRADE_CATEGORY, channel, records)
    }

    pub fn get_trade_range(
        &self,
        symbol: &str,
        channel: &str,
        start_ts: i64,
        end_ts: i64,
    ) -> Result<Vec<TradeRecord>> {
        self.get_fixed_range::<TradeRecord>(symbol, path::TRADE_CATEGORY, channel, start_ts, end_ts)
    }

    pub fn get_trade_latest_n(
        &self,
        symbol: &str,
        channel: &str,
        n: usize,
    ) -> Result<Vec<TradeRecord>> {
        self.get_fixed_latest_n::<TradeRecord>(symbol, path::TRADE_CATEGORY, channel, n)
    }

    pub fn replay_trade(
        &self,
        symbol: &str,
        channel: &str,
        start_ts: i64,
        end_ts: Option<i64>,
    ) -> Result<ReplayCursor<TradeRecord>> {
        self.replay_fixed::<TradeRecord>(symbol, path::TRADE_CATEGORY, channel, start_ts, end_ts)
    }

    /// Scans adjacent trade ids and reports storage-level gaps, duplicates, or regressions.
    ///
    /// This does not apply exchange-specific trade-id policy or repair missing records.
    pub fn scan_trade_id_sequence(
        &self,
        symbol: &str,
        channel: &str,
        start_ts: i64,
        end_ts: i64,
    ) -> Result<SequenceScanReport> {
        self.scan_fixed_sequence::<TradeRecord>(
            symbol,
            path::TRADE_CATEGORY,
            channel,
            start_ts,
            end_ts,
        )
    }

    /// Registers a BBO series. BBO series default to daily partitions.
    pub fn register_bbo_series(
        &mut self,
        symbol: &str,
        channel: &str,
        timeframe_ms: i64,
    ) -> Result<()> {
        self.register_bbo_series_with_partition(
            symbol,
            channel,
            timeframe_ms,
            PartitionPolicy::day(),
        )
    }

    /// Registers a BBO series with an explicit time-based partition policy.
    pub fn register_bbo_series_with_partition(
        &mut self,
        symbol: &str,
        channel: &str,
        timeframe_ms: i64,
        partition_policy: PartitionPolicy,
    ) -> Result<()> {
        self.register_fixed_series::<BboRecord>(
            symbol,
            path::BBO_CATEGORY,
            channel,
            timeframe_ms,
            partition_policy,
        )
    }

    pub fn put_bbo_chunk(
        &mut self,
        symbol: &str,
        channel: &str,
        records: &[BboRecord],
    ) -> Result<()> {
        self.put_fixed_chunk::<BboRecord>(symbol, path::BBO_CATEGORY, channel, records)
    }

    pub fn get_bbo_range(
        &self,
        symbol: &str,
        channel: &str,
        start_ts: i64,
        end_ts: i64,
    ) -> Result<Vec<BboRecord>> {
        self.get_fixed_range::<BboRecord>(symbol, path::BBO_CATEGORY, channel, start_ts, end_ts)
    }

    pub fn get_bbo_latest_n(
        &self,
        symbol: &str,
        channel: &str,
        n: usize,
    ) -> Result<Vec<BboRecord>> {
        self.get_fixed_latest_n::<BboRecord>(symbol, path::BBO_CATEGORY, channel, n)
    }

    pub fn replay_bbo(
        &self,
        symbol: &str,
        channel: &str,
        start_ts: i64,
        end_ts: Option<i64>,
    ) -> Result<ReplayCursor<BboRecord>> {
        self.replay_fixed::<BboRecord>(symbol, path::BBO_CATEGORY, channel, start_ts, end_ts)
    }

    /// Scans adjacent BBO sequence values and reports storage-level issues.
    ///
    /// This does not apply exchange-specific sequence policy or repair missing records.
    pub fn scan_bbo_sequence(
        &self,
        symbol: &str,
        channel: &str,
        start_ts: i64,
        end_ts: i64,
    ) -> Result<SequenceScanReport> {
        self.scan_fixed_sequence::<BboRecord>(symbol, path::BBO_CATEGORY, channel, start_ts, end_ts)
    }

    /// Registers an order-book delta series. Book delta series default to hourly partitions.
    pub fn register_book_delta_series(
        &mut self,
        symbol: &str,
        channel: &str,
        timeframe_ms: i64,
    ) -> Result<()> {
        self.register_book_delta_series_with_partition(
            symbol,
            channel,
            timeframe_ms,
            PartitionPolicy::hour(),
        )
    }

    /// Registers an order-book delta series with an explicit time-based partition policy.
    pub fn register_book_delta_series_with_partition(
        &mut self,
        symbol: &str,
        channel: &str,
        timeframe_ms: i64,
        partition_policy: PartitionPolicy,
    ) -> Result<()> {
        self.register_fixed_series::<BookDeltaRecord>(
            symbol,
            path::BOOK_DELTA_CATEGORY,
            channel,
            timeframe_ms,
            partition_policy,
        )
    }

    pub fn put_book_delta_chunk(
        &mut self,
        symbol: &str,
        channel: &str,
        records: &[BookDeltaRecord],
    ) -> Result<()> {
        self.put_fixed_chunk::<BookDeltaRecord>(symbol, path::BOOK_DELTA_CATEGORY, channel, records)
    }

    pub fn get_book_delta_range(
        &self,
        symbol: &str,
        channel: &str,
        start_ts: i64,
        end_ts: i64,
    ) -> Result<Vec<BookDeltaRecord>> {
        self.get_fixed_range::<BookDeltaRecord>(
            symbol,
            path::BOOK_DELTA_CATEGORY,
            channel,
            start_ts,
            end_ts,
        )
    }

    pub fn get_book_delta_latest_n(
        &self,
        symbol: &str,
        channel: &str,
        n: usize,
    ) -> Result<Vec<BookDeltaRecord>> {
        self.get_fixed_latest_n::<BookDeltaRecord>(symbol, path::BOOK_DELTA_CATEGORY, channel, n)
    }

    pub fn replay_book_delta(
        &self,
        symbol: &str,
        channel: &str,
        start_ts: i64,
        end_ts: Option<i64>,
    ) -> Result<ReplayCursor<BookDeltaRecord>> {
        self.replay_fixed::<BookDeltaRecord>(
            symbol,
            path::BOOK_DELTA_CATEGORY,
            channel,
            start_ts,
            end_ts,
        )
    }

    /// Scans adjacent order-book delta sequence values and reports storage-level issues.
    ///
    /// This does not apply exchange-specific sequence policy or repair missing records.
    pub fn scan_book_delta_sequence(
        &self,
        symbol: &str,
        channel: &str,
        start_ts: i64,
        end_ts: i64,
    ) -> Result<SequenceScanReport> {
        self.scan_fixed_sequence::<BookDeltaRecord>(
            symbol,
            path::BOOK_DELTA_CATEGORY,
            channel,
            start_ts,
            end_ts,
        )
    }

    fn get_kline_at_prepared(
        &self,
        series_dir: &Path,
        meta: &SeriesMeta,
        ts: i64,
    ) -> Result<Option<KlineRecord>> {
        let lookup_started = Instant::now();
        let Some(index) = meta.find_chunk_for_ts(ts) else {
            self.metrics
                .record_chunk_lookup_ns(lookup_started.elapsed().as_nanos() as u64);
            return Ok(None);
        };
        self.metrics
            .record_chunk_lookup_ns(lookup_started.elapsed().as_nanos() as u64);
        kline_reader::find_in_chunk(
            &self.chunk_runtime,
            series_dir,
            meta.series_id,
            &meta.chunks[index],
            ts,
        )
    }

    fn get_kline_range_prepared(
        &self,
        series_dir: &Path,
        meta: &SeriesMeta,
        start_ts: i64,
        end_ts: i64,
    ) -> Result<Vec<KlineRecord>> {
        if start_ts > end_ts {
            return Ok(Vec::new());
        }

        let chunk_range = meta.find_chunks_for_range(start_ts, end_ts);
        if chunk_range.is_empty() {
            return Ok(Vec::new());
        }

        let capacity = meta.chunks[chunk_range.clone()]
            .iter()
            .try_fold(0usize, |sum, chunk| {
                let count = if start_ts <= chunk.start_ts && end_ts >= chunk.end_ts {
                    usize::try_from(chunk.count).map_err(|_| {
                        FastKError::InvalidData("chunk count exceeds address space".into())
                    })?
                } else {
                    (chunk.count as usize).min(estimate_short_range_rows(
                        meta.timeframe_ms,
                        start_ts,
                        end_ts,
                    ))
                };
                sum.checked_add(count)
                    .ok_or_else(|| FastKError::InvalidData("range capacity overflow".into()))
            })?;
        let chunks = &meta.chunks[chunk_range.clone()];
        if self.should_plan_read(meta, chunks, start_ts, end_ts) {
            let spans = chunks
                .iter()
                .map(|chunk| {
                    kline_reader::plan_range(
                        &self.chunk_runtime,
                        series_dir,
                        meta.series_id,
                        chunk,
                        start_ts,
                        end_ts,
                    )
                })
                .collect::<Result<Vec<_>>>()?;
            return self.read_spans(spans, KlineRecord::BYTE_SIZE, kline_reader::read_planned);
        }

        let mut records = Vec::with_capacity(capacity);

        for chunk in &meta.chunks[chunk_range] {
            let _ = kline_reader::append_range_in_chunk(
                &self.chunk_runtime,
                series_dir,
                meta.series_id,
                chunk,
                start_ts,
                end_ts,
                &mut records,
            )?;
        }

        Ok(records)
    }

    fn get_kline_latest_n_prepared(
        &self,
        series_dir: &Path,
        meta: &SeriesMeta,
        n: usize,
    ) -> Result<Vec<KlineRecord>> {
        if n == 0 {
            return Ok(Vec::new());
        }

        let mut remaining = n;
        let mut spans = Vec::new();
        for chunk in meta.chunks.iter().rev() {
            if remaining == 0 {
                break;
            }
            let take = remaining.min(chunk.count as usize);
            spans.push(kline_reader::plan_tail(
                &self.chunk_runtime,
                series_dir,
                meta.series_id,
                chunk,
                take,
            )?);
            remaining -= take;
        }
        spans.reverse();
        self.read_spans(spans, KlineRecord::BYTE_SIZE, kline_reader::read_planned)
    }

    fn get_scalar_at_prepared(
        &self,
        series_dir: &Path,
        meta: &SeriesMeta,
        ts: i64,
    ) -> Result<Option<ScalarRecord>> {
        let lookup_started = Instant::now();
        let Some(index) = meta.find_chunk_for_ts(ts) else {
            self.metrics
                .record_chunk_lookup_ns(lookup_started.elapsed().as_nanos() as u64);
            return Ok(None);
        };
        self.metrics
            .record_chunk_lookup_ns(lookup_started.elapsed().as_nanos() as u64);
        scalar_reader::find_in_chunk(
            &self.chunk_runtime,
            series_dir,
            meta.series_id,
            &meta.chunks[index],
            ts,
        )
    }

    fn get_scalar_range_prepared(
        &self,
        series_dir: &Path,
        meta: &SeriesMeta,
        start_ts: i64,
        end_ts: i64,
    ) -> Result<Vec<ScalarRecord>> {
        if start_ts > end_ts {
            return Ok(Vec::new());
        }

        let chunk_range = meta.find_chunks_for_range(start_ts, end_ts);
        if chunk_range.is_empty() {
            return Ok(Vec::new());
        }

        let chunks = &meta.chunks[chunk_range.clone()];
        if self.should_plan_read(meta, chunks, start_ts, end_ts) {
            let spans = chunks
                .iter()
                .map(|chunk| {
                    scalar_reader::plan_range(
                        &self.chunk_runtime,
                        series_dir,
                        meta.series_id,
                        chunk,
                        start_ts,
                        end_ts,
                    )
                })
                .collect::<Result<Vec<_>>>()?;
            return self.read_spans(spans, ScalarRecord::BYTE_SIZE, scalar_reader::read_planned);
        }

        let mut records = Vec::with_capacity(estimate_short_range_rows(
            meta.timeframe_ms,
            start_ts,
            end_ts,
        ));
        for chunk in &meta.chunks[chunk_range] {
            let _ = scalar_reader::append_range_in_chunk(
                &self.chunk_runtime,
                series_dir,
                meta.series_id,
                chunk,
                start_ts,
                end_ts,
                &mut records,
            )?;
        }
        Ok(records)
    }

    fn find_scalar_timestamps_prepared(
        &self,
        series_dir: &Path,
        meta: &SeriesMeta,
        series_key: &ScalarSeriesKey,
        predicate: &ScalarPredicate,
        start_ts: i64,
        end_ts: i64,
        strategy: ScalarQueryStrategy,
    ) -> Result<Vec<i64>> {
        if start_ts > end_ts {
            return Ok(Vec::new());
        }

        let chunk_range = meta.find_chunks_for_range(start_ts, end_ts);
        if chunk_range.is_empty() {
            return Ok(Vec::new());
        }

        let mut timestamps = Vec::new();

        for chunk in &meta.chunks[chunk_range] {
            match strategy {
                ScalarQueryStrategy::Zmap => {
                    timestamps.extend(
                        self.query_scalar_via_zmap(series_dir, chunk, predicate, start_ts, end_ts)?,
                    );
                }
                ScalarQueryStrategy::Vix => {
                    timestamps.extend(self.query_scalar_via_vix(
                        series_dir, series_key, chunk, predicate, start_ts, end_ts,
                    )?);
                }
                ScalarQueryStrategy::Raw => {
                    timestamps.extend(self.query_scalar_via_raw_scan(
                        series_dir,
                        meta.series_id,
                        chunk,
                        predicate,
                        start_ts,
                        end_ts,
                    )?);
                }
            }
        }

        timestamps.sort_unstable();
        timestamps.dedup();
        Ok(timestamps)
    }

    fn query_scalar_predicate_prepared(
        &self,
        series_dir: &Path,
        meta: &SeriesMeta,
        query: &ScalarPredicateQuery,
    ) -> Result<ScalarPredicateQueryResult> {
        let chunk_range = meta.find_chunks_for_range(query.start_ts, query.end_ts);
        if chunk_range.is_empty() {
            return Ok(ScalarPredicateQueryResult {
                matches: Vec::new(),
                stats: ScalarPredicateQueryStats::default(),
            });
        }

        let mut matches = Vec::new();
        let mut stats = ScalarPredicateQueryStats::default();

        for chunk in &meta.chunks[chunk_range] {
            stats.chunks_considered = stats.chunks_considered.saturating_add(1);

            if query.predicate.is_discrete_lookup_like() && has_sidecar_kind(chunk, "vix") {
                mark_index_used(&mut stats, ScalarIndexKind::ValueIndex);
                self.query_scalar_predicate_via_vix(
                    series_dir,
                    chunk,
                    query,
                    &mut matches,
                    &mut stats,
                )?;
            } else if query.predicate.is_continuous_range_like() && has_sidecar_kind(chunk, "zmap")
            {
                mark_index_used(&mut stats, ScalarIndexKind::ZoneMap);
                self.query_scalar_predicate_via_zmap(
                    series_dir,
                    chunk,
                    query,
                    &mut matches,
                    &mut stats,
                )?;
            } else {
                mark_index_used(&mut stats, ScalarIndexKind::FullScan);
                stats.fallback_scan = true;
                self.query_scalar_predicate_via_raw_scan(
                    series_dir,
                    meta.series_id,
                    chunk,
                    query,
                    &mut matches,
                    &mut stats,
                )?;
            }
        }

        matches.sort_by_key(|entry| entry.ts);
        stats.rows_matched = matches.len() as u64;
        Ok(ScalarPredicateQueryResult { matches, stats })
    }

    fn load_kline_meta(&self, symbol: &str, timeframe: &str) -> Result<Arc<SeriesMeta>> {
        let cache_key = series_cache_key(symbol, path::KLINE_CATEGORY, timeframe);
        if let Some(meta) = self
            .manifest_cache
            .read()
            .expect("manifest cache poisoned")
            .get(&cache_key)
        {
            self.metrics.record_manifest_cache_hit();
            return Ok(meta.clone());
        }

        let started = Instant::now();
        let report = match self
            .catalog
            .load_kline_meta_cached_report(symbol, timeframe)
        {
            Some(report) => report,
            None => self.catalog.load_kline_meta_report(symbol, timeframe)?,
        };
        self.record_manifest_load_report(started.elapsed().as_nanos() as u64, &report.stats, true);
        self.manifest_cache
            .write()
            .expect("manifest cache poisoned")
            .insert(cache_key, report.meta.clone());
        Ok(report.meta)
    }

    fn load_scalar_meta(&self, series_key: &ScalarSeriesKey) -> Result<Arc<SeriesMeta>> {
        let cache_key =
            series_cache_key(&series_key.symbol, &series_key.category, &series_key.name);
        if let Some(meta) = self
            .manifest_cache
            .read()
            .expect("manifest cache poisoned")
            .get(&cache_key)
        {
            self.metrics.record_manifest_cache_hit();
            return Ok(meta.clone());
        }

        let started = Instant::now();
        let report = match self.catalog.load_scalar_meta_cached_report(series_key) {
            Some(report) => report,
            None => self.catalog.load_scalar_meta_report(series_key)?,
        };
        self.record_manifest_load_report(started.elapsed().as_nanos() as u64, &report.stats, true);
        self.manifest_cache
            .write()
            .expect("manifest cache poisoned")
            .insert(cache_key, report.meta.clone());
        Ok(report.meta)
    }

    fn load_fixed_meta(&self, symbol: &str, category: &str, name: &str) -> Result<Arc<SeriesMeta>> {
        let cache_key = series_cache_key(symbol, category, name);
        if let Some(meta) = self
            .manifest_cache
            .read()
            .expect("manifest cache poisoned")
            .get(&cache_key)
        {
            self.metrics.record_manifest_cache_hit();
            return Ok(meta.clone());
        }

        let started = Instant::now();
        let report = match self
            .catalog
            .load_series_meta_cached_report(symbol, category, name)
        {
            Some(report) => report,
            None => self
                .catalog
                .load_series_meta_report(symbol, category, name)?,
        };
        self.record_manifest_load_report(started.elapsed().as_nanos() as u64, &report.stats, true);
        self.manifest_cache
            .write()
            .expect("manifest cache poisoned")
            .insert(cache_key, report.meta.clone());
        Ok(report.meta)
    }

    fn register_fixed_series<R: FixedRecord>(
        &mut self,
        symbol: &str,
        category: &str,
        name: &str,
        timeframe_ms: i64,
        partition_policy: PartitionPolicy,
    ) -> Result<()> {
        if !partition_policy.unit.is_time_based() {
            return Err(FastKError::InvalidInput(format!(
                "partition unit {} is declared but not implemented yet",
                partition_policy.unit.as_str()
            )));
        }
        let _lock = self.begin_mutation()?;
        let series_dir = path::series_dir(&self.root, symbol, category, name);
        let meta_path = path::series_meta_path(&series_dir);
        if meta_path.exists() {
            let existing = self.load_fixed_meta(symbol, category, name)?;
            if existing.timeframe_ms != timeframe_ms {
                return Err(FastKError::InvalidInput(format!(
                    "series already exists with timeframe_ms={}",
                    existing.timeframe_ms
                )));
            }
            if existing.record_type != R::RECORD_TYPE
                || existing.schema_id != R::SCHEMA_ID
                || existing.record_size as usize != R::BYTE_SIZE
            {
                return Err(FastKError::InvalidData(format!(
                    "existing series {}:{}:{} does not match requested fixed record type",
                    symbol, category, name
                )));
            }
            if existing.chunk_unit != partition_policy.unit.as_str() {
                return Err(FastKError::InvalidInput(format!(
                    "series already exists with chunk_unit={}",
                    existing.chunk_unit
                )));
            }
            return Ok(());
        }

        let marker = recovery::RecoveryMarkerGuard::arm(&self.root)?;
        crate::storage::fs::ensure_dir(&path::chunks_dir(&series_dir))?;
        let meta =
            Catalog::build_fixed_meta::<R>(symbol, category, name, timeframe_ms, &partition_policy);
        manifest::save_series_meta(&series_dir, &meta)?;
        self.store_series_in_cache(meta);
        marker.commit()?;
        Ok(())
    }

    fn put_fixed_chunk<R: FixedRecord>(
        &mut self,
        symbol: &str,
        category: &str,
        name: &str,
        records: &[R],
    ) -> Result<()> {
        R::validate_strict_order(records)?;
        let _lock = self.begin_mutation()?;
        let mut meta = (*self.load_fixed_meta(symbol, category, name)?).clone();
        if meta.record_type != R::RECORD_TYPE
            || meta.schema_id != R::SCHEMA_ID
            || meta.record_size as usize != R::BYTE_SIZE
        {
            return Err(FastKError::InvalidData(format!(
                "series {}:{}:{} does not match requested fixed record type",
                symbol, category, name
            )));
        }
        let partition_unit = PartitionUnit::from_str(&meta.chunk_unit)?;
        if !partition_unit.is_time_based() {
            return Err(FastKError::InvalidInput(format!(
                "partition unit {} is not implemented for fixed record writes",
                meta.chunk_unit
            )));
        }

        let batches = split_fixed_records_by_partition(records, partition_unit)?;
        for batch in &batches {
            if let Some(chunk) = meta
                .chunk_indices_for_partition(&batch.partition_key)
                .iter()
                .map(|index| &meta.chunks[*index])
                .filter(|chunk| chunk.state.is_readable())
                .max_by_key(|chunk| chunk.end_ts)
            {
                if batch.records[0].ts() <= chunk.end_ts {
                    return Err(FastKError::InvalidInput(
                        "append overlaps existing partition chunk".into(),
                    ));
                }
            }
        }
        let series_dir = path::series_dir(&self.root, symbol, category, name);
        let marker = recovery::RecoveryMarkerGuard::arm(&self.root)?;
        crate::storage::fs::ensure_dir(&path::chunks_dir(&series_dir))?;
        for batch in batches {
            self.append_fixed_partition_locked::<R>(
                &series_dir,
                &mut meta,
                partition_unit,
                batch.partition_key,
                batch.records,
            )?;
        }
        self.catalog.save_series_meta(&meta)?;
        record_file_bytes_written(self.metrics.as_ref(), &path::series_meta_path(&series_dir));
        self.store_series_in_cache(meta);
        marker.commit()?;
        Ok(())
    }

    fn append_fixed_partition_locked<R: FixedRecord>(
        &self,
        series_dir: &Path,
        meta: &mut SeriesMeta,
        partition_unit: PartitionUnit,
        partition_key: String,
        records: &[R],
    ) -> Result<()> {
        let partition_indices = meta.chunk_indices_for_partition(&partition_key);
        let chunk_id = next_chunk_id(meta);
        let generation = partition_indices
            .iter()
            .map(|index| meta.chunks[*index].generation)
            .max()
            .unwrap_or(0)
            .saturating_add(1);
        let (file_name, state) = if partition_indices.is_empty() {
            (
                path::partition_chunk_file_name(&partition_key),
                ChunkState::Sealed,
            )
        } else {
            let last_chunk = &meta.chunks[*partition_indices
                .last()
                .expect("partition_indices not empty")];
            if records[0].ts() <= last_chunk.end_ts {
                return Err(FastKError::InvalidInput(format!(
                    "append overlaps existing partition chunk: {} <= {}",
                    records[0].ts(),
                    last_chunk.end_ts
                )));
            }
            (
                path::partition_delta_chunk_file_name(&partition_key, generation),
                ChunkState::Active,
            )
        };

        let relative_path = path::chunk_relative_path(&file_name);
        let chunk_path = path::resolve_relative_path(series_dir, &relative_path);
        let chunk_meta = fixed_writer::write_chunk::<R>(
            &chunk_path,
            meta,
            records,
            &fixed_writer::WriteFixedChunkOptions {
                chunk_id,
                generation,
                state,
                relative_path,
                sparse_index_every: crate::chunk::sparse_index::DEFAULT_SPARSE_INDEX_EVERY,
                partition_unit,
            },
        )?;
        manifest::upsert_chunk_meta(meta, chunk_meta.clone())?;
        if chunk_meta.state == ChunkState::Active {
            meta.active_chunk_id = Some(chunk_meta.chunk_id);
        }
        record_file_bytes_written(self.metrics.as_ref(), &chunk_path);
        Ok(())
    }

    fn get_fixed_range<R: FixedRecord>(
        &self,
        symbol: &str,
        category: &str,
        name: &str,
        start_ts: i64,
        end_ts: i64,
    ) -> Result<Vec<R>> {
        if start_ts > end_ts {
            return Ok(Vec::new());
        }
        let meta = self.load_fixed_meta(symbol, category, name)?;
        let series_dir = path::series_dir(&self.root, symbol, category, name);
        let chunk_range = meta.find_chunks_for_range(start_ts, end_ts);
        if chunk_range.is_empty() {
            return Ok(Vec::new());
        }
        let mut records = Vec::new();
        for chunk in &meta.chunks[chunk_range] {
            fixed_reader::append_range_in_chunk::<R>(
                &self.chunk_runtime,
                &series_dir,
                meta.series_id,
                chunk,
                start_ts,
                end_ts,
                &mut records,
            )?;
        }
        Ok(records)
    }

    fn get_fixed_latest_n<R: FixedRecord>(
        &self,
        symbol: &str,
        category: &str,
        name: &str,
        n: usize,
    ) -> Result<Vec<R>> {
        if n == 0 {
            return Ok(Vec::new());
        }
        let meta = self.load_fixed_meta(symbol, category, name)?;
        let series_dir = path::series_dir(&self.root, symbol, category, name);
        let mut remaining = n;
        let mut parts = Vec::new();
        for chunk in meta.chunks.iter().rev() {
            if remaining == 0 {
                break;
            }
            let take = remaining.min(chunk.count as usize);
            parts.push(fixed_reader::read_tail_in_chunk::<R>(
                &self.chunk_runtime,
                &series_dir,
                meta.series_id,
                chunk,
                take,
            )?);
            remaining -= take;
        }
        parts.reverse();
        Ok(parts.into_iter().flatten().collect())
    }

    fn replay_fixed<R: FixedRecord>(
        &self,
        symbol: &str,
        category: &str,
        name: &str,
        start_ts: i64,
        end_ts: Option<i64>,
    ) -> Result<ReplayCursor<R>> {
        let meta = self.load_fixed_meta(symbol, category, name)?;
        ReplayCursor::new(
            path::series_dir(&self.root, symbol, category, name),
            meta.as_ref(),
            ReplayOptions {
                start_ts,
                end_ts,
                batch_hint: None,
            },
        )
    }

    fn scan_fixed_sequence<R: SequencedRecord>(
        &self,
        symbol: &str,
        category: &str,
        name: &str,
        start_ts: i64,
        end_ts: i64,
    ) -> Result<SequenceScanReport> {
        if start_ts > end_ts {
            return Err(FastKError::InvalidInput(format!(
                "sequence scan start_ts {start_ts} is after end_ts {end_ts}"
            )));
        }

        let meta = self.load_fixed_meta(symbol, category, name)?;
        if meta.record_type != R::RECORD_TYPE
            || meta.schema_id != R::SCHEMA_ID
            || meta.record_size as usize != R::BYTE_SIZE
        {
            return Err(FastKError::InvalidData(format!(
                "series {}:{}:{} does not match requested sequence record type",
                symbol, category, name
            )));
        }

        let mut report = SequenceScanReport::new::<R>(symbol, category, name, start_ts, end_ts);
        let chunk_range = meta.find_chunks_for_range(start_ts, end_ts);
        if chunk_range.is_empty() {
            return Ok(report);
        }

        let series_dir = path::series_dir(&self.root, symbol, category, name);
        let mut previous = None;

        for chunk in &meta.chunks[chunk_range] {
            let mut records = Vec::new();
            fixed_reader::append_range_in_chunk::<R>(
                &self.chunk_runtime,
                &series_dir,
                meta.series_id,
                chunk,
                start_ts,
                end_ts,
                &mut records,
            )?;
            if records.is_empty() {
                continue;
            }

            report.scanned_chunk_count += 1;
            for record in records {
                let observation = SequenceObservation {
                    ts: record.ts(),
                    sequence: record.sequence_value(),
                    ordinal: report.scanned_record_count,
                };
                observe_sequence(&mut report, previous, observation);
                previous = Some(observation);
            }
        }

        Ok(report)
    }

    fn build_kline_attachment(&self, symbol: &str, timeframe: &str) -> Result<AttachedKlineSeries> {
        Ok(AttachedKlineSeries {
            symbol: symbol.to_string(),
            timeframe: timeframe.to_string(),
            series_dir: path::kline_series_dir(&self.root, symbol, timeframe),
            meta: self.load_kline_meta(symbol, timeframe)?,
        })
    }

    fn build_scalar_attachment(
        &self,
        series_key: &ScalarSeriesKey,
    ) -> Result<AttachedScalarSeries> {
        let meta = self.load_scalar_meta(series_key)?;
        Ok(AttachedScalarSeries {
            key: series_key.clone(),
            series_dir: path::scalar_series_dir(
                &self.root,
                &series_key.symbol,
                &series_key.category,
                &series_key.name,
            ),
            capabilities: Some(ScalarQueryCapabilities::from_meta(meta.as_ref())),
            meta,
        })
    }

    fn store_meta_in_cache(&self, meta: SeriesMeta) {
        self.store_series_in_cache(meta);
    }

    fn store_series_in_cache(&self, meta: SeriesMeta) {
        self.manifest_cache
            .write()
            .expect("manifest cache poisoned")
            .insert(
                series_cache_key(&meta.symbol, &meta.category, &meta.name),
                Arc::new(meta),
            );
    }

    fn record_manifest_load_report(
        &self,
        elapsed_ns: u64,
        stats: &manifest::ManifestLoadStats,
        counted_local_miss: bool,
    ) {
        if stats.shared_cache_hit {
            self.metrics.record_manifest_cache_hit();
        } else if counted_local_miss {
            self.metrics.record_manifest_cache_miss();
        }
        self.metrics.record_manifest_load_ns(elapsed_ns);
        self.metrics
            .record_manifest_file_read_ns(stats.file_read_ns);
        self.metrics.record_manifest_decode_ns(stats.decode_ns);
        self.metrics
            .record_manifest_chunk_materialize_ns(stats.chunk_materialize_ns);
        self.metrics
            .record_manifest_sidecar_materialize_ns(stats.sidecar_materialize_ns);
    }

    fn load_series_inventory_entry(&self, series_dir: &Path) -> Result<SeriesInventoryEntry> {
        let meta = manifest::load_series_meta_shared_mode(series_dir, self.read_only)?.meta;
        Ok(SeriesInventoryEntry {
            symbol: meta.symbol.clone(),
            category: meta.category.clone(),
            name: meta.name.clone(),
            record_type: meta.record_type,
            timeframe_ms: meta.timeframe_ms,
            chunk_count: meta.chunks.len(),
            record_count: meta.chunks.iter().map(|chunk| chunk.count).sum(),
            sidecar_count: meta.chunks.iter().map(|chunk| chunk.sidecars.len()).sum(),
            active_delta_count: meta
                .chunks
                .iter()
                .filter(|chunk| chunk.state == ChunkState::Active)
                .count(),
        })
    }

    fn merge_kline_month_locked(
        &mut self,
        symbol: &str,
        timeframe: &str,
        month_key: &str,
        meta: &mut SeriesMeta,
    ) -> Result<()> {
        let series_dir = path::kline_series_dir(&self.root, symbol, timeframe);
        let month_indices = meta.chunk_indices_for_month(month_key);
        if month_indices.len() <= 1 {
            return Ok(());
        }

        let source_chunks: Vec<_> = month_indices
            .iter()
            .map(|index| meta.chunks[*index].clone())
            .collect();
        let mut merged_records = Vec::new();
        for chunk in &source_chunks {
            kline_reader::append_range_in_chunk(
                &self.chunk_runtime,
                &series_dir,
                meta.series_id,
                chunk,
                chunk.start_ts,
                chunk.end_ts,
                &mut merged_records,
            )?;
        }
        KlineRecord::validate_strict_order(&merged_records)?;

        for index in &month_indices {
            let chunk = &meta.chunks[*index];
            if !can_transition(chunk.state, ChunkState::Merging) {
                return Err(FastKError::InvalidData(format!(
                    "chunk {} cannot transition from {:?} to {:?}",
                    chunk.relative_path,
                    chunk.state,
                    ChunkState::Merging
                )));
            }
        }
        for index in &month_indices {
            meta.chunks[*index].state = ChunkState::Merging;
        }
        self.catalog.save_kline_meta(meta)?;
        record_file_bytes_written(self.metrics.as_ref(), &path::series_meta_path(&series_dir));

        let next_generation = source_chunks
            .iter()
            .map(|chunk| chunk.generation)
            .max()
            .unwrap_or(0)
            .saturating_add(1);
        let new_chunk_id = next_chunk_id(meta);
        let file_name = path::month_merged_chunk_file_name(month_key, next_generation);
        let relative_path = path::chunk_relative_path(&file_name);
        let chunk_path = path::resolve_relative_path(&series_dir, &relative_path);
        let merged_chunk = kline_writer::write_chunk(
            &chunk_path,
            meta,
            &merged_records,
            &kline_writer::WriteKlineChunkOptions {
                chunk_id: new_chunk_id,
                generation: next_generation,
                state: merge_output_state(&source_chunks),
                relative_path,
                sparse_index_every: crate::chunk::sparse_index::DEFAULT_SPARSE_INDEX_EVERY,
            },
        )?;

        let removed = manifest::replace_month_chunks(meta, month_key, merged_chunk)?;
        self.catalog.save_kline_meta(meta)?;
        record_file_bytes_written(self.metrics.as_ref(), &chunk_path);
        record_file_bytes_written(self.metrics.as_ref(), &path::series_meta_path(&series_dir));
        for chunk in removed {
            let old_path = path::resolve_relative_path(&series_dir, &chunk.relative_path);
            let _ = std::fs::remove_file(old_path);
            self.chunk_runtime.invalidate(&chunk, meta.series_id);
            self.scalar_sidecars.invalidate(&chunk.relative_path);
        }
        self.store_meta_in_cache(meta.clone());
        Ok(())
    }

    fn find_scalar_timestamps(
        &self,
        series_key: &ScalarSeriesKey,
        predicate: &ScalarPredicate,
        start_ts: i64,
        end_ts: i64,
        strategy: ScalarQueryStrategy,
    ) -> Result<Vec<i64>> {
        if start_ts > end_ts {
            return Ok(Vec::new());
        }

        let meta = self.load_scalar_meta(series_key)?;
        let series_dir = path::scalar_series_dir(
            &self.root,
            &series_key.symbol,
            &series_key.category,
            &series_key.name,
        );
        self.find_scalar_timestamps_prepared(
            &series_dir,
            meta.as_ref(),
            series_key,
            predicate,
            start_ts,
            end_ts,
            strategy,
        )
    }

    fn query_scalar_via_zmap(
        &self,
        series_dir: &std::path::Path,
        chunk: &crate::types::ChunkMeta,
        predicate: &ScalarPredicate,
        start_ts: i64,
        end_ts: i64,
    ) -> Result<Vec<i64>> {
        let sidecar = load_sidecar_path(series_dir, chunk, "zmap")?;
        let entries = self.scalar_sidecars.get_zmap(
            SidecarCacheKey {
                generation: chunk.generation,
                checksum: chunk
                    .sidecars
                    .iter()
                    .find(|entry| entry.kind == "zmap")
                    .map_or(0, |entry| entry.checksum),
                relative_path: sidecar.to_string_lossy().into_owned(),
            },
            &sidecar,
        )?;
        let row_ranges = zmap::candidate_row_ranges(&entries, predicate, start_ts, end_ts)?;
        if row_ranges.is_empty() {
            return Ok(Vec::new());
        }

        let merged = merge_row_ranges(&row_ranges);
        let chunk_path = path::resolve_relative_path(series_dir, &chunk.relative_path);
        let records = scalar_reader::read_row_ranges_with_metrics(
            &chunk_path,
            &merged,
            self.metrics.as_ref(),
        )?;
        let mut timestamps = Vec::new();
        for record in records {
            if record.ts >= start_ts && record.ts <= end_ts && predicate.matches(record.value)? {
                timestamps.push(record.ts);
            }
        }
        timestamps.sort_unstable();
        timestamps.dedup();
        Ok(timestamps)
    }

    fn query_scalar_via_vix(
        &self,
        series_dir: &std::path::Path,
        series_key: &ScalarSeriesKey,
        chunk: &crate::types::ChunkMeta,
        predicate: &ScalarPredicate,
        start_ts: i64,
        end_ts: i64,
    ) -> Result<Vec<i64>> {
        let engine = PredicateQueryEngine::new();
        let sidecar = load_sidecar_path(series_dir, chunk, "vix")?;
        let entries = self.scalar_sidecars.get_vix(
            SidecarCacheKey {
                generation: chunk.generation,
                checksum: chunk
                    .sidecars
                    .iter()
                    .find(|entry| entry.kind == "vix")
                    .map_or(0, |entry| entry.checksum),
                relative_path: sidecar.to_string_lossy().into_owned(),
            },
            &sidecar,
        )?;
        engine.find_timestamps_via_vix(series_key, &entries, predicate, start_ts, end_ts)
    }

    fn query_scalar_via_raw_scan(
        &self,
        series_dir: &std::path::Path,
        series_id: u64,
        chunk: &crate::types::ChunkMeta,
        predicate: &ScalarPredicate,
        start_ts: i64,
        end_ts: i64,
    ) -> Result<Vec<i64>> {
        let mut records = Vec::with_capacity((chunk.count as usize).min(2_048));
        let _ = scalar_reader::append_range_in_chunk(
            &self.chunk_runtime,
            series_dir,
            series_id,
            chunk,
            start_ts,
            end_ts,
            &mut records,
        )?;
        let mut timestamps = Vec::new();
        for record in records {
            if record.ts >= start_ts && record.ts <= end_ts && predicate.matches(record.value)? {
                timestamps.push(record.ts);
            }
        }
        Ok(timestamps)
    }

    fn query_scalar_predicate_via_zmap(
        &self,
        series_dir: &std::path::Path,
        chunk: &crate::types::ChunkMeta,
        query: &ScalarPredicateQuery,
        matches: &mut Vec<ScalarPredicateMatch>,
        stats: &mut ScalarPredicateQueryStats,
    ) -> Result<()> {
        let sidecar = load_sidecar_path(series_dir, chunk, "zmap")?;
        let entries = self.scalar_sidecars.get_zmap(
            SidecarCacheKey {
                generation: chunk.generation,
                checksum: chunk
                    .sidecars
                    .iter()
                    .find(|entry| entry.kind == "zmap")
                    .map_or(0, |entry| entry.checksum),
                relative_path: sidecar.to_string_lossy().into_owned(),
            },
            &sidecar,
        )?;
        let blocks_considered = entries
            .iter()
            .filter(|entry| entry.end_ts >= query.start_ts && entry.start_ts <= query.end_ts)
            .count() as u64;
        stats.blocks_considered = stats.blocks_considered.saturating_add(blocks_considered);

        let row_ranges = zmap::candidate_row_ranges_for_expr(
            &entries,
            &query.predicate,
            query.start_ts,
            query.end_ts,
        )?;
        stats.blocks_pruned = stats
            .blocks_pruned
            .saturating_add(blocks_considered.saturating_sub(row_ranges.len() as u64));

        if row_ranges.is_empty() {
            stats.chunks_pruned = stats.chunks_pruned.saturating_add(1);
            return Ok(());
        }

        let merged = merge_row_ranges(&row_ranges);
        stats.blocks_scanned = stats.blocks_scanned.saturating_add(merged.len() as u64);
        stats.chunks_scanned = stats.chunks_scanned.saturating_add(1);

        let chunk_path = path::resolve_relative_path(series_dir, &chunk.relative_path);
        let records = scalar_reader::read_row_ranges_with_metrics(
            &chunk_path,
            &merged,
            self.metrics.as_ref(),
        )?;
        stats.rows_checked = stats.rows_checked.saturating_add(records.len() as u64);

        for record in records {
            if record.ts >= query.start_ts
                && record.ts <= query.end_ts
                && query.predicate.matches(record.value)
            {
                matches.push(ScalarPredicateMatch {
                    ts: record.ts,
                    value: query.return_values.then_some(record.value),
                });
            }
        }

        Ok(())
    }

    fn query_scalar_predicate_via_vix(
        &self,
        series_dir: &std::path::Path,
        chunk: &crate::types::ChunkMeta,
        query: &ScalarPredicateQuery,
        matches: &mut Vec<ScalarPredicateMatch>,
        stats: &mut ScalarPredicateQueryStats,
    ) -> Result<()> {
        let sidecar = load_sidecar_path(series_dir, chunk, "vix")?;
        let entries = self.scalar_sidecars.get_vix(
            SidecarCacheKey {
                generation: chunk.generation,
                checksum: chunk
                    .sidecars
                    .iter()
                    .find(|entry| entry.kind == "vix")
                    .map_or(0, |entry| entry.checksum),
                relative_path: sidecar.to_string_lossy().into_owned(),
            },
            &sidecar,
        )?;
        let slices = vix::matching_slices_for_expr(&entries, &query.predicate)?;
        if slices.is_empty() {
            stats.chunks_pruned = stats.chunks_pruned.saturating_add(1);
            return Ok(());
        }

        stats.chunks_scanned = stats.chunks_scanned.saturating_add(1);
        for slice in slices {
            stats.rows_checked = stats.rows_checked.saturating_add(slice.len() as u64);
            for entry in &entries[slice] {
                if entry.ts >= query.start_ts
                    && entry.ts <= query.end_ts
                    && query.predicate.matches(entry.value)
                {
                    matches.push(ScalarPredicateMatch {
                        ts: entry.ts,
                        value: query.return_values.then_some(entry.value),
                    });
                }
            }
        }

        Ok(())
    }

    fn query_scalar_predicate_via_raw_scan(
        &self,
        series_dir: &std::path::Path,
        series_id: u64,
        chunk: &crate::types::ChunkMeta,
        query: &ScalarPredicateQuery,
        matches: &mut Vec<ScalarPredicateMatch>,
        stats: &mut ScalarPredicateQueryStats,
    ) -> Result<()> {
        let mut records = Vec::with_capacity((chunk.count as usize).min(2_048));
        let _ = scalar_reader::append_range_in_chunk(
            &self.chunk_runtime,
            series_dir,
            series_id,
            chunk,
            query.start_ts,
            query.end_ts,
            &mut records,
        )?;
        if records.is_empty() {
            return Ok(());
        }

        stats.chunks_scanned = stats.chunks_scanned.saturating_add(1);
        stats.rows_checked = stats.rows_checked.saturating_add(records.len() as u64);
        for record in records {
            if query.predicate.matches(record.value) {
                matches.push(ScalarPredicateMatch {
                    ts: record.ts,
                    value: query.return_values.then_some(record.value),
                });
            }
        }

        Ok(())
    }
}

impl<'a> FastKReadSession<'a> {
    pub(crate) fn store(&self) -> &'a FastKStore {
        self.store
    }

    pub fn attach_kline_series(&mut self, symbol: &str, timeframe: &str) -> Result<&mut Self> {
        let started = Instant::now();
        if self
            .attached_kline
            .iter()
            .any(|entry| entry.symbol == symbol && entry.timeframe == timeframe)
        {
            self.store
                .metrics
                .record_session_attach_ns(started.elapsed().as_nanos() as u64);
            return Ok(self);
        }
        self.attached_kline
            .push(self.store.build_kline_attachment(symbol, timeframe)?);
        self.store
            .metrics
            .record_session_attach_ns(started.elapsed().as_nanos() as u64);
        Ok(self)
    }

    pub fn attach_scalar_series(&mut self, series_key: &ScalarSeriesKey) -> Result<&mut Self> {
        let started = Instant::now();
        if self
            .attached_scalar
            .iter()
            .any(|entry| entry.key == *series_key)
        {
            self.store
                .metrics
                .record_session_attach_ns(started.elapsed().as_nanos() as u64);
            return Ok(self);
        }
        self.attached_scalar
            .push(self.store.build_scalar_attachment(series_key)?);
        self.store
            .metrics
            .record_session_attach_ns(started.elapsed().as_nanos() as u64);
        Ok(self)
    }

    pub fn attach_indicator_series(
        &mut self,
        symbol: &str,
        timeframe: &str,
        indicator_name: &str,
    ) -> Result<&mut Self> {
        let key = indicator_series_key(symbol, timeframe, indicator_name);
        self.attach_scalar_series(&key)
    }

    pub fn attach_kline_many<I, S>(&mut self, series: I) -> Result<&mut Self>
    where
        I: IntoIterator<Item = (S, S)>,
        S: AsRef<str>,
    {
        for (symbol, timeframe) in series {
            self.attach_kline_series(symbol.as_ref(), timeframe.as_ref())?;
        }
        Ok(self)
    }

    pub fn attach_scalar_many<I>(&mut self, series: I) -> Result<&mut Self>
    where
        I: IntoIterator<Item = ScalarSeriesKey>,
    {
        for series_key in series {
            self.attach_scalar_series(&series_key)?;
        }
        Ok(self)
    }

    pub fn prewarm(&self) -> Result<()> {
        let started = Instant::now();
        for attached in &self.attached_kline {
            if let Some(first) = attached.meta.chunks.first() {
                let _ = self.store.chunk_runtime.get_layout(
                    first,
                    attached.meta.series_id,
                    &attached.series_dir,
                )?;
            }
            if let Some(last) = attached.meta.chunks.last() {
                let _ = self.store.chunk_runtime.get_layout(
                    last,
                    attached.meta.series_id,
                    &attached.series_dir,
                )?;
            }
        }
        for attached in &self.attached_scalar {
            for chunk in attached
                .meta
                .chunks
                .iter()
                .take(1)
                .chain(attached.meta.chunks.iter().rev().take(1))
            {
                let _ = self.store.chunk_runtime.get_layout(
                    chunk,
                    attached.meta.series_id,
                    &attached.series_dir,
                )?;
                if chunk.sidecars.iter().any(|sidecar| sidecar.kind == "zmap") {
                    let _ = load_sidecar_path(&attached.series_dir, chunk, "zmap").and_then(
                        |path_buf| {
                            self.store.scalar_sidecars.get_zmap(
                                SidecarCacheKey {
                                    generation: chunk.generation,
                                    checksum: chunk
                                        .sidecars
                                        .iter()
                                        .find(|entry| entry.kind == "zmap")
                                        .map_or(0, |entry| entry.checksum),
                                    relative_path: path_buf.to_string_lossy().into_owned(),
                                },
                                &path_buf,
                            )
                        },
                    )?;
                }
                if chunk.sidecars.iter().any(|sidecar| sidecar.kind == "vix") {
                    let _ = load_sidecar_path(&attached.series_dir, chunk, "vix").and_then(
                        |path_buf| {
                            self.store.scalar_sidecars.get_vix(
                                SidecarCacheKey {
                                    generation: chunk.generation,
                                    checksum: chunk
                                        .sidecars
                                        .iter()
                                        .find(|entry| entry.kind == "vix")
                                        .map_or(0, |entry| entry.checksum),
                                    relative_path: path_buf.to_string_lossy().into_owned(),
                                },
                                &path_buf,
                            )
                        },
                    )?;
                }
            }
        }
        self.store
            .metrics
            .record_session_prewarm_ns(started.elapsed().as_nanos() as u64);
        Ok(())
    }

    pub fn attached_inventory(&self) -> Result<Vec<SeriesInventoryEntry>> {
        let mut out = Vec::new();
        for attached in &self.attached_kline {
            out.push(
                self.store
                    .load_series_inventory_entry(&attached.series_dir)?,
            );
        }
        for attached in &self.attached_scalar {
            out.push(
                self.store
                    .load_series_inventory_entry(&attached.series_dir)?,
            );
        }
        Ok(out)
    }

    pub fn attached_health_summary(&self) -> Result<StoreHealthSummary> {
        let validations = self.store.validate_manifest_vs_fs()?;
        let attached_dirs = self.attached_series_dirs();
        let scoped: Vec<_> = validations
            .into_iter()
            .filter(|report| attached_dirs.contains(&report.series_dir))
            .collect();
        let orphans = self.store.list_orphans()?;
        let orphan_count = orphans
            .into_iter()
            .filter(|artifact| attached_dirs.contains(&artifact.series_dir))
            .count();
        let clean_series_count = scoped.iter().filter(|report| report.is_clean()).count();
        let overlap_group_count = scoped
            .iter()
            .map(|report| report.overlap_groups.len())
            .sum();
        Ok(StoreHealthSummary {
            series_count: scoped.len(),
            clean_series_count,
            issue_series_count: scoped.len().saturating_sub(clean_series_count),
            orphan_count,
            overlap_group_count,
            pending_recovery: recovery::has_pending_recovery(&self.store.root)?,
            platform_fsync_guarantee: if cfg!(windows) {
                "best-effort-parent-dir-fsync"
            } else {
                "fsync-parent-dir"
            },
        })
    }

    pub fn attached_scalar_capabilities(
        &self,
    ) -> Result<Vec<(ScalarSeriesKey, ScalarQueryCapabilities)>> {
        let mut out = Vec::with_capacity(self.attached_scalar.len());
        for attached in &self.attached_scalar {
            let capabilities = attached
                .capabilities
                .clone()
                .unwrap_or_else(|| ScalarQueryCapabilities::from_meta(attached.meta.as_ref()));
            out.push((attached.key.clone(), capabilities));
        }
        Ok(out)
    }

    pub fn cache_summary(&self) -> SessionCacheSummary {
        SessionCacheSummary {
            attached_kline_count: self.attached_kline.len(),
            attached_scalar_count: self.attached_scalar.len(),
            metrics_level: self.store.metrics_level(),
            metrics: self.store.metrics_snapshot(),
        }
    }

    pub fn clear_caches(&self) {
        self.store.clear_runtime_caches();
    }

    pub fn reset(&mut self, mode: SessionResetMode) {
        match mode {
            SessionResetMode::QueryOnly => self.store.clear_scalar_query_caches(),
            SessionResetMode::Logical => self.store.clear_runtime_caches(),
            SessionResetMode::FullDetach => {
                self.store.clear_read_caches();
                self.attached_kline.clear();
                self.attached_scalar.clear();
            }
        }
    }

    pub fn get_kline_at(
        &self,
        symbol: &str,
        timeframe: &str,
        ts: i64,
    ) -> Result<Option<KlineRecord>> {
        if let Some(attached) = self.find_attached_kline(symbol, timeframe) {
            return self.store.get_kline_at_prepared(
                &attached.series_dir,
                attached.meta.as_ref(),
                ts,
            );
        }
        self.store.get_kline_at(symbol, timeframe, ts)
    }

    pub fn get_kline_range(
        &self,
        symbol: &str,
        timeframe: &str,
        start_ts: i64,
        end_ts: i64,
    ) -> Result<Vec<KlineRecord>> {
        if let Some(attached) = self.find_attached_kline(symbol, timeframe) {
            return self.store.get_kline_range_prepared(
                &attached.series_dir,
                attached.meta.as_ref(),
                start_ts,
                end_ts,
            );
        }
        self.store
            .get_kline_range(symbol, timeframe, start_ts, end_ts)
    }

    pub fn get_kline_latest_n(
        &self,
        symbol: &str,
        timeframe: &str,
        n: usize,
    ) -> Result<Vec<KlineRecord>> {
        if let Some(attached) = self.find_attached_kline(symbol, timeframe) {
            return self.store.get_kline_latest_n_prepared(
                &attached.series_dir,
                attached.meta.as_ref(),
                n,
            );
        }
        self.store.get_kline_latest_n(symbol, timeframe, n)
    }

    pub fn get_scalar_at(
        &self,
        series_key: &ScalarSeriesKey,
        ts: i64,
    ) -> Result<Option<ScalarRecord>> {
        if let Some(attached) = self.find_attached_scalar(series_key) {
            return self.store.get_scalar_at_prepared(
                &attached.series_dir,
                attached.meta.as_ref(),
                ts,
            );
        }
        self.store.get_scalar_at(series_key, ts)
    }

    pub fn get_scalar_range(
        &self,
        series_key: &ScalarSeriesKey,
        start_ts: i64,
        end_ts: i64,
    ) -> Result<Vec<ScalarRecord>> {
        if let Some(attached) = self.find_attached_scalar(series_key) {
            return self.store.get_scalar_range_prepared(
                &attached.series_dir,
                attached.meta.as_ref(),
                start_ts,
                end_ts,
            );
        }
        self.store.get_scalar_range(series_key, start_ts, end_ts)
    }

    pub fn get_indicator_at(
        &self,
        symbol: &str,
        timeframe: &str,
        indicator_name: &str,
        ts: i64,
    ) -> Result<Option<ScalarRecord>> {
        let key = indicator_series_key(symbol, timeframe, indicator_name);
        self.get_scalar_at(&key, ts)
    }

    pub fn get_indicator_range(
        &self,
        symbol: &str,
        timeframe: &str,
        indicator_name: &str,
        start_ts: i64,
        end_ts: i64,
    ) -> Result<Vec<ScalarRecord>> {
        let key = indicator_series_key(symbol, timeframe, indicator_name);
        self.get_scalar_range(&key, start_ts, end_ts)
    }

    pub fn query_scalar_predicate(
        &self,
        query: ScalarPredicateQuery,
    ) -> Result<ScalarPredicateQueryResult> {
        validate_scalar_predicate_query(&query)?;
        if query.predicate.is_impossible() {
            return Ok(ScalarPredicateQueryResult {
                matches: Vec::new(),
                stats: ScalarPredicateQueryStats::default(),
            });
        }
        if let Some(attached) = self.find_attached_scalar(&query.key) {
            return self.store.query_scalar_predicate_prepared(
                &attached.series_dir,
                attached.meta.as_ref(),
                &query,
            );
        }
        self.store.query_scalar_predicate(query)
    }

    pub fn find_scalar_timestamps_by_predicate(
        &self,
        key: &ScalarSeriesKey,
        start_ts: i64,
        end_ts: i64,
        predicate: ScalarPredicateExpr,
    ) -> Result<Vec<i64>> {
        let result = self.query_scalar_predicate(ScalarPredicateQuery {
            key: key.clone(),
            start_ts,
            end_ts,
            predicate,
            return_values: false,
        })?;
        Ok(result.matches.into_iter().map(|entry| entry.ts).collect())
    }

    pub fn find_scalar_points_by_predicate(
        &self,
        key: &ScalarSeriesKey,
        start_ts: i64,
        end_ts: i64,
        predicate: ScalarPredicateExpr,
    ) -> Result<Vec<ScalarRecord>> {
        let result = self.query_scalar_predicate(ScalarPredicateQuery {
            key: key.clone(),
            start_ts,
            end_ts,
            predicate,
            return_values: true,
        })?;
        Ok(result
            .matches
            .into_iter()
            .filter_map(|entry| {
                entry.value.map(|value| ScalarRecord {
                    ts: entry.ts,
                    value,
                })
            })
            .collect())
    }

    pub fn find_scalar_timestamps_via_zmap(
        &self,
        series_key: &ScalarSeriesKey,
        predicate: &ScalarPredicate,
        start_ts: i64,
        end_ts: i64,
    ) -> Result<Vec<i64>> {
        if let Some(attached) = self.find_attached_scalar(series_key) {
            return self.store.find_scalar_timestamps_prepared(
                &attached.series_dir,
                attached.meta.as_ref(),
                &attached.key,
                predicate,
                start_ts,
                end_ts,
                ScalarQueryStrategy::Zmap,
            );
        }
        self.store
            .find_scalar_timestamps_via_zmap(series_key, predicate, start_ts, end_ts)
    }

    pub fn find_scalar_timestamps_via_vix(
        &self,
        series_key: &ScalarSeriesKey,
        predicate: &ScalarPredicate,
        start_ts: i64,
        end_ts: i64,
    ) -> Result<Vec<i64>> {
        if let Some(attached) = self.find_attached_scalar(series_key) {
            return self.store.find_scalar_timestamps_prepared(
                &attached.series_dir,
                attached.meta.as_ref(),
                &attached.key,
                predicate,
                start_ts,
                end_ts,
                ScalarQueryStrategy::Vix,
            );
        }
        self.store
            .find_scalar_timestamps_via_vix(series_key, predicate, start_ts, end_ts)
    }

    pub fn find_scalar_timestamps_raw(
        &self,
        series_key: &ScalarSeriesKey,
        predicate: &ScalarPredicate,
        start_ts: i64,
        end_ts: i64,
    ) -> Result<Vec<i64>> {
        if let Some(attached) = self.find_attached_scalar(series_key) {
            return self.store.find_scalar_timestamps_prepared(
                &attached.series_dir,
                attached.meta.as_ref(),
                &attached.key,
                predicate,
                start_ts,
                end_ts,
                ScalarQueryStrategy::Raw,
            );
        }
        self.store
            .find_scalar_timestamps_raw(series_key, predicate, start_ts, end_ts)
    }

    fn attached_series_dirs(&self) -> Vec<PathBuf> {
        let mut out = Vec::with_capacity(self.attached_kline.len() + self.attached_scalar.len());
        for attached in &self.attached_kline {
            out.push(attached.series_dir.clone());
        }
        for attached in &self.attached_scalar {
            out.push(attached.series_dir.clone());
        }
        out
    }

    fn find_attached_kline(&self, symbol: &str, timeframe: &str) -> Option<&AttachedKlineSeries> {
        self.attached_kline
            .iter()
            .find(|entry| entry.symbol == symbol && entry.timeframe == timeframe)
    }

    fn find_attached_scalar(&self, series_key: &ScalarSeriesKey) -> Option<&AttachedScalarSeries> {
        self.attached_scalar
            .iter()
            .find(|entry| entry.key == *series_key)
    }
}

fn series_cache_key(symbol: &str, category: &str, name: &str) -> String {
    format!("{symbol}:{category}:{name}")
}

fn month_inventory(meta: &SeriesMeta) -> Vec<MonthInventoryEntry> {
    let mut months: Vec<MonthInventoryEntry> = Vec::new();
    for chunk in &meta.chunks {
        match months.last_mut() {
            Some(entry) if entry.month_key == chunk.month_key => {
                entry.chunk_count += 1;
                entry.record_count = entry.record_count.saturating_add(chunk.count);
                entry.sidecar_count += chunk.sidecars.len();
                match chunk.state {
                    ChunkState::Active => entry.active_delta_count += 1,
                    ChunkState::Sealed => entry.sealed_chunk_count += 1,
                    _ => {}
                }
            }
            _ => months.push(MonthInventoryEntry {
                month_key: chunk.month_key.clone(),
                chunk_count: 1,
                record_count: chunk.count,
                sealed_chunk_count: usize::from(chunk.state == ChunkState::Sealed),
                active_delta_count: usize::from(chunk.state == ChunkState::Active),
                sidecar_count: chunk.sidecars.len(),
            }),
        }
    }
    months
}

fn record_file_bytes_written(metrics: &StoreMetrics, path: &Path) {
    if let Ok(metadata) = std::fs::metadata(path) {
        metrics.record_logical_bytes_written(metadata.len() as usize);
    }
}

fn validate_existing_series(
    existing: &SeriesMeta,
    timeframe_ms: i64,
    price_scale: i64,
    volume_scale: i64,
) -> Result<()> {
    if existing.timeframe_ms != timeframe_ms
        || existing.price_scale != price_scale
        || existing.volume_scale != volume_scale
    {
        return Err(FastKError::InvalidInput(format!(
            "series already exists with different configuration: timeframe_ms={}, price_scale={}, volume_scale={}",
            existing.timeframe_ms, existing.price_scale, existing.volume_scale
        )));
    }
    Ok(())
}

fn next_chunk_id(meta: &SeriesMeta) -> u64 {
    meta.chunks
        .iter()
        .map(|chunk| chunk.chunk_id)
        .max()
        .unwrap_or(0)
        .saturating_add(1)
}

struct FixedPartitionBatch<'a, R> {
    partition_key: String,
    records: &'a [R],
}

fn split_fixed_records_by_partition<R: FixedRecord>(
    records: &[R],
    partition_unit: PartitionUnit,
) -> Result<Vec<FixedPartitionBatch<'_, R>>> {
    if records.is_empty() {
        return Err(FastKError::InvalidInput(
            "records must not be empty".to_string(),
        ));
    }

    let mut batches = Vec::new();
    let mut start = 0usize;
    let mut current = path::partition_key(records[0].ts(), partition_unit)?;
    for (index, record) in records.iter().enumerate().skip(1) {
        let key = path::partition_key(record.ts(), partition_unit)?;
        if key != current {
            batches.push(FixedPartitionBatch {
                partition_key: current,
                records: &records[start..index],
            });
            start = index;
            current = key;
        }
    }
    batches.push(FixedPartitionBatch {
        partition_key: current,
        records: &records[start..],
    });
    Ok(batches)
}

fn estimate_short_range_rows(timeframe_ms: i64, start_ts: i64, end_ts: i64) -> usize {
    if timeframe_ms <= 0 {
        return SHORT_RANGE_MAX_RECORDS as usize;
    }
    ((end_ts.saturating_sub(start_ts) / timeframe_ms).saturating_add(1))
        .clamp(0, SHORT_RANGE_MAX_RECORDS) as usize
}

fn build_scalar_sidecars(
    series_dir: &std::path::Path,
    chunk_meta: &crate::types::ChunkMeta,
    records: &[ScalarRecord],
    zmap_block_size: usize,
) -> Result<Vec<SidecarMeta>> {
    let mut sidecars = Vec::new();

    let zmap_entries = zmap::build_entries(records, zmap_block_size.max(1))?;
    let zmap_relative = path::sidecar_relative_path(&chunk_meta.relative_path, "zmap");
    let zmap_path = path::resolve_relative_path(series_dir, &zmap_relative);
    zmap::write_entries(&zmap_path, &zmap_entries)?;
    let zmap_bytes = std::fs::read(&zmap_path)?;
    sidecars.push(SidecarMeta {
        kind: "zmap".to_string(),
        relative_path: zmap_relative,
        generation: chunk_meta.generation,
        checksum: crate::storage::fs::checksum64(&zmap_bytes),
        block_size: zmap_block_size.max(1) as u32,
        record_count: zmap_entries.len() as u64,
    });

    let vix_entries = vix::build_entries(records);
    let vix_relative = path::sidecar_relative_path(&chunk_meta.relative_path, "vix");
    let vix_path = path::resolve_relative_path(series_dir, &vix_relative);
    vix::write_entries(&vix_path, &vix_entries)?;
    let vix_bytes = std::fs::read(&vix_path)?;
    sidecars.push(SidecarMeta {
        kind: "vix".to_string(),
        relative_path: vix_relative,
        generation: chunk_meta.generation,
        checksum: crate::storage::fs::checksum64(&vix_bytes),
        block_size: 0,
        record_count: vix_entries.len() as u64,
    });

    Ok(sidecars)
}

fn load_sidecar_path(
    series_dir: &std::path::Path,
    chunk: &crate::types::ChunkMeta,
    kind: &str,
) -> Result<PathBuf> {
    let sidecar = chunk
        .sidecars
        .iter()
        .find(|sidecar| sidecar.kind == kind)
        .ok_or_else(|| {
            FastKError::NotFound(format!(
                "missing {kind} sidecar for chunk {}",
                chunk.relative_path
            ))
        })?;
    Ok(path::resolve_relative_path(
        series_dir,
        &sidecar.relative_path,
    ))
}

fn has_sidecar_kind(chunk: &crate::types::ChunkMeta, kind: &str) -> bool {
    chunk.sidecars.iter().any(|sidecar| sidecar.kind == kind)
}

fn validate_scalar_predicate_query(query: &ScalarPredicateQuery) -> Result<()> {
    if query.key.symbol.trim().is_empty()
        || query.key.category.trim().is_empty()
        || query.key.name.trim().is_empty()
    {
        return Err(FastKError::InvalidInput(
            "scalar predicate query key fields must not be empty".to_string(),
        ));
    }
    if query.start_ts > query.end_ts {
        return Err(FastKError::InvalidInput(format!(
            "scalar predicate query start_ts {} must be <= end_ts {}",
            query.start_ts, query.end_ts
        )));
    }
    query.predicate.validate()
}

fn mark_index_used(stats: &mut ScalarPredicateQueryStats, kind: ScalarIndexKind) {
    stats.index_used = match (stats.index_used, kind) {
        (None, next) => Some(next),
        (Some(ScalarIndexKind::FullScan), next) => Some(next),
        (Some(existing), ScalarIndexKind::FullScan) => Some(existing),
        (Some(ScalarIndexKind::ZoneMap), ScalarIndexKind::ValueIndex)
        | (Some(ScalarIndexKind::ValueIndex), ScalarIndexKind::ZoneMap)
        | (Some(ScalarIndexKind::ZoneMapAndValueIndex), _) => {
            Some(ScalarIndexKind::ZoneMapAndValueIndex)
        }
        (Some(existing), next) if existing == next => Some(existing),
        (Some(_), _) => Some(ScalarIndexKind::ZoneMapAndValueIndex),
    };
}

fn remove_sidecars_for_chunks(series_dir: &std::path::Path, chunks: &[crate::types::ChunkMeta]) {
    for chunk in chunks {
        let chunk_path = path::resolve_relative_path(series_dir, &chunk.relative_path);
        let _ = std::fs::remove_file(chunk_path);
        for sidecar in &chunk.sidecars {
            let sidecar_path = path::resolve_relative_path(series_dir, &sidecar.relative_path);
            let _ = std::fs::remove_file(sidecar_path);
        }
    }
}

fn merge_row_ranges(ranges: &[RangeInclusive<u32>]) -> Vec<RangeInclusive<u32>> {
    if ranges.is_empty() {
        return Vec::new();
    }

    let mut sorted = ranges.to_vec();
    sorted.sort_by_key(|range| *range.start());
    let mut merged = Vec::with_capacity(sorted.len());
    let mut current_start = *sorted[0].start();
    let mut current_end = *sorted[0].end();

    for range in sorted.into_iter().skip(1) {
        if *range.start() <= current_end.saturating_add(1) {
            current_end = current_end.max(*range.end());
        } else {
            merged.push(current_start..=current_end);
            current_start = *range.start();
            current_end = *range.end();
        }
    }
    merged.push(current_start..=current_end);
    merged
}

#[cfg(test)]
#[path = "store_tests.rs"]
mod tests;
