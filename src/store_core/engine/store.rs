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
use crate::storage::{lock::StoreWriteLock, manifest, path, recovery};
use crate::types::{
    BboRecord, BookDeltaRecord, ChunkState, FixedRecord, KlineRecord, PartitionPolicy,
    PartitionUnit, ScalarIndexKind, ScalarPredicate, ScalarPredicateExpr, ScalarPredicateMatch,
    ScalarPredicateQuery, ScalarPredicateQueryResult, ScalarPredicateQueryStats, ScalarRecord,
    ScalarSeriesKey, SeriesMeta, SidecarMeta, TradeRecord,
};

const SHORT_RANGE_MAX_RECORDS: i64 = 2_048;
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
}

impl FastKStore {
    /// Opens a store rooted at `root`.
    pub fn open(root: impl Into<PathBuf>) -> Result<Self> {
        let open_started = Instant::now();
        let root = root.into();
        if recovery::has_pending_recovery(&root)? {
            let _ = recovery::startup_recover(&root)?;
        }
        let chunk_runtime = ChunkRuntime::default();
        let metrics = chunk_runtime.metrics();
        let store = Self {
            catalog: Catalog::new(root.clone()),
            root,
            manifest_cache: RwLock::new(HashMap::new()),
            scalar_sidecars: ScalarSidecarRuntime::new(metrics.clone()),
            chunk_runtime,
            metrics,
            compaction_policy: CompactionPolicy::default(),
        };
        store
            .metrics
            .record_store_bootstrap_ns(open_started.elapsed().as_nanos() as u64);
        Ok(store)
    }

    /// Creates the base directory structure and removes leftover temp artifacts.
    pub fn init(&mut self) -> Result<()> {
        self.catalog.init()?;
        let _ = recovery::startup_recover(&self.root)?;
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
        let _lock = StoreWriteLock::acquire(&self.root)?;
        let marker = recovery::RecoveryMarkerGuard::arm(&self.root)?;
        let series_dir = path::kline_series_dir(&self.root, symbol, timeframe);
        let chunks_dir = path::chunks_dir(&series_dir);
        crate::storage::fs::ensure_dir(&chunks_dir)?;

        let meta_path = path::series_meta_path(&series_dir);
        if meta_path.exists() {
            let existing = self.load_kline_meta(symbol, timeframe)?;
            validate_existing_series(&existing, timeframe_ms, price_scale, volume_scale)?;
            marker.commit()?;
            return Ok(());
        }

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
        let _lock = StoreWriteLock::acquire(&self.root)?;
        let marker = recovery::RecoveryMarkerGuard::arm(&self.root)?;
        let mut meta = (*self.load_kline_meta(symbol, timeframe)?).clone();
        KlineRecord::validate_strict_order(records)?;
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

        manifest::upsert_chunk_meta(&mut meta, chunk_meta.clone())?;
        if chunk_meta.state == ChunkState::Active {
            meta.active_chunk_id = Some(chunk_meta.chunk_id);
        }
        self.catalog.save_kline_meta(&meta)?;
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
        let _lock = StoreWriteLock::acquire(&self.root)?;
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
        let _lock = StoreWriteLock::acquire(&self.root)?;
        let report = recovery::startup_recover(&self.root)?;
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
        let _lock = StoreWriteLock::acquire(&self.root)?;
        let marker = recovery::RecoveryMarkerGuard::arm(&self.root)?;
        let series_dir = path::scalar_series_dir(
            &self.root,
            &series_key.symbol,
            &series_key.category,
            &series_key.name,
        );
        crate::storage::fs::ensure_dir(&path::chunks_dir(&series_dir))?;

        let meta_path = path::series_meta_path(&series_dir);
        if meta_path.exists() {
            let existing = self.load_scalar_meta(series_key)?;
            if existing.timeframe_ms != timeframe_ms {
                return Err(FastKError::InvalidInput(format!(
                    "scalar series already exists with timeframe_ms={}",
                    existing.timeframe_ms
                )));
            }
            marker.commit()?;
            return Ok(());
        }

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
        let _lock = StoreWriteLock::acquire(&self.root)?;
        let marker = recovery::RecoveryMarkerGuard::arm(&self.root)?;
        let series_dir = path::scalar_series_dir(
            &self.root,
            &series_key.symbol,
            &series_key.category,
            &series_key.name,
        );
        crate::storage::fs::ensure_dir(&path::chunks_dir(&series_dir))?;

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

        ScalarRecord::validate_strict_order(records)?;
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
        chunk_meta.sidecars =
            build_scalar_sidecars(&series_dir, &chunk_meta, records, zmap_block_size)?;

        let written_sidecars = chunk_meta.sidecars.clone();
        if month_indices.is_empty() {
            manifest::upsert_chunk_meta(&mut meta, chunk_meta)?;
        } else {
            let removed = manifest::replace_month_chunks(&mut meta, &month_key, chunk_meta)?;
            for chunk in &removed {
                self.scalar_sidecars.invalidate(&chunk.relative_path);
            }
            remove_sidecars_for_chunks(&series_dir, &removed);
        }
        self.catalog.save_scalar_meta(&meta)?;
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

    #[allow(dead_code)]
    fn query_full_scan(
        &self,
        symbol: &str,
        timeframe: &str,
        meta: &SeriesMeta,
        chunk_range: std::ops::Range<usize>,
        start_ts: i64,
        end_ts: i64,
    ) -> Result<Vec<KlineRecord>> {
        let series_dir = path::kline_series_dir(&self.root, symbol, timeframe);
        let estimated_rows = meta.chunks[chunk_range.clone()]
            .iter()
            .map(|chunk| chunk.count as usize)
            .sum();
        let mut records = Vec::with_capacity(estimated_rows);

        for chunk in &meta.chunks[chunk_range] {
            kline_reader::append_range_in_chunk(
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

        let mut records = if should_use_short_range_path(meta.timeframe_ms, start_ts, end_ts) {
            Vec::with_capacity(estimate_short_range_rows(
                meta.timeframe_ms,
                start_ts,
                end_ts,
            ))
        } else {
            Vec::with_capacity(
                meta.chunks[chunk_range.clone()]
                    .iter()
                    .map(|chunk| chunk.count as usize)
                    .sum(),
            )
        };

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
        let mut parts = Vec::new();

        for chunk in meta.chunks.iter().rev() {
            if remaining == 0 {
                break;
            }

            let take = remaining.min(chunk.count as usize);
            parts.push(kline_reader::read_tail_in_chunk(
                &self.chunk_runtime,
                series_dir,
                meta.series_id,
                chunk,
                take,
            )?);
            remaining -= take;
        }

        parts.reverse();
        Ok(parts.into_iter().flatten().collect())
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

    #[allow(dead_code)]
    fn query_short_range(
        &self,
        symbol: &str,
        timeframe: &str,
        meta: &SeriesMeta,
        chunk_range: std::ops::Range<usize>,
        start_ts: i64,
        end_ts: i64,
    ) -> Result<Vec<KlineRecord>> {
        let series_dir = path::kline_series_dir(&self.root, symbol, timeframe);
        let estimated_rows = estimate_short_range_rows(meta.timeframe_ms, start_ts, end_ts);
        let mut records = Vec::with_capacity(estimated_rows);

        for chunk in &meta.chunks[chunk_range] {
            kline_reader::append_range_in_chunk(
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
        let _lock = StoreWriteLock::acquire(&self.root)?;
        let marker = recovery::RecoveryMarkerGuard::arm(&self.root)?;
        let series_dir = path::series_dir(&self.root, symbol, category, name);
        crate::storage::fs::ensure_dir(&path::chunks_dir(&series_dir))?;

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
            marker.commit()?;
            return Ok(());
        }

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
        let _lock = StoreWriteLock::acquire(&self.root)?;
        let marker = recovery::RecoveryMarkerGuard::arm(&self.root)?;
        R::validate_strict_order(records)?;
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

        let series_dir = path::series_dir(&self.root, symbol, category, name);
        crate::storage::fs::ensure_dir(&path::chunks_dir(&series_dir))?;
        for batch in split_fixed_records_by_partition(records, partition_unit)? {
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
        let meta = manifest::load_series_meta(series_dir)?;
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

fn should_use_short_range_path(timeframe_ms: i64, start_ts: i64, end_ts: i64) -> bool {
    if timeframe_ms <= 0 {
        return false;
    }
    let estimated = (end_ts.saturating_sub(start_ts) / timeframe_ms).saturating_add(1);
    estimated <= SHORT_RANGE_MAX_RECORDS
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
mod tests {
    use tempfile::TempDir;

    use crate::chunk::kline_reader;
    use crate::engine::store::FastKStore;
    use crate::types::{
        BboRecord, BookDeltaRecord, CompareOp, KlineRecord, PartitionPolicy, RecordType,
        ScalarIndexKind, ScalarPredicate, ScalarPredicateExpr, ScalarPredicateQuery, ScalarRecord,
        ScalarSeriesKey, TradeRecord,
    };

    #[test]
    fn chunk_write_and_read_roundtrip() {
        let fixture = TestFixture::new().expect("fixture should be created");
        let series_dir = crate::storage::path::kline_series_dir(fixture.root(), "BTCUSDT", "1m");
        let chunk_path = crate::storage::path::chunk_path(&series_dir, "2024-02.chunk");

        let records = kline_reader::read_all(&chunk_path).expect("chunk should be readable");

        assert_eq!(records, fixture.records());
    }

    #[test]
    fn get_kline_at_finds_exact_record() {
        let fixture = TestFixture::new().expect("fixture should be created");

        let record = fixture
            .store()
            .get_kline_at("BTCUSDT", "1m", 1_706_745_720_000)
            .expect("query should succeed");

        assert_eq!(
            record,
            Some(KlineRecord {
                ts: 1_706_745_720_000,
                open: 102,
                high: 103,
                low: 101,
                close: 102,
                volume: 12,
            })
        );
    }

    #[test]
    fn get_kline_at_returns_none_for_missing_timestamp() {
        let fixture = TestFixture::new().expect("fixture should be created");

        let record = fixture
            .store()
            .get_kline_at("BTCUSDT", "1m", 1_706_745_750_000)
            .expect("query should succeed");

        assert_eq!(record, None);
    }

    #[test]
    fn get_kline_range_returns_inclusive_rows() {
        let fixture = TestFixture::new().expect("fixture should be created");

        let records = fixture
            .store()
            .get_kline_range("BTCUSDT", "1m", 1_706_745_660_000, 1_706_745_780_000)
            .expect("range query should succeed");

        assert_eq!(
            records.iter().map(|record| record.ts).collect::<Vec<_>>(),
            vec![1_706_745_660_000, 1_706_745_720_000, 1_706_745_780_000]
        );
    }

    #[test]
    fn short_range_cross_chunk_returns_only_needed_rows() {
        let fixture = TestFixture::with_two_month_chunks().expect("fixture should be created");

        let records = fixture
            .store()
            .get_kline_range("BTCUSDT", "1m", 1_706_745_840_000, 1_709_251_260_000)
            .expect("range query should succeed");

        assert_eq!(
            records.iter().map(|record| record.ts).collect::<Vec<_>>(),
            vec![
                1_706_745_840_000,
                1_706_745_900_000,
                1_709_251_200_000,
                1_709_251_260_000
            ]
        );
    }

    #[test]
    fn repeated_point_queries_remain_correct() {
        let fixture = TestFixture::new().expect("fixture should be created");

        for _ in 0..20 {
            let record = fixture
                .store()
                .get_kline_at("BTCUSDT", "1m", 1_706_745_840_000)
                .expect("query should succeed");
            assert_eq!(record.map(|record| record.close), Some(104));
        }
    }

    #[test]
    fn get_kline_latest_n_spans_previous_chunks() {
        let fixture = TestFixture::with_two_month_chunks().expect("fixture should be created");

        let records = fixture
            .store()
            .get_kline_latest_n("BTCUSDT", "1m", 4)
            .expect("latest_n query should succeed");

        assert_eq!(
            records.iter().map(|record| record.ts).collect::<Vec<_>>(),
            vec![
                1_706_745_900_000,
                1_709_251_200_000,
                1_709_251_260_000,
                1_709_251_320_000
            ]
        );
    }

    #[test]
    fn put_kline_chunk_rejects_non_increasing_timestamps() {
        let temp_dir = TempDir::new().expect("temp dir should be created");
        let mut store = FastKStore::open(temp_dir.path()).expect("store should open");
        store.init().expect("store should init");
        store
            .register_kline_series("BTCUSDT", "1m", 60_000, 100_000, 100_000)
            .expect("series should register");

        let err = store
            .put_kline_chunk(
                "BTCUSDT",
                "1m",
                &[
                    KlineRecord {
                        ts: 1_706_745_720_000,
                        open: 1,
                        high: 2,
                        low: 0,
                        close: 1,
                        volume: 1,
                    },
                    KlineRecord {
                        ts: 1_706_745_720_000,
                        open: 1,
                        high: 2,
                        low: 0,
                        close: 1,
                        volume: 1,
                    },
                ],
            )
            .expect_err("put should reject non-increasing timestamps");

        assert!(matches!(err, crate::FastKError::InvalidInput(_)));
    }

    #[test]
    fn put_kline_chunk_rejects_cross_month_records() {
        let temp_dir = TempDir::new().expect("temp dir should be created");
        let mut store = FastKStore::open(temp_dir.path()).expect("store should open");
        store.init().expect("store should init");
        store
            .register_kline_series("BTCUSDT", "1m", 60_000, 100_000, 100_000)
            .expect("series should register");

        let err = store
            .put_kline_chunk(
                "BTCUSDT",
                "1m",
                &[
                    KlineRecord {
                        ts: 1_706_745_600_000,
                        open: 1,
                        high: 2,
                        low: 0,
                        close: 1,
                        volume: 1,
                    },
                    KlineRecord {
                        ts: 1_709_251_200_000,
                        open: 2,
                        high: 3,
                        low: 1,
                        close: 2,
                        volume: 2,
                    },
                ],
            )
            .expect_err("put should reject cross-month records");

        assert!(matches!(err, crate::FastKError::InvalidInput(_)));
    }

    #[test]
    fn append_strictly_increasing_writes_delta_and_merge_compacts_month() {
        let temp_dir = TempDir::new().expect("temp dir should be created");
        let mut store = FastKStore::open(temp_dir.path()).expect("store should open");
        store.init().expect("store should init");
        store
            .register_kline_series("BTCUSDT", "1m", 60_000, 100_000, 100_000)
            .expect("series should register");

        store
            .put_kline_chunk("BTCUSDT", "1m", &base_records())
            .expect("base chunk should write");
        store
            .put_kline_chunk(
                "BTCUSDT",
                "1m",
                &[KlineRecord {
                    ts: 1_706_745_960_000,
                    open: 106,
                    high: 107,
                    low: 105,
                    close: 106,
                    volume: 16,
                }],
            )
            .expect("delta append should write");

        let before_merge = store
            .get_kline_latest_n("BTCUSDT", "1m", 2)
            .expect("latest_n should succeed");
        assert_eq!(
            before_merge
                .iter()
                .map(|record| record.ts)
                .collect::<Vec<_>>(),
            vec![1_706_745_900_000, 1_706_745_960_000]
        );

        store
            .merge_kline_month("BTCUSDT", "1m", "2024-02")
            .expect("merge should succeed");

        let after_merge = store
            .get_kline_latest_n("BTCUSDT", "1m", 2)
            .expect("latest_n should succeed");
        assert_eq!(before_merge, after_merge);
    }

    #[test]
    fn append_overlap_is_rejected() {
        let temp_dir = TempDir::new().expect("temp dir should be created");
        let mut store = FastKStore::open(temp_dir.path()).expect("store should open");
        store.init().expect("store should init");
        store
            .register_kline_series("BTCUSDT", "1m", 60_000, 100_000, 100_000)
            .expect("series should register");
        store
            .put_kline_chunk("BTCUSDT", "1m", &base_records())
            .expect("base chunk should write");

        let err = store
            .put_kline_chunk(
                "BTCUSDT",
                "1m",
                &[KlineRecord {
                    ts: 1_706_745_900_000,
                    open: 200,
                    high: 201,
                    low: 199,
                    close: 200,
                    volume: 20,
                }],
            )
            .expect_err("overlap append should be rejected");

        assert!(matches!(err, crate::FastKError::InvalidInput(_)));
    }

    #[test]
    fn scalar_sidecars_attach_and_query_correctly() {
        let temp_dir = TempDir::new().expect("temp dir should be created");
        let mut store = FastKStore::open(temp_dir.path()).expect("store should open");
        store.init().expect("store should init");
        let key = sample_scalar_key();
        store
            .register_scalar_series(&key, 60_000)
            .expect("scalar series should register");
        let records = sample_scalar_records();
        store
            .put_scalar_chunk(&key, 60_000, &records, 2)
            .expect("scalar chunk should write");

        let zmap_result = store
            .find_scalar_timestamps_via_zmap(
                &key,
                &ScalarPredicate {
                    op: CompareOp::Between,
                    value: 20,
                    value2: Some(40),
                },
                records[0].ts,
                records[records.len() - 1].ts,
            )
            .expect("zmap query should succeed");
        let vix_result = store
            .find_scalar_timestamps_via_vix(
                &key,
                &ScalarPredicate {
                    op: CompareOp::Gte,
                    value: 30,
                    value2: None,
                },
                records[0].ts,
                records[records.len() - 1].ts,
            )
            .expect("vix query should succeed");

        let meta = store.load_scalar_meta(&key).expect("meta should load");
        assert_eq!(meta.chunks.len(), 1);
        assert_eq!(meta.chunks[0].sidecars.len(), 2);
        for sidecar in &meta.chunks[0].sidecars {
            let sidecar_path = crate::storage::path::resolve_relative_path(
                &crate::storage::path::scalar_series_dir(
                    temp_dir.path(),
                    &key.symbol,
                    &key.category,
                    &key.name,
                ),
                &sidecar.relative_path,
            );
            assert!(sidecar_path.exists());
        }

        assert_eq!(
            zmap_result,
            vec![1_706_745_720_000, 1_706_745_780_000, 1_706_745_840_000]
        );
        assert_eq!(
            vix_result,
            vec![1_706_745_780_000, 1_706_745_840_000, 1_706_745_900_000]
        );
    }

    #[test]
    fn replacing_scalar_chunk_replaces_sidecars() {
        let temp_dir = TempDir::new().expect("temp dir should be created");
        let mut store = FastKStore::open(temp_dir.path()).expect("store should open");
        store.init().expect("store should init");
        let key = sample_scalar_key();
        store
            .register_scalar_series(&key, 60_000)
            .expect("scalar series should register");
        store
            .put_scalar_chunk(&key, 60_000, &sample_scalar_records(), 2)
            .expect("initial scalar chunk should write");

        let initial_meta = store.load_scalar_meta(&key).expect("meta should load");
        let old_sidecars = initial_meta.chunks[0].sidecars.clone();

        let replacement_records = vec![
            ScalarRecord {
                ts: 1_706_745_600_000,
                value: 5,
            },
            ScalarRecord {
                ts: 1_706_745_660_000,
                value: 10,
            },
            ScalarRecord {
                ts: 1_706_745_720_000,
                value: 15,
            },
        ];
        store
            .put_scalar_chunk(&key, 60_000, &replacement_records, 2)
            .expect("replacement scalar chunk should write");

        let updated_meta = store
            .load_scalar_meta(&key)
            .expect("updated meta should load");
        assert_eq!(updated_meta.chunks.len(), 1);
        assert!(updated_meta.chunks[0].generation > initial_meta.chunks[0].generation);

        let series_dir = crate::storage::path::scalar_series_dir(
            temp_dir.path(),
            &key.symbol,
            &key.category,
            &key.name,
        );
        for sidecar in old_sidecars {
            let sidecar_path =
                crate::storage::path::resolve_relative_path(&series_dir, &sidecar.relative_path);
            assert!(!sidecar_path.exists());
        }
        for sidecar in &updated_meta.chunks[0].sidecars {
            let sidecar_path =
                crate::storage::path::resolve_relative_path(&series_dir, &sidecar.relative_path);
            assert!(sidecar_path.exists());
        }
    }

    #[test]
    fn sidecar_cache_hits_after_repeated_zmap_queries() {
        let temp_dir = TempDir::new().expect("temp dir should be created");
        let mut store = FastKStore::open(temp_dir.path()).expect("store should open");
        store.init().expect("store should init");
        let key = sample_scalar_key();
        let records = sample_scalar_records();
        store
            .register_scalar_series(&key, 60_000)
            .expect("scalar series should register");
        store
            .put_scalar_chunk(&key, 60_000, &records, 2)
            .expect("scalar chunk should write");

        store.reset_metrics();
        let _ = store
            .find_scalar_timestamps_via_zmap(
                &key,
                &ScalarPredicate {
                    op: CompareOp::Gte,
                    value: 20,
                    value2: None,
                },
                records[0].ts,
                records[records.len() - 1].ts,
            )
            .expect("first zmap query should succeed");
        let first = store.metrics_snapshot();
        assert_eq!(first.sidecar_cache_hits, 0);
        assert!(first.sidecar_cache_misses >= 1);

        store.reset_metrics();
        let _ = store
            .find_scalar_timestamps_via_zmap(
                &key,
                &ScalarPredicate {
                    op: CompareOp::Gte,
                    value: 20,
                    value2: None,
                },
                records[0].ts,
                records[records.len() - 1].ts,
            )
            .expect("second zmap query should succeed");
        let second = store.metrics_snapshot();
        assert!(second.sidecar_cache_hits >= 1);
    }

    #[test]
    fn scalar_raw_query_matches_indexed_paths() {
        let temp_dir = TempDir::new().expect("temp dir should be created");
        let mut store = FastKStore::open(temp_dir.path()).expect("store should open");
        store.init().expect("store should init");
        let key = sample_scalar_key();
        let records = sample_scalar_records();
        let predicate = ScalarPredicate {
            op: CompareOp::Between,
            value: 15,
            value2: Some(40),
        };
        store
            .register_scalar_series(&key, 60_000)
            .expect("scalar series should register");
        store
            .put_scalar_chunk(&key, 60_000, &records, 2)
            .expect("scalar chunk should write");

        let raw = store
            .find_scalar_timestamps_raw(
                &key,
                &predicate,
                records[0].ts,
                records[records.len() - 1].ts,
            )
            .expect("raw query should succeed");
        let zmap = store
            .find_scalar_timestamps_via_zmap(
                &key,
                &predicate,
                records[0].ts,
                records[records.len() - 1].ts,
            )
            .expect("zmap query should succeed");
        let vix = store
            .find_scalar_timestamps_via_vix(
                &key,
                &predicate,
                records[0].ts,
                records[records.len() - 1].ts,
            )
            .expect("vix query should succeed");

        assert_eq!(raw, zmap);
        assert_eq!(raw, vix);
    }

    #[test]
    fn scalar_predicate_query_continuous_gt_uses_zmap() {
        let temp_dir = TempDir::new().expect("temp dir should be created");
        let mut store = FastKStore::open(temp_dir.path()).expect("store should open");
        store.init().expect("store should init");
        let key = feature_scalar_key("rsi14");
        let records = sample_scalar_records();
        store
            .register_scalar_series(&key, 60_000)
            .expect("feature series should register");
        store
            .put_scalar_chunk(&key, 60_000, &records, 2)
            .expect("scalar chunk should write");

        let result = store
            .query_scalar_predicate(ScalarPredicateQuery {
                key,
                start_ts: records[0].ts,
                end_ts: records[records.len() - 1].ts,
                predicate: ScalarPredicateExpr::Gt(30),
                return_values: true,
            })
            .expect("predicate query should succeed");

        assert_eq!(
            scalar_matches_to_records(&result.matches),
            vec![
                ScalarRecord {
                    ts: 1_706_745_840_000,
                    value: 40
                },
                ScalarRecord {
                    ts: 1_706_745_900_000,
                    value: 50
                },
            ]
        );
        assert_eq!(result.stats.index_used, Some(ScalarIndexKind::ZoneMap));
        assert!(!result.stats.fallback_scan);
        assert_eq!(result.stats.rows_matched, 2);
        assert!(result.stats.blocks_considered > 0);
        assert!(result.stats.rows_checked > 0);
    }

    #[test]
    fn scalar_predicate_query_between_inclusive_and_exclusive() {
        let temp_dir = TempDir::new().expect("temp dir should be created");
        let mut store = FastKStore::open(temp_dir.path()).expect("store should open");
        store.init().expect("store should init");
        let key = feature_scalar_key("ma20_gap");
        let records = sample_scalar_records();
        store
            .register_scalar_series(&key, 60_000)
            .expect("feature series should register");
        store
            .put_scalar_chunk(&key, 60_000, &records, 2)
            .expect("scalar chunk should write");

        let inclusive = store
            .find_scalar_points_by_predicate(
                &key,
                records[0].ts,
                records[records.len() - 1].ts,
                ScalarPredicateExpr::Between {
                    min: 20,
                    max: 40,
                    inclusive: true,
                },
            )
            .expect("inclusive between should succeed");
        let exclusive = store
            .find_scalar_points_by_predicate(
                &key,
                records[0].ts,
                records[records.len() - 1].ts,
                ScalarPredicateExpr::Between {
                    min: 20,
                    max: 40,
                    inclusive: false,
                },
            )
            .expect("exclusive between should succeed");

        assert_eq!(
            inclusive
                .iter()
                .map(|record| record.value)
                .collect::<Vec<_>>(),
            vec![20, 30, 40]
        );
        assert_eq!(
            exclusive
                .iter()
                .map(|record| record.value)
                .collect::<Vec<_>>(),
            vec![30]
        );
    }

    #[test]
    fn scalar_predicate_query_discrete_eq_and_in_set_use_vix() {
        let temp_dir = TempDir::new().expect("temp dir should be created");
        let mut store = FastKStore::open(temp_dir.path()).expect("store should open");
        store.init().expect("store should init");
        let key = feature_scalar_key("state");
        let records = sample_scalar_records();
        store
            .register_scalar_series(&key, 60_000)
            .expect("feature series should register");
        store
            .put_scalar_chunk(&key, 60_000, &records, 2)
            .expect("scalar chunk should write");

        let eq = store
            .query_scalar_predicate(ScalarPredicateQuery {
                key: key.clone(),
                start_ts: records[0].ts,
                end_ts: records[records.len() - 1].ts,
                predicate: ScalarPredicateExpr::Eq(30),
                return_values: false,
            })
            .expect("eq query should succeed");
        let in_set = store
            .query_scalar_predicate(ScalarPredicateQuery {
                key,
                start_ts: records[0].ts,
                end_ts: records[records.len() - 1].ts,
                predicate: ScalarPredicateExpr::InSet(vec![10, 40]),
                return_values: true,
            })
            .expect("in-set query should succeed");

        assert_eq!(eq.matches.len(), 1);
        assert_eq!(eq.matches[0].ts, 1_706_745_780_000);
        assert_eq!(eq.matches[0].value, None);
        assert_eq!(eq.stats.index_used, Some(ScalarIndexKind::ValueIndex));
        assert!(!eq.stats.fallback_scan);
        assert_eq!(
            scalar_matches_to_records(&in_set.matches)
                .iter()
                .map(|record| record.value)
                .collect::<Vec<_>>(),
            vec![10, 40]
        );
        assert_eq!(in_set.stats.index_used, Some(ScalarIndexKind::ValueIndex));
    }

    #[test]
    fn scalar_predicate_query_not_in_set_falls_back_to_scan() {
        let temp_dir = TempDir::new().expect("temp dir should be created");
        let mut store = FastKStore::open(temp_dir.path()).expect("store should open");
        store.init().expect("store should init");
        let key = feature_scalar_key("state");
        let records = sample_scalar_records();
        store
            .register_scalar_series(&key, 60_000)
            .expect("feature series should register");
        store
            .put_scalar_chunk(&key, 60_000, &records, 2)
            .expect("scalar chunk should write");

        let result = store
            .query_scalar_predicate(ScalarPredicateQuery {
                key,
                start_ts: records[0].ts,
                end_ts: records[records.len() - 1].ts,
                predicate: ScalarPredicateExpr::NotInSet(vec![10, 50]),
                return_values: true,
            })
            .expect("not-in-set query should succeed");

        assert_eq!(
            scalar_matches_to_records(&result.matches)
                .iter()
                .map(|record| record.value)
                .collect::<Vec<_>>(),
            vec![15, 20, 30, 40]
        );
        assert_eq!(result.stats.index_used, Some(ScalarIndexKind::FullScan));
        assert!(result.stats.fallback_scan);
        assert_eq!(result.stats.rows_checked, records.len() as u64);
    }

    #[test]
    fn scalar_predicate_query_missing_sidecar_falls_back_to_scan() {
        let temp_dir = TempDir::new().expect("temp dir should be created");
        let mut store = FastKStore::open(temp_dir.path()).expect("store should open");
        store.init().expect("store should init");
        let key = factor_scalar_key("momentum_score");
        let records = sample_scalar_records();
        store
            .register_scalar_series(&key, 60_000)
            .expect("factor series should register");
        store
            .put_scalar_chunk(&key, 60_000, &records, 2)
            .expect("scalar chunk should write");

        let mut meta = (*store.load_scalar_meta(&key).expect("meta should load")).clone();
        for chunk in &mut meta.chunks {
            chunk.sidecars.clear();
        }
        store
            .catalog
            .save_scalar_meta(&meta)
            .expect("meta should save without sidecars");
        store
            .manifest_cache
            .write()
            .expect("manifest cache should lock")
            .clear();

        let result = store
            .query_scalar_predicate(ScalarPredicateQuery {
                key,
                start_ts: records[0].ts,
                end_ts: records[records.len() - 1].ts,
                predicate: ScalarPredicateExpr::Gt(30),
                return_values: false,
            })
            .expect("missing sidecar should fall back");

        assert_eq!(
            result
                .matches
                .iter()
                .map(|entry| entry.ts)
                .collect::<Vec<_>>(),
            vec![1_706_745_840_000, 1_706_745_900_000]
        );
        assert_eq!(result.stats.index_used, Some(ScalarIndexKind::FullScan));
        assert!(result.stats.fallback_scan);
    }

    #[test]
    fn scalar_predicate_query_invalid_and_empty_ranges_are_explicit() {
        let temp_dir = TempDir::new().expect("temp dir should be created");
        let mut store = FastKStore::open(temp_dir.path()).expect("store should open");
        store.init().expect("store should init");
        let key = sample_scalar_key();
        let records = sample_scalar_records();
        store
            .register_scalar_series(&key, 60_000)
            .expect("scalar series should register");
        store
            .put_scalar_chunk(&key, 60_000, &records, 2)
            .expect("scalar chunk should write");

        let err = store
            .query_scalar_predicate(ScalarPredicateQuery {
                key: key.clone(),
                start_ts: records[records.len() - 1].ts,
                end_ts: records[0].ts,
                predicate: ScalarPredicateExpr::Gt(30),
                return_values: false,
            })
            .expect_err("invalid range should fail");
        assert!(matches!(err, crate::FastKError::InvalidInput(_)));

        let empty = store
            .query_scalar_predicate(ScalarPredicateQuery {
                key,
                start_ts: 1_800_000_000_000,
                end_ts: 1_800_000_060_000,
                predicate: ScalarPredicateExpr::Gt(30),
                return_values: false,
            })
            .expect("empty range should succeed");
        assert!(empty.matches.is_empty());
        assert_eq!(empty.stats.rows_checked, 0);
    }

    #[test]
    fn scalar_predicate_query_indicator_compatibility_still_works() {
        let temp_dir = TempDir::new().expect("temp dir should be created");
        let mut store = FastKStore::open(temp_dir.path()).expect("store should open");
        store.init().expect("store should init");
        store
            .register_kline_series("BTCUSDT", "1m", 60_000, 100_000, 100_000)
            .expect("kline should register");
        store
            .put_kline_chunk("BTCUSDT", "1m", &base_records())
            .expect("kline should write");
        store
            .register_indicator_series("BTCUSDT", "1m", "ma20")
            .expect("indicator should register");
        store
            .put_indicator_chunk("BTCUSDT", "1m", "ma20", &sample_scalar_records())
            .expect("indicator should write");

        let key = crate::indicator_series_key("BTCUSDT", "1m", "ma20");
        let points = store
            .find_scalar_points_by_predicate(
                &key,
                1_706_745_600_000,
                1_706_745_900_000,
                ScalarPredicateExpr::Gte(40),
            )
            .expect("indicator predicate should query");
        let indicator_range = store
            .get_indicator_range(
                "BTCUSDT",
                "1m",
                "ma20",
                1_706_745_600_000,
                1_706_745_900_000,
            )
            .expect("existing indicator API should still work");

        assert_eq!(points.len(), 2);
        assert_eq!(indicator_range.len(), 6);
    }

    #[test]
    fn inventory_stats_and_capabilities_are_reported() {
        let temp_dir = TempDir::new().expect("temp dir should be created");
        let mut store = FastKStore::open(temp_dir.path()).expect("store should open");
        store.init().expect("store should init");
        store
            .register_kline_series("BTCUSDT", "1m", 60_000, 100_000, 100_000)
            .expect("kline series should register");
        store
            .put_kline_chunk("BTCUSDT", "1m", &base_records())
            .expect("kline chunk should write");

        let key = sample_scalar_key();
        store
            .register_scalar_series(&key, 60_000)
            .expect("scalar series should register");
        store
            .put_scalar_chunk(&key, 60_000, &sample_scalar_records(), 2)
            .expect("scalar chunk should write");

        let all_series = store.list_series().expect("series list should load");
        let kline_series = store
            .list_kline_series()
            .expect("kline series list should load");
        let scalar_series = store
            .list_scalar_series()
            .expect("scalar series list should load");
        let stats = store.store_stats().expect("store stats should load");
        let capabilities = store
            .scalar_query_capabilities(&key)
            .expect("capabilities should load");

        assert_eq!(all_series.len(), 2);
        assert_eq!(kline_series.len(), 1);
        assert_eq!(scalar_series.len(), 1);
        assert_eq!(stats.series_count, 2);
        assert_eq!(stats.kline_series_count, 1);
        assert_eq!(stats.scalar_series_count, 1);
        assert_eq!(stats.chunk_count, 2);
        assert_eq!(stats.sidecar_count, 2);
        assert!(capabilities.raw_scan);
        assert!(capabilities.has_zmap);
        assert!(capabilities.has_vix);
    }

    #[test]
    fn indicator_series_wrappers_roundtrip_records() {
        let temp_dir = TempDir::new().expect("temp dir should be created");
        let mut store = FastKStore::open(temp_dir.path()).expect("store should open");
        store.init().expect("store should init");
        store
            .register_kline_series("BTCUSDT", "1m", 60_000, 100_000, 100_000)
            .expect("kline series should register");
        store
            .put_kline_chunk("BTCUSDT", "1m", &base_records())
            .expect("kline chunk should write");

        store
            .register_indicator_series("BTCUSDT", "1m", "rsi14")
            .expect("indicator series should register");
        store
            .put_indicator_chunk("BTCUSDT", "1m", "rsi14", &sample_scalar_records())
            .expect("indicator chunk should write");

        let hit = store
            .get_indicator_at("BTCUSDT", "1m", "rsi14", 1_706_745_780_000)
            .expect("indicator point lookup should succeed")
            .expect("indicator record should exist");
        let rows = store
            .get_indicator_range(
                "BTCUSDT",
                "1m",
                "rsi14",
                1_706_745_660_000,
                1_706_745_840_000,
            )
            .expect("indicator range query should succeed");

        assert_eq!(hit.value, 30);
        assert_eq!(rows.len(), 4);
        assert_eq!(rows[0].value, 15);
        assert_eq!(rows[3].value, 40);
    }

    #[test]
    fn indicator_inventory_and_capabilities_are_reported() {
        let temp_dir = TempDir::new().expect("temp dir should be created");
        let mut store = FastKStore::open(temp_dir.path()).expect("store should open");
        store.init().expect("store should init");
        store
            .register_kline_series("BTCUSDT", "1m", 60_000, 100_000, 100_000)
            .expect("kline series should register");
        store
            .put_kline_chunk("BTCUSDT", "1m", &base_records())
            .expect("kline chunk should write");
        store
            .put_indicator_chunk("BTCUSDT", "1m", "rsi14", &sample_scalar_records())
            .expect("indicator chunk should write");

        let indicators = store
            .list_indicators("BTCUSDT", "1m")
            .expect("indicator list should load");
        let inventory = store
            .indicator_inventory("BTCUSDT", "1m")
            .expect("indicator inventory should load");
        let capabilities = store
            .indicator_capabilities("BTCUSDT", "1m", "rsi14")
            .expect("indicator capabilities should load");

        assert_eq!(indicators, vec!["rsi14".to_string()]);
        assert_eq!(inventory.len(), 1);
        assert_eq!(inventory[0].timeframe, "1m");
        assert_eq!(inventory[0].indicator_name, "rsi14");
        assert_eq!(
            inventory[0].record_count,
            sample_scalar_records().len() as u64
        );
        assert!(capabilities.raw_scan);
        assert!(capabilities.has_zmap);
        assert!(capabilities.has_vix);
    }

    #[test]
    fn read_session_inventory_and_health_summary_are_reported() {
        let temp_dir = TempDir::new().expect("temp dir should be created");
        let mut store = FastKStore::open(temp_dir.path()).expect("store should open");
        store.init().expect("store should init");
        store
            .register_kline_series("BTCUSDT", "1m", 60_000, 100_000, 100_000)
            .expect("kline series should register");
        store
            .put_kline_chunk("BTCUSDT", "1m", &base_records())
            .expect("kline chunk should write");

        let key = sample_scalar_key();
        store
            .register_scalar_series(&key, 60_000)
            .expect("scalar series should register");
        store
            .put_scalar_chunk(&key, 60_000, &sample_scalar_records(), 2)
            .expect("scalar chunk should write");

        let mut session = store.read_session();
        session
            .attach_kline_series("BTCUSDT", "1m")
            .expect("kline attach should succeed");
        session
            .attach_scalar_series(&key)
            .expect("scalar attach should succeed");

        let inventory = session
            .attached_inventory()
            .expect("attached inventory should load");
        let health = session
            .attached_health_summary()
            .expect("attached health should load");
        let months = store
            .kline_month_inventory("BTCUSDT", "1m")
            .expect("month inventory should load");
        let scalar_months = store
            .scalar_month_inventory(&key)
            .expect("scalar month inventory should load");
        let scalar_caps = session
            .attached_scalar_capabilities()
            .expect("scalar capabilities should load");

        assert_eq!(inventory.len(), 2);
        assert_eq!(health.series_count, 2);
        assert_eq!(health.issue_series_count, 0);
        assert_eq!(months.len(), 1);
        assert_eq!(months[0].record_count, base_records().len() as u64);
        assert_eq!(scalar_months.len(), 1);
        assert_eq!(scalar_caps.len(), 1);

        session.clear_caches();
        let record = session
            .get_kline_at("BTCUSDT", "1m", 1_706_745_720_000)
            .expect("point query should succeed")
            .expect("record should exist");
        assert_eq!(record.close, 102);
    }

    #[test]
    fn session_cache_clear_retains_manifest_snapshot() {
        let temp_dir = TempDir::new().expect("temp dir should be created");
        let mut store = FastKStore::open(temp_dir.path()).expect("store should open");
        store.init().expect("store should init");
        store
            .register_kline_series("BTCUSDT", "1m", 60_000, 100_000, 100_000)
            .expect("kline series should register");
        store
            .put_kline_chunk("BTCUSDT", "1m", &base_records())
            .expect("kline chunk should write");

        let mut session = store.read_session();
        session
            .attach_kline_series("BTCUSDT", "1m")
            .expect("attach should succeed");

        store.reset_metrics();
        session.clear_caches();
        let _ = session
            .get_kline_at("BTCUSDT", "1m", 1_706_745_720_000)
            .expect("query should succeed");
        let snapshot = store.metrics_snapshot();

        assert_eq!(snapshot.manifest_load_ns, 0);
        assert!(snapshot.chunk_header_cache_misses >= 1);
    }

    #[test]
    fn metrics_level_controls_runtime_overhead_and_detail() {
        let fixture = TestFixture::new().expect("fixture should build");
        let store = fixture.store();

        store.set_metrics_level(crate::MetricsLevel::Off);
        store.reset_metrics();
        let _ = store
            .get_kline_at("BTCUSDT", "1m", 1_706_745_720_000)
            .expect("point query should succeed");
        let off = store.metrics_snapshot();
        assert_eq!(off.metrics_level, crate::MetricsLevel::Off);
        assert_eq!(off.bytes_read, 0);
        assert_eq!(
            off.chunk_header_cache_hits + off.chunk_header_cache_misses,
            0
        );
        assert_eq!(off.point_record_read_ns, 0);

        store.set_metrics_level(crate::MetricsLevel::Basic);
        store.clear_read_caches();
        store.reset_metrics();
        let _ = store
            .get_kline_at("BTCUSDT", "1m", 1_706_745_720_000)
            .expect("point query should succeed");
        let basic = store.metrics_snapshot();
        assert_eq!(basic.metrics_level, crate::MetricsLevel::Basic);
        assert!(basic.bytes_read > 0);
        assert!(basic.chunk_header_cache_hits + basic.chunk_header_cache_misses >= 1);
        assert_eq!(basic.manifest_load_ns, 0);
        assert_eq!(basic.point_record_read_ns, 0);

        store.set_metrics_level(crate::MetricsLevel::Detailed);
        store.clear_read_caches();
        store.reset_metrics();
        let _ = store
            .get_kline_at("BTCUSDT", "1m", 1_706_745_720_000)
            .expect("point query should succeed");
        let detailed = store.metrics_snapshot();
        assert_eq!(detailed.metrics_level, crate::MetricsLevel::Detailed);
        assert!(detailed.chunk_lookup_ns > 0);
        assert!(detailed.point_record_read_ns > 0);
    }

    #[test]
    fn shared_manifest_cache_shortens_reopen_like_query_bootstrap() {
        let temp_dir = TempDir::new().expect("temp dir should be created");
        let mut writer = FastKStore::open(temp_dir.path()).expect("store should open");
        writer.init().expect("store should init");
        writer
            .register_kline_series("BTCUSDT", "1m", 60_000, 100_000, 100_000)
            .expect("series should register");
        writer
            .put_kline_chunk("BTCUSDT", "1m", &base_records())
            .expect("chunk should write");

        let first_reader = FastKStore::open(temp_dir.path()).expect("reader should open");
        first_reader.set_metrics_level(crate::MetricsLevel::Detailed);
        first_reader.reset_metrics();
        let _ = first_reader
            .get_kline_at("BTCUSDT", "1m", 1_706_745_720_000)
            .expect("first query should succeed");
        let first = first_reader.metrics_snapshot();
        assert!(first.manifest_cache_hits >= 1);

        let second_reader = FastKStore::open(temp_dir.path()).expect("reader should reopen");
        second_reader.set_metrics_level(crate::MetricsLevel::Detailed);
        second_reader.reset_metrics();
        let _ = second_reader
            .get_kline_at("BTCUSDT", "1m", 1_706_745_720_000)
            .expect("second query should succeed");
        let second = second_reader.metrics_snapshot();
        assert!(second.manifest_cache_hits >= 1);
        assert_eq!(second.manifest_file_read_ns, 0);
        assert_eq!(second.manifest_decode_ns, 0);
    }

    #[test]
    fn scalar_point_lookup_and_short_range_are_correct() {
        let temp_dir = TempDir::new().expect("temp dir should be created");
        let mut store = FastKStore::open(temp_dir.path()).expect("store should open");
        store.init().expect("store should init");
        let key = sample_scalar_key();
        let records = sample_scalar_records();
        store
            .register_scalar_series(&key, 60_000)
            .expect("scalar series should register");
        store
            .put_scalar_chunk(&key, 60_000, &records, 2)
            .expect("scalar chunk should write");

        let hit = store
            .get_scalar_at(&key, 1_706_745_780_000)
            .expect("scalar point lookup should succeed")
            .expect("scalar record should exist");
        let miss = store
            .get_scalar_at(&key, 1_706_745_750_000)
            .expect("scalar miss should succeed");
        let range = store
            .get_scalar_range(&key, 1_706_745_660_000, 1_706_745_840_000)
            .expect("scalar short range should succeed");

        assert_eq!(hit.value, 30);
        assert!(miss.is_none());
        assert_eq!(
            range.iter().map(|record| record.ts).collect::<Vec<_>>(),
            vec![
                1_706_745_660_000,
                1_706_745_720_000,
                1_706_745_780_000,
                1_706_745_840_000
            ]
        );
    }

    #[test]
    fn high_frequency_record_types_write_read_latest_and_inventory() {
        let temp_dir = TempDir::new().expect("temp dir should be created");
        let mut store = FastKStore::open(temp_dir.path()).expect("store should open");
        store.init().expect("store should init");

        store
            .register_trade_series_with_partition(
                "BTCUSDT",
                "binance_spot",
                0,
                PartitionPolicy::hour(),
            )
            .expect("trade series should register");
        store
            .register_bbo_series("BTCUSDT", "binance_spot", 0)
            .expect("bbo series should register");
        store
            .register_book_delta_series("BTCUSDT", "binance_spot", 0)
            .expect("book delta series should register");

        store
            .put_trade_chunk("BTCUSDT", "binance_spot", &sample_trade_records())
            .expect("trade chunk should write");
        store
            .put_bbo_chunk("BTCUSDT", "binance_spot", &sample_bbo_records())
            .expect("bbo chunk should write");
        store
            .put_book_delta_chunk("BTCUSDT", "binance_spot", &sample_book_delta_records())
            .expect("book delta chunk should write");

        let trades = store
            .get_trade_range(
                "BTCUSDT",
                "binance_spot",
                1_706_745_600_000,
                1_706_749_320_000,
            )
            .expect("trade range should read");
        let bbo = store
            .get_bbo_range(
                "BTCUSDT",
                "binance_spot",
                1_706_745_600_000,
                1_706_832_060_000,
            )
            .expect("bbo range should read");
        let deltas = store
            .get_book_delta_range(
                "BTCUSDT",
                "binance_spot",
                1_706_745_600_000,
                1_706_749_320_000,
            )
            .expect("book delta range should read");

        assert_eq!(trades, sample_trade_records());
        assert_eq!(bbo, sample_bbo_records());
        assert_eq!(deltas, sample_book_delta_records());
        assert_eq!(
            store
                .get_trade_latest_n("BTCUSDT", "binance_spot", 3)
                .expect("latest trades should read")
                .iter()
                .map(|record| record.trade_id)
                .collect::<Vec<_>>(),
            vec![2, 3, 4]
        );
        assert_eq!(
            store
                .get_bbo_latest_n("BTCUSDT", "binance_spot", 1)
                .expect("latest bbo should read")[0]
                .sequence,
            2
        );
        assert_eq!(
            store
                .get_book_delta_latest_n("BTCUSDT", "binance_spot", 2)
                .expect("latest deltas should read")
                .iter()
                .map(|record| record.sequence)
                .collect::<Vec<_>>(),
            vec![102, 103]
        );

        let inventory = store.list_series().expect("inventory should load");
        assert!(inventory
            .iter()
            .any(|entry| { entry.category == "trade" && entry.record_type == RecordType::Trade }));
        assert!(inventory
            .iter()
            .any(|entry| entry.category == "bbo" && entry.record_type == RecordType::Bbo));
        assert!(inventory.iter().any(|entry| {
            entry.category == "book_delta" && entry.record_type == RecordType::BookDelta
        }));
    }

    #[test]
    fn sequence_scan_reports_clean_book_delta_across_partitions() {
        let temp_dir = TempDir::new().expect("temp dir should be created");
        let mut store = FastKStore::open(temp_dir.path()).expect("store should open");
        store.init().expect("store should init");
        store
            .register_book_delta_series("BTCUSDT", "binance_spot", 0)
            .expect("book delta series should register");
        store
            .put_book_delta_chunk("BTCUSDT", "binance_spot", &sample_book_delta_records())
            .expect("book delta chunk should write");

        let report = store
            .scan_book_delta_sequence(
                "BTCUSDT",
                "binance_spot",
                1_706_745_600_000,
                1_706_749_320_000,
            )
            .expect("sequence scan should succeed");

        assert!(report.is_clean());
        assert_eq!(report.record_type, RecordType::BookDelta);
        assert_eq!(report.sequence_field, "sequence");
        assert_eq!(report.scanned_record_count, 4);
        assert_eq!(report.scanned_chunk_count, 2);
        assert_eq!(report.first_sequence, Some(100));
        assert_eq!(report.last_sequence, Some(103));
    }

    #[test]
    fn sequence_scan_reports_gap_duplicate_and_non_monotonic_bbo() {
        let temp_dir = TempDir::new().expect("temp dir should be created");
        let mut store = FastKStore::open(temp_dir.path()).expect("store should open");
        store.init().expect("store should init");
        store
            .register_bbo_series("BTCUSDT", "binance_spot", 0)
            .expect("bbo series should register");
        store
            .put_bbo_chunk("BTCUSDT", "binance_spot", &sample_bbo_sequence_issues())
            .expect("bbo chunk should write");

        let report = store
            .scan_bbo_sequence(
                "BTCUSDT",
                "binance_spot",
                1_706_745_600_000,
                1_706_745_900_000,
            )
            .expect("sequence scan should succeed");

        assert!(!report.is_clean());
        assert_eq!(report.gap_count(), 1);
        assert_eq!(report.duplicate_count(), 1);
        assert_eq!(report.violation_count(), 1);
        assert_eq!(report.gaps[0].previous_sequence, 2);
        assert_eq!(report.gaps[0].expected_sequence, 3);
        assert_eq!(report.gaps[0].next_sequence, 4);
        assert_eq!(report.gaps[0].missing_count, 1);
        assert_eq!(report.duplicates[0].sequence, 4);
        assert_eq!(report.violations[0].previous_sequence, 4);
        assert_eq!(report.violations[0].next_sequence, 3);
    }

    #[test]
    fn sequence_scan_supports_trade_id_and_empty_ranges() {
        let temp_dir = TempDir::new().expect("temp dir should be created");
        let mut store = FastKStore::open(temp_dir.path()).expect("store should open");
        store.init().expect("store should init");
        store
            .register_trade_series("BTCUSDT", "binance_spot", 0)
            .expect("trade series should register");
        store
            .put_trade_chunk("BTCUSDT", "binance_spot", &sample_trade_id_gap_records())
            .expect("trade chunk should write");

        let report = store
            .scan_trade_id_sequence(
                "BTCUSDT",
                "binance_spot",
                1_706_745_600_000,
                1_706_745_780_000,
            )
            .expect("trade id scan should succeed");
        assert_eq!(report.sequence_field, "trade_id");
        assert_eq!(report.gap_count(), 1);
        assert_eq!(report.gaps[0].expected_sequence, 3);
        assert_eq!(report.gaps[0].next_sequence, 4);

        let empty = store
            .scan_trade_id_sequence(
                "BTCUSDT",
                "binance_spot",
                1_800_000_000_000,
                1_800_000_060_000,
            )
            .expect("empty scan should succeed");
        assert!(empty.is_clean());
        assert_eq!(empty.scanned_record_count, 0);
        assert_eq!(empty.first_sequence, None);
    }

    #[test]
    fn sequence_scan_invalid_range_is_error() {
        let temp_dir = TempDir::new().expect("temp dir should be created");
        let mut store = FastKStore::open(temp_dir.path()).expect("store should open");
        store.init().expect("store should init");
        store
            .register_bbo_series("BTCUSDT", "binance_spot", 0)
            .expect("bbo series should register");

        let err = store
            .scan_bbo_sequence(
                "BTCUSDT",
                "binance_spot",
                1_706_745_900_000,
                1_706_745_600_000,
            )
            .expect_err("invalid range should fail");

        assert!(matches!(err, crate::FastKError::InvalidInput(_)));
    }

    #[test]
    fn replay_kline_from_middle_of_chunk() {
        let fixture = TestFixture::new().expect("fixture should be created");
        let mut cursor = fixture
            .store()
            .replay_kline("BTCUSDT", "1m", 1_706_745_710_000, None)
            .expect("replay cursor should open");

        let batch = cursor.next_batch(3).expect("batch should read");

        assert_eq!(
            batch.iter().map(|record| record.ts).collect::<Vec<_>>(),
            vec![1_706_745_720_000, 1_706_745_780_000, 1_706_745_840_000]
        );
    }

    #[test]
    fn replay_scalar_across_month_chunks() {
        let temp_dir = TempDir::new().expect("temp dir should be created");
        let mut store = FastKStore::open(temp_dir.path()).expect("store should open");
        store.init().expect("store should init");
        let key = sample_scalar_key();
        store
            .register_scalar_series(&key, 60_000)
            .expect("scalar series should register");
        store
            .put_scalar_chunk(&key, 60_000, &sample_scalar_records(), 2)
            .expect("feb scalar chunk should write");
        store
            .put_scalar_chunk(&key, 60_000, &sample_march_scalar_records(), 2)
            .expect("mar scalar chunk should write");

        let mut cursor = store
            .replay_scalar(&key, 1_706_745_780_000, None)
            .expect("scalar replay should open");
        let first = cursor.next_batch(3).expect("first batch should read");
        let second = cursor.next_batch(10).expect("second batch should read");

        assert_eq!(
            first.iter().map(|record| record.ts).collect::<Vec<_>>(),
            vec![1_706_745_780_000, 1_706_745_840_000, 1_706_745_900_000]
        );
        assert_eq!(
            second.iter().map(|record| record.ts).collect::<Vec<_>>(),
            vec![1_709_251_200_000, 1_709_251_260_000]
        );
    }

    #[test]
    fn replay_trade_across_hour_partitions() {
        let temp_dir = TempDir::new().expect("temp dir should be created");
        let mut store = FastKStore::open(temp_dir.path()).expect("store should open");
        store.init().expect("store should init");
        store
            .register_trade_series("BTCUSDT", "binance_spot", 0)
            .expect("trade series should register");
        store
            .put_trade_chunk("BTCUSDT", "binance_spot", &sample_trade_records())
            .expect("trade chunk should write");

        let mut cursor = store
            .replay_trade("BTCUSDT", "binance_spot", 1_706_745_600_000, None)
            .expect("trade replay should open");
        let batch = cursor.next_batch(10).expect("trade replay should read");

        assert_eq!(batch, sample_trade_records());
    }

    #[test]
    fn replay_bbo_across_day_partitions() {
        let temp_dir = TempDir::new().expect("temp dir should be created");
        let mut store = FastKStore::open(temp_dir.path()).expect("store should open");
        store.init().expect("store should init");
        store
            .register_bbo_series("BTCUSDT", "binance_spot", 0)
            .expect("bbo series should register");
        store
            .put_bbo_chunk("BTCUSDT", "binance_spot", &sample_bbo_records())
            .expect("bbo chunk should write");

        let mut cursor = store
            .replay_bbo("BTCUSDT", "binance_spot", 1_706_745_600_000, None)
            .expect("bbo replay should open");

        assert_eq!(
            cursor.next_batch(10).expect("bbo replay should read"),
            sample_bbo_records()
        );
    }

    #[test]
    fn replay_book_delta_batches_and_exhaustion_are_stable() {
        let temp_dir = TempDir::new().expect("temp dir should be created");
        let mut store = FastKStore::open(temp_dir.path()).expect("store should open");
        store.init().expect("store should init");
        store
            .register_book_delta_series("BTCUSDT", "binance_spot", 0)
            .expect("book delta series should register");
        store
            .put_book_delta_chunk("BTCUSDT", "binance_spot", &sample_book_delta_records())
            .expect("book delta chunk should write");

        let mut cursor = store
            .replay_book_delta("BTCUSDT", "binance_spot", 1_706_745_600_000, None)
            .expect("book delta replay should open");

        assert_eq!(
            cursor
                .next_batch(2)
                .expect("first batch should read")
                .iter()
                .map(|record| record.sequence)
                .collect::<Vec<_>>(),
            vec![100, 101]
        );
        assert_eq!(
            cursor
                .next_batch(2)
                .expect("second batch should read")
                .iter()
                .map(|record| record.sequence)
                .collect::<Vec<_>>(),
            vec![102, 103]
        );
        assert!(cursor
            .next_batch(2)
            .expect("tail batch should read")
            .is_empty());
        assert!(cursor.next_batch(2).expect("tail stays empty").is_empty());
        assert!(cursor.is_exhausted());
    }

    #[test]
    fn replay_respects_end_ts_and_empty_ranges() {
        let temp_dir = TempDir::new().expect("temp dir should be created");
        let mut store = FastKStore::open(temp_dir.path()).expect("store should open");
        store.init().expect("store should init");
        store
            .register_trade_series("BTCUSDT", "binance_spot", 0)
            .expect("trade series should register");
        store
            .put_trade_chunk("BTCUSDT", "binance_spot", &sample_trade_records())
            .expect("trade chunk should write");

        let mut bounded = store
            .replay_trade(
                "BTCUSDT",
                "binance_spot",
                1_706_745_600_000,
                Some(1_706_745_660_000),
            )
            .expect("bounded replay should open");
        assert_eq!(
            bounded
                .next_batch(10)
                .expect("bounded batch should read")
                .iter()
                .map(|record| record.trade_id)
                .collect::<Vec<_>>(),
            vec![1, 2]
        );

        let mut empty = store
            .replay_trade("BTCUSDT", "binance_spot", 1_800_000_000_000, None)
            .expect("empty replay should open");
        assert!(empty
            .next_batch(10)
            .expect("empty batch should read")
            .is_empty());
        assert!(empty.is_exhausted());
    }

    #[test]
    fn replay_invalid_range_and_zero_batch_are_errors() {
        let fixture = TestFixture::new().expect("fixture should be created");
        let err = fixture
            .store()
            .replay_kline("BTCUSDT", "1m", 1_706_745_900_000, Some(1_706_745_600_000))
            .expect_err("invalid range should fail");
        assert!(matches!(err, crate::FastKError::InvalidInput(_)));

        let mut cursor = fixture
            .store()
            .replay_kline("BTCUSDT", "1m", 1_706_745_600_000, None)
            .expect("cursor should open");
        let err = cursor
            .next_batch(0)
            .expect_err("zero max_records should fail");
        assert!(matches!(err, crate::FastKError::InvalidInput(_)));
    }

    #[test]
    fn replay_does_not_break_existing_range_query() {
        let fixture = TestFixture::new().expect("fixture should be created");
        let mut cursor = fixture
            .store()
            .replay_kline("BTCUSDT", "1m", 1_706_745_600_000, None)
            .expect("cursor should open");
        let replay_rows = cursor.next_batch(100).expect("replay should read");
        let range_rows = fixture
            .store()
            .get_kline_range("BTCUSDT", "1m", 1_706_745_600_000, 1_706_745_900_000)
            .expect("range should read");

        assert_eq!(replay_rows, range_rows);
    }

    #[test]
    fn replay_same_ts_preserves_file_order() {
        let temp_dir = TempDir::new().expect("temp dir should be created");
        let mut store = FastKStore::open(temp_dir.path()).expect("store should open");
        store.init().expect("store should init");
        store
            .register_trade_series("BTCUSDT", "binance_spot", 0)
            .expect("trade series should register");
        let records = sample_same_ts_trade_records();
        store
            .put_trade_chunk("BTCUSDT", "binance_spot", &records)
            .expect("same-ts trade chunk should write");

        let mut cursor = store
            .replay_trade("BTCUSDT", "binance_spot", 1_706_745_600_000, None)
            .expect("trade replay should open");

        assert_eq!(
            cursor
                .next_batch(10)
                .expect("same-ts replay should read")
                .iter()
                .map(|record| record.trade_id)
                .collect::<Vec<_>>(),
            vec![1, 2, 3]
        );
    }

    #[test]
    fn partition_policy_defaults_and_cross_partition_queries_work() {
        let temp_dir = TempDir::new().expect("temp dir should be created");
        let mut store = FastKStore::open(temp_dir.path()).expect("store should open");
        store.init().expect("store should init");
        store
            .register_kline_series("BTCUSDT", "1m", 60_000, 100_000, 100_000)
            .expect("kline series should register");
        let scalar_key = sample_scalar_key();
        store
            .register_scalar_series(&scalar_key, 60_000)
            .expect("scalar series should register");
        store
            .register_trade_series_with_partition(
                "ETHUSDT",
                "binance_spot",
                0,
                PartitionPolicy::hour(),
            )
            .expect("trade series should register");
        store
            .put_trade_chunk("ETHUSDT", "binance_spot", &sample_trade_records())
            .expect("trade chunk should write");

        assert_eq!(
            store
                .load_kline_meta("BTCUSDT", "1m")
                .expect("kline meta should load")
                .chunk_unit,
            "month"
        );
        assert_eq!(
            store
                .load_scalar_meta(&scalar_key)
                .expect("scalar meta should load")
                .chunk_unit,
            "month"
        );
        let trade_meta = store
            .load_fixed_meta("ETHUSDT", "trade", "binance_spot")
            .expect("trade meta should load");
        assert_eq!(trade_meta.chunk_unit, "hour");
        assert_eq!(trade_meta.chunks.len(), 2);
        assert_eq!(
            store
                .get_trade_range(
                    "ETHUSDT",
                    "binance_spot",
                    1_706_745_600_000,
                    1_706_749_320_000,
                )
                .expect("trade range should read")
                .len(),
            4
        );
    }

    #[test]
    fn session_prewarm_and_reset_modes_are_explicit() {
        let temp_dir = TempDir::new().expect("temp dir should be created");
        let mut store = FastKStore::open(temp_dir.path()).expect("store should open");
        store.init().expect("store should init");
        store.set_metrics_level(crate::MetricsLevel::Detailed);
        store
            .register_kline_series("BTCUSDT", "1m", 60_000, 100_000, 100_000)
            .expect("kline series should register");
        store
            .put_kline_chunk("BTCUSDT", "1m", &base_records())
            .expect("kline chunk should write");
        let key = sample_scalar_key();
        store
            .register_scalar_series(&key, 60_000)
            .expect("scalar series should register");
        store
            .put_scalar_chunk(&key, 60_000, &sample_scalar_records(), 2)
            .expect("scalar chunk should write");

        let mut session = store.read_session();
        session
            .attach_kline_series("BTCUSDT", "1m")
            .expect("attach should succeed");
        session
            .attach_scalar_series(&key)
            .expect("scalar attach should succeed");
        store.reset_metrics();
        session.prewarm().expect("prewarm should succeed");
        let warmed = store.metrics_snapshot();
        assert!(warmed.session_prewarm_ns > 0);

        session.reset(crate::SessionResetMode::QueryOnly);
        let query_only = session.cache_summary();
        assert_eq!(query_only.attached_kline_count, 1);
        assert_eq!(query_only.attached_scalar_count, 1);

        session.reset(crate::SessionResetMode::FullDetach);
        let detached = session.cache_summary();
        assert_eq!(detached.attached_kline_count, 0);
        assert_eq!(detached.attached_scalar_count, 0);
        assert!(session
            .attached_inventory()
            .expect("inventory should load")
            .is_empty());
    }

    struct TestFixture {
        temp_dir: TempDir,
        store: FastKStore,
        records: Vec<KlineRecord>,
    }

    impl TestFixture {
        fn new() -> crate::Result<Self> {
            let temp_dir = TempDir::new().map_err(crate::FastKError::from)?;
            let mut store = FastKStore::open(temp_dir.path())?;
            store.init()?;
            store.register_kline_series("BTCUSDT", "1m", 60_000, 100_000, 100_000)?;

            let records = base_records();
            store.put_kline_chunk("BTCUSDT", "1m", &records)?;

            Ok(Self {
                temp_dir,
                store,
                records,
            })
        }

        fn with_two_month_chunks() -> crate::Result<Self> {
            let temp_dir = TempDir::new().map_err(crate::FastKError::from)?;
            let mut store = FastKStore::open(temp_dir.path())?;
            store.init()?;
            store.register_kline_series("BTCUSDT", "1m", 60_000, 100_000, 100_000)?;

            let mut records = base_records();
            store.put_kline_chunk("BTCUSDT", "1m", &records)?;
            let march_records = vec![
                KlineRecord {
                    ts: 1_709_251_200_000,
                    open: 106,
                    high: 107,
                    low: 105,
                    close: 106,
                    volume: 16,
                },
                KlineRecord {
                    ts: 1_709_251_260_000,
                    open: 107,
                    high: 108,
                    low: 106,
                    close: 107,
                    volume: 17,
                },
                KlineRecord {
                    ts: 1_709_251_320_000,
                    open: 108,
                    high: 109,
                    low: 107,
                    close: 108,
                    volume: 18,
                },
            ];
            store.put_kline_chunk("BTCUSDT", "1m", &march_records)?;
            records.extend_from_slice(&march_records);

            Ok(Self {
                temp_dir,
                store,
                records,
            })
        }

        fn root(&self) -> &std::path::Path {
            self.temp_dir.path()
        }

        fn store(&self) -> &FastKStore {
            &self.store
        }

        fn records(&self) -> Vec<KlineRecord> {
            self.records.clone()
        }
    }

    fn base_records() -> Vec<KlineRecord> {
        vec![
            KlineRecord {
                ts: 1_706_745_600_000,
                open: 100,
                high: 101,
                low: 99,
                close: 100,
                volume: 10,
            },
            KlineRecord {
                ts: 1_706_745_660_000,
                open: 101,
                high: 102,
                low: 100,
                close: 101,
                volume: 11,
            },
            KlineRecord {
                ts: 1_706_745_720_000,
                open: 102,
                high: 103,
                low: 101,
                close: 102,
                volume: 12,
            },
            KlineRecord {
                ts: 1_706_745_780_000,
                open: 103,
                high: 104,
                low: 102,
                close: 103,
                volume: 13,
            },
            KlineRecord {
                ts: 1_706_745_840_000,
                open: 104,
                high: 105,
                low: 103,
                close: 104,
                volume: 14,
            },
            KlineRecord {
                ts: 1_706_745_900_000,
                open: 105,
                high: 106,
                low: 104,
                close: 105,
                volume: 15,
            },
        ]
    }

    fn sample_scalar_key() -> ScalarSeriesKey {
        ScalarSeriesKey {
            symbol: "BTCUSDT".to_string(),
            category: "indicator".to_string(),
            name: "rsi14".to_string(),
        }
    }

    fn feature_scalar_key(name: &str) -> ScalarSeriesKey {
        ScalarSeriesKey {
            symbol: "BTCUSDT".to_string(),
            category: "feature".to_string(),
            name: format!("1m@@{name}"),
        }
    }

    fn factor_scalar_key(name: &str) -> ScalarSeriesKey {
        ScalarSeriesKey {
            symbol: "BTCUSDT".to_string(),
            category: "factor".to_string(),
            name: format!("1m@@{name}"),
        }
    }

    fn scalar_matches_to_records(matches: &[crate::ScalarPredicateMatch]) -> Vec<ScalarRecord> {
        matches
            .iter()
            .filter_map(|entry| {
                entry.value.map(|value| ScalarRecord {
                    ts: entry.ts,
                    value,
                })
            })
            .collect()
    }

    fn sample_scalar_records() -> Vec<ScalarRecord> {
        vec![
            ScalarRecord {
                ts: 1_706_745_600_000,
                value: 10,
            },
            ScalarRecord {
                ts: 1_706_745_660_000,
                value: 15,
            },
            ScalarRecord {
                ts: 1_706_745_720_000,
                value: 20,
            },
            ScalarRecord {
                ts: 1_706_745_780_000,
                value: 30,
            },
            ScalarRecord {
                ts: 1_706_745_840_000,
                value: 40,
            },
            ScalarRecord {
                ts: 1_706_745_900_000,
                value: 50,
            },
        ]
    }

    fn sample_trade_records() -> Vec<TradeRecord> {
        vec![
            TradeRecord {
                ts: 1_706_745_600_000,
                recv_ts: 1_706_745_600_010,
                trade_id: 1,
                price: 10_000,
                qty: 100,
                side: 1,
                flags: 0,
                _pad: [0; 6],
            },
            TradeRecord {
                ts: 1_706_745_660_000,
                recv_ts: 1_706_745_660_010,
                trade_id: 2,
                price: 10_010,
                qty: 101,
                side: -1,
                flags: 0,
                _pad: [0; 6],
            },
            TradeRecord {
                ts: 1_706_749_200_000,
                recv_ts: 1_706_749_200_010,
                trade_id: 3,
                price: 10_020,
                qty: 102,
                side: 1,
                flags: 1,
                _pad: [0; 6],
            },
            TradeRecord {
                ts: 1_706_749_260_000,
                recv_ts: 1_706_749_260_010,
                trade_id: 4,
                price: 10_030,
                qty: 103,
                side: 0,
                flags: 1,
                _pad: [0; 6],
            },
        ]
    }

    fn sample_trade_id_gap_records() -> Vec<TradeRecord> {
        vec![
            TradeRecord {
                ts: 1_706_745_600_000,
                recv_ts: 1_706_745_600_010,
                trade_id: 1,
                price: 10_000,
                qty: 100,
                side: 1,
                flags: 0,
                _pad: [0; 6],
            },
            TradeRecord {
                ts: 1_706_745_660_000,
                recv_ts: 1_706_745_660_010,
                trade_id: 2,
                price: 10_010,
                qty: 101,
                side: -1,
                flags: 0,
                _pad: [0; 6],
            },
            TradeRecord {
                ts: 1_706_745_720_000,
                recv_ts: 1_706_745_720_010,
                trade_id: 4,
                price: 10_020,
                qty: 102,
                side: 1,
                flags: 0,
                _pad: [0; 6],
            },
        ]
    }

    fn sample_same_ts_trade_records() -> Vec<TradeRecord> {
        vec![
            TradeRecord {
                ts: 1_706_745_600_000,
                recv_ts: 1_706_745_600_010,
                trade_id: 1,
                price: 10_000,
                qty: 100,
                side: 1,
                flags: 0,
                _pad: [0; 6],
            },
            TradeRecord {
                ts: 1_706_745_600_000,
                recv_ts: 1_706_745_600_011,
                trade_id: 2,
                price: 10_001,
                qty: 101,
                side: -1,
                flags: 0,
                _pad: [0; 6],
            },
            TradeRecord {
                ts: 1_706_745_660_000,
                recv_ts: 1_706_745_660_010,
                trade_id: 3,
                price: 10_010,
                qty: 102,
                side: 1,
                flags: 0,
                _pad: [0; 6],
            },
        ]
    }

    fn sample_bbo_records() -> Vec<BboRecord> {
        vec![
            BboRecord {
                ts: 1_706_745_600_000,
                recv_ts: 1_706_745_600_010,
                bid_price: 9_990,
                bid_qty: 100,
                ask_price: 10_010,
                ask_qty: 101,
                sequence: 1,
            },
            BboRecord {
                ts: 1_706_832_000_000,
                recv_ts: 1_706_832_000_010,
                bid_price: 10_090,
                bid_qty: 110,
                ask_price: 10_110,
                ask_qty: 111,
                sequence: 2,
            },
        ]
    }

    fn sample_bbo_sequence_issues() -> Vec<BboRecord> {
        vec![
            BboRecord {
                ts: 1_706_745_600_000,
                recv_ts: 1_706_745_600_010,
                bid_price: 9_990,
                bid_qty: 100,
                ask_price: 10_010,
                ask_qty: 101,
                sequence: 1,
            },
            BboRecord {
                ts: 1_706_745_660_000,
                recv_ts: 1_706_745_660_010,
                bid_price: 9_991,
                bid_qty: 100,
                ask_price: 10_011,
                ask_qty: 101,
                sequence: 2,
            },
            BboRecord {
                ts: 1_706_745_720_000,
                recv_ts: 1_706_745_720_010,
                bid_price: 9_992,
                bid_qty: 100,
                ask_price: 10_012,
                ask_qty: 101,
                sequence: 4,
            },
            BboRecord {
                ts: 1_706_745_780_000,
                recv_ts: 1_706_745_780_010,
                bid_price: 9_993,
                bid_qty: 100,
                ask_price: 10_013,
                ask_qty: 101,
                sequence: 4,
            },
            BboRecord {
                ts: 1_706_745_840_000,
                recv_ts: 1_706_745_840_010,
                bid_price: 9_994,
                bid_qty: 100,
                ask_price: 10_014,
                ask_qty: 101,
                sequence: 3,
            },
        ]
    }

    fn sample_book_delta_records() -> Vec<BookDeltaRecord> {
        vec![
            BookDeltaRecord {
                ts: 1_706_745_600_000,
                recv_ts: 1_706_745_600_010,
                sequence: 100,
                price: 9_990,
                qty: 10,
                side: 1,
                action: 1,
                level: 1,
                flags: 0,
            },
            BookDeltaRecord {
                ts: 1_706_745_660_000,
                recv_ts: 1_706_745_660_010,
                sequence: 101,
                price: 10_010,
                qty: 0,
                side: -1,
                action: 2,
                level: 1,
                flags: 0,
            },
            BookDeltaRecord {
                ts: 1_706_749_200_000,
                recv_ts: 1_706_749_200_010,
                sequence: 102,
                price: 10_020,
                qty: 12,
                side: 1,
                action: 1,
                level: 2,
                flags: 1,
            },
            BookDeltaRecord {
                ts: 1_706_749_260_000,
                recv_ts: 1_706_749_260_010,
                sequence: 103,
                price: 10_030,
                qty: 13,
                side: -1,
                action: 1,
                level: 2,
                flags: 1,
            },
        ]
    }

    fn sample_march_scalar_records() -> Vec<ScalarRecord> {
        vec![
            ScalarRecord {
                ts: 1_709_251_200_000,
                value: 60,
            },
            ScalarRecord {
                ts: 1_709_251_260_000,
                value: 70,
            },
        ]
    }
}
