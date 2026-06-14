//! FastK is a read-heavy, fixed-record, chunk-based time-series storage engine.
//!
//! FastK only stores and reads already-normalized records. It does not own market-data
//! ingestion, indicator/feature/factor calculation, strategy research, backtest execution,
//! order handling, live trading, alerting, or dashboards.
//!
//! Recommended stable entry points:
//!
//! - [`FastKStore`] for register/read/write and validation flows
//! - [`BacktestStoreView`] for storage-level read sessions used by backtest adapters
//! - [`KlineRecord`], [`ScalarRecord`], [`ScalarSeriesKey`] for data exchange
//! - bridge helpers such as [`bridge_read_kline_range`] when you need JSON-oriented integration
//!
//! FastK intentionally does **not** calculate indicators, features, or factors. The expected
//! workflow is:
//!
//! 1. read indicator/scalar coverage
//! 2. if missing, read the required kline window
//! 3. calculate the indicator in an upper-layer module
//! 4. write the derived scalar series back into FastK
//!
//! Minimal Rust usage:
//!
//! ```no_run
//! use fastk::{FastKStore, KlineRecord};
//!
//! fn main() -> fastk::Result<()> {
//!     let mut store = FastKStore::open("data/fastk")?;
//!     store.init()?;
//!     store.register_kline_series("BTCUSDT", "1m", 60_000, 100_000, 100_000)?;
//!     store.put_kline_chunk(
//!         "BTCUSDT",
//!         "1m",
//!         &[KlineRecord {
//!             ts: 1_706_745_600_000,
//!             open: 10_000_000,
//!             high: 10_020_000,
//!             low: 9_980_000,
//!             close: 10_010_000,
//!             volume: 10_000,
//!         }],
//!     )?;
//!     let rows = store.get_kline_range("BTCUSDT", "1m", 1_706_745_600_000, 1_706_745_600_000)?;
//!     println!("rows={}", rows.len());
//!     Ok(())
//! }
//! ```

pub mod benchmark;
pub mod bridge;
pub mod control;
mod error;
pub mod feature;
pub mod kline;
mod metrics;
mod store_core;
mod types;

pub(crate) use store_core::{chunk, engine, index, storage};

pub use benchmark::{
    build_acceptance_matrix, BenchmarkMetrics, LatencySummary, MetricsAccumulator, Temperature,
    WorkloadDescriptor,
};
pub use bridge::{
    indicator_inventory as bridge_indicator_inventory, kline_inventory as bridge_kline_inventory,
    query_scalar_predicate as bridge_query_scalar_predicate,
    read_indicator_range as bridge_read_indicator_range,
    read_kline_range as bridge_read_kline_range, read_scalar_range as bridge_read_scalar_range,
    scalar_inventory as bridge_scalar_inventory,
    write_indicator_range as bridge_write_indicator_range,
    write_kline_range as bridge_write_kline_range, write_scalar_range as bridge_write_scalar_range,
    IndicatorRangeResponse, InventoryResponse, KlineBridgeRecord, KlineInventoryResponse,
    KlineRangeResponse, ScalarBridgeRecord, ScalarPredicateQueryResponse, ScalarRangeResponse,
    WriteIndicatorRequest, WriteIndicatorResponse, WriteKlineRequest, WriteKlineResponse,
    WriteScalarRequest, WriteScalarResponse,
};
pub use control::{
    versioned_dataset_root, DatasetManifestRecord, DatasetRef, DatasetRegistry,
    FactorRegistryRecord, FeatureRegistryRecord,
};
pub use engine::{
    can_chunk_state_transition, factor_series_key, feature_series_key, indicator_series_key,
    metric_series_key, portfolio_series_key, reserved_scalar_categories, risk_series_key,
    scoped_scalar_series_key, signal_series_key, BacktestCapabilitySnapshot, BacktestKlineBinding,
    BacktestPreparePlan, BacktestScalarBinding, BacktestStoreView, CompactionDecision,
    CompactionPolicy, DerivedSeriesBuilder, DerivedSeriesRequest, FastKReadSession, FastKStore,
    IndicatorInventoryEntry, MonthInventoryEntry, ReplayCursor, ReplayOptions,
    ScalarQueryCapabilities, ScopedScalarBinding, SequenceDuplicate, SequenceGap,
    SequenceScanReport, SequenceViolation, SeriesInventoryEntry, SessionCacheSummary,
    SessionResetMode, StoreHealthSummary, StoreStats, DEFAULT_SCALAR_ZMAP_BLOCK_SIZE,
    FACTOR_CATEGORY, FEATURE_CATEGORY, INDICATOR_CATEGORY, METRIC_CATEGORY, PORTFOLIO_CATEGORY,
    RISK_CATEGORY, SIGNAL_CATEGORY,
};
pub use error::{FastKError, Result};
pub use feature::{
    CompareOp, ScalarIndexKind, ScalarPredicate, ScalarPredicateExpr, ScalarPredicateMatch,
    ScalarPredicateQuery, ScalarPredicateQueryResult, ScalarPredicateQueryStats, ScalarRecord,
    ScalarSeriesKey,
};
pub use index::{PredicateQueryEngine, ValueIndexEntry, ZoneMapEntry};
pub use kline::KlineRecord;
pub use metrics::{MetricsLevel, StoreMetricsSnapshot};
pub use storage::recovery::{explain_overlaps, rebuild_all_manifests_from_fs};
pub use storage::recovery::{
    ManifestValidation, OrphanArtifact, OverlapResolution, RecoveryOptions, RecoveryReport,
    ScrubSeriesReport, SeriesOverlapExplanation, ValidationOptions,
};
pub use types::{
    BboRecord, BookDeltaRecord, ChunkMeta, ChunkState, PartitionPolicy, PartitionUnit, RecordType,
    TradeRecord,
};
