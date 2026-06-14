mod backtest;
mod catalog;
mod compaction;
mod derived;
mod replay;
mod sequence;
mod store;

pub use backtest::{
    BacktestCapabilitySnapshot, BacktestKlineBinding, BacktestPreparePlan, BacktestScalarBinding,
    BacktestStoreView,
};
pub(crate) use catalog::Catalog;
pub use compaction::{
    can_transition as can_chunk_state_transition, CompactionDecision, CompactionPolicy,
};
pub use derived::{
    factor_series_key, feature_series_key, indicator_series_key, metric_series_key,
    portfolio_series_key, reserved_scalar_categories, risk_series_key, scoped_scalar_series_key,
    signal_series_key, DerivedSeriesBuilder, DerivedSeriesRequest, ScopedScalarBinding,
    FACTOR_CATEGORY, FEATURE_CATEGORY, INDICATOR_CATEGORY, METRIC_CATEGORY, PORTFOLIO_CATEGORY,
    RISK_CATEGORY, SIGNAL_CATEGORY,
};
pub use replay::{ReplayCursor, ReplayOptions};
pub use sequence::{SequenceDuplicate, SequenceGap, SequenceScanReport, SequenceViolation};
pub use store::{
    FastKReadSession, FastKStore, IndicatorInventoryEntry, MonthInventoryEntry,
    ScalarQueryCapabilities, SeriesInventoryEntry, SessionCacheSummary, SessionResetMode,
    StoreHealthSummary, StoreStats, DEFAULT_SCALAR_ZMAP_BLOCK_SIZE,
};
