use crate::engine::derived::{indicator_series_key, scoped_scalar_series_key, ScopedScalarBinding};
use crate::engine::store::{
    FastKReadSession, FastKStore, ScalarQueryCapabilities, SeriesInventoryEntry,
    SessionCacheSummary, SessionResetMode, StoreHealthSummary, StoreStats,
};
use crate::error::Result;
use crate::types::{
    KlineRecord, ScalarPredicate, ScalarPredicateExpr, ScalarPredicateQuery,
    ScalarPredicateQueryResult, ScalarRecord, ScalarSeriesKey,
};

/// Storage-level kline binding used by backtest adapter initialization plans.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BacktestKlineBinding {
    pub symbol: String,
    pub timeframe: String,
}

/// Storage-level scalar binding scoped by `(symbol, timeframe, category, name)`.
pub type BacktestScalarBinding = ScopedScalarBinding;

/// Batch initialization plan for a storage read view used by backtest adapters.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct BacktestPreparePlan {
    pub kline: Vec<BacktestKlineBinding>,
    pub scalar: Vec<BacktestScalarBinding>,
    pub prewarm: bool,
}

/// Session-scoped storage snapshot exposed to upper-layer backtest adapters.
#[derive(Debug, Clone, PartialEq)]
pub struct BacktestCapabilitySnapshot {
    pub inventory: Vec<SeriesInventoryEntry>,
    pub health: StoreHealthSummary,
    pub scalar_capabilities: Vec<(ScalarSeriesKey, ScalarQueryCapabilities)>,
    pub cache: SessionCacheSummary,
    pub store_stats: StoreStats,
}

/// Lightweight storage read facade over [`FastKReadSession`].
///
/// This type does not execute backtests, drive strategy clocks, calculate signals, match orders,
/// or own portfolio/accounting logic. It exists only to make repeated storage reads and prewarming
/// convenient for an upper-layer backtest adapter.
#[derive(Debug)]
pub struct BacktestStoreView<'a> {
    session: FastKReadSession<'a>,
}

impl<'a> BacktestStoreView<'a> {
    pub(crate) fn new(store: &'a FastKStore) -> Self {
        Self {
            session: store.read_session(),
        }
    }

    /// Applies a batched attach plan and optionally prewarms the read path.
    pub fn initialize(&mut self, plan: &BacktestPreparePlan) -> Result<&mut Self> {
        if !plan.kline.is_empty() {
            self.attach_kline_many(
                plan.kline
                    .iter()
                    .map(|binding| (binding.symbol.as_str(), binding.timeframe.as_str())),
            )?;
        }
        if !plan.scalar.is_empty() {
            self.attach_scalar_many(plan.scalar.clone())?;
        }
        if plan.prewarm {
            self.prewarm()?;
        }
        Ok(self)
    }

    /// Attaches one kline series.
    pub fn attach_kline_series(&mut self, symbol: &str, timeframe: &str) -> Result<&mut Self> {
        self.session.attach_kline_series(symbol, timeframe)?;
        Ok(self)
    }

    /// Release-facing alias for attaching one kline series.
    pub fn attach_kline(&mut self, symbol: &str, timeframe: &str) -> Result<&mut Self> {
        self.attach_kline_series(symbol, timeframe)
    }

    /// Attaches many kline series in one call.
    pub fn attach_kline_many<I, S>(&mut self, bindings: I) -> Result<&mut Self>
    where
        I: IntoIterator<Item = (S, S)>,
        S: AsRef<str>,
    {
        self.session.attach_kline_many(bindings)?;
        Ok(self)
    }

    /// Attaches multiple symbols that share one timeframe.
    pub fn attach_symbols_for_timeframe<I, S>(
        &mut self,
        timeframe: &str,
        symbols: I,
    ) -> Result<&mut Self>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        for symbol in symbols {
            self.session
                .attach_kline_series(symbol.as_ref(), timeframe)?;
        }
        Ok(self)
    }

    /// Attaches one scalar series.
    pub fn attach_scalar_series(&mut self, series_key: &ScalarSeriesKey) -> Result<&mut Self> {
        self.session.attach_scalar_series(series_key)?;
        Ok(self)
    }

    /// Attaches one scalar series through `(symbol, timeframe, category, name)`.
    pub fn attach_scalar(
        &mut self,
        symbol: &str,
        timeframe: &str,
        category: &str,
        name: &str,
    ) -> Result<&mut Self> {
        let key = scoped_scalar_series_key(symbol, timeframe, category, name);
        self.attach_scalar_series(&key)
    }

    /// Attaches one scalar series from an already-built key.
    pub fn attach_scalar_key(&mut self, series_key: &ScalarSeriesKey) -> Result<&mut Self> {
        self.attach_scalar_series(series_key)
    }

    /// Attaches one indicator series under `category="indicator"`.
    pub fn attach_indicator(
        &mut self,
        symbol: &str,
        timeframe: &str,
        indicator_name: &str,
    ) -> Result<&mut Self> {
        let key = indicator_series_key(symbol, timeframe, indicator_name);
        self.attach_scalar_series(&key)
    }

    /// Attaches many scalar series in one call.
    pub fn attach_scalar_many<I>(&mut self, series: I) -> Result<&mut Self>
    where
        I: IntoIterator<Item = BacktestScalarBinding>,
    {
        self.session
            .attach_scalar_many(series.into_iter().map(|binding| binding.to_series_key()))?;
        Ok(self)
    }

    /// Prewarms attached chunks and sidecars.
    pub fn prewarm(&self) -> Result<()> {
        self.session.prewarm()
    }

    /// Returns a snapshot of attached series inventory.
    pub fn inventory_snapshot(&self) -> Result<Vec<SeriesInventoryEntry>> {
        self.session.attached_inventory()
    }

    /// Returns a scoped health summary over attached series.
    pub fn health_check(&self) -> Result<StoreHealthSummary> {
        self.session.attached_health_summary()
    }

    /// Returns a scoped health snapshot over attached series.
    pub fn health_snapshot(&self) -> Result<StoreHealthSummary> {
        self.health_check()
    }

    /// Returns session-level capabilities and cache state for attached series.
    pub fn capability_snapshot(&self) -> Result<BacktestCapabilitySnapshot> {
        Ok(BacktestCapabilitySnapshot {
            inventory: self.session.attached_inventory()?,
            health: self.session.attached_health_summary()?,
            scalar_capabilities: self.session.attached_scalar_capabilities()?,
            cache: self.session.cache_summary(),
            store_stats: self.session.store().store_stats()?,
        })
    }

    /// Returns session cache counters and metrics.
    pub fn cache_summary(&self) -> SessionCacheSummary {
        self.session.cache_summary()
    }

    /// Resets the underlying read session.
    pub fn reset(&mut self, mode: SessionResetMode) {
        self.session.reset(mode);
    }

    /// Clears query-local caches while preserving attached series.
    pub fn reset_query_cache(&mut self) {
        self.reset(SessionResetMode::QueryOnly);
    }

    /// Clears logical caches while preserving attached series.
    pub fn reset_logical_cache(&mut self) {
        self.reset(SessionResetMode::Logical);
    }

    /// Drops all attached series and logical cache state.
    pub fn full_detach(&mut self) {
        self.reset(SessionResetMode::FullDetach);
    }

    /// Exposes the underlying read session for advanced callers.
    pub fn session(&self) -> &FastKReadSession<'a> {
        &self.session
    }

    /// Exposes the underlying read session mutably for advanced callers.
    pub fn session_mut(&mut self) -> &mut FastKReadSession<'a> {
        &mut self.session
    }

    pub fn get_kline_at(
        &self,
        symbol: &str,
        timeframe: &str,
        ts: i64,
    ) -> Result<Option<KlineRecord>> {
        self.session.get_kline_at(symbol, timeframe, ts)
    }

    pub fn get_kline_range(
        &self,
        symbol: &str,
        timeframe: &str,
        start_ts: i64,
        end_ts: i64,
    ) -> Result<Vec<KlineRecord>> {
        self.session
            .get_kline_range(symbol, timeframe, start_ts, end_ts)
    }

    pub fn get_latest_n(
        &self,
        symbol: &str,
        timeframe: &str,
        n: usize,
    ) -> Result<Vec<KlineRecord>> {
        self.session.get_kline_latest_n(symbol, timeframe, n)
    }

    pub fn get_scalar_at(
        &self,
        symbol: &str,
        timeframe: &str,
        category: &str,
        name: &str,
        ts: i64,
    ) -> Result<Option<ScalarRecord>> {
        let key = scoped_scalar_series_key(symbol, timeframe, category, name);
        self.session.get_scalar_at(&key, ts)
    }

    pub fn get_scalar_at_key(
        &self,
        series_key: &ScalarSeriesKey,
        ts: i64,
    ) -> Result<Option<ScalarRecord>> {
        self.session.get_scalar_at(series_key, ts)
    }

    pub fn get_scalar_range(
        &self,
        symbol: &str,
        timeframe: &str,
        category: &str,
        name: &str,
        start_ts: i64,
        end_ts: i64,
    ) -> Result<Vec<ScalarRecord>> {
        let key = scoped_scalar_series_key(symbol, timeframe, category, name);
        self.session.get_scalar_range(&key, start_ts, end_ts)
    }

    pub fn get_scalar_range_key(
        &self,
        series_key: &ScalarSeriesKey,
        start_ts: i64,
        end_ts: i64,
    ) -> Result<Vec<ScalarRecord>> {
        self.session.get_scalar_range(series_key, start_ts, end_ts)
    }

    pub fn find_scalar_timestamps_via_zmap(
        &self,
        symbol: &str,
        timeframe: &str,
        category: &str,
        name: &str,
        predicate: &ScalarPredicate,
        start_ts: i64,
        end_ts: i64,
    ) -> Result<Vec<i64>> {
        let key = scoped_scalar_series_key(symbol, timeframe, category, name);
        self.session
            .find_scalar_timestamps_via_zmap(&key, predicate, start_ts, end_ts)
    }

    pub fn find_scalar_timestamps_via_zmap_key(
        &self,
        series_key: &ScalarSeriesKey,
        predicate: &ScalarPredicate,
        start_ts: i64,
        end_ts: i64,
    ) -> Result<Vec<i64>> {
        self.session
            .find_scalar_timestamps_via_zmap(series_key, predicate, start_ts, end_ts)
    }

    pub fn find_scalar_timestamps_via_vix(
        &self,
        symbol: &str,
        timeframe: &str,
        category: &str,
        name: &str,
        predicate: &ScalarPredicate,
        start_ts: i64,
        end_ts: i64,
    ) -> Result<Vec<i64>> {
        let key = scoped_scalar_series_key(symbol, timeframe, category, name);
        self.session
            .find_scalar_timestamps_via_vix(&key, predicate, start_ts, end_ts)
    }

    pub fn find_scalar_timestamps_via_vix_key(
        &self,
        series_key: &ScalarSeriesKey,
        predicate: &ScalarPredicate,
        start_ts: i64,
        end_ts: i64,
    ) -> Result<Vec<i64>> {
        self.session
            .find_scalar_timestamps_via_vix(series_key, predicate, start_ts, end_ts)
    }

    pub fn find_scalar_timestamps_raw(
        &self,
        symbol: &str,
        timeframe: &str,
        category: &str,
        name: &str,
        predicate: &ScalarPredicate,
        start_ts: i64,
        end_ts: i64,
    ) -> Result<Vec<i64>> {
        let key = scoped_scalar_series_key(symbol, timeframe, category, name);
        self.session
            .find_scalar_timestamps_raw(&key, predicate, start_ts, end_ts)
    }

    pub fn find_scalar_timestamps_raw_key(
        &self,
        series_key: &ScalarSeriesKey,
        predicate: &ScalarPredicate,
        start_ts: i64,
        end_ts: i64,
    ) -> Result<Vec<i64>> {
        self.session
            .find_scalar_timestamps_raw(series_key, predicate, start_ts, end_ts)
    }

    /// Runs a storage-level scalar predicate query through the read session.
    ///
    /// This only compares stored `ScalarRecord.value` integers. It does not
    /// execute backtests or interpret feature, factor, signal, or strategy semantics.
    pub fn query_scalar_predicate(
        &self,
        key: &ScalarSeriesKey,
        start_ts: i64,
        end_ts: i64,
        predicate: ScalarPredicateExpr,
    ) -> Result<ScalarPredicateQueryResult> {
        self.session.query_scalar_predicate(ScalarPredicateQuery {
            key: key.clone(),
            start_ts,
            end_ts,
            predicate,
            return_values: true,
        })
    }

    pub fn query_scalar_predicate_with_options(
        &self,
        query: ScalarPredicateQuery,
    ) -> Result<ScalarPredicateQueryResult> {
        self.session.query_scalar_predicate(query)
    }

    pub fn get_indicator_at(
        &self,
        symbol: &str,
        timeframe: &str,
        indicator_name: &str,
        ts: i64,
    ) -> Result<Option<ScalarRecord>> {
        let key = indicator_series_key(symbol, timeframe, indicator_name);
        self.session.get_scalar_at(&key, ts)
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
        self.session.get_scalar_range(&key, start_ts, end_ts)
    }

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
        self.session
            .find_scalar_timestamps_via_zmap(&key, predicate, start_ts, end_ts)
    }

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
        self.session
            .find_scalar_timestamps_via_vix(&key, predicate, start_ts, end_ts)
    }
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;

    use super::{BacktestKlineBinding, BacktestPreparePlan};
    use crate::{
        CompareOp, FastKStore, MetricsLevel, ScalarPredicate, ScalarPredicateExpr, ScalarRecord,
    };

    #[test]
    fn facade_initialize_and_prewarm_cover_attached_series() {
        let temp_dir = TempDir::new().expect("temp dir should be created");
        let mut store = FastKStore::open(temp_dir.path()).expect("store should open");
        store.init().expect("store should init");
        store
            .register_kline_series("BTCUSDT", "1m", 60_000, 100_000, 100_000)
            .expect("kline series should register");
        store
            .put_kline_chunk("BTCUSDT", "1m", &sample_kline_records())
            .expect("kline chunk should write");
        let scalar_binding = sample_scalar_binding();
        let scalar_key = scalar_binding.to_series_key();
        store
            .register_scalar_series(&scalar_key, 60_000)
            .expect("scalar series should register");
        store
            .put_scalar_chunk(&scalar_key, 60_000, &sample_scalar_records(), 2)
            .expect("scalar chunk should write");

        let mut facade = store.backtest_view();
        facade
            .initialize(&BacktestPreparePlan {
                kline: vec![BacktestKlineBinding {
                    symbol: "BTCUSDT".to_string(),
                    timeframe: "1m".to_string(),
                }],
                scalar: vec![scalar_binding.clone()],
                prewarm: true,
            })
            .expect("facade should initialize");

        let snapshot = facade
            .capability_snapshot()
            .expect("capability snapshot should load");
        assert_eq!(snapshot.inventory.len(), 2);
        assert_eq!(snapshot.scalar_capabilities.len(), 1);
        assert_eq!(snapshot.health.issue_series_count, 0);

        let row = facade
            .get_kline_at("BTCUSDT", "1m", 1_706_745_720_000)
            .expect("point query should succeed")
            .expect("row should exist");
        assert_eq!(row.close, 102);

        let scalar_rows = facade
            .get_scalar_range(
                "BTCUSDT",
                "1m",
                "indicator",
                "rsi14",
                1_706_745_660_000,
                1_706_745_840_000,
            )
            .expect("scalar range query should succeed");
        assert_eq!(scalar_rows.len(), 4);
        assert_eq!(scalar_rows[0].value, 15);
        assert_eq!(scalar_rows[3].value, 40);
    }

    #[test]
    fn facade_scalar_queries_match_raw_paths() {
        let temp_dir = TempDir::new().expect("temp dir should be created");
        let mut store = FastKStore::open(temp_dir.path()).expect("store should open");
        store.init().expect("store should init");
        let scalar_binding = sample_scalar_binding();
        let scalar_key = scalar_binding.to_series_key();
        store
            .register_scalar_series(&scalar_key, 60_000)
            .expect("scalar series should register");
        store
            .put_scalar_chunk(&scalar_key, 60_000, &sample_scalar_records(), 2)
            .expect("scalar chunk should write");

        let mut facade = store.backtest_view();
        facade
            .attach_scalar("BTCUSDT", "1m", "indicator", "rsi14")
            .expect("scalar attach should succeed");
        facade.prewarm().expect("prewarm should succeed");

        let predicate = ScalarPredicate {
            op: CompareOp::Between,
            value: 15,
            value2: Some(40),
        };
        let zmap = facade
            .find_scalar_timestamps_via_zmap(
                "BTCUSDT",
                "1m",
                "indicator",
                "rsi14",
                &predicate,
                1_706_745_600_000,
                1_706_745_900_000,
            )
            .expect("zmap query should succeed");
        let vix = facade
            .find_scalar_timestamps_via_vix(
                "BTCUSDT",
                "1m",
                "indicator",
                "rsi14",
                &predicate,
                1_706_745_600_000,
                1_706_745_900_000,
            )
            .expect("vix query should succeed");
        let raw = facade
            .find_scalar_timestamps_raw(
                "BTCUSDT",
                "1m",
                "indicator",
                "rsi14",
                &predicate,
                1_706_745_600_000,
                1_706_745_900_000,
            )
            .expect("raw query should succeed");

        assert_eq!(zmap, raw);
        assert_eq!(vix, raw);
    }

    #[test]
    fn facade_query_scalar_predicate_stays_storage_level() {
        let temp_dir = TempDir::new().expect("temp dir should be created");
        let mut store = FastKStore::open(temp_dir.path()).expect("store should open");
        store.init().expect("store should init");
        let scalar_binding = super::BacktestScalarBinding::feature("BTCUSDT", "1m", "rsi_14");
        let scalar_key = scalar_binding.to_series_key();
        store
            .register_scalar_series(&scalar_key, 60_000)
            .expect("scalar series should register");
        store
            .put_scalar_chunk(&scalar_key, 60_000, &sample_scalar_records(), 2)
            .expect("scalar chunk should write");

        let mut facade = store.backtest_view();
        facade
            .attach_scalar_key(&scalar_key)
            .expect("scalar attach should succeed");
        let result = facade
            .query_scalar_predicate(
                &scalar_key,
                1_706_745_600_000,
                1_706_745_900_000,
                ScalarPredicateExpr::Gt(30),
            )
            .expect("predicate query should succeed");

        assert_eq!(result.matches.len(), 2);
        assert!(result.matches.iter().all(|entry| entry.value.is_some()));
        assert_eq!(
            result
                .matches
                .iter()
                .map(|entry| entry.value.expect("value returned"))
                .collect::<Vec<_>>(),
            vec![40, 50]
        );
    }

    #[test]
    fn facade_attaches_feature_and_factor_scalar_bindings() {
        let temp_dir = TempDir::new().expect("temp dir should be created");
        let mut store = FastKStore::open(temp_dir.path()).expect("store should open");
        store.init().expect("store should init");

        let feature_binding = super::BacktestScalarBinding::feature("BTCUSDT", "1m", "rsi_14");
        let factor_binding =
            super::BacktestScalarBinding::factor("BTCUSDT", "1m", "momentum_score");
        let feature_key = feature_binding.to_series_key();
        let factor_key = factor_binding.to_series_key();

        store
            .register_scalar_series(&feature_key, 60_000)
            .expect("feature series should register");
        store
            .register_scalar_series(&factor_key, 60_000)
            .expect("factor series should register");
        store
            .put_scalar_chunk(&feature_key, 60_000, &sample_scalar_records(), 2)
            .expect("feature chunk should write");
        store
            .put_scalar_chunk(&factor_key, 60_000, &sample_factor_records(), 2)
            .expect("factor chunk should write");

        let mut facade = store.backtest_view();
        facade
            .initialize(&BacktestPreparePlan {
                kline: Vec::new(),
                scalar: vec![feature_binding, factor_binding],
                prewarm: true,
            })
            .expect("facade should initialize");

        let feature_rows = facade
            .get_scalar_range(
                "BTCUSDT",
                "1m",
                "feature",
                "rsi_14",
                1_706_745_660_000,
                1_706_745_840_000,
            )
            .expect("feature range should load");
        let factor_rows = facade
            .get_scalar_range(
                "BTCUSDT",
                "1m",
                "factor",
                "momentum_score",
                1_706_745_660_000,
                1_706_745_840_000,
            )
            .expect("factor range should load");

        assert_eq!(feature_rows.len(), 4);
        assert_eq!(feature_rows[0].value, 15);
        assert_eq!(factor_rows.len(), 4);
        assert_eq!(factor_rows[0].value, 115);

        let snapshot = facade
            .capability_snapshot()
            .expect("capability snapshot should load");
        assert_eq!(snapshot.scalar_capabilities.len(), 2);
    }

    #[test]
    fn facade_alias_methods_and_reset_modes_work() {
        let temp_dir = TempDir::new().expect("temp dir should be created");
        let mut store = FastKStore::open(temp_dir.path()).expect("store should open");
        store.init().expect("store should init");
        store
            .register_kline_series("BTCUSDT", "1m", 60_000, 100_000, 100_000)
            .expect("kline series should register");
        store
            .put_kline_chunk("BTCUSDT", "1m", &sample_kline_records())
            .expect("kline chunk should write");
        let scalar_binding = sample_scalar_binding();
        let scalar_key = scalar_binding.to_series_key();
        store
            .register_scalar_series(&scalar_key, 60_000)
            .expect("scalar series should register");
        store
            .put_scalar_chunk(&scalar_key, 60_000, &sample_scalar_records(), 2)
            .expect("scalar chunk should write");

        let mut facade = store.backtest_view();
        facade
            .attach_kline("BTCUSDT", "1m")
            .expect("kline attach should succeed");
        facade
            .attach_indicator("BTCUSDT", "1m", "rsi14")
            .expect("scalar attach should succeed");

        let health = facade
            .health_snapshot()
            .expect("health snapshot should load");
        assert_eq!(health.series_count, 2);
        assert_eq!(health.issue_series_count, 0);

        let cache = facade.cache_summary();
        assert_eq!(cache.attached_kline_count, 1);
        assert_eq!(cache.attached_scalar_count, 1);

        facade.reset_query_cache();
        let query_only = facade.cache_summary();
        assert_eq!(query_only.attached_kline_count, 1);
        assert_eq!(query_only.attached_scalar_count, 1);

        facade.reset_logical_cache();
        let logical = facade.cache_summary();
        assert_eq!(logical.attached_kline_count, 1);
        assert_eq!(logical.attached_scalar_count, 1);

        facade.full_detach();
        let detached = facade.cache_summary();
        assert_eq!(detached.attached_kline_count, 0);
        assert_eq!(detached.attached_scalar_count, 0);
    }

    #[test]
    fn facade_attach_stays_lazy_until_first_query_and_sidecar_query() {
        let temp_dir = TempDir::new().expect("temp dir should be created");
        let mut store = FastKStore::open(temp_dir.path()).expect("store should open");
        store.init().expect("store should init");
        store.set_metrics_level(MetricsLevel::Detailed);
        store
            .register_kline_series("BTCUSDT", "1m", 60_000, 100_000, 100_000)
            .expect("kline series should register");
        store
            .put_kline_chunk("BTCUSDT", "1m", &sample_kline_records())
            .expect("kline chunk should write");
        let scalar_binding = sample_scalar_binding();
        let scalar_key = scalar_binding.to_series_key();
        store
            .register_scalar_series(&scalar_key, 60_000)
            .expect("scalar series should register");
        store
            .put_scalar_chunk(&scalar_key, 60_000, &sample_scalar_records(), 2)
            .expect("scalar chunk should write");

        let mut facade = store.backtest_view();
        store.reset_metrics();
        facade
            .attach_kline("BTCUSDT", "1m")
            .expect("kline attach should succeed");
        facade
            .attach_indicator("BTCUSDT", "1m", "rsi14")
            .expect("scalar attach should succeed");
        let after_attach = store.metrics_snapshot();
        assert_eq!(
            after_attach.chunk_header_cache_hits + after_attach.chunk_header_cache_misses,
            0
        );
        assert_eq!(
            after_attach.chunk_file_cache_hits + after_attach.chunk_file_cache_misses,
            0
        );
        assert_eq!(
            after_attach.sidecar_cache_hits + after_attach.sidecar_cache_misses,
            0
        );
        assert_eq!(after_attach.sidecar_load_ns, 0);

        store.reset_metrics();
        let _ = facade
            .get_kline_at("BTCUSDT", "1m", 1_706_745_720_000)
            .expect("point query should succeed");
        let after_kline_query = store.metrics_snapshot();
        assert!(
            after_kline_query.chunk_header_cache_hits + after_kline_query.chunk_header_cache_misses
                >= 1
        );
        assert!(
            after_kline_query.chunk_file_cache_hits + after_kline_query.chunk_file_cache_misses
                >= 1
        );

        store.reset_metrics();
        let _ = facade
            .get_indicator_at("BTCUSDT", "1m", "rsi14", 1_706_745_780_000)
            .expect("scalar point query should succeed");
        let scalar_point = store.metrics_snapshot();
        assert_eq!(
            scalar_point.sidecar_cache_hits + scalar_point.sidecar_cache_misses,
            0
        );

        store.reset_metrics();
        let _ = facade
            .find_scalar_timestamps_via_zmap(
                "BTCUSDT",
                "1m",
                "indicator",
                "rsi14",
                &ScalarPredicate {
                    op: CompareOp::Between,
                    value: 15,
                    value2: Some(40),
                },
                1_706_745_600_000,
                1_706_745_900_000,
            )
            .expect("zmap query should succeed");
        let zmap_query = store.metrics_snapshot();
        assert!(zmap_query.sidecar_cache_hits + zmap_query.sidecar_cache_misses >= 1);
    }

    #[test]
    fn release_check_smoke_surfaces_are_clean() {
        let temp_dir = TempDir::new().expect("temp dir should be created");
        let mut store = FastKStore::open(temp_dir.path()).expect("store should open");
        store.init().expect("store should init");
        store
            .register_kline_series("BTCUSDT", "1m", 60_000, 100_000, 100_000)
            .expect("kline series should register");
        store
            .put_kline_chunk("BTCUSDT", "1m", &sample_kline_records())
            .expect("kline chunk should write");

        let scalar_binding = sample_scalar_binding();
        let scalar_key = scalar_binding.to_series_key();
        store
            .register_scalar_series(&scalar_key, 60_000)
            .expect("scalar series should register");
        store
            .put_scalar_chunk(&scalar_key, 60_000, &sample_scalar_records(), 2)
            .expect("scalar chunk should write");

        store.validate_store().expect("validate should succeed");
        let validations = store
            .validate_manifest_vs_fs()
            .expect("validation report should load");
        assert!(validations.iter().all(|report| report.is_clean()));

        let scrub = store.scrub_store(true).expect("scrub should succeed");
        assert_eq!(scrub.len(), 2);
        assert!(scrub.iter().all(|report| report.validation.is_clean()));

        let dry_run = store
            .repair_store_dry_run()
            .expect("repair dry-run should succeed");
        assert_eq!(dry_run.removed_temp_files, 0);
        assert_eq!(dry_run.rebuilt_manifests, 0);
        assert_eq!(dry_run.adopted_chunks, 0);

        let health = store.health_summary().expect("health summary should load");
        assert_eq!(health.issue_series_count, 0);
        assert_eq!(health.orphan_count, 0);
    }

    fn sample_scalar_binding() -> super::BacktestScalarBinding {
        super::BacktestScalarBinding::indicator("BTCUSDT", "1m", "rsi14")
    }

    fn sample_kline_records() -> Vec<crate::KlineRecord> {
        vec![
            crate::KlineRecord {
                ts: 1_706_745_600_000,
                open: 100,
                high: 101,
                low: 99,
                close: 100,
                volume: 10,
            },
            crate::KlineRecord {
                ts: 1_706_745_660_000,
                open: 101,
                high: 102,
                low: 100,
                close: 101,
                volume: 11,
            },
            crate::KlineRecord {
                ts: 1_706_745_720_000,
                open: 102,
                high: 103,
                low: 101,
                close: 102,
                volume: 12,
            },
            crate::KlineRecord {
                ts: 1_706_745_780_000,
                open: 103,
                high: 104,
                low: 102,
                close: 103,
                volume: 13,
            },
            crate::KlineRecord {
                ts: 1_706_745_840_000,
                open: 104,
                high: 105,
                low: 103,
                close: 104,
                volume: 14,
            },
            crate::KlineRecord {
                ts: 1_706_745_900_000,
                open: 105,
                high: 106,
                low: 104,
                close: 105,
                volume: 15,
            },
        ]
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

    fn sample_factor_records() -> Vec<ScalarRecord> {
        sample_scalar_records()
            .into_iter()
            .map(|record| ScalarRecord {
                ts: record.ts,
                value: record.value + 100,
            })
            .collect()
    }
}
