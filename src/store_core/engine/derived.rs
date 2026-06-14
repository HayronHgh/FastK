use crate::error::Result;
use crate::types::{KlineRecord, ScalarRecord, ScalarSeriesKey};

/// Canonical category used for derived indicator series.
pub const INDICATOR_CATEGORY: &str = "indicator";
/// Canonical category for feature scalar series.
pub const FEATURE_CATEGORY: &str = "feature";
/// Canonical category for factor scalar series.
pub const FACTOR_CATEGORY: &str = "factor";
/// Canonical category for strategy signal scalar series.
pub const SIGNAL_CATEGORY: &str = "signal";
/// Canonical category for portfolio-level scalar series.
pub const PORTFOLIO_CATEGORY: &str = "portfolio";
/// Canonical category for risk scalar series.
pub const RISK_CATEGORY: &str = "risk";
/// Canonical category for runtime or query metric scalar series.
pub const METRIC_CATEGORY: &str = "metric";

const SCALAR_SCOPE_SEPARATOR: &str = "@@";

/// Logical binding used by backtest-facing APIs to scope scalar series by timeframe.
///
/// FastK persists scalar series through [`ScalarSeriesKey`]. The recommended integration path
/// for derived series encodes the source timeframe into the logical scalar name so backtest
/// callers can address indicator data by `(symbol, timeframe, category, name)` without creating
/// a second store model.
#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct ScopedScalarBinding {
    pub symbol: String,
    pub timeframe: String,
    pub category: String,
    pub name: String,
}

impl ScopedScalarBinding {
    /// Creates a new scoped binding.
    pub fn new(symbol: &str, timeframe: &str, category: &str, name: &str) -> Self {
        Self {
            symbol: symbol.to_string(),
            timeframe: timeframe.to_string(),
            category: category.to_string(),
            name: name.to_string(),
        }
    }

    /// Creates a binding for an indicator series.
    pub fn indicator(symbol: &str, timeframe: &str, indicator_name: &str) -> Self {
        Self::new(symbol, timeframe, INDICATOR_CATEGORY, indicator_name)
    }

    /// Creates a binding for a feature series.
    pub fn feature(symbol: &str, timeframe: &str, feature_name: &str) -> Self {
        Self::new(symbol, timeframe, FEATURE_CATEGORY, feature_name)
    }

    /// Creates a binding for a factor series.
    pub fn factor(symbol: &str, timeframe: &str, factor_name: &str) -> Self {
        Self::new(symbol, timeframe, FACTOR_CATEGORY, factor_name)
    }

    /// Creates a binding for a strategy signal series.
    pub fn signal(symbol: &str, timeframe: &str, signal_name: &str) -> Self {
        Self::new(symbol, timeframe, SIGNAL_CATEGORY, signal_name)
    }

    /// Creates a binding for a portfolio-level series.
    pub fn portfolio(timeframe: &str, series_name: &str) -> Self {
        Self::new("__portfolio__", timeframe, PORTFOLIO_CATEGORY, series_name)
    }

    /// Creates a binding for a risk series.
    pub fn risk(symbol: &str, timeframe: &str, risk_name: &str) -> Self {
        Self::new(symbol, timeframe, RISK_CATEGORY, risk_name)
    }

    /// Creates a binding for a runtime metric series.
    pub fn metric(symbol: &str, timeframe: &str, metric_name: &str) -> Self {
        Self::new(symbol, timeframe, METRIC_CATEGORY, metric_name)
    }

    /// Returns the underlying scalar-series key used by the store.
    pub fn to_series_key(&self) -> ScalarSeriesKey {
        scoped_scalar_series_key(&self.symbol, &self.timeframe, &self.category, &self.name)
    }
}

/// Legacy request shape used by external derived-series materializers.
///
/// FastK stores materialized scalar rows, but it does not calculate them. New integrations should
/// keep materializer traits in an application or adapter crate and use FastK only for storage.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct DerivedSeriesRequest {
    pub symbol: String,
    pub timeframe: String,
    pub start_ts: i64,
    pub end_ts: i64,
}

/// Legacy integration-facing shape for "read source rows -> write scalar rows" flows.
///
/// FastK does not implement indicator, feature, or factor math. This trait is retained for
/// compatibility with older examples; it is not a FastK core engine responsibility. Prefer
/// defining calculation traits in upper-layer materializer crates.
pub trait DerivedSeriesBuilder {
    /// Returns the output binding that will receive persisted scalar rows.
    fn output_binding(&self, symbol: &str, timeframe: &str) -> ScopedScalarBinding;

    /// Returns how many source rows the caller should fetch before the requested start.
    fn required_lookback(&self) -> usize {
        0
    }

    /// Builds derived scalar rows from already-fetched kline records.
    fn materialize_range(
        &self,
        request: &DerivedSeriesRequest,
        klines: &[KlineRecord],
    ) -> Result<Vec<ScalarRecord>>;
}

/// Builds the scalar-series key used for a scoped scalar binding.
pub fn scoped_scalar_series_key(
    symbol: &str,
    timeframe: &str,
    category: &str,
    name: &str,
) -> ScalarSeriesKey {
    ScalarSeriesKey {
        symbol: symbol.to_string(),
        category: category.to_string(),
        name: encode_scoped_scalar_name(timeframe, name),
    }
}

/// Builds the canonical scalar-series key for an indicator series.
pub fn indicator_series_key(
    symbol: &str,
    timeframe: &str,
    indicator_name: &str,
) -> ScalarSeriesKey {
    scoped_scalar_series_key(symbol, timeframe, INDICATOR_CATEGORY, indicator_name)
}

/// Builds the canonical scalar-series key for a feature series.
pub fn feature_series_key(symbol: &str, timeframe: &str, feature_name: &str) -> ScalarSeriesKey {
    scoped_scalar_series_key(symbol, timeframe, FEATURE_CATEGORY, feature_name)
}

/// Builds the canonical scalar-series key for a factor series.
pub fn factor_series_key(symbol: &str, timeframe: &str, factor_name: &str) -> ScalarSeriesKey {
    scoped_scalar_series_key(symbol, timeframe, FACTOR_CATEGORY, factor_name)
}

/// Builds the canonical scalar-series key for a signal series.
pub fn signal_series_key(symbol: &str, timeframe: &str, signal_name: &str) -> ScalarSeriesKey {
    scoped_scalar_series_key(symbol, timeframe, SIGNAL_CATEGORY, signal_name)
}

/// Builds the canonical scalar-series key for a portfolio-level series.
pub fn portfolio_series_key(timeframe: &str, series_name: &str) -> ScalarSeriesKey {
    scoped_scalar_series_key("__portfolio__", timeframe, PORTFOLIO_CATEGORY, series_name)
}

/// Builds the canonical scalar-series key for a risk series.
pub fn risk_series_key(symbol: &str, timeframe: &str, risk_name: &str) -> ScalarSeriesKey {
    scoped_scalar_series_key(symbol, timeframe, RISK_CATEGORY, risk_name)
}

/// Builds the canonical scalar-series key for a runtime metric series.
pub fn metric_series_key(symbol: &str, timeframe: &str, metric_name: &str) -> ScalarSeriesKey {
    scoped_scalar_series_key(symbol, timeframe, METRIC_CATEGORY, metric_name)
}

/// Returns the scalar categories reserved by FastK's public namespace helpers.
pub fn reserved_scalar_categories() -> &'static [&'static str] {
    &[
        INDICATOR_CATEGORY,
        FEATURE_CATEGORY,
        FACTOR_CATEGORY,
        SIGNAL_CATEGORY,
        PORTFOLIO_CATEGORY,
        RISK_CATEGORY,
        METRIC_CATEGORY,
    ]
}

/// Encodes a timeframe/name pair into the persisted scalar `name`.
pub fn encode_scoped_scalar_name(timeframe: &str, name: &str) -> String {
    format!("{timeframe}{SCALAR_SCOPE_SEPARATOR}{name}")
}

/// Decodes a persisted scalar `name` into `(timeframe, name)`.
pub fn decode_scoped_scalar_name(name: &str) -> Option<(&str, &str)> {
    name.split_once(SCALAR_SCOPE_SEPARATOR)
}

/// Attempts to decode a persisted scalar key back into a scoped binding.
pub fn decode_scoped_scalar_key(series_key: &ScalarSeriesKey) -> Option<ScopedScalarBinding> {
    let (timeframe, name) = decode_scoped_scalar_name(&series_key.name)?;
    Some(ScopedScalarBinding {
        symbol: series_key.symbol.clone(),
        timeframe: timeframe.to_string(),
        category: series_key.category.clone(),
        name: name.to_string(),
    })
}

#[cfg(test)]
mod tests {
    use super::{
        decode_scoped_scalar_key, decode_scoped_scalar_name, encode_scoped_scalar_name,
        factor_series_key, feature_series_key, indicator_series_key, metric_series_key,
        portfolio_series_key, reserved_scalar_categories, risk_series_key, signal_series_key,
        ScopedScalarBinding, FACTOR_CATEGORY, FEATURE_CATEGORY, INDICATOR_CATEGORY,
        METRIC_CATEGORY, PORTFOLIO_CATEGORY, RISK_CATEGORY, SIGNAL_CATEGORY,
    };

    #[test]
    fn scoped_scalar_name_roundtrip_is_stable() {
        let encoded = encode_scoped_scalar_name("1m", "rsi14");
        assert_eq!(encoded, "1m@@rsi14");
        assert_eq!(decode_scoped_scalar_name(&encoded), Some(("1m", "rsi14")));
    }

    #[test]
    fn scoped_binding_roundtrip_matches_series_key() {
        let binding = ScopedScalarBinding::indicator("BTCUSDT", "1m", "rsi14");
        let key = binding.to_series_key();
        assert_eq!(key, indicator_series_key("BTCUSDT", "1m", "rsi14"));

        let decoded = decode_scoped_scalar_key(&key).expect("scoped key should decode");
        assert_eq!(decoded.symbol, "BTCUSDT");
        assert_eq!(decoded.timeframe, "1m");
        assert_eq!(decoded.category, INDICATOR_CATEGORY);
        assert_eq!(decoded.name, "rsi14");
    }

    #[test]
    fn canonical_scalar_categories_build_expected_keys() {
        assert_eq!(
            feature_series_key("BTCUSDT", "1m", "spread"),
            ScopedScalarBinding::feature("BTCUSDT", "1m", "spread").to_series_key()
        );
        assert_eq!(
            factor_series_key("BTCUSDT", "1m", "momentum_20").category,
            FACTOR_CATEGORY
        );
        assert_eq!(
            signal_series_key("BTCUSDT", "strategy_v2", "target_weight").name,
            "strategy_v2@@target_weight"
        );
        assert_eq!(
            portfolio_series_key("run_20260424", "nav").symbol,
            "__portfolio__"
        );
        assert_eq!(
            risk_series_key("BTCUSDT", "1m", "drawdown").category,
            RISK_CATEGORY
        );
        assert_eq!(
            metric_series_key("__system__", "1s", "query_latency").category,
            METRIC_CATEGORY
        );
        assert_eq!(
            reserved_scalar_categories(),
            &[
                INDICATOR_CATEGORY,
                FEATURE_CATEGORY,
                FACTOR_CATEGORY,
                SIGNAL_CATEGORY,
                PORTFOLIO_CATEGORY,
                RISK_CATEGORY,
                METRIC_CATEGORY,
            ]
        );
    }
}
