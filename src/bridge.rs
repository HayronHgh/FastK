//! Minimal storage-level JSON bridge for external tooling such as Python scripts.
//!
//! The bridge only reads/writes records and inventory JSON. It does not fetch market data,
//! calculate indicators/features/factors, route records to other modules, or make workflow
//! decisions.

use std::path::Path;

use serde::{Deserialize, Serialize};

use crate::engine::{
    indicator_series_key, kline_schema_name, scoped_scalar_series_key, Catalog, FastKStore,
    MonthInventoryEntry, ScalarQueryCapabilities, INDICATOR_CATEGORY,
};
use crate::error::{FastKError, Result};
use crate::types::{
    KlineFlowRecord, ScalarPredicateExpr, ScalarPredicateMatch, ScalarPredicateQuery,
    ScalarPredicateQueryStats, ScalarRecord, SeriesMeta,
};

/// JSON contract used by `read-kline-range`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct KlineRangeResponse {
    pub symbol: String,
    pub timeframe: String,
    pub timeframe_ms: i64,
    pub start_ts: i64,
    pub end_ts: i64,
    pub price_scale: i64,
    pub volume_scale: i64,
    pub records: Vec<KlineBridgeRecord>,
}

/// JSON contract accepted by `write-kline-range`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WriteKlineRequest {
    pub timeframe_ms: i64,
    pub price_scale: i64,
    pub volume_scale: i64,
    pub records: Vec<KlineBridgeRecord>,
}

/// JSON response returned by `write-kline-range`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct WriteKlineResponse {
    pub symbol: String,
    pub timeframe: String,
    pub timeframe_ms: i64,
    pub price_scale: i64,
    pub volume_scale: i64,
    pub registered: bool,
    pub requested_record_count: usize,
    pub written_record_count: usize,
    pub month_batches_written: usize,
}

/// JSON contract used by `read-indicator-range`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct IndicatorRangeResponse {
    pub symbol: String,
    pub timeframe: String,
    pub category: String,
    pub name: String,
    pub timeframe_ms: i64,
    pub start_ts: i64,
    pub end_ts: i64,
    pub exists: bool,
    pub base_price_scale: i64,
    pub coverage_start_ts: Option<i64>,
    pub coverage_end_ts: Option<i64>,
    pub total_record_count: u64,
    pub records: Vec<ScalarBridgeRecord>,
}

/// JSON contract accepted by `write-indicator-range`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WriteIndicatorRequest {
    pub records: Vec<ScalarBridgeRecord>,
}

/// JSON response returned by `write-indicator-range`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct WriteIndicatorResponse {
    pub symbol: String,
    pub timeframe: String,
    pub category: String,
    pub name: String,
    pub registered: bool,
    pub requested_record_count: usize,
    pub written_record_count: usize,
    pub month_batches_written: usize,
}

/// JSON contract used by generic `read-scalar-range`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ScalarRangeResponse {
    pub symbol: String,
    pub timeframe: String,
    pub category: String,
    pub name: String,
    pub timeframe_ms: Option<i64>,
    pub start_ts: i64,
    pub end_ts: i64,
    pub exists: bool,
    pub coverage_start_ts: Option<i64>,
    pub coverage_end_ts: Option<i64>,
    pub total_record_count: u64,
    pub records: Vec<ScalarBridgeRecord>,
}

/// JSON contract used by `query-scalar-predicate`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ScalarPredicateQueryResponse {
    pub symbol: String,
    pub timeframe: String,
    pub category: String,
    pub name: String,
    pub start_ts: i64,
    pub end_ts: i64,
    pub predicate: ScalarPredicateExpr,
    pub return_values: bool,
    pub matches: Vec<ScalarPredicateMatch>,
    pub stats: ScalarPredicateQueryStats,
}

/// JSON contract accepted by generic `write-scalar-range`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WriteScalarRequest {
    pub timeframe_ms: Option<i64>,
    pub records: Vec<ScalarBridgeRecord>,
}

/// JSON response returned by generic `write-scalar-range`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct WriteScalarResponse {
    pub symbol: String,
    pub timeframe: String,
    pub category: String,
    pub name: String,
    pub timeframe_ms: i64,
    pub registered: bool,
    pub requested_record_count: usize,
    pub written_record_count: usize,
    pub month_batches_written: usize,
}

/// JSON contract returned by `indicator-inventory`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct InventoryResponse {
    pub symbol: String,
    pub timeframe: String,
    pub category: String,
    pub name: String,
    pub exists: bool,
    pub record_count: u64,
    pub coverage_start_ts: Option<i64>,
    pub coverage_end_ts: Option<i64>,
    pub inventory: Vec<MonthInventoryEntry>,
    pub capabilities: Option<ScalarQueryCapabilities>,
}

/// JSON contract returned by `kline-inventory`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct KlineInventoryResponse {
    pub symbol: String,
    pub timeframe: String,
    pub exists: bool,
    pub timeframe_ms: Option<i64>,
    pub price_scale: Option<i64>,
    pub volume_scale: Option<i64>,
    /// `"ohlcv_flow"` for kline flow series; omitted for OHLCV series so their
    /// payload stays byte-identical to contract v1.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub kline_schema: Option<String>,
    /// Scale of quote-denominated flow fields; omitted for OHLCV series.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub quote_scale: Option<i64>,
    pub record_count: u64,
    pub coverage_start_ts: Option<i64>,
    pub coverage_end_ts: Option<i64>,
    pub inventory: Vec<MonthInventoryEntry>,
}

/// Kline row with optional order-flow fields rendered for JSON transport.
///
/// An absent flow value is `null` on output and may be `null` or omitted on input.
/// It is never reported as `0`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct KlineFlowBridgeRecord {
    pub ts: i64,
    pub open: i64,
    pub high: i64,
    pub low: i64,
    pub close: i64,
    pub volume: i64,
    #[serde(default)]
    pub quote_volume: Option<i64>,
    #[serde(default)]
    pub trade_count: Option<i64>,
    #[serde(default)]
    pub taker_buy_volume: Option<i64>,
    #[serde(default)]
    pub taker_buy_quote_volume: Option<i64>,
}

impl From<KlineFlowRecord> for KlineFlowBridgeRecord {
    fn from(value: KlineFlowRecord) -> Self {
        Self {
            ts: value.ts,
            open: value.open,
            high: value.high,
            low: value.low,
            close: value.close,
            volume: value.volume,
            quote_volume: value.quote_volume_opt(),
            trade_count: value.trade_count_opt(),
            taker_buy_volume: value.taker_buy_volume_opt(),
            taker_buy_quote_volume: value.taker_buy_quote_volume_opt(),
        }
    }
}

impl KlineFlowBridgeRecord {
    /// Converts to a stored row, rejecting values reserved for absence.
    fn to_record(self) -> Result<KlineFlowRecord> {
        for value in [
            self.quote_volume,
            self.trade_count,
            self.taker_buy_volume,
            self.taker_buy_quote_volume,
        ]
        .into_iter()
        .flatten()
        {
            if value == crate::KLINE_FLOW_MISSING {
                return Err(FastKError::InvalidInput(format!(
                    "flow value {value} at ts {} is reserved; use null for an absent value",
                    self.ts
                )));
            }
        }
        Ok(KlineFlowRecord::new(
            crate::types::KlineRecord {
                ts: self.ts,
                open: self.open,
                high: self.high,
                low: self.low,
                close: self.close,
                volume: self.volume,
            },
            self.quote_volume,
            self.trade_count,
            self.taker_buy_volume,
            self.taker_buy_quote_volume,
        ))
    }
}

/// JSON contract used by `read-kline-flow-range`.
///
/// For an OHLCV series `kline_schema` is `"ohlcv"`, `quote_scale` is `null` and
/// every flow field is `null`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct KlineFlowRangeResponse {
    pub symbol: String,
    pub timeframe: String,
    pub kline_schema: String,
    pub timeframe_ms: i64,
    pub start_ts: i64,
    pub end_ts: i64,
    pub price_scale: i64,
    pub volume_scale: i64,
    pub quote_scale: Option<i64>,
    pub records: Vec<KlineFlowBridgeRecord>,
}

/// JSON contract accepted by `write-kline-flow-range`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WriteKlineFlowRequest {
    pub timeframe_ms: i64,
    pub price_scale: i64,
    pub volume_scale: i64,
    pub quote_scale: i64,
    pub records: Vec<KlineFlowBridgeRecord>,
}

/// JSON response returned by `write-kline-flow-range`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct WriteKlineFlowResponse {
    pub symbol: String,
    pub timeframe: String,
    pub kline_schema: String,
    pub timeframe_ms: i64,
    pub price_scale: i64,
    pub volume_scale: i64,
    pub quote_scale: i64,
    pub registered: bool,
    pub requested_record_count: usize,
    pub written_record_count: usize,
    pub month_batches_written: usize,
}

/// Fixed-width kline row rendered for JSON transport.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct KlineBridgeRecord {
    pub ts: i64,
    pub open: i64,
    pub high: i64,
    pub low: i64,
    pub close: i64,
    pub volume: i64,
}

/// Fixed-width scalar row rendered for JSON transport.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScalarBridgeRecord {
    pub ts: i64,
    pub value: i64,
}

impl From<crate::types::KlineRecord> for KlineBridgeRecord {
    fn from(value: crate::types::KlineRecord) -> Self {
        Self {
            ts: value.ts,
            open: value.open,
            high: value.high,
            low: value.low,
            close: value.close,
            volume: value.volume,
        }
    }
}

impl From<KlineBridgeRecord> for crate::types::KlineRecord {
    fn from(value: KlineBridgeRecord) -> Self {
        Self {
            ts: value.ts,
            open: value.open,
            high: value.high,
            low: value.low,
            close: value.close,
            volume: value.volume,
        }
    }
}

impl From<ScalarRecord> for ScalarBridgeRecord {
    fn from(value: ScalarRecord) -> Self {
        Self {
            ts: value.ts,
            value: value.value,
        }
    }
}

impl From<ScalarBridgeRecord> for ScalarRecord {
    fn from(value: ScalarBridgeRecord) -> Self {
        Self {
            ts: value.ts,
            value: value.value,
        }
    }
}

/// Reads a kline range and renders it as bridge JSON.
pub fn read_kline_range(
    root: impl AsRef<Path>,
    symbol: &str,
    timeframe: &str,
    start_ts: i64,
    end_ts: i64,
) -> Result<KlineRangeResponse> {
    let root = root.as_ref();
    let store = FastKStore::open_read_only(root.to_path_buf())?;
    let catalog = Catalog::read_only(root.to_path_buf());
    let meta = catalog.load_kline_meta(symbol, timeframe)?;
    let records = store.get_kline_range(symbol, timeframe, start_ts, end_ts)?;

    Ok(KlineRangeResponse {
        symbol: symbol.to_string(),
        timeframe: timeframe.to_string(),
        timeframe_ms: meta.timeframe_ms,
        start_ts,
        end_ts,
        price_scale: meta.price_scale,
        volume_scale: meta.volume_scale,
        records: records.into_iter().map(Into::into).collect(),
    })
}

/// Writes kline rows through the existing kline lifecycle.
///
/// Records may span multiple months. The bridge sorts them, rejects duplicate timestamps,
/// preflights overlap with existing readable month coverage, then writes one month batch at a time.
pub fn write_kline_range(
    root: impl AsRef<Path>,
    symbol: &str,
    timeframe: &str,
    request: WriteKlineRequest,
) -> Result<WriteKlineResponse> {
    let normalized = normalize_kline_records(request.records)?;
    let mut store = FastKStore::open(root.as_ref().to_path_buf())?;
    let summary = store.append_kline_range(
        symbol,
        timeframe,
        &normalized,
        request.timeframe_ms,
        request.price_scale,
        request.volume_scale,
    )?;
    Ok(WriteKlineResponse {
        symbol: symbol.into(),
        timeframe: timeframe.into(),
        timeframe_ms: summary.timeframe_ms,
        price_scale: request.price_scale,
        volume_scale: request.volume_scale,
        registered: summary.registered,
        requested_record_count: summary.requested_record_count,
        written_record_count: summary.written_record_count,
        month_batches_written: summary.month_batches_written,
    })
}

/// Reads a kline range with order-flow fields and renders it as bridge JSON.
pub fn read_kline_flow_range(
    root: impl AsRef<Path>,
    symbol: &str,
    timeframe: &str,
    start_ts: i64,
    end_ts: i64,
) -> Result<KlineFlowRangeResponse> {
    let root = root.as_ref();
    let store = FastKStore::open_read_only(root.to_path_buf())?;
    let meta = store.kline_metadata(symbol, timeframe)?;
    let records = store.get_kline_flow_range(symbol, timeframe, start_ts, end_ts)?;
    Ok(KlineFlowRangeResponse {
        symbol: symbol.to_string(),
        timeframe: timeframe.to_string(),
        kline_schema: kline_schema_name(meta.schema_id).to_string(),
        timeframe_ms: meta.timeframe_ms,
        start_ts,
        end_ts,
        price_scale: meta.price_scale,
        volume_scale: meta.volume_scale,
        quote_scale: meta.quote_scale,
        records: records.into_iter().map(Into::into).collect(),
    })
}

/// Writes kline flow rows, registering a kline flow series when absent.
///
/// Rows are sorted, duplicate timestamps are rejected, and every row is validated
/// (non-negative flow values, taker-buy totals within their totals) before any
/// month is written. Writing into an existing OHLCV series is rejected.
pub fn write_kline_flow_range(
    root: impl AsRef<Path>,
    symbol: &str,
    timeframe: &str,
    request: WriteKlineFlowRequest,
) -> Result<WriteKlineFlowResponse> {
    let normalized = normalize_kline_flow_records(request.records)?;
    let mut store = FastKStore::open(root.as_ref().to_path_buf())?;
    let summary = store.append_kline_flow_range(
        symbol,
        timeframe,
        &normalized,
        request.timeframe_ms,
        request.price_scale,
        request.volume_scale,
        request.quote_scale,
    )?;
    Ok(WriteKlineFlowResponse {
        symbol: symbol.into(),
        timeframe: timeframe.into(),
        kline_schema: kline_schema_name(crate::chunk::header::KLINE_FLOW_SCHEMA_ID).to_string(),
        timeframe_ms: summary.timeframe_ms,
        price_scale: request.price_scale,
        volume_scale: request.volume_scale,
        quote_scale: request.quote_scale,
        registered: summary.registered,
        requested_record_count: summary.requested_record_count,
        written_record_count: summary.written_record_count,
        month_batches_written: summary.month_batches_written,
    })
}

/// Returns kline existence, month inventory and coverage details.
pub fn kline_inventory(
    root: impl AsRef<Path>,
    symbol: &str,
    timeframe: &str,
) -> Result<KlineInventoryResponse> {
    let root = root.as_ref();
    let store = FastKStore::open_read_only(root.to_path_buf())?;
    let catalog = Catalog::read_only(root.to_path_buf());
    let meta = match catalog.load_kline_meta(symbol, timeframe) {
        Ok(meta) => Some(meta),
        Err(FastKError::NotFound(_)) => None,
        Err(err) => return Err(err),
    };

    if let Some(meta) = meta {
        let inventory = store.kline_month_inventory(symbol, timeframe)?;
        let (coverage_start_ts, coverage_end_ts, total_record_count) = series_coverage(&meta);
        Ok(KlineInventoryResponse {
            symbol: symbol.to_string(),
            timeframe: timeframe.to_string(),
            exists: true,
            timeframe_ms: Some(meta.timeframe_ms),
            price_scale: Some(meta.price_scale),
            volume_scale: Some(meta.volume_scale),
            kline_schema: meta
                .quote_scale
                .map(|_| kline_schema_name(meta.schema_id).to_string()),
            quote_scale: meta.quote_scale,
            record_count: total_record_count,
            coverage_start_ts,
            coverage_end_ts,
            inventory,
        })
    } else {
        Ok(KlineInventoryResponse {
            symbol: symbol.to_string(),
            timeframe: timeframe.to_string(),
            exists: false,
            timeframe_ms: None,
            price_scale: None,
            volume_scale: None,
            kline_schema: None,
            quote_scale: None,
            record_count: 0,
            coverage_start_ts: None,
            coverage_end_ts: None,
            inventory: Vec::new(),
        })
    }
}

/// Reads an indicator range and renders it as bridge JSON.
pub fn read_indicator_range(
    root: impl AsRef<Path>,
    symbol: &str,
    timeframe: &str,
    indicator_name: &str,
    start_ts: i64,
    end_ts: i64,
) -> Result<IndicatorRangeResponse> {
    let root = root.as_ref();
    let store = FastKStore::open_read_only(root.to_path_buf())?;
    let catalog = Catalog::read_only(root.to_path_buf());
    let base_meta = catalog.load_kline_meta(symbol, timeframe)?;
    let key = indicator_series_key(symbol, timeframe, indicator_name);

    let indicator_meta = match catalog.load_scalar_meta(&key) {
        Ok(meta) => Some(meta),
        Err(FastKError::NotFound(_)) => None,
        Err(err) => return Err(err),
    };

    if let Some(meta) = indicator_meta {
        let records =
            store.get_indicator_range(symbol, timeframe, indicator_name, start_ts, end_ts)?;
        let (coverage_start_ts, coverage_end_ts, total_record_count) = series_coverage(&meta);

        Ok(IndicatorRangeResponse {
            symbol: symbol.to_string(),
            timeframe: timeframe.to_string(),
            category: "indicator".to_string(),
            name: indicator_name.to_string(),
            timeframe_ms: meta.timeframe_ms,
            start_ts,
            end_ts,
            exists: true,
            base_price_scale: base_meta.price_scale,
            coverage_start_ts,
            coverage_end_ts,
            total_record_count,
            records: records.into_iter().map(Into::into).collect(),
        })
    } else {
        Ok(IndicatorRangeResponse {
            symbol: symbol.to_string(),
            timeframe: timeframe.to_string(),
            category: "indicator".to_string(),
            name: indicator_name.to_string(),
            timeframe_ms: base_meta.timeframe_ms,
            start_ts,
            end_ts,
            exists: false,
            base_price_scale: base_meta.price_scale,
            coverage_start_ts: None,
            coverage_end_ts: None,
            total_record_count: 0,
            records: Vec::new(),
        })
    }
}

/// Reads a generic scalar range and renders it as bridge JSON.
pub fn read_scalar_range(
    root: impl AsRef<Path>,
    symbol: &str,
    timeframe: &str,
    category: &str,
    name: &str,
    start_ts: i64,
    end_ts: i64,
) -> Result<ScalarRangeResponse> {
    let root = root.as_ref();
    let store = FastKStore::open_read_only(root.to_path_buf())?;
    let catalog = Catalog::read_only(root.to_path_buf());
    let key = scoped_scalar_series_key(symbol, timeframe, category, name);
    let meta = match catalog.load_scalar_meta(&key) {
        Ok(meta) => Some(meta),
        Err(FastKError::NotFound(_)) => None,
        Err(err) => return Err(err),
    };

    if let Some(meta) = meta {
        let records = store.get_scalar_range(&key, start_ts, end_ts)?;
        let (coverage_start_ts, coverage_end_ts, total_record_count) = series_coverage(&meta);
        Ok(ScalarRangeResponse {
            symbol: symbol.to_string(),
            timeframe: timeframe.to_string(),
            category: category.to_string(),
            name: name.to_string(),
            timeframe_ms: Some(meta.timeframe_ms),
            start_ts,
            end_ts,
            exists: true,
            coverage_start_ts,
            coverage_end_ts,
            total_record_count,
            records: records.into_iter().map(Into::into).collect(),
        })
    } else {
        Ok(ScalarRangeResponse {
            symbol: symbol.to_string(),
            timeframe: timeframe.to_string(),
            category: category.to_string(),
            name: name.to_string(),
            timeframe_ms: None,
            start_ts,
            end_ts,
            exists: false,
            coverage_start_ts: None,
            coverage_end_ts: None,
            total_record_count: 0,
            records: Vec::new(),
        })
    }
}

/// Runs a storage-level scalar predicate query and renders it as bridge JSON.
pub fn query_scalar_predicate(
    root: impl AsRef<Path>,
    symbol: &str,
    timeframe: &str,
    category: &str,
    name: &str,
    start_ts: i64,
    end_ts: i64,
    predicate: ScalarPredicateExpr,
    return_values: bool,
) -> Result<ScalarPredicateQueryResponse> {
    let root = root.as_ref();
    let store = FastKStore::open_read_only(root.to_path_buf())?;
    let key = scoped_scalar_series_key(symbol, timeframe, category, name);
    let result = store.query_scalar_predicate(ScalarPredicateQuery {
        key,
        start_ts,
        end_ts,
        predicate: predicate.clone(),
        return_values,
    })?;

    Ok(ScalarPredicateQueryResponse {
        symbol: symbol.to_string(),
        timeframe: timeframe.to_string(),
        category: category.to_string(),
        name: name.to_string(),
        start_ts,
        end_ts,
        predicate,
        return_values,
        matches: result.matches,
        stats: result.stats,
    })
}

/// Writes indicator rows through the existing scalar/indicator lifecycle.
///
/// Records may span multiple months. The bridge sorts them, rejects duplicate timestamps,
/// preflights overlap with existing month coverage, then writes one month batch at a time.
pub fn write_indicator_range(
    root: impl AsRef<Path>,
    symbol: &str,
    timeframe: &str,
    indicator_name: &str,
    request: WriteIndicatorRequest,
) -> Result<WriteIndicatorResponse> {
    let normalized = normalize_scalar_records(request.records)?;
    let mut store = FastKStore::open(root.as_ref().to_path_buf())?;
    let key = indicator_series_key(symbol, timeframe, indicator_name);
    let timeframe_ms = store.kline_metadata(symbol, timeframe)?.timeframe_ms;
    let summary = store.append_scalar_range(&key, &normalized, Some(timeframe_ms))?;
    Ok(WriteIndicatorResponse {
        symbol: symbol.into(),
        timeframe: timeframe.into(),
        category: INDICATOR_CATEGORY.into(),
        name: indicator_name.into(),
        registered: summary.registered,
        requested_record_count: summary.requested_record_count,
        written_record_count: summary.written_record_count,
        month_batches_written: summary.month_batches_written,
    })
}

/// Writes generic scalar rows through the scalar lifecycle.
///
/// If the scalar series does not exist, `timeframe_ms` is taken from the request when provided.
/// Otherwise FastK falls back to the matching kline `(symbol, timeframe)` metadata.
pub fn write_scalar_range(
    root: impl AsRef<Path>,
    symbol: &str,
    timeframe: &str,
    category: &str,
    name: &str,
    request: WriteScalarRequest,
) -> Result<WriteScalarResponse> {
    let normalized = normalize_scalar_records(request.records)?;
    let mut store = FastKStore::open(root.as_ref().to_path_buf())?;
    let key = scoped_scalar_series_key(symbol, timeframe, category, name);
    let summary = store.append_scalar_range(&key, &normalized, request.timeframe_ms)?;
    Ok(WriteScalarResponse {
        symbol: symbol.into(),
        timeframe: timeframe.into(),
        category: category.into(),
        name: name.into(),
        timeframe_ms: summary.timeframe_ms,
        registered: summary.registered,
        requested_record_count: summary.requested_record_count,
        written_record_count: summary.written_record_count,
        month_batches_written: summary.month_batches_written,
    })
}

/// Returns indicator existence, month inventory, coverage and capabilities.
pub fn indicator_inventory(
    root: impl AsRef<Path>,
    symbol: &str,
    timeframe: &str,
    indicator_name: &str,
) -> Result<InventoryResponse> {
    let root = root.as_ref();
    let store = FastKStore::open_read_only(root.to_path_buf())?;
    let catalog = Catalog::read_only(root.to_path_buf());
    let key = indicator_series_key(symbol, timeframe, indicator_name);
    let meta = match catalog.load_scalar_meta(&key) {
        Ok(meta) => Some(meta),
        Err(FastKError::NotFound(_)) => None,
        Err(err) => return Err(err),
    };

    if let Some(meta) = meta {
        let inventory = store.scalar_month_inventory(&key)?;
        let capabilities = store.indicator_capabilities(symbol, timeframe, indicator_name)?;
        let (coverage_start_ts, coverage_end_ts, total_record_count) = series_coverage(&meta);
        Ok(InventoryResponse {
            symbol: symbol.to_string(),
            timeframe: timeframe.to_string(),
            category: "indicator".to_string(),
            name: indicator_name.to_string(),
            exists: true,
            record_count: total_record_count,
            coverage_start_ts,
            coverage_end_ts,
            inventory,
            capabilities: Some(capabilities),
        })
    } else {
        Ok(InventoryResponse {
            symbol: symbol.to_string(),
            timeframe: timeframe.to_string(),
            category: "indicator".to_string(),
            name: indicator_name.to_string(),
            exists: false,
            record_count: 0,
            coverage_start_ts: None,
            coverage_end_ts: None,
            inventory: Vec::new(),
            capabilities: None,
        })
    }
}

/// Returns generic scalar existence, month inventory, coverage and capabilities.
pub fn scalar_inventory(
    root: impl AsRef<Path>,
    symbol: &str,
    timeframe: &str,
    category: &str,
    name: &str,
) -> Result<InventoryResponse> {
    let root = root.as_ref();
    let store = FastKStore::open_read_only(root.to_path_buf())?;
    let catalog = Catalog::read_only(root.to_path_buf());
    let key = scoped_scalar_series_key(symbol, timeframe, category, name);
    let meta = match catalog.load_scalar_meta(&key) {
        Ok(meta) => Some(meta),
        Err(FastKError::NotFound(_)) => None,
        Err(err) => return Err(err),
    };

    if let Some(meta) = meta {
        let inventory = store.scalar_month_inventory(&key)?;
        let capabilities = store.scalar_query_capabilities(&key)?;
        let (coverage_start_ts, coverage_end_ts, total_record_count) = series_coverage(&meta);
        Ok(InventoryResponse {
            symbol: symbol.to_string(),
            timeframe: timeframe.to_string(),
            category: category.to_string(),
            name: name.to_string(),
            exists: true,
            record_count: total_record_count,
            coverage_start_ts,
            coverage_end_ts,
            inventory,
            capabilities: Some(capabilities),
        })
    } else {
        Ok(InventoryResponse {
            symbol: symbol.to_string(),
            timeframe: timeframe.to_string(),
            category: category.to_string(),
            name: name.to_string(),
            exists: false,
            record_count: 0,
            coverage_start_ts: None,
            coverage_end_ts: None,
            inventory: Vec::new(),
            capabilities: None,
        })
    }
}

fn normalize_scalar_records(records: Vec<ScalarBridgeRecord>) -> Result<Vec<ScalarRecord>> {
    if records.is_empty() {
        return Ok(Vec::new());
    }

    let mut normalized: Vec<_> = records.into_iter().map(ScalarRecord::from).collect();
    normalized.sort_by_key(|record| record.ts);
    for window in normalized.windows(2) {
        if window[0].ts == window[1].ts {
            return Err(FastKError::InvalidInput(format!(
                "write-indicator-range contains duplicate timestamp {}",
                window[0].ts
            )));
        }
    }
    Ok(normalized)
}

fn normalize_kline_records(
    records: Vec<KlineBridgeRecord>,
) -> Result<Vec<crate::types::KlineRecord>> {
    if records.is_empty() {
        return Ok(Vec::new());
    }

    let mut normalized: Vec<_> = records
        .into_iter()
        .map(crate::types::KlineRecord::from)
        .collect();
    normalized.sort_by_key(|record| record.ts);
    for window in normalized.windows(2) {
        if window[0].ts == window[1].ts {
            return Err(FastKError::InvalidInput(format!(
                "write-kline-range contains duplicate timestamp {}",
                window[0].ts
            )));
        }
    }
    Ok(normalized)
}

fn normalize_kline_flow_records(
    records: Vec<KlineFlowBridgeRecord>,
) -> Result<Vec<KlineFlowRecord>> {
    let mut normalized = records
        .into_iter()
        .map(KlineFlowBridgeRecord::to_record)
        .collect::<Result<Vec<_>>>()?;
    normalized.sort_by_key(|record| record.ts);
    for window in normalized.windows(2) {
        if window[0].ts == window[1].ts {
            return Err(FastKError::InvalidInput(format!(
                "write-kline-flow-range contains duplicate timestamp {}",
                window[0].ts
            )));
        }
    }
    Ok(normalized)
}

fn series_coverage(meta: &SeriesMeta) -> (Option<i64>, Option<i64>, u64) {
    let readable: Vec<_> = meta
        .chunks
        .iter()
        .filter(|chunk| chunk.state.is_readable())
        .collect();
    if readable.is_empty() {
        return (None, None, 0);
    }

    (
        readable.first().map(|chunk| chunk.start_ts),
        readable.last().map(|chunk| chunk.end_ts),
        readable.iter().map(|chunk| chunk.count).sum(),
    )
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use tempfile::TempDir;

    use super::{
        indicator_inventory, kline_inventory, read_indicator_range, read_kline_range,
        read_scalar_range, scalar_inventory, write_indicator_range, write_kline_range,
        write_scalar_range, KlineBridgeRecord, ScalarBridgeRecord, WriteIndicatorRequest,
        WriteKlineRequest, WriteScalarRequest,
    };
    use crate::{FastKStore, KlineRecord, ScalarRecord};

    #[test]
    fn read_kline_range_bridge_returns_expected_json_shape() {
        let temp_dir = TempDir::new().expect("temp dir should be created");
        let store = seed_store(temp_dir.path());

        let response = read_kline_range(
            temp_dir.path(),
            "BTCUSDT",
            "1m",
            1_706_745_600_000,
            1_706_745_780_000,
        )
        .expect("bridge should read kline range");

        assert_eq!(response.symbol, "BTCUSDT");
        assert_eq!(response.timeframe, "1m");
        assert_eq!(response.price_scale, 100_000);
        assert_eq!(response.records.len(), 4);

        assert_eq!(
            store
                .list_indicators("BTCUSDT", "1m")
                .expect("indicator list should load"),
            Vec::<String>::new()
        );
    }

    #[test]
    fn read_indicator_range_bridge_returns_empty_when_series_is_missing() {
        let temp_dir = TempDir::new().expect("temp dir should be created");
        let _store = seed_store(temp_dir.path());

        let response = read_indicator_range(
            temp_dir.path(),
            "BTCUSDT",
            "1m",
            "ma20",
            1_706_745_600_000,
            1_706_745_840_000,
        )
        .expect("bridge should return empty indicator response");

        assert!(!response.exists);
        assert!(response.records.is_empty());
        assert_eq!(response.base_price_scale, 100_000);
    }

    #[test]
    fn write_indicator_range_bridge_auto_registers_and_reads_back() {
        let temp_dir = TempDir::new().expect("temp dir should be created");
        let _store = seed_store(temp_dir.path());

        let write_response = write_indicator_range(
            temp_dir.path(),
            "BTCUSDT",
            "1m",
            "ma20",
            WriteIndicatorRequest {
                records: sample_indicator_records()
                    .into_iter()
                    .map(Into::into)
                    .collect(),
            },
        )
        .expect("bridge write should succeed");

        assert!(write_response.registered);
        assert_eq!(write_response.written_record_count, 4);

        let read_response = read_indicator_range(
            temp_dir.path(),
            "BTCUSDT",
            "1m",
            "ma20",
            1_706_745_660_000,
            1_706_745_900_000,
        )
        .expect("bridge should read written indicator");

        assert!(read_response.exists);
        assert_eq!(read_response.records.len(), 4);
        assert_eq!(read_response.records[0].value, 10_150_000);
    }

    #[test]
    fn indicator_inventory_bridge_reports_capabilities_and_counts() {
        let temp_dir = TempDir::new().expect("temp dir should be created");
        let _store = seed_store(temp_dir.path());
        write_indicator_range(
            temp_dir.path(),
            "BTCUSDT",
            "1m",
            "ma20",
            WriteIndicatorRequest {
                records: sample_indicator_records()
                    .into_iter()
                    .map(Into::into)
                    .collect(),
            },
        )
        .expect("indicator write should succeed");

        let response = indicator_inventory(temp_dir.path(), "BTCUSDT", "1m", "ma20")
            .expect("inventory should load");

        assert!(response.exists);
        assert_eq!(response.record_count, 4);
        assert_eq!(response.inventory.len(), 1);
        assert!(
            response
                .capabilities
                .expect("capabilities should exist")
                .has_zmap
        );
    }

    #[test]
    fn generic_scalar_bridge_writes_and_reads_feature_series() {
        let temp_dir = TempDir::new().expect("temp dir should be created");
        let _store = seed_store(temp_dir.path());

        let write_response = write_scalar_range(
            temp_dir.path(),
            "BTCUSDT",
            "1m",
            "feature",
            "rsi_14",
            WriteScalarRequest {
                timeframe_ms: None,
                records: sample_indicator_records()
                    .into_iter()
                    .map(Into::into)
                    .collect(),
            },
        )
        .expect("generic scalar write should succeed");

        assert!(write_response.registered);
        assert_eq!(write_response.category, "feature");
        assert_eq!(write_response.timeframe_ms, 60_000);
        assert_eq!(write_response.written_record_count, 4);

        let read_response = read_scalar_range(
            temp_dir.path(),
            "BTCUSDT",
            "1m",
            "feature",
            "rsi_14",
            1_706_745_720_000,
            1_706_745_900_000,
        )
        .expect("generic scalar read should succeed");

        assert!(read_response.exists);
        assert_eq!(read_response.category, "feature");
        assert_eq!(read_response.records.len(), 4);
        assert_eq!(read_response.records[0].value, 10_150_000);
    }

    #[test]
    fn generic_scalar_inventory_reports_factor_series() {
        let temp_dir = TempDir::new().expect("temp dir should be created");
        let _store = seed_store(temp_dir.path());

        write_scalar_range(
            temp_dir.path(),
            "BTCUSDT",
            "1m",
            "factor",
            "momentum_score",
            WriteScalarRequest {
                timeframe_ms: None,
                records: sample_indicator_records()
                    .into_iter()
                    .map(Into::into)
                    .collect(),
            },
        )
        .expect("factor write should succeed");

        let response =
            scalar_inventory(temp_dir.path(), "BTCUSDT", "1m", "factor", "momentum_score")
                .expect("scalar inventory should load");

        assert!(response.exists);
        assert_eq!(response.category, "factor");
        assert_eq!(response.name, "momentum_score");
        assert_eq!(response.record_count, 4);
        assert!(
            response
                .capabilities
                .expect("capabilities should exist")
                .has_vix
        );
    }

    #[test]
    fn write_indicator_range_rejects_overlap_with_existing_month() {
        let temp_dir = TempDir::new().expect("temp dir should be created");
        let _store = seed_store(temp_dir.path());
        write_indicator_range(
            temp_dir.path(),
            "BTCUSDT",
            "1m",
            "ma20",
            WriteIndicatorRequest {
                records: sample_indicator_records()
                    .into_iter()
                    .map(Into::into)
                    .collect(),
            },
        )
        .expect("initial indicator write should succeed");

        let err = write_indicator_range(
            temp_dir.path(),
            "BTCUSDT",
            "1m",
            "ma20",
            WriteIndicatorRequest {
                records: vec![ScalarBridgeRecord {
                    ts: 1_706_745_720_000,
                    value: 10_250_000,
                }],
            },
        )
        .expect_err("overlapping bridge write should be rejected");

        assert!(matches!(err, crate::FastKError::InvalidInput(_)));
    }

    #[test]
    fn write_kline_range_bridge_auto_registers_and_reads_back() {
        let temp_dir = TempDir::new().expect("temp dir should be created");

        let write_response = write_kline_range(
            temp_dir.path(),
            "BTCUSDT",
            "1m",
            WriteKlineRequest {
                timeframe_ms: 60_000,
                price_scale: 100_000,
                volume_scale: 100_000,
                records: sample_kline_records().into_iter().map(Into::into).collect(),
            },
        )
        .expect("kline write should succeed");

        assert!(write_response.registered);
        assert_eq!(write_response.written_record_count, 6);

        let read_response = read_kline_range(
            temp_dir.path(),
            "BTCUSDT",
            "1m",
            1_706_745_600_000,
            1_706_745_900_000,
        )
        .expect("kline read should succeed");
        assert_eq!(read_response.records.len(), 6);
    }

    #[test]
    fn kline_inventory_bridge_reports_coverage() {
        let temp_dir = TempDir::new().expect("temp dir should be created");
        let _store = seed_store(temp_dir.path());

        let response =
            kline_inventory(temp_dir.path(), "BTCUSDT", "1m").expect("kline inventory should load");

        assert!(response.exists);
        assert_eq!(response.record_count, 6);
        assert_eq!(response.inventory.len(), 1);
        assert_eq!(response.timeframe_ms, Some(60_000));
        assert_eq!(response.price_scale, Some(100_000));
    }

    #[test]
    fn write_kline_range_rejects_overlap_with_existing_month() {
        let temp_dir = TempDir::new().expect("temp dir should be created");
        let _store = seed_store(temp_dir.path());

        let err = write_kline_range(
            temp_dir.path(),
            "BTCUSDT",
            "1m",
            WriteKlineRequest {
                timeframe_ms: 60_000,
                price_scale: 100_000,
                volume_scale: 100_000,
                records: vec![KlineBridgeRecord {
                    ts: 1_706_745_780_000,
                    open: 1,
                    high: 2,
                    low: 1,
                    close: 2,
                    volume: 3,
                }],
            },
        )
        .expect_err("overlapping kline write should fail");

        assert!(matches!(err, crate::FastKError::InvalidInput(_)));
    }

    fn seed_store(root: &Path) -> FastKStore {
        let mut store = FastKStore::open(root).expect("store should open");
        store.init().expect("store should init");
        store
            .register_kline_series("BTCUSDT", "1m", 60_000, 100_000, 100_000)
            .expect("kline series should register");
        store
            .put_kline_chunk("BTCUSDT", "1m", &sample_kline_records())
            .expect("kline chunk should write");
        store
    }

    fn sample_kline_records() -> Vec<KlineRecord> {
        vec![
            KlineRecord {
                ts: 1_706_745_600_000,
                open: 10_000_000,
                high: 10_020_000,
                low: 9_980_000,
                close: 10_010_000,
                volume: 10_000,
            },
            KlineRecord {
                ts: 1_706_745_660_000,
                open: 10_010_000,
                high: 10_040_000,
                low: 10_000_000,
                close: 10_020_000,
                volume: 10_200,
            },
            KlineRecord {
                ts: 1_706_745_720_000,
                open: 10_020_000,
                high: 10_050_000,
                low: 10_010_000,
                close: 10_030_000,
                volume: 10_300,
            },
            KlineRecord {
                ts: 1_706_745_780_000,
                open: 10_030_000,
                high: 10_060_000,
                low: 10_020_000,
                close: 10_040_000,
                volume: 10_400,
            },
            KlineRecord {
                ts: 1_706_745_840_000,
                open: 10_040_000,
                high: 10_070_000,
                low: 10_030_000,
                close: 10_050_000,
                volume: 10_500,
            },
            KlineRecord {
                ts: 1_706_745_900_000,
                open: 10_050_000,
                high: 10_080_000,
                low: 10_040_000,
                close: 10_060_000,
                volume: 10_600,
            },
        ]
    }

    fn sample_indicator_records() -> Vec<ScalarRecord> {
        vec![
            ScalarRecord {
                ts: 1_706_745_720_000,
                value: 10_150_000,
            },
            ScalarRecord {
                ts: 1_706_745_780_000,
                value: 10_250_000,
            },
            ScalarRecord {
                ts: 1_706_745_840_000,
                value: 10_350_000,
            },
            ScalarRecord {
                ts: 1_706_745_900_000,
                value: 10_450_000,
            },
        ]
    }
}
