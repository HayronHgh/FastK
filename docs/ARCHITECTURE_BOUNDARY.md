# Architecture Boundary

FastK is a storage engine.

The architectural contract is:

```text
FastK = Versioned Fixed-Record Time-Series Storage Engine
```

FastK is not a market-data service, feature engine, factor engine, backtest
engine, trading engine, or research platform.

## FastK Responsibilities

FastK owns storage-level behavior only:

- accept already-normalized fixed records from callers
- write records into caller-selected series, dataset versions, categories, and partitions
- read records by range, latest-N, scalar predicate, or sealed-chunk replay
- manage storage metadata such as dataset id, dataset version, root path, series key, category, timeframe, partition, chunk manifest, checksum, record type, and schema id
- report storage inventory, manifest health, chunk health, checksums, and sealed replay status
- provide storage-level scan primitives, such as sequence scans, without exchange-specific repair policy

FastK does not choose data sources, compute derived values, decide where records
should go, or invoke downstream systems.

## FastK Non-Responsibilities

FastK does not:

- connect to exchanges
- open WebSocket streams
- fetch REST API pages
- parse exchange-native payloads
- decide ingestion cadence
- choose dataset destinations
- calculate indicators, features, or factors
- evaluate factor quality
- generate strategy signals
- construct portfolios
- execute backtests
- place, match, route, or execute orders
- implement broker adapters
- run live trading
- implement market-data gap recovery policy
- repair exchange-specific sequence gaps
- run promotion gates
- evaluate alert rules
- serve dashboards

Those behaviors belong in upper-layer services or applications.

## Write Boundary

FastK write APIs assume records have already been normalized by the caller.

For example:

- Kline callers decide the source, cleaning policy, dataset id, version, symbol, and timeframe. FastK writes `KlineRecord` rows to the requested series and chunk partition.
- Feature callers calculate the feature, choose the name/version/params/lineage, and write `ScalarRecord` rows under `category = "feature"`.
- Factor callers calculate the factor, choose formula/version/input refs, and write `ScalarRecord` rows under `category = "factor"`.
- Signal, portfolio, risk, and metric callers produce scalar records. FastK stores them without interpreting strategy meaning.
- Market-data layers parse exchange payloads and normalize `TradeRecord`, `BboRecord`, or `BookDeltaRecord`. FastK does not know Binance, Bybit, OKX, or any exchange-native schema.

## Read Boundary

FastK read APIs provide storage-level access:

- range queries
- latest-N queries
- scalar predicate queries
- sealed-chunk replay
- inventory
- validation, scrub, and repair
- sequence scan reports for stored numeric sequence fields

FastK does not:

- choose which dataset is suitable for a strategy
- silently fall back to latest datasets for deterministic backtests
- push records into strategies or factor evaluators
- calculate forward returns or factor IC
- generate trading signals
- repair market-data sequence gaps
- decide dataset promotion status

Deterministic research and backtests must use an explicit
`DatasetRef(dataset_id, version)` or `resolve_fastk_root(dataset_id, version)`.
`resolve_latest_fastk_root(dataset_id)` is for CLI and interactive inspection
only.

## Market Data Layer Relationship

The market-data layer owns source-specific behavior:

```text
External Exchange / CSV / API
    |
    v
Market Data Layer
    |
    v
FastK Storage Adapter
    |
    v
FastKStore
```

The market-data layer handles REST/WebSocket clients, source payload parsing,
normalization, sequence policy, gap recovery policy, and scheduling. The FastK
adapter only converts already-normalized records into FastK storage calls.

## Feature And Factor Engine Relationship

Feature and factor engines own calculation:

```text
Feature Engine / Factor Engine
    |
    v
FastK Storage Adapter
    |
    v
FastKStore
```

FastK provides scalar namespaces and storage APIs. It does not decide feature
definitions, factor formulas, lookback windows, validation metrics, or promotion
policy.

Scalar predicate query is still a storage API. FastK can answer questions such
as "which stored scalar rows have `value > 7000`" or "`value in [1, 2]`" for a
specific `ScalarSeriesKey` and time range. FastK does not know whether that
series is RSI, a factor score, a signal state, or a risk flag, and it does not
decide whether a match should trigger a strategy, route data, or promote a
dataset.

## Backtest Engine Relationship

Backtest engines should depend on an upper-layer replay abstraction, not on
FastK as a business entry point:

```text
Backtest Engine
    |
    v
MarketDataReplay abstraction
    |
    v
FastK Replay Adapter
    |
    v
FastK ReplayCursor
```

`ReplayCursor` is a sealed-chunk storage cursor. It returns stored records in a
deterministic order; it does not own event simulation, clock semantics, order
matching, fills, slippage, strategy state, or portfolio accounting.

## Trading Engine Relationship

Trading systems should not call FastK as a trading gateway. They should depend
on execution, risk, and market-data abstractions owned by the trading layer.

FastK can store signal, portfolio, risk, and metric series after another module
produces those records. It does not interpret the records or act on them.

## Upper-Layer Gateway Shape

Other modules should depend on business-level gateways. FastK is one possible
storage implementation behind those gateways.

Example shape only:

```rust
trait MarketDataGateway {
    fn get_kline_range(&self, req: KlineRangeRequest) -> Result<Vec<KlineBar>>;
    fn replay(&self, req: MarketReplayRequest) -> Result<Box<dyn MarketEventCursor>>;
}

trait FeatureStoreGateway {
    fn get_feature_range(&self, req: FeatureRangeRequest) -> Result<Vec<FeaturePoint>>;
}

trait FactorStoreGateway {
    fn get_factor_range(&self, req: FactorRangeRequest) -> Result<Vec<FactorPoint>>;
}
```

Do not implement these gateways inside FastK core. Implement them in the
application layer or an adapter crate.
