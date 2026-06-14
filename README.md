# FastK

FastK is a read-heavy, fixed-record, chunk-based time-series storage engine.

Current release target: `v0.1.0-rc.1`

## What FastK Is

- A Rust storage engine for sorted time-series data.
- Optimized for fixed-length records and read-heavy workloads.
- Built around binary manifests and time-based chunk partitions.
- Designed to store records that have already been normalized by an upper layer.
- Suitable for:
  - kline / OHLCV base series
  - scalar / indicator derived series
  - feature / factor / signal / portfolio / risk / metric scalar series
  - experimental trade / BBO / orderbook delta fixed-record market data
  - storage adapters used by backtest, research, and replay systems
  - benchmark-driven storage experiments

## What FastK Is Not

- Not a SQL database.
- Not a general-purpose database.
- Not an ANN / vector database.
- Not a market-data service.
- Not an indicator calculation engine.
- Not a feature, factor, strategy, backtest, or trading engine.
- Not a router that decides where data goes.
- Not a live ingestion service, exchange connector, broker adapter, alert engine, or dashboard.

FastK only stores records. Indicator, feature, factor, signal, portfolio, risk, and metric values are produced outside FastK and then written as fixed records. Source selection, cleaning, lineage policy, exchange gap recovery, and routing decisions also belong outside FastK.

The intended derived-series workflow is:

1. Read indicator coverage from FastK.
2. If coverage is missing, read the required kline range.
3. Compute the indicator outside FastK.
4. Write the derived scalar series back into FastK.

## Core Concepts

- `kline`
  - Base series storing `ts/open/high/low/close/volume`.
- `scalar / indicator`
  - Derived series storing `ts/value`.
  - Indicator series are scalar series with `category = "indicator"`.
- `feature / factor / signal / portfolio / risk / metric`
  - Canonical scalar namespaces for values produced by upper-layer systems.
- `trade / bbo / book_delta`
  - Experimental fixed-width high-frequency market-data records.
  - FastK stores normalized records only; exchange payload parsing and sequence policy are outside FastK.
- Chunk partitions
  - Kline and scalar series default to UTC-month chunks.
  - Trade and book-delta series default to UTC-hour chunks.
  - BBO series defaults to UTC-day chunks.
- Dataset registry
  - SQLite control-plane metadata for dataset versions, feature outputs, and factor outputs.
- Binary manifest
  - Tracks chunk metadata, state, checksum, and sidecars.
- Sidecars
  - Scalar series can attach `.zmap` and `.vix` sidecars for predicate queries.

## Cargo Dependency

Published version:

```toml
[dependencies]
fastk = "0.1.0-rc.1"
```

Path dependency during local integration:

```toml
[dependencies]
fastk = { path = "../FastKDB/kline_store" }
```

## Rust Quick Start

Create a store, register a kline series, write a chunk, then query it:

```rust
use fastk::{FastKStore, KlineRecord};

fn main() -> fastk::Result<()> {
    let mut store = FastKStore::open("data/fastk")?;
    store.init()?;

    store.register_kline_series("BTCUSDT", "1m", 60_000, 100_000, 100_000)?;
    store.put_kline_chunk(
        "BTCUSDT",
        "1m",
        &[KlineRecord {
            ts: 1_706_745_600_000,
            open: 10_000_000,
            high: 10_020_000,
            low: 9_980_000,
            close: 10_010_000,
            volume: 10_000,
        }],
    )?;

    let rows = store.get_kline_range("BTCUSDT", "1m", 1_706_745_600_000, 1_706_745_600_000)?;
    println!("rows={}", rows.len());
    Ok(())
}
```

Register and use an indicator series:

```rust
use fastk::{FastKStore, ScalarRecord};

fn main() -> fastk::Result<()> {
    let mut store = FastKStore::open("data/fastk")?;
    store.init()?;

    store.register_indicator_series("BTCUSDT", "1m", "ma20")?;
    store.put_indicator_chunk(
        "BTCUSDT",
        "1m",
        "ma20",
        &[ScalarRecord {
            ts: 1_706_745_720_000,
            value: 10_150_000,
        }],
    )?;

    let rows = store.get_indicator_range(
        "BTCUSDT",
        "1m",
        "ma20",
        1_706_745_720_000,
        1_706_745_720_000,
    )?;
    println!("indicator rows={}", rows.len());
    Ok(())
}
```

Use the storage read facade from a backtest adapter:

```rust
use fastk::{BacktestKlineBinding, BacktestPreparePlan, FastKStore, ScopedScalarBinding};

fn main() -> fastk::Result<()> {
    let store = FastKStore::open("data/fastk")?;
    let mut view = store.backtest_view();

    view.initialize(&BacktestPreparePlan {
        kline: vec![BacktestKlineBinding {
            symbol: "BTCUSDT".into(),
            timeframe: "1m".into(),
        }],
        scalar: vec![ScopedScalarBinding::indicator("BTCUSDT", "1m", "ma20")],
        prewarm: true,
    })?;

    let rows = view.get_kline_range("BTCUSDT", "1m", 1_706_745_600_000, 1_706_745_960_000)?;
    println!("backtest rows={}", rows.len());
    Ok(())
}
```

Register versioned dataset metadata in the control-plane catalog:

```rust
use fastk::{DatasetManifestRecord, DatasetRef, DatasetRegistry};

fn main() -> fastk::Result<()> {
    let registry = DatasetRegistry::open("data/fastk/control.sqlite")?;
    registry.upsert_dataset(&DatasetManifestRecord {
        dataset_id: "binance_spot_clean".into(),
        version: "v20260424".into(),
        fastk_root: "data/fastk/datasets/binance_spot_clean/v20260424".into(),
        source: "binance".into(),
        market: "spot".into(),
        start_ts: 1_714_000_000_000,
        end_ts: 1_714_086_400_000,
        schema_version: "v1".into(),
        checksum: None,
        status: "sealed".into(),
        created_at: 1_714_086_400_000,
    })?;

    let dataset_ref = DatasetRef::new(
        "binance_spot_clean",
        "v20260424",
        "data/fastk/datasets/binance_spot_clean/v20260424",
    )?;
    let root = registry.resolve_dataset_ref(&dataset_ref)?;
    println!("fastk_root={}", root.display());

    // Explicit version resolving is deterministic and safe for backtests.
    let same_root = registry.resolve_fastk_root("binance_spot_clean", "v20260424")?;
    assert_eq!(root, same_root);

    // Latest resolving is for CLI/interactive workflows, not deterministic backtests.
    let _latest = registry.resolve_latest_fastk_root("binance_spot_clean")?;
    Ok(())
}
```

Build canonical scalar keys without calculating the values inside FastK:

```rust
use fastk::{
    factor_series_key, feature_series_key, indicator_series_key, metric_series_key,
    portfolio_series_key, risk_series_key, signal_series_key,
};

let _indicator = indicator_series_key("BTCUSDT", "1m", "ma20");
let _feature = feature_series_key("BTCUSDT", "1m", "rsi_14");
let _factor = factor_series_key("BTCUSDT", "1m", "momentum_score");
let _signal = signal_series_key("BTCUSDT", "strategy_v2", "target_weight");
let _portfolio = portfolio_series_key("run_20260424", "nav");
let _risk = risk_series_key("BTCUSDT", "1m", "drawdown");
let _metric = metric_series_key("__system__", "1s", "query_latency");
```

Query stored scalar values with storage-level predicates:

```rust
use fastk::{feature_series_key, FastKStore, ScalarPredicateExpr, ScalarPredicateQuery};

fn main() -> fastk::Result<()> {
    let store = FastKStore::open("data/fastk")?;
    let key = feature_series_key("BTCUSDT", "15m", "rsi14");
    let start_ts = 1_700_000_000_000;
    let end_ts = 1_710_000_000_000;

    let result = store.query_scalar_predicate(ScalarPredicateQuery {
        key,
        start_ts,
        end_ts,
        predicate: ScalarPredicateExpr::Gt(7000),
        return_values: true,
    })?;

    println!("matches={}", result.matches.len());
    println!("stats={:?}", result.stats);
    Ok(())
}
```

FastK only compares `ScalarRecord.value`. The meaning of `rsi14 > 7000`,
`momentum_score > 12000`, or `state in [1, 2]` belongs to the caller. Continuous
values are usually stored as scaled integers. Discrete states can be encoded by
the caller into `i64` values.

Predicate type naming:

- `ScalarPredicate` is the legacy / compatibility predicate shape used by older
  zmap/vix helper APIs.
- `ScalarPredicateExpr` is the full predicate expression for new
  `query_scalar_predicate` calls. It supports `eq`, `ne`, `gt`, `gte`, `lt`,
  `lte`, inclusive/exclusive `between`, `in-set`, and `not-in-set`.

Index cost expectations:

- `Eq` and `InSet` usually fit the `.vix` value index.
- `Gt`, `Gte`, `Lt`, `Lte`, and `Between` usually fit `.zmap` zone-map pruning.
- `Ne` and `NotInSet` often require full scan fallback. Check
  `ScalarPredicateQueryStats.fallback_scan`, `index_used`, and `rows_checked`
  before treating a predicate query as index-accelerated.

Write and query experimental high-frequency fixed records:

```rust
use fastk::{FastKStore, TradeRecord};

fn main() -> fastk::Result<()> {
    let mut store = FastKStore::open("data/fastk")?;
    store.init()?;
    store.register_trade_series("BTCUSDT", "binance_spot", 0)?;
    store.put_trade_chunk(
        "BTCUSDT",
        "binance_spot",
        &[TradeRecord {
            ts: 1_706_745_600_000,
            recv_ts: 1_706_745_600_010,
            trade_id: 1,
            price: 10_000_000,
            qty: 100,
            side: 1,
            flags: 0,
            _pad: [0; 6],
        }],
    )?;
    let rows = store.get_trade_range("BTCUSDT", "binance_spot", 1_706_745_600_000, 1_706_745_600_000)?;
    println!("trade rows={}", rows.len());
    Ok(())
}
```

Replay sealed chunks from a storage-level cursor:

```rust
let start_ts = 1_706_745_600_000;
let end_ts = 1_706_749_260_000;
let mut cursor = store.replay_trade(
    "BTCUSDT",
    "binance_spot",
    start_ts,
    Some(end_ts),
)?;

loop {
    let batch = cursor.next_batch(1024)?;
    if batch.is_empty() {
        break;
    }

    for trade in batch {
        // Pass the stored record to an upper-layer replay/backtest adapter.
        let _ = trade;
    }
}
```

Scan stored high-frequency sequence fields without applying exchange policy:

```rust
let report = store.scan_book_delta_sequence(
    "BTCUSDT",
    "binance_spot",
    start_ts,
    end_ts,
)?;

if !report.is_clean() {
    println!(
        "gaps={} duplicates={} violations={}",
        report.gap_count(),
        report.duplicate_count(),
        report.violation_count()
    );
}
```

Sequence scans only report adjacent numeric gaps, duplicates, and non-monotonic
values observed in stored records. Market-data layers decide whether and how to
repair gaps, downgrade dataset status, or block research/backtest/live usage.

## Recommended Public API

For other Rust modules, the recommended entry points are:

- `fastk::FastKStore`
- `fastk::FastKReadSession`
- `fastk::BacktestStoreView`
- `fastk::KlineRecord`
- `fastk::ScalarRecord`
- `fastk::TradeRecord`
- `fastk::BboRecord`
- `fastk::BookDeltaRecord`
- `fastk::PartitionPolicy`
- `fastk::ScalarPredicateExpr`
- `fastk::ScalarPredicateQuery`
- `fastk::ScalarPredicateQueryResult`
- `fastk::ScalarPredicateQueryStats`
- `fastk::ScalarIndexKind`
- `fastk::ReplayCursor`
- `fastk::ReplayOptions`
- `fastk::SequenceScanReport`
- `fastk::SequenceGap`
- `fastk::SequenceDuplicate`
- `fastk::SequenceViolation`
- `fastk::ScalarSeriesKey`
- `fastk::DatasetRegistry`
- `fastk::DatasetRef`
- `fastk::indicator_series_key`
- `fastk::feature_series_key`
- `fastk::factor_series_key`
- `fastk::signal_series_key`
- `fastk::portfolio_series_key`
- `fastk::risk_series_key`
- `fastk::metric_series_key`
- `fastk::ScopedScalarBinding`
- `fastk::StoreStats`
- `fastk::StoreHealthSummary`
- `fastk::SeriesInventoryEntry`
- `fastk::ScalarQueryCapabilities`

Compatibility surface:

- `fastk::ScalarPredicate` remains available for legacy zmap/vix helper APIs.
  New backend adapters should prefer `fastk::ScalarPredicateExpr` with
  `query_scalar_predicate`.

The following implementation areas are intentionally internal and should not be treated as stable integration surface:

- chunk header/layout internals
- manifest binary codec internals
- storage path helpers
- low-level cache implementation details

## Bridge CLI in the Architecture

`fastk_bridge` is the minimal JSON bridge used by external tools such as Python scripts.
It only reads and writes JSON records; it does not calculate indicators, fetch market data, or decide data flow.

Supported commands include:

- `read-kline-range`
- `write-kline-range`
- `read-indicator-range`
- `write-indicator-range`
- `indicator-inventory`
- `read-scalar-range`
- `query-scalar-predicate`
- `write-scalar-range`
- `scalar-inventory`
- `kline-inventory`

Build and inspect it:

```bash
cargo run --example fastk_bridge -- --help
cargo run --example fastk_bridge -- read-scalar-range --root ./data/store --symbol BTCUSDT --timeframe 1m --category feature --name rsi_14 --start-ts 1706745600000 --end-ts 1706749200000
cargo run --example fastk_bridge -- query-scalar-predicate --root ./data/store --symbol BTCUSDT --timeframe 1m --category feature --name rsi_14 --start-ts 1706745600000 --end-ts 1706749200000 --predicate gt --value 7000 --return-values
```

Signal-like values can use the same scalar bridge as a caller-side convention:

```bash
cargo run --example fastk_bridge -- write-scalar-range --root ./data/store --symbol BTCUSDT --timeframe 15m --category signal --name sigvec_test --input-json ./signal_rows.json
cargo run --example fastk_bridge -- read-scalar-range --root ./data/store --symbol BTCUSDT --timeframe 15m --category signal --name sigvec_test --start-ts 1706745600000 --end-ts 1706747400000
cargo run --example fastk_bridge -- query-scalar-predicate --root ./data/store --symbol BTCUSDT --timeframe 15m --category signal --name sigvec_test --start-ts 1706745600000 --end-ts 1706747400000 --predicate ne --value 0 --return-values
cargo run --example fastk_bridge -- query-scalar-predicate --root ./data/store --symbol BTCUSDT --timeframe 15m --category signal --name sigvec_test --start-ts 1706745600000 --end-ts 1706747400000 --predicate eq --value 1 --return-values
cargo run --example fastk_bridge -- query-scalar-predicate --root ./data/store --symbol BTCUSDT --timeframe 15m --category signal --name sigvec_test --start-ts 1706745600000 --end-ts 1706747400000 --predicate eq --value -1 --return-values
```

FastK does not interpret `signal` categories or values such as `1` and `-1`.
They are ordinary scalar categories and integer scalar values; any active,
long, short, approval, rule, report, or strategy meaning belongs to the caller.
See [Signal-as-Scalar Bridge](docs/SIGNAL_SCALAR_STORAGE.md).

## Python Workflow in the Architecture

Repo-root `tools/python/` is intentionally outside the crate. It is an integration layer, not part of the core Rust storage engine.

The Python workflow demonstrates:

1. fetch kline data from Binance,
2. seed FastK once,
3. read indicator coverage,
4. compute MA20 outside FastK if missing,
5. write MA20 back into FastK,
6. render a chart.

This is an integration example only. Binance fetching, MA20 calculation, and chart rendering happen outside FastK. FastK only stores the kline and computed scalar rows passed through `fastk_bridge`.

Example:

```bash
python tools/python/binance_kline_ma20_workflow.py --help
python tools/python/plot_kline_ma20.py --help
```

## Admin / Recovery Tooling

The example admin CLI covers:

- `validate`
- `scrub`
- `repair --dry-run`
- `rebuild-manifest`
- inventory / overlap explanation

Example:

```bash
cargo run --example fastk_admin -- validate --root ./data/store --verbose
cargo run --example fastk_admin -- scrub --root ./data/store --verbose --dry-run
```

## Project Layout

- `src/`
  - public crate API plus domain-oriented modules
  - `store_core/`: engine, chunk IO, manifest, index, cache, recovery
  - `kline/`: kline domain record and public namespace
  - `feature/`: scalar / feature / predicate records and public namespace
- `examples/`
  - `cli/`: bridge and admin subprocess tools
  - `demos/`: integration demos
  - `benchmarks/`: benchmark and acceptance runners
- `docs/`
  - architecture, lifecycle, integration, release, and benchmark reports
- `scripts/`
  - portable release packaging and offline smoke scripts
- `schemas/`
  - documentation schemas for bridge commands and release manifests
- `tests/`
  - integration and release smoke tests
- repo-root `tools/python/`
  - external Python integration workflow and tests, not FastK core
- `docs/ARCHITECTURE_BOUNDARY.md`
  - storage-engine responsibility boundary
- `docs/BACKTEST_INTEGRATION.md`
  - backtest-facing integration guide
- `docs/STORE_LIFECYCLE.md`
  - chunk, manifest, state, and sidecar lifecycle
- `docs/RELEASE_CHECKLIST.md`
  - release validation checklist
- `docs/BACKEND_INTEGRATION.md`
  - Rust crate and subprocess bridge integration guide
- `docs/BRIDGE_CONTRACT.md`
  - current JSON bridge command, response, and error contract

## Known Limitations

- ApproxCold benchmarks are not the same as true OS cold-cache measurements.
- Parent-directory `fsync` on Windows is best-effort.
- Overlap handling is intentionally conservative.
- FastK does not calculate indicators.
- FastK does not calculate features or factors; it stores their computed scalar outputs.
- FastK does not own market-data ingestion, exchange clients, routing, strategies, backtests, trading, alerts, or dashboards.
- FastK does not decide where data goes; callers choose dataset, series key, category, and partition policy.
- `resolve_latest_fastk_root` is intended for CLI/interactive inspection and should not be used as a deterministic backtest default.
- `ReplayCursor` currently reads sealed chunks only.
- `ReplayCursor` does not follow live hot segments.
- `TailCursor` is not implemented yet.
- WAL / Hot Segment is not implemented yet.
- Trade/BBO/book-delta APIs are fixed-record only and do not yet include WAL, hot segments, or tail cursors.
- Sequence scan is a storage-level primitive only; it does not repair gaps or apply exchange-specific sequence policy.
- repo-root `tools/python/` intentionally lives outside the crate as an integration layer.

## RC Status

FastK is being prepared for `v0.1.0-rc.1`.

Stable-by-intent surfaces for this RC:

- `FastKStore`
- `BacktestStoreView`
- kline / scalar / indicator register-read-write APIs
- dataset registry explicit version resolver
- validation / inventory / health / capability APIs
- bridge contracts used by the Python workflow

Not guaranteed fully frozen yet:

- trade / BBO / book-delta APIs
- `ReplayCursor`
- trade / BBO / book-delta replay
- BBO/book-delta/trade-id sequence scan reports
- day/hour partition policy internals for high-frequency series
- lower-level internal modules
- binary layout evolution details
- benchmark and instrumentation scaffolding

## Related Documents

- [Architecture Boundary](docs/ARCHITECTURE_BOUNDARY.md)
- [Backtest Integration](docs/BACKTEST_INTEGRATION.md)
- [Store Lifecycle](docs/STORE_LIFECYCLE.md)
- [Replay and Tail](docs/REPLAY_AND_TAIL.md)
- [Reading Guide](docs/READING_GUIDE.md)
- [Release Checklist](docs/RELEASE_CHECKLIST.md)
- [Release Notes](docs/RELEASE_NOTES.md)
- [Backend Integration](docs/BACKEND_INTEGRATION.md)
- [Bridge Contract](docs/BRIDGE_CONTRACT.md)
- [Project Structure](docs/PROJECT_STRUCTURE.md)
- [Kline Storage Comparison](docs/KLINE_STORAGE_COMPARISON.md)
- [Signal-as-Scalar Bridge](docs/SIGNAL_SCALAR_STORAGE.md)
