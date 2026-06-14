# Backend Integration

FastK is a versioned fixed-record time-series storage engine. It is a storage
component that backend services can call through the Rust crate API or through
the `fastk_bridge` CLI.

```text
Backend / Application
    |
    v
Storage Adapter
    |
    v
FastK crate or fastk_bridge
```

FastK does not become the backend service. It does not own market-data
ingestion, exchange clients, feature or factor computation, strategy execution,
backtests, trading, broker adapters, alerts, dashboards, or routing decisions.

## Rust Backend

Use the crate directly when the backend is written in Rust:

```toml
[dependencies]
fastk = { path = "../FastKDB/kline_store" }
```

When the crate is published, replace the path dependency with the release
version:

```toml
[dependencies]
fastk = "0.1.0-rc.1"
```

Recommended entry points:

- `fastk::FastKStore`
- `fastk::DatasetRegistry`
- `fastk::DatasetRef`
- `fastk::ReplayCursor`
- `fastk::KlineRecord`
- `fastk::ScalarRecord`
- `fastk::TradeRecord`
- `fastk::BboRecord`
- `fastk::BookDeltaRecord`
- `fastk::SequenceScanReport`

Deterministic workflows must resolve an explicit dataset version:

```rust
use fastk::{DatasetRef, DatasetRegistry, FastKStore};

fn open_dataset() -> fastk::Result<FastKStore> {
    let registry = DatasetRegistry::open("control.sqlite")?;
    let dataset_ref = DatasetRef::new(
        "binance_spot_clean",
        "v20260424",
        "data/fastk/datasets/binance_spot_clean/v20260424",
    )?;
    let root = registry.resolve_dataset_ref(&dataset_ref)?;
    FastKStore::open(root)
}
```

`resolve_latest_fastk_root(dataset_id)` is for CLI and interactive inspection.
It must not be the default for deterministic backtests or reproducible research.

## Non-Rust Backend

Use `fastk_bridge` as a subprocess when the backend is not Rust:

```text
Backend process
    |
    v
subprocess fastk_bridge
    |
    v
FastK root
```

`fastk_bridge` is a static release contract for JSON read/write/inventory
operations. It is not a server, daemon, scheduler, fetcher, calculator, router,
or strategy component.

Example:

```bash
fastk_bridge read-scalar-range \
  --root ./store \
  --symbol BTCUSDT \
  --timeframe 1m \
  --category feature \
  --name rsi_14 \
  --start-ts 1706745600000 \
  --end-ts 1706749200000
```

## Backend-Owned Responsibilities

The backend or application layer owns:

- job scheduling
- market-data source selection
- exchange clients and payload parsing
- cleaning and normalization policy
- dataset version promotion policy
- feature and factor computation
- factor evaluation
- strategy, backtest, and trading decisions
- broker adapters
- alerts and dashboards
- routing records to downstream modules

## FastK-Owned Responsibilities

FastK owns storage-level behavior:

- write normalized fixed records
- read ranges and latest-N rows
- query stored scalar values with generic predicates
- replay sealed chunks through `ReplayCursor`
- scan storage-level sequence fields
- maintain manifest, checksum, inventory, and health metadata
- maintain SQLite control-plane dataset, feature, and factor registries
- expose the `fastk_bridge` JSON contract for subprocess integration

Scalar predicate queries only compare `ScalarRecord.value`:

```rust
use fastk::{feature_series_key, ScalarPredicateExpr, ScalarPredicateQuery};

let query = ScalarPredicateQuery {
    key: feature_series_key("BTCUSDT", "15m", "rsi14"),
    start_ts,
    end_ts,
    predicate: ScalarPredicateExpr::Gt(7000),
    return_values: true,
};

let result = store.query_scalar_predicate(query)?;
```

The backend decides what `rsi14 > 7000` means. FastK only returns stored rows
whose integer value matches the predicate.

Predicate type naming for adapters:

- `ScalarPredicate` is the legacy / compatibility predicate shape used by older
  zmap/vix helper APIs.
- `ScalarPredicateExpr` is the full predicate expression for new
  `query_scalar_predicate` calls. Backend adapters and gateway wrappers should
  use this type for `eq`, `ne`, `gt`, `gte`, `lt`, `lte`, inclusive/exclusive
  `between`, `in-set`, and `not-in-set`.

Query cost expectations:

- `Eq` and `InSet` usually use the `.vix` value index when present.
- `Gt`, `Gte`, `Lt`, `Lte`, and `Between` usually use `.zmap` zone-map pruning
  when present.
- `Ne` and `NotInSet` commonly require full scan fallback. Inspect
  `ScalarPredicateQueryStats.fallback_scan`, `index_used`, and `rows_checked`
  before assuming a query was index-accelerated.

## Boundary Rule

Backend adapters should map business requests onto FastK storage calls. They
should not push market-data gateway, feature engine, factor engine, backtest
engine, trading, or service runtime code into FastK core.
