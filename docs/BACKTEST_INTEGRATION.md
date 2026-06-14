# Backtest Integration Boundary

FastK is not a backtest engine.

This document describes how an upper-layer backtest system can use FastK as a
storage implementation without making FastK responsible for simulation logic.

## Responsibility Split

FastK provides:

- kline, scalar, and high-frequency fixed-record storage
- range queries and latest-N queries
- sealed-chunk replay through `ReplayCursor`
- storage inventory, health, checksum, scrub, and repair tools
- `BacktestStoreView` as a storage-level read-session facade

FastK does not provide:

- event-loop simulation
- strategy execution
- signal generation
- portfolio construction
- order matching
- fill modeling
- slippage modeling
- broker adapters
- performance attribution
- factor IC or forward-return analysis

## Preferred Dependency Direction

Production architecture should avoid making the backtest engine depend directly
on FastK as a business entry point.

Preferred dependency:

```text
Backtest Engine
    |
    v
MarketDataReplay abstraction
    |
    v
FastK adapter
    |
    v
FastKStore / ReplayCursor
```

The adapter maps business requests to storage calls. FastK remains replaceable
as the storage implementation behind the adapter.

## Dataset Version Rule

Deterministic backtests must use explicit dataset versions:

```rust
let dataset_ref = registry
    .dataset_ref("binance_spot_clean", "v20260424")?
    .expect("dataset must exist");
let root = registry.resolve_dataset_ref(&dataset_ref)?;
let store = FastKStore::open(root)?;
```

`resolve_latest_fastk_root(dataset_id)` is intended for CLI and interactive
inspection only. It must not be the default for deterministic backtests.

## BacktestStoreView Scope

`BacktestStoreView` is a read-session convenience facade over `FastKReadSession`.
It can attach series, prewarm caches, and run storage reads.

It does not run a backtest.

Allowed usage:

- `view.initialize(&plan)`
- `view.attach_kline(...)`
- `view.attach_scalar(...)`
- `view.get_kline_range(...)`
- `view.get_scalar_range(...)`
- `view.find_scalar_timestamps_via_zmap(...)`
- `view.find_scalar_timestamps_via_vix(...)`

The upper-layer backtest engine owns clock progression, event scheduling,
strategy state, orders, fills, and portfolio accounting.

## ReplayCursor Scope

`ReplayCursor` can feed stored records to an upper-layer replay adapter:

```rust
let mut cursor = store.replay_trade("BTCUSDT", "binance_spot", start_ts, Some(end_ts))?;

loop {
    let batch = cursor.next_batch(1024)?;
    if batch.is_empty() {
        break;
    }
    for record in batch {
        // Hand record to the upper-layer event loop.
        let _ = record;
    }
}
```

`ReplayCursor` reads sealed chunks only. It does not follow live hot segments,
own paper trading, or execute a strategy.

## Integration Examples

The examples in this repository are integration examples only:

- [examples/demos/backtest_facade_demo.rs](../examples/demos/backtest_facade_demo.rs)
- [examples/demos/indicator_series_demo.rs](../examples/demos/indicator_series_demo.rs)

They demonstrate storage access patterns. They are not FastK core business
logic and should not be copied as a complete research or trading architecture.
