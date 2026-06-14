# Examples

Examples are integration demonstrations and benchmark runners. They are not
FastK core business logic.

FastK core remains a storage engine. Any example that fetches data, calculates a
derived value, or simulates a workflow is demonstrating how an upper layer can
call FastK storage APIs.

## Layout

```text
examples/
  cli/          # subprocess tools used by external systems
  demos/        # small integration demos
  benchmarks/   # benchmark and acceptance runners
```

Cargo example names are kept stable through `Cargo.toml`, so commands such as
`cargo run --example fastk_bridge -- --help` still work.

## Demo / Integration

- `demos/backtest_facade_demo.rs`
  - Demonstrates storage-level read-session access through `BacktestStoreView`.
  - It does not execute a backtest, generate strategy signals, match orders, or manage a portfolio.
- `demos/indicator_series_demo.rs`
  - Demonstrates an external materializer calculating rows outside FastK and writing the computed scalar results back.
  - Calculation happens in the example, not in FastK core.
- `demos/prepare_python_demo_store.rs`
  - Creates a small demo store for Python workflow tests.
  - It is setup code for examples only.

## Bridge / Admin

- `cli/fastk_bridge.rs`
  - Minimal subprocess JSON bridge for external tools.
  - It only reads/writes records and inventory JSON.
- `cli/fastk_admin.rs`
  - Storage health tooling: validate, scrub, repair, inventory, health, and capabilities.

## Benchmarks

- `benchmarks/bench_acceptance.rs`
  - Release acceptance benchmark for storage read/write paths.
- `benchmarks/bench_backtest_facade.rs`
  - Benchmark for the storage read facade and cache behavior.
- `benchmarks/bench_scalar_sidecars.rs`
  - Benchmark for scalar sidecars: point lookup, short-range, zmap, and vix.
- `benchmarks/bench_kline_storage_comparison.rs`
  - Rust-only storage comparison runner for FastK, CSV, JSONL, and SQLite3.
  - The default configuration writes about ten million synthetic kline rows.

## Boundary Note

Examples may mention backtests, indicators, Binance, or charts because they are
integration scenarios. Those responsibilities belong to external layers:

- market-data fetch belongs outside FastK
- indicator/feature/factor calculation belongs outside FastK
- backtest and paper-trading execution belongs outside FastK
- charting and dashboards belong outside FastK

FastK only stores and reads the records passed to it.
