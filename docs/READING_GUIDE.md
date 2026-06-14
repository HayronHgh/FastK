# FastK Reading Guide

Read this repository as a storage engine first.

The fastest way to understand the codebase is to keep this boundary in mind:

```text
FastK = Versioned Fixed-Record Time-Series Storage Engine
```

FastK does not fetch market data, calculate indicators/features/factors, run
strategies, execute backtests, place orders, or render dashboards.

## First Pass

Start with:

1. `docs/ARCHITECTURE_BOUNDARY.md`
2. `README.md`
3. `src/lib.rs`
4. `src/store_core/engine/store.rs`
5. `src/types/meta.rs`

These files explain the public storage API, fixed-record model, manifest model,
and partition rules.

## Storage Write Path

Read:

1. `src/store_core/engine/store.rs`
2. `src/store_core/chunk/kline_writer.rs`
3. `src/store_core/chunk/scalar_writer.rs`
4. `src/store_core/storage/manifest.rs`
5. `src/store_core/storage/fs.rs`

Focus on:

- caller-normalized records entering FastK
- fixed record validation
- chunk file creation
- manifest updates
- checksum and atomic replace behavior

## Storage Read Path

Read:

1. `src/store_core/engine/store.rs`
2. `src/store_core/engine/replay.rs`
3. `src/store_core/engine/sequence.rs`
4. `src/store_core/chunk/kline_reader.rs`
5. `src/store_core/chunk/scalar_reader.rs`
6. `src/store_core/index/zmap.rs`
7. `src/store_core/index/vix.rs`

Focus on:

- range reads
- latest-N reads
- scalar predicate reads
- sealed-chunk replay
- storage-level sequence scan reports
- cache behavior

## Metadata And Control Plane

Read:

1. `src/control/dataset_registry.rs`
2. `docs/ARCHITECTURE_BOUNDARY.md`

The SQLite registry is a control-plane catalog. It tracks dataset versions,
feature outputs, factor outputs, and lineage metadata. It does not make
research, promotion, or trading decisions.

## Backtest Adapter Surface

Read:

1. `docs/BACKTEST_INTEGRATION.md`
2. `src/store_core/engine/backtest.rs`
3. `examples/demos/backtest_facade_demo.rs`

`BacktestStoreView` is a storage read facade. It is useful behind an upper-layer
backtest adapter, but it is not a backtest engine.

## Examples And Tools

Read:

1. `examples/README.md`
2. repo-root `tools/python/README.md`

Examples can contain integration logic such as MA20 calculation or Binance
fetching because they live outside core storage. Do not move that behavior into
`src/`.

## Invariants To Preserve

- Kline and scalar binary layouts remain stable.
- Existing indicator compatibility remains stable.
- Public storage APIs remain storage-oriented.
- Deterministic workflows use explicit `DatasetRef(dataset_id, version)`.
- `resolve_latest_fastk_root` is limited to CLI and interactive inspection.
- FastK stores computed results; it does not compute them.
