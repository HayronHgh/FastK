# FastK v0.1.0-rc.1

## Release Positioning

`v0.1.0-rc.1` is the first release candidate for FastK as a Rust crate.

This release candidate focuses on:

- read-heavy fixed-record time-series storage,
- storage-level access patterns used by backtest adapters,
- scalar / indicator derived-series persistence for already-computed values,
- operational tooling,
- external integration through a JSON bridge and Python workflow.

FastK remains a storage engine. It does not perform market-data ingestion,
indicator/feature/factor calculation, strategy execution, backtest execution, or
trading itself.

## Main Capabilities

### Kline Base Series

- fixed-record `KlineRecord` storage
- monthly UTC chunk layout
- binary chunk header and manifest
- point lookup, range read, latest-N, and full scan

### Scalar / Indicator Derived Series

- fixed-record `ScalarRecord` storage
- formal indicator register / read / write interface
- sidecar lifecycle for `.zmap` and `.vix`
- predicate-query support for derived series
- storage-level scalar predicate query over `ScalarRecord.value` for continuous and discrete predicates

### Backtest Adapter Integration

- `BacktestStoreView` as a storage-level read facade
- session attach / prewarm / cache summary / reset
- inventory, capability, and health snapshots

### Tooling and Integration

- `fastk_bridge` for JSON subprocess integration
- `fastk_bridge query-scalar-predicate` for storage-level scalar predicate queries
- `ScalarPredicateExpr` as the full predicate expression for new scalar
  predicate queries; `ScalarPredicate` remains the legacy compatibility shape
  for older zmap/vix helper APIs
- predicate query stats that expose fallback scan behavior through
  `fallback_scan`, `index_used`, and `rows_checked`
- Python workflow as an external integration example for Binance data fetch, MA20 materialization, and chart output
- `fastk_admin` for validate / scrub / repair / rebuild-manifest workflows
- release smoke tests and benchmark runners
- portable release packaging scripts for `fastk_bridge` and `fastk_admin`
- release manifest and SHA256 checksum generation
- backend integration and bridge contract documentation

## Known Limitations

- ApproxCold benchmarks are not equivalent to true OS cold-cache measurements.
- Parent-directory `fsync` on Windows is best-effort.
- Overlap handling is intentionally conservative.
- FastK does not compute indicators internally.
- FastK does not own market-data ingestion, feature/factor calculation, backtest execution, or trading.
- `tools/python/` intentionally remains outside the crate as an integration layer.

## Usage Notes

- This is a release candidate, not a final 1.0 stability promise.
- The recommended public surface is approaching stability:
  - `FastKStore`
  - `BacktestStoreView`
  - kline / scalar / indicator records and keys
  - inventory / capability / health APIs
  - bridge contracts used by the Python workflow
- Internal chunk / manifest / cache implementation details should still be treated as non-stable.

## Suggested Release Gates

- `cargo test`
- `cargo check --examples`
- `python -m unittest discover tools/python`
- `cargo test --test release_smoke`
- `cargo run --example backtest_facade_demo`
- `cargo run --example indicator_series_demo`
- `cargo run --example prepare_python_demo_store -- --root <path>`
- `cargo run --example fastk_bridge -- --help`
- `cargo run --example fastk_admin -- validate --root <path> --verbose`
- `cargo run --example fastk_admin -- scrub --root <path> --verbose --dry-run`
- `scripts/build_release.ps1` or `scripts/build_release.sh`
- `scripts/smoke_release.ps1` or `scripts/smoke_release.sh`

## Upgrade and Integration Guidance

- Prefer integrating through crate-root re-exports such as `fastk::FastKStore` and `fastk::BacktestStoreView`.
- Treat `tools/python/` as integration examples rather than core library API.
- Keep indicator, feature, factor, strategy, backtest, and trading logic outside FastK. Use FastK as the persistence layer for records produced by those upper layers.
