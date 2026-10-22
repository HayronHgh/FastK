# FastK v0.2.0-rc.1

The independent `fastk-native` Python module returns named NumPy int64 batches
for Kline/Scalar reads and accepts typed batches for writes. Existing Rust,
CLI and subprocess contracts remain; native calls explicitly have no hard
timeout. Disk records stay 48-byte Kline / 16-byte Scalar.

Core changes fix cursor races with positional IO, keep write guards on the
stack, validate rejected inputs before recovery markers, reset compaction debt,
reuse sparse windows and bound capacity. Optional per-handle parallel reads
fill one ordered buffer with bounded workers. The main external comparison now
uses five in-process adapters with the same fully materialized NumPy output.

See [native contract](NATIVE_ENGINE.md), [verification](NATIVE_VALIDATION.md), and
[native quickstart](../clients/python-native/README.md).

# FastK v0.1.0-rc.1

## Private compact JSON update — 2026-10-20

Scoped sessions serialize the existing typed response as compact JSON directly
into their single Vec. A private CLI output wrapper selects presentation at the
adapter boundary. The same dispatcher, engine APIs and Python return dictionaries
remain; public one-shot CLI pretty JSON bytes are unchanged. No parser dependency,
public format option or automatic retry is introduced. Deadline, no-replay and
per-request ownership behavior are unchanged. See [compact verification](COMPACT_SESSION_RESULTS.md)
for paired bulk/1,430-short replay, memory measurements and compatibility evidence.

## Scoped buffer update — 2026-10-20

Scoped reads receive validated frames directly into a preallocated bytearray.
The Rust worker serializes its existing pretty response directly into one Vec,
clearing partial success bytes before returning a business error. Public methods,
result shapes, JSON bytes, timeout/no-replay and ownership behavior are retained.
Compact JSON was deferred in this earlier step. See [paired buffer verification](BUFFER_REUSE_RESULTS.md)
for same-workload latency, external process memory sampling and fault checks.

## Scoped Python worker update — 2026-10-20

`with Client(root) as store` reuses a local bridge worker through the existing
methods and return dictionaries. Calls outside a context retain one-shot
behavior. Ownership is acquired per request; an idle worker retains no store
guard. Timeout or protocol failure terminates and reaps the worker without
retry/restart, including interrupted writes. Concurrent requests are serialized;
a deadline while waiting sends nothing and leaves the active call alone.

See [transport semantics](SESSION_TRANSPORT.md) and [verified replay results](SESSION_REUSE_RESULTS.md).
This update retains the existing JSON serializer, record format and contract 1;
large JSON optimization remains separate. New local wheels are development
artifacts and do not overwrite the earlier technical-validation packages.

## Technical validation update — 2026-10-20

This working-tree update keeps MIT and the research/technical scope. Local
packages are labelled development artifacts; it does not claim enterprise
readiness or change the existing binary record format.

- Persistent OS-owned reader/writer locks; startup recovery and rebuild share
  ownership. Guards explicitly unlock to avoid transient inherited-fd locks.
- Writer metadata refresh, append-safe scalar bridge writes, atomic synced
  sidecars, and old-file cleanup after manifest commit.
- Healthy bridge writes no longer scan every unrelated chunk via repeated
  initialization; pending recovery remains handled under writer ownership.
- Manifest rebuild preserves valid scales and refuses to guess missing
  caller-specific semantic metadata. Repair reports its own work accurately.
- Recovery never lets an older orphan roll back a committed replacement.
- Buffered bridge JSON output preserves existing response bytes while reducing
  bulk pipe-transfer overhead; subprocess startup remains part of client cost.
- Explicit read-only opening, verified physical snapshots, complete sealed
  replay, and additive immutable registry publication (legacy statuses preserved).
- Dependency-free Python client, bundled Windows/Linux wheels, contract
  discovery, opt-in JSON errors and offline installation checks.
- Isolated public-API comparisons against SQLite WITHOUT ROWID, DuckDB,
  Parquet and Arrow IPC; original CSV/JSONL numbers remain historical evidence.

See [Core Contract](CORE_CONTRACT.md), [Installation](INSTALLATION.md) and
[External Validation](EXTERNAL_VALIDATION.md) for exact boundaries and commands.
Python subprocess overhead must be measured and must not be hidden behind
Rust-only timings.

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
