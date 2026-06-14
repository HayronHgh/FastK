# Release Checklist

This checklist validates FastK as a storage engine. It does not validate
strategy, research, trading, market-data sourcing, or indicator correctness.

## Correctness Gate

Run:

```powershell
cargo fmt -- --check
cargo test
cargo test --all-targets
cargo build --release --examples
cargo run --example fastk_bridge -- --help
cargo run --example fastk_admin -- validate --root target\release-check-store --verbose
```

Recommended additional checks:

```powershell
python -m unittest discover ..\tools\python
cargo run --quiet --example backtest_facade_demo
cargo run --quiet --example indicator_series_demo
cargo run --quiet --example prepare_python_demo_store -- --root target\python-demo-store
cargo run --quiet --example fastk_admin -- validate --root target\python-demo-store --verbose
cargo run --quiet --example fastk_admin -- scrub --root target\python-demo-store --verbose --dry-run
```

Shell equivalent:

```bash
cargo fmt -- --check
cargo test
cargo test --all-targets
cargo build --release --examples
cargo run --example fastk_bridge -- --help
cargo run --example fastk_admin -- validate --root ./target/release-check-store --verbose
python -m unittest discover ../tools/python
./scripts/build_release.sh
./scripts/smoke_release.sh
```

## Storage Boundary Gate

Before release, confirm:

- `src/` does not contain exchange clients, WebSocket clients, or REST fetchers.
- `src/` does not calculate indicators, features, factors, signals, or portfolio state.
- `src/` does not run strategies, backtests, order matching, broker adapters, or live trading.
- sequence scan APIs only report storage observations and do not repair gaps or apply exchange-specific policy.
- `fastk_bridge` only reads/writes JSON records and inventory.
- examples and repo-root `tools/python/` are clearly marked as integration examples only.
- deterministic workflows use explicit `DatasetRef(dataset_id, version)`.
- `resolve_latest_fastk_root` is documented as CLI/interactive only.
- scalar predicate query APIs remain storage-level and only compare `ScalarRecord.value`.
- no feature semantic API, factor evaluation API, strategy condition API, or signal generation API is added to FastK.

## Core Dependency Boundary

Run:

```powershell
cargo tree
```

Confirm:

- no exchange client in core
- no websocket client in core
- no web server in core
- no strategy, backtest engine, or trading dependency in core
- no `reqwest`, `tungstenite`, `tokio-tungstenite`, exchange SDK, `axum`, `actix-web`, or HTTP server framework in core dependencies

SQLite control-plane support through `rusqlite` is allowed. External examples
and repo-root `tools/python/` may demonstrate integration workflows, but those
are not FastK core service dependencies.

## Operability Gate

Validate storage health tooling:

- `fastk_admin validate`
- `fastk_admin scrub`
- `fastk_admin repair --dry-run`
- `fastk_admin inventory`
- `fastk_admin health`
- `fastk_admin capabilities`

## Scalar Predicate Query Gate

Confirm:

- scalar predicate query tests pass
- docs identify `ScalarPredicate` as the legacy / compatibility shape and
  `ScalarPredicateExpr` as the full new predicate expression
- `Eq`, `Ne`, `Gt`, `Gte`, `Lt`, `Lte`, inclusive/exclusive `Between`, `InSet`, and `NotInSet` are covered
- empty `InSet` is safe
- invalid `Between { min > max }` is rejected
- zmap/vix sidecar compatibility is preserved
- missing sidecar metadata falls back to raw scan
- `Ne` and `NotInSet` fallback behavior is visible through
  `fallback_scan`, `index_used`, and `rows_checked`
- corrupt sidecars follow the existing sidecar error policy
- bridge `query-scalar-predicate` remains storage-level if enabled
- no RSI, factor, strategy, market regime, routing, or trading semantics are added

## Static Release Artifact Gate

PowerShell:

```powershell
.\scripts\build_release.ps1
.\scripts\smoke_release.ps1
```

Shell:

```bash
./scripts/build_release.sh
./scripts/smoke_release.sh
```

Confirm the generated `dist/fastk-<version>-<target>/` package contains:

- `bin/fastk_bridge`
- `bin/fastk_admin`
- `crate/fastk-<version>.crate`
- `docs/`
- `schemas/bridge_commands.json`
- `schemas/release_manifest.schema.json`
- `release_manifest.json`
- `SHA256SUMS`

Do not claim a fully static binary unless a static target was actually used and
verified. macOS packages are portable release binaries, not fully static builds.

## Performance Gate

Benchmark storage paths only:

- kline range scan
- scalar range scan
- scalar predicate sidecars
- high-frequency fixed-record range scan
- sealed-chunk replay
- storage-level sequence scan reports
- manifest load and cache behavior

Do not publish performance claims unless the benchmark was actually run for the
release candidate.

## Example Benchmark Commands

```powershell
cargo run --release --example bench_acceptance -- --root target\acceptance-rc --metrics-level off --symbol-count 4 --months 4 --records-per-month 10000 --point-ops 256 --range-ops 64 --full-scan-ops 4 --latest-n-ops 64 --latest-n-rows 1024 --medium-range-rows 4096 --append-batches 8 --append-batch-rows 64 --write-samples 3 --merge-samples 4 --attach-samples 16 --cold-cache-mib 64

cargo run --release --example bench_backtest_facade -- --root target\backtest-facade-bench-off --metrics-level off --symbol-count 4 --months 4 --records-per-month 10000 --range-rows 256 --attach-samples 32 --query-ops 256 --zmap-block-size 256

cargo run --release --example bench_scalar_sidecars -- --root target\scalar-sidecars-rc --rows 50000 --query-ops 32 --range-rows 256 --metrics-level basic
```

## RC Sign-Off

Sign off only after:

- correctness gate passes
- boundary gate passes
- bridge contract is documented
- release artifact is generated
- smoke release passes
- checksums are generated
- no network dependency exists in core
- no backend service is embedded in FastK
- storage boundary gate passes
- admin validate/scrub/repair smoke checks pass
- docs describe FastK as a storage engine, not a market-data, feature, factor, backtest, or trading engine
- examples and tools are marked as external integration code

## Full RC Gate

PowerShell:

```powershell
cargo fmt -- --check
cargo test
cargo test --all-targets
cargo build --release --examples
cargo run --example fastk_bridge -- --help
cargo run --example fastk_admin -- validate --root target\release-check-store --verbose
python -m unittest discover ..\tools\python
.\scripts\build_release.ps1
.\scripts\smoke_release.ps1
```

Shell:

```bash
cargo fmt -- --check
cargo test
cargo test --all-targets
cargo build --release --examples
cargo run --example fastk_bridge -- --help
cargo run --example fastk_admin -- validate --root ./target/release-check-store --verbose
python -m unittest discover ../tools/python
./scripts/build_release.sh
./scripts/smoke_release.sh
```
