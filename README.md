# FastK

FastK is a Rust storage engine for normalized, fixed-record time-series data.
It is designed for read-heavy market-data and research workflows that need fast
local range reads without bringing in a full SQL database.

Current release target: `v0.1.0-rc.1`

## What This Project Is For

FastK is useful when an upper-layer system already produces clean, normalized
records and needs a compact local persistence layer for:

- kline / OHLCV base series
- scalar indicator and feature outputs
- factor, signal, risk, portfolio, and metric scalar series
- deterministic research and backtest adapters
- storage benchmarks around fixed-width time-series records

FastK owns storage behavior only: chunk writes, manifests, checksums, range
reads, latest-N reads, scalar predicate queries, inventory, recovery tooling,
and release bridge contracts.

FastK intentionally does not fetch market data, parse exchange payloads,
calculate indicators/features/factors, generate trading signals, run backtests,
place orders, manage approvals, or decide whether a dataset is tradable.

## Design Highlights

- Fixed-width binary records for predictable layout and low overhead.
- UTC time-partitioned chunks, with kline/scalar series defaulting to month
  partitions.
- Binary manifests that track chunk metadata, checksums, sidecars, and state.
- Scalar sidecars:
  - `.zmap` for zone-map pruning.
  - `.vix` for value-index lookup.
- JSON subprocess bridge for Python, TypeScript, shell, and other non-Rust
  callers.
- SQLite control-plane registry for dataset, feature, and factor metadata.
- Admin tooling for validate, scrub, repair, rebuild-manifest, inventory, and
  health checks.

## Repository Layout

```text
kline_store/
  src/
    store_core/     # engine, chunk IO, index, manifest, recovery internals
    kline/          # kline record domain
    feature/        # scalar, feature, predicate records
    control/        # SQLite control-plane registry
    bridge.rs       # JSON bridge contract helpers
  examples/
    cli/            # fastk_bridge, fastk_admin
    demos/          # integration demos
    benchmarks/     # benchmark runners
  docs/             # architecture and integration documents
  schemas/          # JSON contract schemas
  scripts/          # release build and smoke scripts
  tests/            # bridge and release smoke tests
```

See [Project Structure](docs/PROJECT_STRUCTURE.md) for the full tree.

## Install Prerequisites

Required:

- Rust stable toolchain
- Cargo

Recommended:

- PowerShell 7+ on Windows for release scripts
- Git

Check your toolchain:

```bash
rustc --version
cargo --version
```

## Build

From `kline_store/`:

```bash
cargo build
cargo build --examples
cargo build --release --examples
```

Run all checks used for development:

```bash
cargo fmt --check
cargo check --examples
cargo test
```

Build a release package on Windows:

```powershell
.\scripts\build_release.ps1
.\scripts\smoke_release.ps1
```

The release output is generated under `dist/`, which is intentionally ignored by
git.

## Rust Quick Start

Write and read kline records:

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

    let rows = store.get_kline_range(
        "BTCUSDT",
        "1m",
        1_706_745_600_000,
        1_706_745_600_000,
    )?;
    println!("rows={}", rows.len());
    Ok(())
}
```

Store any derived scalar output, such as an externally computed feature:

```rust
use fastk::{feature_series_key, FastKStore, ScalarRecord};

fn main() -> fastk::Result<()> {
    let mut store = FastKStore::open("data/fastk")?;
    store.init()?;

    let key = feature_series_key("BTCUSDT", "1m", "rsi_14");
    store.register_scalar_series(&key, 60_000)?;
    store.put_scalar_chunk(
        &key,
        60_000,
        &[ScalarRecord {
            ts: 1_706_745_600_000,
            value: 7_000,
        }],
        fastk::DEFAULT_SCALAR_ZMAP_BLOCK_SIZE,
    )?;

    Ok(())
}
```

## Bridge CLI

`fastk_bridge` exposes storage-level JSON commands for non-Rust callers. It only
reads and writes records; it does not fetch, compute, route, or run a service.

```bash
cargo run --example fastk_bridge -- --help
```

Write kline rows:

```bash
cargo run --example fastk_bridge -- write-kline-range \
  --root ./data/store \
  --symbol BTCUSDT \
  --timeframe 1m \
  --input-json ./kline_rows.json
```

Write feature rows with the generic scalar bridge:

```bash
cargo run --example fastk_bridge -- write-scalar-range \
  --root ./data/store \
  --symbol BTCUSDT \
  --timeframe 1m \
  --category feature \
  --name rsi_14 \
  --input-json ./feature_rows.json
```

Query scalar rows by value:

```bash
cargo run --example fastk_bridge -- query-scalar-predicate \
  --root ./data/store \
  --symbol BTCUSDT \
  --timeframe 1m \
  --category feature \
  --name rsi_14 \
  --start-ts 1706745600000 \
  --end-ts 1706749200000 \
  --predicate gt \
  --value 7000 \
  --return-values
```

## Signal-as-Scalar Convention

Signal-like values can use the same scalar bridge as a caller-side convention:

```bash
cargo run --example fastk_bridge -- write-scalar-range \
  --root ./data/store \
  --symbol BTCUSDT \
  --timeframe 15m \
  --category signal \
  --name sigvec_test \
  --input-json ./signal_rows.json
```

FastK does not interpret `signal` categories or values such as `1` and `-1`.
They are ordinary scalar categories and integer scalar values. Any active, long,
short, approval, rule, report, or strategy meaning belongs to the caller.

See [Signal-as-Scalar Bridge](docs/SIGNAL_SCALAR_STORAGE.md).

## Admin CLI

```bash
cargo run --example fastk_admin -- validate --root ./data/store --verbose
cargo run --example fastk_admin -- scrub --root ./data/store --verbose --dry-run
cargo run --example fastk_admin -- inventory --root ./data/store
```

## Examples

```bash
cargo run --example backtest_facade_demo
cargo run --example indicator_series_demo
cargo run --example prepare_python_demo_store -- --root target/python-demo-store
cargo run --release --example bench_acceptance -- --help
```

Examples are integration demonstrations and benchmark runners. They are not
FastK core business logic.

## Documentation

- [Architecture Boundary](docs/ARCHITECTURE_BOUNDARY.md)
- [Store Lifecycle](docs/STORE_LIFECYCLE.md)
- [Backend Integration](docs/BACKEND_INTEGRATION.md)
- [Backtest Integration](docs/BACKTEST_INTEGRATION.md)
- [Bridge Contract](docs/BRIDGE_CONTRACT.md)
- [Signal-as-Scalar Bridge](docs/SIGNAL_SCALAR_STORAGE.md)
- [Kline Storage Comparison](docs/KLINE_STORAGE_COMPARISON.md)
- [Release Checklist](docs/RELEASE_CHECKLIST.md)
- [Release Notes](docs/RELEASE_NOTES.md)

## Status

FastK is a release-candidate storage crate. Stable-by-intent surfaces include:

- `FastKStore`
- kline and scalar register/read/write APIs
- scalar predicate query APIs
- `BacktestStoreView`
- `fastk_bridge` JSON contract
- admin validation and recovery tooling

Experimental surfaces include high-frequency trade/BBO/book-delta storage,
sealed replay details, and benchmark scaffolding.

## License

Licensed under the [MIT License](LICENSE).
