# FastK Project Structure

FastK is organized as a storage-engine crate with clear boundaries between
public domain namespaces, core storage internals, integration tooling, and
release/support material.

```text
kline_store/
  Cargo.toml
  Cargo.lock
  CONTRIBUTING.md
  LICENSE
  README.md
  .github/
    workflows/
      ci.yml
  docs/
    ARCHITECTURE_BOUNDARY.md
    BACKEND_INTEGRATION.md
    BACKTEST_INTEGRATION.md
    BRIDGE_CONTRACT.md
    KLINE_STORAGE_COMPARISON.md
    PROJECT_STRUCTURE.md
    READING_GUIDE.md
    RELEASE_CHECKLIST.md
    RELEASE_NOTES.md
    REPLAY_AND_TAIL.md
    SIGNAL_SCALAR_STORAGE.md
    STORE_LIFECYCLE.md
  examples/
    cli/
      fastk_admin.rs
      fastk_bridge.rs
    demos/
      backtest_facade_demo.rs
      indicator_series_demo.rs
      prepare_python_demo_store.rs
    benchmarks/
      bench_acceptance.rs
      bench_backtest_facade.rs
      bench_scalar_sidecars.rs
  schemas/
    bridge_commands.json
    release_manifest.schema.json
  scripts/
    build_release.ps1
    build_release.sh
    smoke_release.ps1
    smoke_release.sh
  src/
    lib.rs
    bridge.rs
    benchmark.rs
    error.rs
    metrics.rs
    kline/
      mod.rs
      record.rs
    feature/
      mod.rs
      query.rs
      scalar.rs
    control/
      dataset_registry.rs
      mod.rs
    store_core/
      chunk/
      engine/
      index/
      storage/
    types/
      bbo.rs
      book_delta.rs
      fixed.rs
      meta.rs
      mod.rs
      trade.rs
  tests/
    bridge_cli_contract.rs
    release_smoke.rs
```

## Layer Responsibilities

- `src/kline/`: kline domain records and public namespace.
- `src/feature/`: scalar records, feature-compatible predicates, and public namespace.
- `src/store_core/`: fixed-record chunk IO, manifests, read/write engine, indexes, recovery, and cache internals.
- `src/control/`: SQLite control-plane metadata for datasets, features, and factors.
- `examples/cli/`: subprocess tools intended for external integration.
- `examples/demos/`: small workflows that show how upper layers call FastK.
- `examples/benchmarks/`: benchmark runners and acceptance measurements.
- `docs/`: architecture, lifecycle, integration, release, and comparison reports.

## Generated Artifacts

The repository intentionally excludes generated and heavy files:

- `target/`
- `dist/`
- `data/`
- `series/`
- `tmp/`
- `*.db`, `*.sqlite`, `*.zip`, `*.7z`

These artifacts can be recreated by running tests, benchmarks, or release
scripts and should not be committed.
