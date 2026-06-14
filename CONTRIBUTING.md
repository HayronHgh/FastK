# Contributing

FastK is a storage-engine crate. Contributions should preserve the boundary
that FastK stores normalized records and does not own market-data ingestion,
feature calculation, trading decisions, or strategy semantics.

## Development Checks

Run these before opening a pull request:

```bash
cargo fmt --check
cargo check --examples
cargo test
```

For release packaging changes on Windows:

```powershell
.\scripts\build_release.ps1
.\scripts\smoke_release.ps1
```

## Contribution Guidelines

- Keep public APIs storage-oriented.
- Prefer existing kline/scalar abstractions over adding domain-specific storage
  branches.
- Do not add signal, strategy, approval, or exchange-specific semantics to
  FastK core.
- Keep generated data, build outputs, and release packages out of git.
- Add focused tests for bridge contracts, storage invariants, and recovery
  behavior when changing those areas.
