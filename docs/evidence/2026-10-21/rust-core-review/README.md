# Native core review evidence

Date: 2026-10-21. Windows observations against the current Rust source.
No production fix, instrumentation feature or new public API was used.
The full engine suite and Linux reproduction were not run in this audit.
This bundle contains correctness and operation-count evidence, not throughput
measurements or a measured optimization gain. Earlier pilot outputs are excluded.

`shared-run-1.json` and `shared-run-2.json`: one shared read-only store,
8 threads x 2,000 range reads. `independent.json`: an independently opened store
per thread. Each has 400 serial controls and all six fields of four references.
`oracle.json` records the independent Arrow row hashes. `input-manifest.json`
identifies canonical inputs; provenance verifies the actual S00 bar part used.
`allocation.json` exposes public Vec capacities and existing read-byte metrics.
`compaction.json` and `invalid-input.json` use new isolated fixture roots.

To rebuild, copy `reproducer/` to the repository's `target/rust-core-review/`;
its preserved Cargo path `../..` then resolves to the repository root. Use the
current source matching `provenance.json`. Do not run Cargo from this evidence
directory. The immutable snapshot is kept here only for reproducibility.

```powershell
$env:RUSTFLAGS = '-C target-feature=+crt-static'
$env:CARGO_TARGET_DIR = "$PWD/target/rust-core-review-build"
cargo build --offline --release --locked --manifest-path target/rust-core-review/Cargo.toml
& target/rust-core-review-build/release/fastk-core-review.exe target/external-standard-evidence/fastk shared
& target/rust-core-review-build/release/fastk-core-review.exe target/external-standard-evidence/fastk independent
& target/rust-core-review-build/release/fastk-core-review.exe target/external-standard-evidence/fastk allocation
& target/rust-core-review-build/release/fastk-core-review.exe target/new-core-compaction-fixture compaction
& target/rust-core-review-build/release/fastk-core-review.exe target/new-core-invalid-fixture invalid-input
```

The first three commands require the existing standard prepared FastK store,
matching `target/external-standard-final/input` / this input manifest. Use the
external benchmark preparation documented in `docs/EXTERNAL_VALIDATION.md` if
those local fixtures are absent. The last two commands require nonexistent
fixture roots and refuse to overwrite an existing root. The shared race counts
vary with scheduling; the saved runs are observations, not universal rates.
No command should target a user's production store for the mutable fixtures.

See `docs/RUST_CORE_REVIEW.md` for the proposed fixes and acceptance criteria.
`SHA256SUMS` covers every bundle file except itself. Production and previous
release/evidence integrity checks are recorded in `provenance.json`.
