# FastK read-cost attribution tools

The probe retains the historical Record-to-word flattening implementation. After
the native ownership handoff optimization, its flatten phase no longer describes
the current production binding. Use `external-tests/native_handoff` for the
current before/after comparison and `docs/NATIVE_HANDOFF_RESULTS.md` for results.
Published attribution numbers require their frozen source, not a newer core.

These are opt-in diagnostic programs. Nothing here is linked into the production
engine, native package or CLI. The independent Cargo workspace mirrors existing
conversion steps and adds clocks; it is not an optimized implementation. No
production dependency, interface, timeout, lock or replay policy is changed.

The current analysis is in `docs/NATIVE_COST_ANALYSIS.md`; frozen queries,
raw results, provenance and checksums are in
`docs/evidence/2026-10-21/native-cost`. Use the project's root as the working
directory. Python 3.12.14, NumPy 2.3.5, PyArrow 26.0.0, psutil 7.2.2,
Maturin 1.15.0 and Rust 1.97.1 were used. The independent lockfile keeps exactly
the production registry dependency versions. Cargo builds require a populated
cache with `--offline`; omit that flag for an ordinary first build.

```sh
python -m maturin build --manifest-path external-tests/attribution/probe/Cargo.toml --release --locked --offline --interpreter /path/to/python --out target/attribution-wheels
python -m pip install --no-deps /path/to/generated/fastk_attribution_probe.whl
python external-tests/attribution/verify_probe.py --store /path/to/stores/fastk --bulk /path/to/bulk.json --short /path/to/short.json
python external-tests/attribution/run.py --stores /path/to/stores --input /path/to/input --bulk /path/to/bulk.json --short /path/to/short.json --output target/attribution-results --rounds 3 --repetitions 30
```

Install the unchanged `fastk-native` development wheel separately. Both modules
coexist; the diagnostic package does not replace the release module. The stores
directory contains the prepared `fastk` and `arrow` stores, and input contains
their common `manifest.json`. This is the **pre-maintenance** standard dataset;
do not substitute a store already appended/rebuilt by another experiment.
Large datasets and build caches are deliberately excluded from evidence exports.

On Linux, add `--parallel` for internal budgets 1/2/4/8, outer concurrency,
shared-handle controls and isolated interpreter-switch-interval experiments.
All primary outputs are fully materialized packed readonly NumPy int64 batches.
Canonical validation, previous-result destruction and warmup are outside timed
query calls. Every timed result is validated against a canonical-checked buffer.
GIL-only jobs are explicitly negative controls, not data-read validations.

The phases distinguish core read, the original allocate/flatten/drop, GIL
reattachment, dtype lookup, Vec ownership handoff, readonly flags and structured
view creation. Pointer checks verify that Vec-to-NumPy handoff has no element
copy; the separate `core_control layout` checks Record-to-word allocation identity.
Clock and metadata-return overhead means probe timings are diagnostic estimates;
the unchanged wheel is the product control. Arrow's mmap data movement may occur
inside its final column-to-packed copy, not while obtaining column views.

For additional Linux controls:

```sh
PYO3_PYTHON=/path/to/python cargo build --manifest-path external-tests/attribution/probe/Cargo.toml --release --locked --offline --bin core_control
python external-tests/attribution/controls.py --binary /path/to/core_control --stores /path/to/stores --input /path/to/input --bulk /path/to/bulk.json --short /path/to/short.json --output target/attribution-controls
python external-tests/attribution/fd_controls.py --binary /path/to/core_control --store /path/to/stores/fastk --input /path/to/input --short /path/to/short.json --output target/attribution-fd-controls
```

Set `CARGO_TARGET_DIR` outside this source directory when building. On Windows,
set `PYO3_PYTHON` to the intended interpreter for the standalone binary build.
The FD controls are Linux only. They prepare the descriptor table on the same
process's main thread, close every dummy descriptor, then create normal readers
and workers. Preparation time is recorded outside the timed query groups.
This is a causal control, **not a proposed dummy-file startup workaround**.

Rust parallel timers use a ready barrier to exclude prior validation and record
the last worker's completion before validation. A first diagnostic attempt with
the wrong group boundary is retained as invalid diagnostics in the evidence.
The primary experiment was fully replayed after correcting that boundary.
Warm-handle controls explicitly replay all 1,430 queries with the same handles.
OS cold cache is never claimed. Results are specific to the measured WSL2 kernel;
descriptor growth and scheduling delays can differ on another Linux system.
