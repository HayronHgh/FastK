# Rust core parallel fix evidence

Read ../../../CORE_PARALLEL_RESULTS.md for results and scope. The only changed
product modules are chunk/cache.rs and engine/parallel.rs; tests add cache
lifetime/eviction/concurrency and shared bulk/truncated-read regressions.
Python binding, transport, public API and on-disk records are unchanged.

windows/ and linux/primary are the adopted final three paired process rounds:
72 processes per OS, 1/2/4/8 budgets, shared and independent stores, first/warm
passes of the same 1,430 queries and the original seven serial/bulk/latest
probes. All timed short outputs are canonical-checked outside the timer.
Bulk: 32 warmups +30 samples, canonical first output and complete equality for
every later result. Reference lifetimes affect allocator/page reuse; compare
paired binaries rather than subtracting older Python attribution measurements.

serial-gate retains nine prespecified extra paired processes per version after
a primary serial warning. The original three rounds are included: 12 paired
rounds, 360 observations/version/case. Nothing is discarded to pass the gate.
Summary records individual paired-process ratios and bootstrap intervals.
Memory/FD runs are separate and their perturbed query timings are excluded.
5 ms sampled peaks can miss transients. Windows private commit is not Linux USS.

before-target-layout.zip preserves the exact frozen pre-task core and actual
before-harness source, with its relative Cargo path intact. Extract at a project
root and build target/core-parallel-before-bench/Cargo.toml. after-source.zip
preserves current sources and standalone external-tests/core-parallel; build
that manifest for the after executable. Dependency checksums match the product
lockfile; no Python runtime is linked into either binary. Actual before/after
binaries and SHA256 values are included. Absolute compiler paths may prevent
bit-identical rebuilds; compiler versions and profiles are recorded.

Input expected-row manifests and frozen queries are included; stores/data and
build caches are not. Use the existing standard prepared store. Linux data was
copied onto the container native filesystem, not read from a Windows bind mount.
Fresh processes and first passes do not establish OS cold cache.

Diagnostics contain rejected/earlier controls. selected-* is the earlier cache
version; accepted-* still had a serial warning. Only verified-* drives the final
primary report. A universal 4 MiB worker grain hurt Windows Scalar, so it was
rejected in favor of platform defaults. Full header RwLock was unnecessary and
was reverted; final headers keep their original short Mutex. An initial cache
exploration overlapped compilation, has invalid timing and is excluded entirely.
The original Windows sandbox PermissionDenied test log is retained separately;
authorized final tests with workspace-local TMP pass.

The existing OS leases/recovery/no replay behavior is retained. Independent
handles still duplicate caches and can encounter Linux fdtable-growth stalls;
use the existing Arc<FastKStore> for same-process readers. No dummy descriptors
or global FD registry were added. Additional threads need not improve throughput.
Native wheels, historical reports and dist packages were not rebuilt/rewritten.
