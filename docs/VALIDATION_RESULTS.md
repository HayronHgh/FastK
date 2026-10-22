# Technical validation — 2026-10-20

FastK remains a local fixed-record storage engine under MIT. This work does not
add a database server, SQL execution, trading logic, or enterprise operation.

## The three deliverables

| Area | Delivered behavior | Review surface |
|---|---|---|
| Core quality and completeness | Explicit module ownership; OS writer/shared-reader locks; recovery under the same ownership; append-safe scalar writes; checksum validation; metadata-preserving rebuild; immutable snapshot publication | CORE_CONTRACT.md, src/store_core, src/snapshot.rs, tests/storage_contract.rs |
| External deployment | Python stdlib client; executable bundled in platform wheels; versioned JSON discovery; optional structured errors; static Windows/Linux executables; offline installation | INSTALLATION.md, clients/python, scripts/package_release.py |
| Independent evidence and reasons to adopt | Five public adapters; canonical checksummed inputs; randomized repeated rounds; field-level result hashes; feature-inclusive research kernel; scalar space accounting; append and new-version rebuild costs | EXTERNAL_VALIDATION.md, external-tests, evidence/2026-10-20 |

The refactor keeps existing Rust symbols, record layouts, and successful bridge
JSON payloads. Scalar `put` still replaces a month; the bridge now uses additive
append semantics. New snapshot/client surfaces remain release-candidate APIs.
See the core contract for the one-time legacy lock migration and partial-month
commit behavior before using existing datasets.

## Correctness and deployment checks

Windows x64 and Linux x64 each passed 134 root Rust tests: 103 library, 9 CLI,
2 release integration, 19 storage contracts and 1 documentation test. Fault
contracts include actual child termination, lock release, three scalar commit
crash stages, injected flush/sync/rename failures, older orphan rollback,
sidecar corruption, semantic metadata preservation, append boundaries, and
snapshot tampering/relocation. The optional fault-injection feature is absent
from production executables.

Linux was executed in a Rust 1.90 Debian container with real subprocess tests;
its static musl wheel was installed in a temporary Python 3.11 environment
outside the checkout and worked with Cargo absent from PATH. Windows uses the
GNU toolchain (Rust 1.97.1) and OS-provided UCRT, with an equivalent offline
installation check. Import inspection found only Windows system DLLs, including
UCRT API sets; no MinGW runtime DLL is required. MSVC is configured in artifact
CI but has not been independently executed here. Compressed
execution logs and their hashes are under `evidence/2026-10-20/correctness/`.
The two existing external plotting-helper tests also passed in a separate
Matplotlib environment; the legacy tools were preserved.

These checks establish the named process-crash and injected-IO contracts. They
do not simulate a physical power cut, prove every filesystem's durability, or
provide a long-term format-support commitment. Windows directory sync remains
best-effort. CI workflows are authored; no remote workflow run is claimed here.

## Bulk JSON diagnostic

A 437,115-row scalar response (30,161,245 bytes) was identical across all six
before/after responses. Adding a 64KiB stdout buffer reduced median process plus
pipe-transfer time from 2.543s to 0.080s in three samples per binary. Parsing was
outside this timer, OS cache was uncontrolled, and order was before then after.
This is one transport diagnostic, not a general speedup claim. Binary hashes,
samples and a reproduction command are in
[bulk JSON evidence](evidence/2026-10-20/bulk-json/README.md).

## Standard comparison

The completed run used 24 symbols over ten synthetic calendar months:
10,490,760 bars and 41,963,040 scalar records. All 25 backend/round groups and
all append/rebuild validations passed. Five fresh-process rounds each used
2,000 mixed short requests and five full scans. The host was Windows 11,
AMD Ryzen 9 9900X (12 cores / 24 threads), with 100,591,579,136 bytes RAM;
this was not a dedicated isolated benchmark host. OS cache was uncontrolled.

| Backend | Range median (ms) | Research median (s) | Files (decimal GB) | Peak process-tree RSS (MiB) |
|---|---:|---:|---:|---:|
| FastK Python bridge | 24.4209 | 60.6784 | 1.860 | 858.02 |
| SQLite WITHOUT ROWID | 0.1128 | 46.3894 | 3.005 | 769.62 |
| DuckDB | 2.8905 | 32.2011 | 4.255 | 1023.02 |
| Parquet | 2.4603 | 44.4724 | 0.496 | 774.97 |
| Arrow IPC | 0.6643 | 41.3283 | 1.176 | 1856.68 |

FastK range latency has 1,430 samples: p95 29.6183ms and p99 32.8078ms.
The follow-up [bridge cost study](PYTHON_BRIDGE_COST.md) replays these exact
range requests and separates Windows startup, engine, JSON, pipe and caller
conversion costs; it proposes a scoped transport optimization for review.
The research result is a five-sample median, not a tail-latency estimate.
FastK's lower memory usage versus Arrow does not extend to SQLite or Parquet
in this run. RSS includes Python materialization and child processes; it is
not an engine-only memory measurement.

These Python results establish neither a query-speed nor a compression
advantage. They include startup, JSON and ordered Python tuples. Native adapters
keep their connection/maps for each round; FastK starts a process per request.
This difference is intentional because it is the current public integration.
The shared research kernel consumes every feature but is not a complete trading
backtest. The old Rust full-read numbers are a different timing boundary.

Initial build times are omitted because prepared stores were copied. Fresh
full-version rebuilds were timed: FastK 137.3270s, SQLite 109.9500s, DuckDB
125.8774s, Parquet 20.8218s, Arrow 8.6469s. Later-month append was 4.8500s for
FastK. These are single descriptive samples with different documented durability
policies, not equivalent crash-guarantee rankings.

The common revision/extension input preparation, including feature
recalculation, took another 79.6898s outside backend-specific write timers.
That caller-side work must also be included when estimating a complete
correction workflow; it is recorded separately rather than charged to one engine.

FastK value indexes alone use 671,408,640 bytes: 16 bytes for each of the
41,963,040 scalar records, equal to their raw record payload again. Zone maps
add 6,570,240 bytes. The total includes chunks, sparse indexes, headers,
manifests and sidecars; kline-only bytes/row cannot describe this workload.

See the [full report](evidence/2026-10-20/standard/REPORT.md) and adjacent
summary, environment, input/maintenance manifests, compressed raw samples and
SHA256SUMS for exact configuration and results.

## Real market input

The real profile used 24 upstream-checksummed Binance monthly archives for
BTCUSDT and ETHUSDT, January through December 2024: 1,054,080 bars plus
4,216,320 externally calculated scalar features. All ten backend/round groups,
later-month appends and fresh v2 rebuild checks passed. Archive URLs, checksums,
normalization and every canonical part hash are included in the input manifest.

This was a descriptive two-round run with 140 short requests and one full scan
per round. Each short-query kind has only 40 samples, so p95/p99 are omitted.
FastK range median was 23.7523ms versus SQLite 0.1052ms. The two full research
runs took 6.2462–6.3052s for FastK and 3.1589–3.2868s for DuckDB. FastK also had
the largest sampled process-tree RSS in this profile. Initial build times were
measured here, but are single samples. This run does not establish a Python
performance or memory advantage.

See the [real-input report](evidence/2026-10-20/real/REPORT.md), raw samples and
hash manifests for all five backends. Summary quantiles use nearest rank;
two-round research results should be read as a min/max pair, not a distribution.

## Adoption boundary

The original criticism is relevant where it concerns acknowledged writes,
version identity, repair ownership, scalar index cost, and fair measurement.
Those are engine responsibilities. SQL planning, distributed operation and
enterprise support are separate scope choices, not missing requirements for
this technical engine. Comparing DuckDB/Parquet/Arrow still matters because a
caller can choose them without FastK implementing their feature sets.

FastK offers a narrow Rust record API, explicit maintenance and recovery tools,
integer-preserving transport and verifiable physical dataset versions. Those
properties can justify evaluating it in normalized local research workflows.
Its fit must be demonstrated using the caller's actual workload.

Python callers should load ranges in bulk and compute outside the engine.
Per-request subprocess and JSON costs remain visible. Prefer mature alternatives
for fine-grained Python queries, SQL joins/aggregations, compressed analytical
storage, concurrent writers, or network services. Keeping FastK's engine scope
does not exempt it from correctness, compatibility, or fair format comparisons.
