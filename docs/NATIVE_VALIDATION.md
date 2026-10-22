# FastK v0.2 native verification

Version 0.2 adds the independent `fastk-native` CPython module, with named NumPy
int64 batches and a bounded per-reader IO pool. The Rust library, existing
dependency-free subprocess client and public pretty JSON CLI remain available.
Kline and Scalar disk records remain 48 and 16 bytes respectively.

The [native contract](NATIVE_ENGINE.md) and
[quickstart](../clients/python-native/README.md) describe interfaces and ownership.
Native calls are synchronous and have no hard timeout. The subprocess client
retains its kill/reap deadline and failure-without-replay behavior.

## Executed correctness and deployment checks

| Check | Command / boundary | Exit / status | Evidence | Result |
|---|---|---|---|---|
| Windows Rust | `cargo test --locked --features fault-injection` | 0; 138 tests | Windows core log in the evidence bundle | PASS |
| Linux Rust | Same command, Rust 1.97.1 in the isolated container | 0; 138 tests | Linux final validation log | PASS |
| CLI framing/presentation | `cargo test --locked --example fastk_bridge` | 0; four tests per OS | Framing logs | PASS |
| Native and legacy Python APIs | `unittest discover -s external-tests -p 'test_*.py'` with preserved baseline CLI | 0; 42 passed, one archive check skipped per ABI | Final lease validation logs | PASS / one SKIPPED |
| Offline native install/use | `scripts/smoke_native_install.py` in a fresh external directory, `--no-index`, compiler/CLI absent from PATH | 0 on four ABI/platform combinations | Offline probes in validation logs | PASS |
| Old reader / new files | v0.1 bridge reads v0.2 native Kline/Scalar writes and value-index queries | 0 on Windows/Linux | `test_native_compat.py` | PASS |
| Historical evidence | Recompute all prior evidence/artifact SHA manifests | 24 manifests, 668 entries unchanged | Preserved-history manifest | PASS |
| Windows MSVC execution | Authored CI job | Not executed locally; local Windows uses GNU | CI workflow | NOT RUN |

The one skipped check requires a complete release archive and is excluded from
ABI-suite counts. Archive manifests and hashes are validated separately after
assembly; the delivery manifest records those checks.

The executed ABI matrix is Windows GNU / CPython 3.11.15 and 3.12.14, plus Linux
GNU / CPython 3.11.2 and 3.12.14. All use NumPy 2.3.5. Native Linux wheels are
tagged `manylinux_2_34_x86_64`; the independent CLI/client uses static musl.
The Linux measurements use copied stores in the container filesystem, on the
same physical host as Windows. They are separate environments, not a bare-metal
OS comparison. No GitHub workflow, package publication or production deployment
is claimed by these local checks.

The public tests cover integer extrema and values above 2^53, exact dtypes and
offsets, noncontiguous inputs, nine Scalar predicates, empty results, arrays
surviving close, writer exclusion, close/read races and 16,000 shared Rust reads.
A cached-header payload truncation forces parallel read tasks to fail; no partial
array escapes, and the valid prefix remains readable. A missing/empty-root
regression verifies that Native Reader rejects initialization-free attachment.
The existing CLI inventory probe can remain stateless without weakening the
persistent native reader's lease.

Parallel month prefetch is validated separately from strategy execution. The
caller processes months chronologically and carries the open position/cash state;
its result matches a full-range run. There is no backtest executor in the engine.

## Measurement boundaries

The primary comparison uses five in-process adapters and the same fully
materialized, ordered, read-only packed NumPy output. Execution and conversion
are inside the query timer. Complete validation and previous-result destruction
are outside it. SQLite uses WITHOUT ROWID and a 64 MiB pager cache; DuckDB uses
native table queries and NumPy fetch. Parquet uses statistics/filter/projection
pushdown. Arrow IPC uses memory maps, timestamp binary search, zero-copy column
views, a 64-map LRU and one final packed-output allocation. The file adapters
receive the common partition manifest, while FastK owns its storage manifests.

The fixed workloads retain the original 1,430 Kline ranges, four bulk cases and
the standard/real research checksum. Internal budgets 1/2/4/8 and outer query
concurrency are measured separately. Outer handles each use one internal thread;
five 286-query groups produce the same total 1,430 queries. Library worker budgets
are caps, and SQLite/cached IPC need not consume every available worker.

Internal cases receive 32 untimed warmups before ten measured bulk repetitions.
The Rust-only probe records its 32 warmups separately, then measures 32 repeats
per short/latest case and ten per bulk case, across three paired process rounds.
An earlier one-warmup/ten-sample probe straddled allocator transitions: both
engines had several slow initial reads, and their medians fell on different
phases. Those diagnostic runs are retained rather than substituted for the final
steady-state gate. The final probe avoids per-row validation allocations and
serializes its sample metadata after measurements.

OS cache is uncontrolled throughout. Process restart is not called cold cache.
p95/p99 use nearest rank and require at least 100 observations; ten bulk samples
are descriptive. An external monitor samples the actual Python PID with a nominal
5 ms interval. RSS and Windows private commit / Linux USS include imported backend
state and validation; sampled peaks can miss brief transients. The private-memory
definitions differ across OSes. Legacy delivery records child PIDs separately
and uses simultaneous tree RSS, without adding independently observed peaks.

All artifacts are development builds for technical evaluation. Scalar append
still rewrites the month and its sidecars; `.vix` stores an additional 16 bytes
per Scalar record. Kline density is therefore not the whole-engine space cost.
This release provides no MVCC, concurrent-writer or enterprise service guarantee.

## Final measured results

### Rust engine before/after

Medians below are microseconds, serial reads, three paired process rounds.
Ratios above 1 mean slower. Every category passes the 1.05 regression cap.

| OS | Case | Before us | After us | After/before |
|---|---|---|---|---|
| windows | short | 5.000 | 4.100 | 0.8200 |
| windows | small_kline | 5.100 | 4.100 | 0.8039 |
| windows | medium_kline | 73.300 | 37.400 | 0.5102 |
| windows | bulk_kline | 3349.100 | 3401.400 | 1.0156 |
| windows | bulk_scalar | 2375.600 | 917.100 | 0.3860 |
| windows | latest | 2.600 | 1.900 | 0.7308 |
| windows | bulk_latest | 12063.000 | 3365.400 | 0.2790 |
| linux | short | 3.897 | 2.485 | 0.6377 |
| linux | small_kline | 3.917 | 2.485 | 0.6344 |
| linux | medium_kline | 29.687 | 19.417 | 0.6541 |
| linux | bulk_kline | 1344.264 | 1366.787 | 1.0168 |
| linux | bulk_scalar | 300.362 | 223.323 | 0.7435 |
| linux | latest | 2.134 | 1.143 | 0.5356 |
| linux | bulk_latest | 8433.013 | 1326.502 | 0.1573 |

The concurrency fix is part of the after build. Bulk Kline is about 1.6–1.7%
slower in this serial gate; the strongest core improvements are Scalar,
latest and small reads. Removing JSON is a separate delivery improvement.

### Five in-process adapters: standard and real data

All queries include conversion to the same packed NumPy result. The standard
run has five randomized backend rounds, 2,000 mixed operations and five scans
per round; the real-data run has two rounds, 140 mixed operations and one scan.
Range distributions pool all measured range operations. Research is the common
four-feature integration consumer; its timings include Python computation.

**Standard / Windows / internal budget 1**

| Backend | Range n | Median ms | p95 ms | p99 ms | Research s | Store MiB | Peak RSS MiB | Peak private MiB |
|---|---|---|---|---|---|---|---|---|
| fastk | 1430 | 0.0579 | 0.3481 | 4.1289 | 2.072 | 1773.8 | 176.9 | 165.1 |
| sqlite | 1430 | 0.1317 | 0.3885 | 0.4850 | 34.351 | 2866.1 | 303.8 | 291.6 |
| duckdb | 1430 | 2.3399 | 3.0097 | 3.6615 | 5.361 | 4058.3 | 445.6 | 427.2 |
| parquet | 1430 | 1.6554 | 2.7110 | 6.5455 | 4.111 | 473.3 | 264.8 | 270.6 |
| arrow | 1430 | 0.3688 | 0.7329 | 4.9216 | 2.485 | 1121.4 | 195.3 | 114.3 |

Maintenance uses one observation per backend and includes close/checkpoint.
Corrections produce a fresh complete version for every backend; these are
not sparse-update timings. Initial standard stores were reused, so their
build time is not reported. Durability guarantees differ between adapters;
the environment JSON records month commits, sync and checkpoint policies.

| Backend | Initial build s | Later-month append s | Fresh version rebuild s |
|---|---|---|---|
| fastk | reused | 1.423 | 24.588 |
| sqlite | reused | 0.201 | 83.499 |
| duckdb | reused | 0.929 | 116.367 |
| parquet | reused | 0.898 | 15.970 |
| arrow | reused | 0.342 | 7.192 |

**Real / Windows / internal budget 1**

| Backend | Range n | Median ms | p95 ms | p99 ms | Research s | Store MiB | Peak RSS MiB | Peak private MiB |
|---|---|---|---|---|---|---|---|---|
| fastk | 40 | 0.0215 | n<100 | n<100 | 0.241 | 178.2 | 125.3 | 114.7 |
| sqlite | 40 | 0.1277 | n<100 | n<100 | 3.527 | 329.6 | 338.8 | 328.2 |
| duckdb | 40 | 2.1042 | n<100 | n<100 | 0.623 | 426.0 | 248.1 | 229.6 |
| parquet | 40 | 1.5947 | n<100 | n<100 | 0.427 | 78.9 | 236.7 | 242.5 |
| arrow | 40 | 0.0437 | n<100 | n<100 | 0.258 | 112.7 | 191.0 | 117.6 |

Maintenance uses one observation per backend and includes close/checkpoint.
Corrections produce a fresh complete version for every backend; these are
not sparse-update timings. Initial standard stores were reused, so their
build time is not reported. Durability guarantees differ between adapters;
the environment JSON records month commits, sync and checkpoint policies.

| Backend | Initial build s | Later-month append s | Fresh version rebuild s |
|---|---|---|---|
| fastk | 2.597 | 0.112 | 2.162 |
| sqlite | 8.542 | 0.019 | 8.403 |
| duckdb | 11.427 | 0.096 | 11.131 |
| parquet | 1.648 | 0.050 | 1.613 |
| arrow | 0.697 | 0.019 | 0.669 |

The standard FastK store includes four Scalar features, not only Klines.
Its `.vix` files add 16 bytes per Scalar row. Storage components are:

| Component | MiB |
|---|---|
| other_or_journal | 0.00 |
| manifest | 0.24 |
| chunk_with_sparse_index_and_header | 1126.94 |
| value_index | 640.31 |
| zone_map | 6.27 |

FastK has a lower range median in this standard mix, but its range p99 is
worse than SQLite. Parquet and Arrow use less total storage. The common
research result favors FastK modestly over Arrow; it includes a substantial
Python consumer cost and is not a general backtest speedup guarantee.

### Frozen bulk and 1,430 short-query matrix

Bulk Kline and Scalar each contain 437,115 rows. All five adapters are shown
at internal budget 1; ten timed samples follow 32 warmups for each case.
RSS/private cover the entire worker, not just the output allocation.

**Windows**

| Backend | 127 Kline ms | 4,077 Kline ms | Bulk Kline ms | Bulk Scalar ms | RSS MiB | Private MiB |
|---|---|---|---|---|---|---|
| fastk | 0.0444 | 0.0688 | 11.4680 | 3.1409 | 76.4 | 64.2 |
| sqlite | 0.2097 | 3.6889 | 457.7979 | 220.6645 | 224.8 | 212.4 |
| duckdb | 3.0867 | 3.0665 | 50.9432 | 34.2635 | 133.2 | 120.1 |
| parquet | 2.3912 | 2.8872 | 42.2724 | 16.0067 | 149.7 | 139.4 |
| arrow | 0.1035 | 0.0532 | 4.9706 | 1.5048 | 103.9 | 57.2 |

| FastK budget | Internal bulk Kline ms | Internal bulk Scalar ms | Outer queries/s | Outer median ms | Outer p95 ms | Outer p99 ms | Outer RSS MiB |
|---|---|---|---|---|---|---|---|
| 1 | 11.4680 | 3.1409 | 25176 | 0.0394 | 0.0662 | 0.1521 | 41.4 |
| 2 | 9.0916 | 2.5536 | 6210 | 0.2767 | 0.6761 | 0.8509 | 42.6 |
| 4 | 5.7396 | 1.7624 | 51819 | 0.0601 | 0.1506 | 0.2835 | 45.0 |
| 8 | 4.7561 | 1.7721 | 50874 | 0.1238 | 0.3120 | 0.4889 | 47.4 |

**Linux**

| Backend | 127 Kline ms | 4,077 Kline ms | Bulk Kline ms | Bulk Scalar ms | RSS MiB | Private MiB |
|---|---|---|---|---|---|---|
| fastk | 0.0361 | 0.0853 | 10.5491 | 4.1266 | 79.2 | 61.8 |
| sqlite | 0.1614 | 2.1602 | 324.2767 | 167.9092 | 222.1 | 203.8 |
| duckdb | 3.7948 | 3.0503 | 31.1987 | 21.8707 | 185.1 | 167.3 |
| parquet | 2.0969 | 2.5264 | 34.5211 | 14.7896 | 181.2 | 163.3 |
| arrow | 0.0789 | 0.0436 | 1.7116 | 0.4727 | 134.2 | 116.7 |

| FastK budget | Internal bulk Kline ms | Internal bulk Scalar ms | Outer queries/s | Outer median ms | Outer p95 ms | Outer p99 ms | Outer RSS MiB |
|---|---|---|---|---|---|---|---|
| 1 | 10.5491 | 4.1266 | 41438 | 0.0132 | 0.0405 | 0.0788 | 44.2 |
| 2 | 9.3251 | 3.5272 | 3740 | 0.4048 | 0.8453 | 1.3155 | 45.9 |
| 4 | 9.8669 | 3.3555 | 9909 | 0.1915 | 0.4061 | 0.8596 | 47.5 |
| 8 | 8.4836 | 3.4344 | 7743 | 0.3941 | 0.9117 | 29.1347 | 49.1 |

Internal and outer columns are different experiments: one query can use the
internal pool; outer runs execute the same 1,430 queries concurrently with
one internal thread per handle. Throughput uses the sum of five 286-query
wall intervals, including observed scheduler effects. The Windows two-worker
outer run and every Linux multiworker outer run regress. These are retained;
there is no claim of monotonically improving concurrency. Default remains 1.

Arrow IPC wins serial bulk on both environments. Native Python still copies
Rust records into a safe owned int64 batch; this result does not claim that
the core or Python adapter beats optimized memory-mapped column delivery.

### Historical compact JSON versus native delivery

This separate experiment uses the same four bulk queries and 1,430 ranges.
Both routes return the same NumPy layout; the old Client additionally pays
JSON parse and dictionary-to-batch conversion. The preserved v0.1 bridge
and current core differ, so this is a delivery-path comparison, not isolated
JSON serializer timing. Ten bulk samples and 1,430 short observations are used.

| Route | Case | Median ms | p95 ms | p99 ms | Python RSS MiB | Python private MiB |
|---|---|---|---|---|---|---|
| legacy | bulk_kline | 473.4466 | n<100 | n<100 | 340.4 | 328.7 |
| legacy | bulk_scalar | 189.3899 | n<100 | n<100 | 340.4 | 328.7 |
| legacy | medium_kline | 4.7636 | n<100 | n<100 | 340.4 | 328.7 |
| legacy | small_kline | 1.5445 | n<100 | n<100 | 340.4 | 328.7 |
| legacy | short | 0.4499 | 0.5655 | 0.7091 | 39.4 | 26.7 |
| native | bulk_kline | 7.2321 | n<100 | n<100 | 74.6 | 64.6 |
| native | bulk_scalar | 1.9126 | n<100 | n<100 | 74.6 | 64.6 |
| native | medium_kline | 0.0624 | n<100 | n<100 | 74.6 | 64.6 |
| native | small_kline | 0.0642 | n<100 | n<100 | 74.6 | 64.6 |
| native | short | 0.0335 | 0.0428 | 0.0677 | 39.5 | 26.4 |

| Route | Group | Handle open ms | Python RSS MiB | Simultaneous tree RSS MiB |
|---|---|---|---|---|
| legacy | bulk | 45.1360 | 340.4 | 354.7 |
| legacy | short | 45.9578 | 39.4 | 52.2 |
| native | bulk | 1.4627 | 74.6 | 74.6 |
| native | short | 1.4468 | 39.5 | 39.5 |

Handle open has one observation per group, includes bridge startup for the
legacy route, and excludes Python interpreter bootstrap. Individual child
PID RSS peaks are retained in raw results; their independent maxima are never
added together. Python-only RSS is not the complete subprocess cost.

## Adoption boundary

Use FastK when the application wants an embedded Rust engine for fixed,
scaled integer records, ordered timestamp ranges, latest reads and indexed
Scalar predicates, with explicit snapshot/maintenance tools and no SQL layer.
Python can now consume named numeric fields without a bridge process or JSON.
Keep strategy state in the caller and prefetch only when measured useful.

Prefer Arrow IPC for read-mostly bulk column delivery where its cache and
partition management fit the application; prefer DuckDB for SQL/analytical
composition and SQLite for general transactional relational needs. The results
establish concrete FastK strengths in this workload, not general superiority
over databases or storage formats. Scalar index space, whole-month Scalar
rewrites, native conversion, snapshot copying and controlled cold-cache
experiments remain material limits.

Raw results, environments, immutable before/final source archives, measured
wheel hashes and validation logs are in the
[checksummed evidence bundle](evidence/2026-10-21/native-engine/README.md).
