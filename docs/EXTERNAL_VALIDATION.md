# External validation and adoption criteria

`external-tests/` depends on public clients and artifacts, not FastK internals.
Transport tests, correctness smoke, performance runs and data preparation are
separate. Its dependencies are isolated from the engine and Python client.

## Reproduce

Use CPython 3.12.14 for performance runs. Correctness also runs on 3.11.
Install the native wheel and pinned environment:

```sh
python -m venv .venv
# Activate .venv using the command for your shell.
python -m pip install /path/to/fastk_native-0.2.0rc1-<python-abi-and-platform>.whl
python -m pip install -r external-tests/requirements-native.lock
python -m unittest discover -s external-tests -p test_native.py
python external-tests/run_benchmarks.py --root results/smoke --profile smoke
```

The output directory must not exist. No runner resets existing stores. Default
measurements use five independent rounds, 2,000 mixed short queries per round,
and five full scans. Use `--rounds 2 --short-ops 25 --full-scans 1` only for quick
correctness smoke. Such small samples are not evidence about tail latency.

To repeat against an existing verified dataset, pass
`--prepared-input /absolute/path/to/input` with its matching `--profile` and a
new output root. Inputs are read-only and checksummed again. Each run records a
hash of the frozen harness source. Query cases separate point hits/misses,
equality and inclusive range predicates, alongside range/latest/projection.

An interrupted run whose stores finished preparation but have not entered
maintenance can be copied with `--reuse-stores-from /path/to/prior-run`.
The input manifest must match exactly. The original stores remain untouched;
initial build time is explicitly omitted, while fresh full-version rebuilds
are still timed after the read experiments. Never point this option at live
application stores.

`--profile standard` prepares roughly ten million bars across 24 symbols and ten
UTC months, plus four scalar features. `--profile real` downloads BTCUSDT and
ETHUSDT monthly 1m archives for 2024 from Binance's public historical archive,
checks each upstream SHA-256, preserves gaps, and normalizes decimal values with
integer scale 1e8. Downloads occur only in this explicit external data profile,
never inside FastK. Archive URLs, checksums and normalization are recorded in the
input manifest. Upstream availability and data terms remain the caller's concern.

The research kernel loads bars and four precomputed features, applies identical
integer arithmetic, and verifies the same output checksum for every backend.
Rolling means and normalization are computed outside the engine. This is an
integration workload, not a strategy-quality or full trading-backtest benchmark.

## Comparisons and measurement boundaries

| Backend | Configuration |
|---|---|
| FastK | In-process native Reader; core format, default scalar indexes, one read thread |
| SQLite | Composite keys, WITHOUT ROWID, WAL, synchronous FULL, prepared/cached statements, scalar value index |
| DuckDB | Native tables, sorted input, scalar index, one execution thread |
| Parquet | Symbol/month files, Zstd, 8,192-row groups, statistics, column/filter pushdown |
| Arrow IPC | Memory-mapped uncompressed symbol/month files, timestamp binary search, column views, 64-map LRU |

Every adapter fully materializes an ordered, read-only, packed NumPy int64
batch: six fields for Kline, two for Scalar, timestamp/close for projection.
SQLite fetchall-to-NumPy and DuckDB/Arrow column-to-packed conversion are inside
the timer, as is FastK's native transfer. Hashes/counts are checked outside it.
FastK projection reads full records; column engines may push projection down.

Backend order is randomized. Each round starts a fresh worker, measures open
and first touch separately, then reuses an in-process handle for every backend.
OS cache is uncontrolled; a new process is not called cold cache. The primary
comparison uses one thread. `run_read_matrix.py` separately measures 1/2/4/8
internal budgets and outer query concurrency with one internal thread per handle.
Current FastK outer workers share one native Reader; other adapters retain
thread-owned connections. Output records `reader_ownership`; older frozen results
used independent FastK handles and must not be mixed without labeling this change.
Each internal case has 32 untimed warmups to settle allocation/cache state.
Bulk warm-up matches the historical canonical row hash; each measured result
then matches the complete packed-byte checksum of that verified output.

Raw samples, environment, native module checksum and input hashes are saved.
An external monitor samples the actual Python PID every 5ms for RSS and Windows
private commit / Linux USS. The private-memory definitions differ across OSes;
sampled peaks are lower bounds. Peaks from different processes are never added.
Outer-throughput workers retain results for validation after the timed group.
p95/p99 are omitted for fewer than 100 observations and for full scans.
Summary quantiles use the nearest-rank definition, including the reported
median. Very small round counts are descriptive; inspect raw/min/max values.

Build, later-month extension and historical correction costs are recorded as
single descriptive samples. Corrections regenerate a separate complete version
and recompute all four features using the same code for every adapter; no sparse
update or cheap copy-on-write claim is made. Original input checksums are checked
again after maintenance. SQLite/DuckDB per-month commits and independently synced
files do not have identical crash guarantees; the environment report states the
differences rather than ranking write speed as equivalent durability.

Disk usage separates chunks (including header/sparse index), manifests, zone
maps, value indexes, native databases and remaining journals. A kline-only
bytes/row figure must not describe scalar storage: the raw scalar is 16 bytes,
its value index adds 16 bytes per row, and zone maps add block-level overhead.
Reported space is the sum of file lengths, excluding filesystem allocation and
directory metadata; input bar/scalar counts and logical payload are also saved.

After a complete run, export compact public evidence without copying stores:

```sh
python scripts/export_benchmark_evidence.py --results results/standard --output docs/evidence/standard
```

The export includes the report, summary/environment, input manifest, compressed
raw requests/samples and SHA256SUMS. An incomplete run cannot be exported as
verified evidence.

## Why consider FastK?

Consider it for Rust applications wanting a narrow normalized-record API,
explicit chunk/recovery tooling, scalar predicates and verifiable physical
dataset versions without operating a server. These are functional properties;
they do not establish a universal performance advantage over mature formats.

Python callers can select the same-process NumPy module for precise named
batches or the dependency-free subprocess client for isolation and deadlines.
Benchmark the chosen interface. Prefer alternatives for SQL joins/aggregations,
compressed columns, concurrent writers, network access or enterprise support.
The native module removes JSON/process costs but does not establish a universal
advantage. See [v0.2 results](NATIVE_VALIDATION.md) and the historical reports.

Published recommendations must name the measured workload, artifact and result.
Historical CSV/JSONL comparisons are descriptive format comparisons. Failed or
unfavorable comparisons remain evidence; FastK need not win to make the tests
useful.
