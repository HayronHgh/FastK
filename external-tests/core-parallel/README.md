# Pure Rust parallel acceptance

This standalone crate depends only on the public FastK API. It never imports
Python bindings or puts JSON encoding, hashing, result validation or process
startup inside a query timer. It adds no hooks to the engine.

Build this same crate against a frozen before checkout and the changed checkout,
using the same compiler, lockfile versions and release profile. Each checkout
keeps this directory at `external-tests/core-parallel`, so its `fastk` path
dependency resolves to that checkout. Use distinct target directories and retain
both binaries before measuring.

```powershell
cargo build --release --locked --offline --manifest-path external-tests/core-parallel/Cargo.toml
python external-tests/core-parallel/run.py --before BEFORE_BINARY --after AFTER_BINARY --store STORE --short SHORT_QUERIES --bulk BULK_QUERIES --output NEW_OUTPUT
```

`run.py` uses only Python's standard library to launch sequential Rust processes.
It records three paired rounds, alternates version order, and tests 1/2/4/8
workers. Never compile or run another performance job concurrently. On Linux,
copy the prepared read-only store onto the native filesystem before measuring.
Neither a fresh process nor the first pass implies OS cold cache.

Short input contains the existing 1,430 queries with `round` 0..4 and canonical
`expected.rows` / `expected.sha256`. Each worker preloads one query, then the
same handles execute two complete passes; first and second pass timings remain
separate. Shared mode clones an `Arc<FastKStore>`; independent mode opens one
store per worker. Every timed output is canonical-checked after the group timer.

Bulk input uses the existing small/medium/bulk Kline, bulk Scalar and latest
probes, with all expected fields and ordered rows hashed. Each case records 32
warmups and 30 measurements; every later result equals the canonical-checked
first result. Three rounds give 90 observations per case, so no bulk p95/p99 is
reported. The retained reference output affects allocation/page reuse; compare
paired versions, not unrelated Python attribution timings.

`memory.py --output NEW_JSON BINARY MODE STORE QUERIES 8 SHARING` performs a
separate 5 ms sampled RSS/private run. Linux uses `/proc` for USS and FD counts;
Windows uses the existing development dependency `psutil` for private commit.
These are different metrics, and sampled peaks can miss short transients. Its
query timings must not enter the primary performance results.

The report and checksummed query/source/raw evidence are linked from
[CORE_PARALLEL_RESULTS](../../docs/CORE_PARALLEL_RESULTS.md). Historical probes
and release packages retain their original source and results.
