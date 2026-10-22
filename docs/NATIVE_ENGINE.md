# Version 0.2 native engine

FastK remains an embedded fixed-record storage engine. This release removes
the mandatory JSON/process boundary for Python callers; it does not add SQL,
a daemon, an ingestion framework or a strategy executor.

## Interfaces and representation

Rust callers keep `FastKStore`. The separate `fastk-native` package exposes
`Reader` and `Writer`, using PyO3 and rust-numpy. Kline range/latest and Scalar
range/predicates return one-dimensional, named, packed NumPy arrays. No caller
needs to search dictionaries for OHLCV fields. The native adapter owns type
conversion, while typed range append orchestration lives once in the engine
and is shared with the existing bridge.

| Batch | Fields in order | Bytes per row |
|---|---|---:|
| Kline | ts, open, high, low, close, volume | 48 |
| Scalar | ts, value | 16 |

All fields remain little-endian signed int64, with the original disk format,
UTC millisecond timestamps and fixed-point scales. Smaller integer types would
reduce supported dates, precision or magnitude, so no narrowing is performed.
Metadata is queried separately from batches. NumPy field views may be strided.
Kline range/latest and Scalar range hand their original Rust allocation to NumPy
through a checked int64 ownership cast; no second record-to-word allocation or
copy is needed. Compile-time layout checks pin field offsets and reject padding.
Predicate matches still validate optional values and materialize their output.
This is not a claim of zero-copy disk IO or a memory-mapped result.

`fastk-client` stays independent and dependency-free. Its existing dictionaries,
private compact session JSON, hard timeouts and no-replay behavior remain.
Public CLI pretty JSON and JSON contract version 1 are retained. Native calls
have no hard timeout: callers needing forcible cancellation use the subprocess
client or their own worker process. There is no silent retry or fallback.

Read-only decoded manifests belong to the store and are released on close.
A fresh Reader reloads the requested series metadata without publishing it in the
process-global writable cache. Shared OS leases and pending-recovery checks remain.
See [request-scoped measurements](LAZY_READ_RESULTS.md) for open/read/close costs.

## Core changes

Cached files now share `Arc<File>` but each reader owns its logical offset.
Unix uses positional `pread`; Windows uses synchronous `ReadFile` with an
explicit OVERLAPPED offset on every read. Layout loading, sparse probes and fixed-record reads use this path.
File cache hits use shared read locks and mark a second-chance bit instead of
scanning and rewriting an LRU queue; insertion and eviction take the write lock.
File opening, positional IO and eviction-time file closing run outside that lock.
The former OS cursor race is covered by cold/shared and cached concurrent tests.

Sparse lookup uses one binary-search position to obtain both window bounds.
Short Kline reads coalesce the same or adjacent sparse blocks into the owned
output and trim in place, preserving earlier chunks. Scalar range rows append to one
buffer, latest Kline rows are planned into one output, and partial Kline
capacity no longer reserves a whole month at the 2,049-slot boundary. Byte
views never expose uninitialized integer records; failed reads return no partial data.

Writers hold OS guards on the stack. Range operations retain one guard over
preflight and all month commits, without a stored batch-lock flag or callback
wrapper. Invalid ordering, month boundaries, metadata conflicts and overlap
are rejected before arming recovery where they can be determined in advance.
Each month remains an independent commit; an error can report completed months.
Scalar append still rewrites that month and its indexes. Compaction debt counts
the currently readable fragments after the baseline, rather than historical
generation numbers, so a completed merge has zero remaining delta debt.

## Parallel reading and caller state

One thread is the default and allocates no pool. `set_read_threads(n)` or
`Reader(..., threads=n)` creates a bounded pool owned by that handle. Candidate
reads estimated at least 4 MiB are planned using sparse boundaries, including
serial reads. Planned output stays `MaybeUninit<Record>` until all positional
IO succeeds. Unix uses rustix's uninitialized byte buffer support; Windows passes
only raw destination pointers to synchronous ReadFile. Record views are published
only after exact completion; no uninitialized `&mut [u8]` is formed. Actual
parallel results are split into approximately 1 MiB record-aligned tasks,
then grouped according to payload size and the configured worker upper bound.
The internal grain is 1 MiB per useful worker on Windows and a conservative
4 MiB elsewhere; the existing 4 MiB minimum for entering the pool is retained.
An effectively serial read fills each span without splitting it into 1 MiB tasks.
Tasks write disjoint regions of one output buffer. Ordering is determined by
the plan, not task completion order. Small reads stay serial. There is no global
Rayon pool or nested unbounded fan-out.

For concurrent Rust queries, share one `Arc<FastKStore>` opened with
`open_read_only`, rather than opening a separate store in each worker. Its
bounded file cache and internal pool then belong to the store, not each caller.
Independent stores retain independent caches and leases. More threads do not
guarantee higher throughput; compare first-touch and warm reads separately.
See [core parallel measurements](CORE_PARALLEL_RESULTS.md) for the cache fix,
pure Rust results and remaining Linux startup limitations.

Initialize the root before creating a native reader; missing or uninitialized
roots are rejected so the reader always owns a real shared lease.
Python IO releases the GIL. A native reader allows concurrent calls and retains
shared OS ownership until close; its writer serializes calls and acquires
exclusive ownership per operation. Close waits for active operations. Returned
arrays survive close and do not retain the store lease.

Parallel prefetch and sequential state evolution are separate concerns. A caller
may prefetch several months concurrently, then feed them chronologically to its
strategy while carrying position, cash and indicator warm-up state. Arbitrary
month backtests cannot be declared independent when positions cross month
boundaries. The public integration test compares carried-state execution with
a full-range run; FastK does not implement a backtest executor.

## Evaluation

The [verification report](NATIVE_VALIDATION.md) separates Rust-before/after
timings from native Python delivery gains. All five main Python adapters fully
materialize the same read-only NumPy layout, with conversion inside the timer.
Internal single-query thread budgets and concurrent-query throughput are
reported separately. Historical JSON reports remain available and unchanged.

The synchronous Windows buffer/offset lifetime follows the documented
[ReadFile contract](https://learn.microsoft.com/en-us/windows/win32/api/fileapi/nf-fileapi-readfile).
