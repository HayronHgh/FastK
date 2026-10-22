# Private compact JSON: implemented and verified

Scoped sessions now serialize the existing typed result directly into their
single Rust response Vec with compact JSON. A private `JsonOutput<W>` enum in
the CLI adapter selects `Pretty` for stdout and `Compact` for session buffers.
Both routes use the same dispatcher and response structs. The engine and Python
source files are unchanged. There is no public format flag, new method, parser
dependency, compression, buffer pool or caller-side mapping change.

Existing dict/list/int returns, field order, metadata, scales and errors are
preserved. The public one-shot CLI retains byte-identical pretty JSON including
its newline. Private wire whitespace changes intentionally; the value/schema
contract and session protocol remain version 1. Nothing is written until a full
frame is ready; errors clear any partial success bytes before being framed.

## Paired latency against the buffer build

The control was copied before editing from the already verified readinto/single
Vec build. Both routes have those two optimizations. These are freshly rerun,
interleaved observations isolating compact JSON, not differences between historical
tables. Windows 11, Python 3.12.14, Ryzen 9 9900X; OS cache/scheduling uncontrolled.
No compilation, installation or packaging ran during this measurement.

The unchanged `benchmark_buffers.py` shuffles 90 fresh Python/Rust jobs with seed
20261023. Each bulk case/route/round has an untimed, canonically checked warm-up,
then a timed read. Ten observations per shape/route support descriptive medians,
not bulk p95/p99. Constructor/handshake and validation are excluded from latency.
The original caller tuple conversion/releasing records is measured separately.

| Shape | Rows | Client API before | After | Latency reduction | Client + original caller before | After |
|---|---:|---:|---:|---:|---:|---:|
| small_kline | 127 | 0.6186ms | 0.5903ms | 4.57% | 0.6697ms | 0.6346ms |
| medium_kline | 4,077 | 4.7053ms | 4.2553ms | 9.56% | 5.9350ms | 5.4250ms |
| bulk_kline | 437,115 | 415.4773ms | 378.0066ms | 9.02% | 573.1814ms | 535.0556ms |
| bulk_scalar | 437,115 | 168.1316ms | 148.1797ms | 11.87% | 271.7070ms | 253.7400ms |

The combined timer is measured directly; separately computed medians must not
be added. These numbers establish this scoped workload's behavior only. The
remaining Python JSON/object and caller conversion work is still substantial.

Each route also replays the exact original five batches of 286 short ranges,
preserving within-batch order and the original adapter/validation boundaries.

| Exact 1,430 short ranges per route, Client + adapter | Before | After |
|---|---:|---:|
| Median | 0.5839ms | 0.5381ms |
| p95 | 0.7442ms | 0.7229ms |
| p99 | 0.9870ms | 0.9042ms |

All 2,940 timed responses (2,860 short + 80 size reads) and 80 warm-ups match
full canonical row hashes and expected metadata/integer semantics. Every job
checks imported client source paths and reaps its children. No retry or worker
replacement is used. Short distributions describe this host; no CI timing gate.

## Wire sizes and memory

Sizes below include the success envelope, exclude the eight-byte length prefix,
and are exact byte counts. JSON whitespace removal reduces transfer/buffer size;
it does not reduce the engine's on-disk storage or change returned object count.

| Shape | Before bytes | After bytes | Reduction |
|---|---:|---:|---:|
| small_kline | 23,342 | 15,939 | 31.72% |
| medium_kline | 742,242 | 505,739 | 31.86% |
| bulk_kline | 79,555,158 | 54,202,451 | 31.87% |
| bulk_scalar | 30,161,273 | 18,796,230 | 37.68% |

A separate coordinator samples actual child-reported Python PID, Rust worker and
any distinct Windows venv launcher every 5ms. PID ancestry is checked. The window
includes warm-up, timed read, caller conversion, validation and request cleanup
until result publication; it excludes explicit Client shutdown and coordinator
memory. Initial/startup memory is recorded. OS per-process peak working sets
include startup and catch peaks between samples; these independent high-water
marks must never be added as a simultaneous tree peak.

The concurrent RSS/private-commit figures are maximum simultaneous sampled sums
within each run, then medians across ten fresh runs. RSS can count shared pages
twice; private commit is committed memory, not resident physical memory. Sampled
maxima are lower bounds at 5ms resolution.

| Bulk workload peak metric (MiB) | Kline before | After | Scalar before | After |
|---|---:|---:|---:|---:|
| Rust worker OS peak working set | 154.66 | 79.22 | 44.44 | 39.59 |
| Python OS peak working set | 382.81 | 335.43 | 204.17 | 194.09 |
| Concurrent process-tree RSS, sampled | 393.70 | 347.90 | 213.65 | 203.93 |
| Concurrent process-tree private commit, sampled | 373.62 | 327.13 | 193.13 | 183.72 |

Compaction shrinks frame bodies and Python's temporary decoding input. Returned
record dictionaries and integers remain, so this is not a zero-copy or bounded
memory API. The receiver still allocates a validated body bytearray; the IO loop
can retain its last exchange until another request or context close, as before.
Use the actual table above, rather than wire-size reduction, to assess memory.

## Failure and compatibility checks

Per-request store/OS guard ownership is unchanged: idle sessions retain no lock,
and later requests observe external writes/recovery/sealing. The deadline still
covers queue wait plus framed IO; payload encoding and Python decode are outside
the IO timeout. A queued timeout sends nothing. Timeout/protocol failure after
dispatch kills/reaps the worker, poisons the session and never replays the call.
Partial-month writes still require explicit inspection/recovery before another
explicit operation. Business errors leave the existing worker usable.

New Rust serializer checks cover signed-i64 extremes, Unicode/escaped strings,
null/empty shapes, exact pretty bytes and propagated serialization errors.
The raw-frame verifier checks all four sizes, a business error and success after
that error on the same workers. The new frame must equal compact re-encoding of
the old ordered object, preserving integer/boolean/string representation and
key order, not merely Python's loose numeric equality. Error bytes are identical.
One-shot version/small/medium output bytes are identical to the preserved worker.

Existing real-child tests cover truncated/oversized frames, allocation failure,
fragmented reads, wrong IDs/JSON, worker death, blocked request/body pipes,
stderr flooding, queue deadlines, concurrent close, no replay and reaping.
Storage integration checks real shared/exclusive locks, timeout lock release,
external-write freshness, partial-month failure/recovery and sealed writes.

| Check | Result |
|---|---|
| Windows Rust root + CLI serializer/framing | 134 + 4 PASS |
| Linux Rust root + CLI serializer/framing | 134 + 4 PASS |
| Python, each OS | 31 PASS, 1 SKIP |
| Private response encoding/value pairs; one-shot bytes | 6; 3 PASS |
| Paired bulk/short/memory | 90 jobs, all results/children verified |
| Offline installed Windows/Linux wheels, PATH stripped | PASS |
| Ruff check + format check; cargo fmt | PASS, 29 Python files |
| Both private diagnostic source generators compile | PASS |

The skipped Python test validates the entire release archive. This local follow-up
instead ships independently hashed development wheels/bridges and does not
replace the old full release archive. Windows uses Rust1.97.1 GNU/static CRT;
Linux uses Rust1.90/static musl. Linux correctness/install passed; performance
and memory were measured on Windows only. Source generators were adapted to the
private adapter and UTF-8 source; earlier frozen evidence remains unchanged.

## Reproduce and inspect

Use the pinned external-test environment with psutil, the same prepared store,
and a new output directory. Extract fastk_client from the prior buffer wheel
into target/compact-baseline; pair it with that delivery's standalone bridge.
Do not substitute a pre-readinto worker, which would combine two optimization
steps. Current before/after Python sources have identical hashes.

```powershell
$env:PYTHONPATH = (Resolve-Path clients/python/src).Path
python external-tests/benchmark_buffers.py --baseline-client target/compact-baseline --baseline-bridge dist/session-buffers-2026-10-20/windows/fastk_bridge.exe --client clients/python/src --bridge target/release/examples/fastk_bridge.exe --store target/external-standard-evidence/fastk --bulk-queries docs/evidence/2026-10-20/compact/queries.json --prior-results docs/evidence/2026-10-20/standard/raw --output target/compact-new
python external-tests/verify_session_bytes.py --before dist/session-buffers-2026-10-20/windows/fastk_bridge.exe --after target/release/examples/fastk_bridge.exe --store target/external-standard-evidence/fastk --queries docs/evidence/2026-10-20/compact/queries.json --compact-session --output target/compact-new-parity.json
```

Source/binary hashes, raw jobs with timing/memory/PIDs, query shapes, exact
wire/CLI checks and test logs are exported in [evidence](evidence/2026-10-20/compact/manifest.json).

- before Windows bridge SHA-256: `8fc5d79b3fcc8ac8cd6b551a2e39b1bfa9e51578f9496ab09b933ac62d659fad`.
- after Windows bridge SHA-256: `56af92b6d64e4e4bd85482e230a70d0cf36e522a93a61f2741b3afa9852489b1`.

Matching Windows/Linux wheels and standalone bridges are under
`dist/session-compact-2026-10-20/`, with SHA256SUMS and a development manifest.
Use `pip install --force-reinstall` when replacing a same-version local wheel.
Version remains 0.1.0rc1 / public contract 1 / private session protocol 1.
Prior dist/evidence directories remain unchanged; no publication is implied.
