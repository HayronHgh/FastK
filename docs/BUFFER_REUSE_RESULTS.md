# Scoped buffers: implemented and verified

This report freezes the buffer-only step. The subsequent private compact JSON
implementation and its separate paired control are in [compact results](COMPACT_SESSION_RESULTS.md).

The scoped Python receiver now fills one preallocated bytearray with bounded
readinto calls, passing it directly to the existing stdlib JSON decoder. Rust
builds the envelope and pretty result in one Vec; an application error clears
the partial success and serializes the error into that same allocation before
writing any frame. No new buffer pool is introduced. As before, the IO loop can
retain its last completed exchange until the next request or context close;
this change does not optimize that idle-frame lifetime.

Only the two private transport implementations changed. Existing Client methods,
dict/list/int returns, metadata/scales, business errors, one-shot calls and
pretty JSON bytes are retained. Store ownership remains per request. Compact
JSON, alternative decoders and caller-conversion optimization were not added.

## Paired same-workload latency

The control is the preserved scoped-worker client and executable from the
previous JSON study, rerun alongside the new build. Both execute the same
prepared pre-maintenance store and frozen queries. This is not a subtraction
from a historical table. The host is Windows 11 / Python 3.12.14 / Ryzen 9 9900X;
OS cache and scheduling are uncontrolled. No compilation/install ran during the
authoritative replay. Jobs are shuffled with seed 20261023.

For each shape, route and round, a fresh Python process creates one scoped Rust
worker. A validated bulk warm-up precedes the timed read. There are ten timed
observations per shape/route and descriptive medians only, without bulk tail
claims. Constructor/handshake, warm-up and validation are outside latency timers.
The original ordered tuple adapter remains unchanged and is reported separately.

| Shape | Rows | Client API before | After | Latency reduction | Client + caller adapter before | After |
|---|---:|---:|---:|---:|---:|---:|
| small_kline | 127 | 0.6957ms | 0.7134ms | -2.54% | 0.7404ms | 0.7884ms |
| medium_kline | 4,077 | 5.4709ms | 5.0548ms | 7.61% | 6.7434ms | 6.4106ms |
| bulk_kline | 437,115 | 586.5089ms | 440.9077ms | 24.83% | 763.3764ms | 615.8756ms |
| bulk_scalar | 437,115 | 227.4467ms | 167.7474ms | 26.25% | 341.6511ms | 279.0085ms |

The bulk Kline and Scalar API medians exceed the proposed >=15% improvement
gate. Caller tuple conversion/releasing record dicts is still a caller cost;
separate medians must not be added. In the small mixed-size control there is
no improvement claim; the representative short replay below has 1,430 samples.

Each route replays the exact original five batches of 286 ranges, in their
recorded within-batch order. It uses the original short adapter, retaining
record dicts through conversion and excluding canonical checks from the timer.

| Exact 1,430 short ranges per route | Before | After |
|---|---:|---:|
| Median, Client + original adapter | 0.6498ms | 0.6529ms |
| p95, Client + original adapter | 0.8427ms | 0.8300ms |
| p99, Client + original adapter | 0.9950ms | 1.0431ms |

Short median and p95 have no >5% regression in this paired run. These are host
observations, not hard CI thresholds. All 2,940 timed results (2,860 short + 80
size reads) and 80 bulk warm-ups match full canonical row hashes and expected
metadata/integer semantics. No process was silently replaced or request retried.

## Memory: worker peak falls, whole-workload RSS stays similar

A separate coordinator samples actual Python and Rust PIDs every 5ms while the
workload runs, including warm-up, timed read, tuple mapping, validation and
request/response cleanup. Sampling stops when the result file is written,
before explicit Client shutdown. The Python process reports os.getpid; ancestry is checked, so a Windows
venv launcher cannot be mistaken for Python. A distinct launcher is included
in tree totals. The coordinator itself is excluded.

OS per-process peak working-set counters are also read before processes exit;
these include startup and can catch peaks between samples. They are separate
high-water marks and must not be summed as a simultaneous tree peak. Tree RSS
and private commit below are the maximum concurrent sums actually sampled per
run, then medians across ten fresh runs. RSS may double-count shared pages;
private commit is committed process memory, not resident physical memory.

| Bulk workload peak metric (MiB) | Kline before | After | Scalar before | After |
|---|---:|---:|---:|---:|
| Rust worker OS peak working set | 234.26 | 154.67 | 91.97 | 44.91 |
| Python OS peak working set | 383.56 | 383.89 | 204.09 | 205.09 |
| Concurrent process-tree RSS, sampled | 395.44 | 395.62 | 213.59 | 214.60 |
| Concurrent process-tree private commit, sampled | 463.82 | 375.32 | 193.93 | 193.95 |

Removing the full Rust envelope copy substantially lowers worker peak working
set. Overall RSS is effectively unchanged: Python parsing buffers, record objects
and caller-side work dominate that peak. Kline private commit falls, while Scalar
private commit remains similar. This does not establish an across-the-board
whole-application memory reduction or a storage-footprint improvement. A 5ms
sampled tree maximum is a lower bound, not an exact OS aggregate peak.

The memory window is intentionally broader than the latency timer. These are
same-process-layout comparisons, not a cache-cold or zero-copy claim. Raw jobs
record Python/worker/launcher PIDs, initial memory, individual sampled peaks,
OS peaks and sample counts. The baseline/new results use identical monitoring.

## Failure, ownership and byte compatibility

Frame lengths are still validated against 1GiB before allocating the body.
Partial readinto fills advance by the actual number read; EOF/truncation fails
closed. The existing IO thread, serial deadline and process kill/reap path are
unchanged. Payload serialization and Python decoding remain outside the IO
timeout, as before. A queued timeout sends nothing; failures after dispatch
never restart a worker or replay an uncertain operation.

New focused tests cover fragmented prefix/body, exact Unicode/signed-i64 values,
zero/oversized lengths before body allocation, truncated input, body allocation
failure, timeout while blocked in a partial body, concurrent close, reaping and
no replay. Existing tests still cover blocked request writes, malformed frames,
worker death, stderr flooding, queue deadlines, external writes/metadata refresh,
real shared/exclusive OS locks, timeout lock release, partial-month failure and
explicit recovery, immutable snapshots and application-error continuation.

Full raw session frames match the preserved worker for all four query shapes,
an application error and a successful request after that error. One-shot version,
small and medium success bytes also match exactly. There is no whitespace,
field-order, integer/scaling or public response-shape change in this release.

| Check | Command / evidence | Exit / status | Result |
|---|---|---|---|
| Windows root + framing | cargo test --locked --features fault-injection; cargo test --locked --example fastk_bridge | 0; 134 + 2 tests | PASS |
| Linux root + framing | Same, Rust1.90 Docker | 0; 134 + 2 tests | PASS |
| Python checks, each OS | unittest discover -s external-tests -p test_*.py | 0; 31 PASS, 1 SKIP | PASS / SKIP |
| Exact frames / one-shot | verify_session_bytes.py | 0; six + three pairs | PASS |
| Paired bulk/short + memory | benchmark_buffers.py | 0; 90 fresh jobs | PASS |
| Offline installed wheels | smoke_install.py, PATH stripped | 0 on Windows/Linux | PASS |
| Python static checks | Ruff check + format check | 0; 29 files | PASS |

The skipped Python check validates an entire release-package archive. This
follow-up builds wheels and bridges with an independently hashed development
delivery; it does not replace the previous complete release archives. Diagnostic
source generators also compile with the new layout; their earlier frozen
measurement sources/evidence remain unchanged. Windows used Rust1.97.1 GNU
with static CRT flags; Linux used a static musl bridge. Performance/memory were
measured on Windows only; Linux correctness/installation passed.

## Reproduce and inspect

Preserve the earlier session-reuse wheel/executable and extract its fastk_client
package into a separate baseline source directory before rebuilding. The
baseline binary SHA is recorded below; using the new source for both routes
would invalidate the comparison.

```powershell
$env:PYTHONPATH = (Resolve-Path clients/python/src).Path
python external-tests/benchmark_buffers.py --baseline-client target/buffer-baseline --baseline-bridge target/buffer-baseline/fastk_bridge.exe --client clients/python/src --bridge target/release/examples/fastk_bridge.exe --store target/external-standard-evidence/fastk --bulk-queries docs/evidence/2026-10-20/buffers/queries.json --prior-results docs/evidence/2026-10-20/standard/raw --output target/buffers-new
```

Use the pinned external-test environment (psutil) and the same prepared store;
the output directory must be new. Both fresh child routes assert their imported
client source path. Sampled memory and raw query timings are exported together
with source/binary hashes, raw byte parity and test logs in
[evidence](evidence/2026-10-20/buffers/manifest.json).

- before Windows bridge SHA-256: `b1d34929a311b72173849da0d12e5ad064a9e6ad6a1e50485d7903d11397ed85`.
- after Windows bridge SHA-256: `8fc5d79b3fcc8ac8cd6b551a2e39b1bfa9e51578f9496ab09b933ac62d659fad`.

New local development wheels and standalone bridges are under
`dist/session-buffers-2026-10-20/`, with source/binary consistency checks and
SHA256SUMS. Version remains 0.1.0rc1 / public contract1 / session protocol1;
use pip --force-reinstall when replacing an earlier local build of the same
version. Previous dist directories remain unchanged. Compact JSON is explicitly
left for a separate implementation and same-workload validation.

