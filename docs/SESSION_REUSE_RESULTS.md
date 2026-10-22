# Scoped Python bridge: executed results

`with Client(...)` removes repeated worker startup while keeping the current
methods, return dictionaries, integer fields, scales and business errors.
The same 1,430 historical range queries now have **0.6329ms median** latency,
versus **24.0264ms** for the preserved baseline executable in this final replay
(38.0x). This is evidence for repeated short reads, not a full-research or
large-response speed claim.

```python
from fastk_client import Client

with Client("./research-store") as store:
    first = store.read_kline_range("BTCUSDT", "1m", 1706745600000, 1706745660000)
    second = store.read_kline_range("BTCUSDT", "1m", 1706745720000, 1706745780000)
```

Outside a context, existing one-process-per-call behavior remains. No alternate
read methods, engine transport traits, database service or third-party Python
runtime dependency were added. See [transport contract](SESSION_TRANSPORT.md).

## Same-workload comparison

Five rounds each replay the original 286 range queries in their recorded order.
Route order is shuffled per round with seed 20261020. All routes use the same
prepared pre-maintenance store. Timing includes the public Client call and the
original ordered tuple conversion; canonical hashing/field validation is outside
the timer. The host is Windows 11, Python 3.12.14, Ryzen 9 9900X; OS cache and
background scheduling are uncontrolled. No compilation/install job ran during
this replay. Percentiles use nearest rank over 1,430 samples per route.

| Route | Median | p95 | p99 | Processes over five rounds |
|---|---:|---:|---:|---:|
| Preserved baseline, one-shot | 24.0264ms | 26.9243ms | 30.2520ms | 1,435 |
| New bridge, one-shot | 24.1776ms | 27.3389ms | 30.8351ms | 1,435 |
| New bridge, scoped worker | **0.6329ms** | **0.8456ms** | **1.0223ms** | **10** |

Each round constructs a Client. One-shot counts include one version subprocess
plus 286 query subprocesses. Session counts include one version subprocess plus
one worker for all 286 queries. All observed children were reaped. The original
historical median **24.4209ms** and diagnostic replay **25.5967ms** remain separate
measurements; the comparison above reruns the preserved baseline alongside this
implementation rather than treating historical timing as a current control.

All 4,290 responses (three routes) match the canonical full-row hashes and
expected response keys, metadata/scales and integer types. Five representative
one-shot responses are also byte-identical between old/new CLI executables.
Existing Client/schema checks and session parity tests also cover scalar,
indicator, latest, predicate, inventory and snapshot behavior.

Startup was measured separately, with only five observations per route:

| Route | Constructor/version median | Context-entry median | Constructor to first result median |
|---|---:|---:|---:|
| Baseline one-shot | 23.4658ms | — | 48.3882ms |
| New one-shot | 23.0373ms | — | 46.5057ms |
| Session | 24.6260ms | 20.5237ms | 46.1584ms |

These are separate medians and should not be added. There is no first-call
latency claim or startup-tail estimate. Both proposed short-query gates
(median <=3ms, p95 <=10ms) passed on this host/workload. No CI test asserts these
machine-dependent thresholds.

## Failure and ownership checks

The bridge shares its existing command dispatcher and pretty JSON serializer
between one-shot and session execution. Only the process lives across requests;
each request opens/drops its own store and guards. Tests alternate session reads
with external writes and check refreshed inventory/metadata. Real OS shared
locks allow reads and block writes; exclusive locks block both. A timed-out
child holding the actual write lock is killed/reaped, after which an independent
writer succeeds. No lock file is deleted to achieve recovery.

Requests are serialized per Client. A deadline while waiting sends no frame and
does not kill an active request. After dispatch, a timeout kills/reaps the worker
and joins its IO/stderr threads. Tests cover blocked request pipes, missing and
truncated responses, invalid sizes/IDs/JSON/errors, a 4MiB stderr stream, external
process death, thread startup failure, interruption and concurrent close. A
failed session never starts a replacement worker or resends a request.

An application error leaves the worker usable. A real cross-month write is
forced to fail after its first month commits; reads then require explicit
recovery, `init()` recovers, and the committed row is not duplicated. A timeout
cannot promise rollback: inspect inventory/recover before an explicit new write.
Sealed-root mutation remains rejected and snapshot verification still passes.

| Executed check | Windows | Linux |
|---|---|---|
| Rust root suite, fault injection enabled | 134 PASS | 134 PASS |
| Private framing boundaries | 2 PASS | 2 PASS |
| Python client/session/schema checks | 26 PASS, 1 SKIP | 26 PASS, 1 SKIP |
| Offline wheel install outside checkout, Rust removed from PATH | PASS | PASS |
| Installed bundled worker reused for real calls | PASS | PASS |

The Python total includes 18 new session tests, seven existing client tests and
one response-schema test. The skipped check validates an entire release-package
manifest; this follow-up builds wheels/standalone bridges rather than replacing
the prior complete release archives. New delivery hashes are verified separately.
Windows used Rust 1.97.1 GNU with static CRT flags; Linux used Rust 1.90 in a
local Docker container and a static musl bridge. MSVC CI is authored, not run here.

## Reproduce and inspect

```powershell
$env:PYTHONPATH = (Resolve-Path clients/python/src).Path
$env:FASTK_BRIDGE_BIN = (Resolve-Path target/release/examples/fastk_bridge.exe).Path
python -m unittest discover -s external-tests -p 'test_*.py' -v
python external-tests/benchmark_session.py --baseline /path/to/preserved/fastk_bridge.exe --bridge target/release/examples/fastk_bridge.exe --store target/external-standard-evidence/fastk --prior-results docs/evidence/2026-10-20/standard/raw --output target/session-replay-new
```

Use the pinned external-test environment for schema validation and prepare the
same standard store using the existing harness; datasets/executables are not
embedded in evidence. `--output` must be new. Query files may be JSON or JSON.gz.
The old baseline SHA-256 is
`543d5e41f92b153c573dffd30c2561871fc465b5c67c13a0d982fbd27dc91333`;
the new Windows bridge is
`b1d34929a311b72173849da0d12e5ad064a9e6ad6a1e50485d7903d11397ed85`.

[Evidence](evidence/2026-10-20/session-reuse/summary.json) contains the summary,
environment/query/source hashes, compressed raw samples and verification logs,
with SHA256SUMS. The original measurement-only evidence remains untouched.
New local development wheels and standalone bridges are delivered under
`dist/session-reuse-2026-10-20/`, with their own manifest and checksums. Both use
the existing 0.1.0rc1 version; use `pip install --force-reinstall /path/to/wheel`
when replacing an earlier local build. Earlier dist packages remain unchanged.

## Remaining scope

The one-shot serializer and the JSON result object remain unchanged. Session
framing adds a 1GiB request/response limit and buffers a complete response to
prefix its length. It does not establish improved bulk throughput or memory use.
Python decoding/object conversion still applies; large JSON optimization and a
new end-to-end research comparison remain separate work. Closed or broken
Clients require a new instance; there is no automatic reconnect or retry.

The later [large JSON cost study](SESSION_JSON_COST.md) separately measures
serialization, copies, pipe receiving, Python parsing and caller conversion.
It is a proposal for review; the session build measured above remains unchanged.
