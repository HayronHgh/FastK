# Compact JSON: Python cost breakdown and optimization proposal

The remaining bulk cost is Python response parsing/object creation plus the
research caller's dict-to-tuple mapping. The strongest low-risk first change is
the external caller adapter. The native decoder trial is faster but substantially
increases memory and has input-semantic differences; it should not replace the
default decoder on this evidence.

This is measurement and a proposal for review. Engine/CLI/Python client sources,
runtime dependencies, public methods/returns, timeout/no-replay/OS ownership and
distributed wheels/binaries are unchanged. Only external diagnostic tools,
optional test requirements and documentation were added/updated. No new release
package or production speedup is claimed.

## Actual compact build: where time goes

The bridge is the already delivered compact build, SHA-256
`56af92b6d64e4e4bd85482e230a70d0cf36e522a93a61f2741b3afa9852489b1`.
It has readinto, one Rust Vec and compact private JSON. Frozen queries, canonical
expected rows and the pre-maintenance store are identical to the previous study.
There are 437,115 rows in each bulk case; frames are 54,202,451 bytes Kline and
18,796,230 bytes Scalar. Public CLI pretty JSON stays unchanged.

Ten rounds interleave an unchanged production worker and a private instrumented
copy, each reused throughout the experiment. Four initial fixture pairs have
identical full frame bytes. Eighty timed results and eight initial responses
match complete independent row hashes and metadata. A shared Windows QPC clock
partitions each observed timeline exactly. Startup, validation/hashing and later
tuple destruction are outside the timed read+caller interval.

The private Python wrapper follows the installed CPython json.loads bytes path:
detect encoding, decode with surrogatepass, parse the resulting str, release the
temporary text. It adds timestamps around these steps; default JSON decoding
remains in the unchanged production control. JSON parsing below includes all
dict/list/int creation. This probe does not isolate lexical scanning from object
allocation; subtracting unrelated microbenchmark medians would not establish that.

| Non-overlapping observed mean stage | Bulk Kline | Share | Bulk Scalar | Share |
|---|---:|---:|---:|---:|
| Python request / Rust dispatch | 0.479ms | 0.09% | 0.919ms | 0.38% |
| Engine and response construction | 5.982ms | 1.15% | 4.128ms | 1.71% |
| Rust compact JSON and Vec growth | 42.561ms | 8.18% | 15.437ms | 6.41% |
| Rust cleanup / probe log | 0.753ms | 0.14% | 0.274ms | 0.11% |
| Frame finalization, no full copy | 0.000ms | 0.00% | 0.000ms | 0.00% |
| Wire + Python receive | 15.518ms | 2.98% | 5.578ms | 2.32% |
| IO-to-caller handoff | 0.081ms | 0.02% | 0.080ms | 0.03% |
| Encoding detection | 0.016ms | 0.00% | 0.010ms | 0.00% |
| UTF-8 decode | 7.444ms | 1.43% | 2.548ms | 1.06% |
| JSON parse + dict/list/int creation | 289.019ms | 55.58% | 107.428ms | 44.61% |
| Temporary decoded text release | 1.708ms | 0.33% | 0.678ms | 0.28% |
| Python envelope return | 0.037ms | 0.01% | 0.036ms | 0.01% |
| Caller tuple conversion + record release | 156.437ms | 30.08% | 103.722ms | 43.07% |
| **Combined mean** | **520.035ms** | **100%** | **240.839ms** | **100%** |

These are means of the same ten samples per shape, so the columns close to their
combined means. Sender write/flush overlaps receiver work and is not another
additive stage. Engine timing includes dispatch, open/metadata/read, response
mapping and guard release, not pure disk latency. GC pauses are already inside
their measured operation; they are not another cost to add.

The Python JSON parse/object and caller mapping stages together account for
about 86-88% of the observed combined time. UTF-8 conversion is about 1-1.4%.
Further indexing or storage-record changes would target a small part of this
particular workload. The caller conversion is not work performed by Client:
Client already returns the requested record dictionaries.

| Unchanged production observations, medians | Client API | json.loads including text/objects | Caller mapping/release | Combined |
|---|---:|---:|---:|---:|
| small_kline | 2.181ms | 0.114ms | 0.044ms | 2.261ms |
| medium_kline | 4.443ms | 2.425ms | 1.281ms | 5.774ms |
| bulk_kline | 360.724ms | 296.653ms | 151.492ms | 521.279ms |
| bulk_scalar | 135.151ms | 109.690ms | 102.928ms | 238.150ms |

Component medians do not add. Small reads here are mixed with bulk allocations;
use the separate 1,430-range replay below for short-call distributions. The
instrumented-minus-normal combined mean is bulk_kline: 0.038ms, bulk_scalar: 2.180ms.
This is observed probe/control variation, not a correction applied to production.

## Controlled parse, conversion and destruction

Ten shuffled cached-control rounds per shape use the exact compact frame and
verify recursive built-in types, values, key order and full row hashes. Text is
prepared outside the existing-text parse timer. The UTF-8-only control allocates
text but omits parsing. All result destruction is outside parse-only timers.
References/text/raw coexist in these cached tests, so they establish operation
costs, not realistic process memory.

| Cached operation median | Bulk Kline | Bulk Scalar |
|---|---:|---:|
| UTF-8 decode only | 6.810ms | 2.463ms |
| stdlib loads(bytearray), current input | 292.325ms | 109.424ms |
| stdlib loads(existing str) | 284.042ms | 108.407ms |
| orjson 3.13.0 loads(bytearray), candidate only | 151.003ms | 60.105ms |
| Generator mapping, record dicts retained | 130.403ms | 86.603ms |
| itemgetter mapping, record dicts retained | 64.171ms | 33.503ms |
| Generator mapping + consumed record release | 155.790ms | 104.494ms |
| itemgetter mapping + consumed record release | 80.970ms | 43.072ms |
| Release records only, no tuple result | 34.331ms | 15.101ms |

Consumed-record controls parse a fresh response outside the conversion timer,
detach its records, build ordered tuples, then release their input dicts. Those
dicts have no retained fixture reference. Returned tuples keep their integer
values alive; dropping dicts therefore differs from releasing all records with
no output. Separate phase medians must not be added.

| Consumed mapping phases, medians | Kline generator | itemgetter | Scalar generator | itemgetter |
|---|---:|---:|---:|---:|
| Build tuples | 138.307ms | 63.805ms | 96.396ms | 34.038ms |
| Release input record dicts | 17.679ms | 17.329ms | 8.222ms | 8.426ms |

GC callbacks are present only in cached diagnostics; thresholds/collector state
are never retuned or globally disabled. Median nested GC pauses during stdlib
bytearray parsing are 3.945ms / 3.718ms (Kline/Scalar). Full raw collection counts and pauses are exported. Tuple
allocation can trigger different generations depending on prior controls, so
these GC observations are not a pure allocator benchmark or a production tuning
recommendation. itemgetter still materially reduces the measured whole mapping
operation, including such pauses.

## Native parser trial: faster, memory tradeoff fails the default-adoption case

The same compact worker and identical client source are used on both routes.
Only the measurement child's private session loads callable is swapped to
orjson. Request serialization, source files, IO/deadline/cleanup paths and the
original caller adapter stay unchanged. No monkeypatch is shipped in a wheel.
Ninety shuffled fresh Python/Rust jobs run ten bulk observations per shape/route
and the exact five original batches of 286 short ranges. Bulk warm-ups, startup
and validation are outside latency timers. All 2,940 timed responses plus 80
warm-ups match canonical results; all workers are reaped.

| Same-source experiment | stdlib API median | Native trial API median | Reduction | stdlib + original caller | Native + original caller |
|---|---:|---:|---:|---:|---:|
| small_kline | 0.6342ms | 0.5654ms | 10.85% | 0.6783ms | 0.6083ms |
| medium_kline | 4.4559ms | 2.8918ms | 35.10% | 5.6491ms | 4.1691ms |
| bulk_kline | 360.2455ms | 220.3064ms | 38.85% | 519.4872ms | 376.1108ms |
| bulk_scalar | 142.0869ms | 94.7376ms | 33.32% | 244.3429ms | 200.3199ms |

| Exact 1,430 short ranges per route, Client + original adapter | stdlib | Native trial |
|---|---:|---:|
| Median | 0.5213ms | 0.4576ms |
| p95 | 0.7097ms | 0.5949ms |
| p99 | 0.8787ms | 0.7765ms |

The fresh default control is not a subtraction from previous report timings.
The native parser and itemgetter have not been combined end-to-end: do not add
their isolated savings or claim that combination as an achieved Client speedup.

Memory uses the prior independent coordinator: actual Python PID + Rust + any
venv launcher, sampled every 5ms through warm-up, read, caller mapping, validation
and request cleanup until result publication. Coordinator/explicit Client
shutdown are excluded. OS per-process high-water counters include startup and
are never added as simultaneous peaks. Concurrent sampled maxima are lower
bounds; RSS can double-count shared pages and private commit is committed process
memory, not resident physical RAM. Each table entry is a median of ten run peaks.

| Bulk peak memory (MiB) | Kline stdlib | Native trial | Scalar stdlib | Native trial |
|---|---:|---:|---:|---:|
| Rust worker OS peak working set | 79.21 | 79.20 | 39.57 | 39.56 |
| Python OS peak working set | 334.76 | 421.55 | 193.08 | 213.22 |
| Concurrent tree RSS, sampled | 347.02 | 429.55 | 203.75 | 222.34 |
| Concurrent tree private commit, sampled | 326.70 | 896.30 | 183.44 | 366.87 |

The worker is unchanged; the increase is on the Python side. This workload's
observed peak does not establish a leak, but it does rule out treating this
candidate as an unconditional memory/performance win. In particular, the large
private-commit increase matters when several research processes run together.

## Compatibility and failure evidence

Recursive type/order checks pass on the canonical frames and on Unicode, escaped
strings, null/empty containers, bool, signed-i64 endpoints, unsigned-u64 maximum
and integers beyond 2^53. Existing Python tests with the measurement-only native
loads patch run on Windows: 31 PASS / 1 SKIP (the full release archive manifest
check). They include real-child timeout, blocked writes/partial reads, queue wait,
concurrent close, malformed frames, worker death, no replay/restart, OS lock
release, external-write freshness, partial-month recovery and sealed writes.
Passing these fixtures is not general parser semantic equivalence.

The explicit compatibility matrix finds important differences:

- `18446744073709551616` and `-9223372036854775809` become floats in the candidate,
  while stdlib returns exact ints. No such values occur in valid engine i64/u64
  response fields, but a general replacement cannot silently claim equivalence.
- NaN, lone surrogate escapes and UTF-16-BOM input are accepted by stdlib and
  rejected by the candidate. Truncated/invalid UTF-8 fails under both.

The [pinned official deserialize documentation](https://github.com/ijl/orjson/blob/3.13.0/README.md#deserialize)
describes direct bytearray input, stricter UTF-8/nonfinite handling, GIL retention
and ValueError-compatible decode errors. Local compatibility outcomes are exported;
performance claims here come from these measurements, not vendor benchmarks.

The existing IO timeout excludes payload encoding and response parsing. A faster
decoder must stay in the caller after frame receipt. It must not move decoding
into the IO thread or change the deadline into a new end-to-end timeout. GIL-held
native parsing also does not create a new promise of mid-parse cancellation.

## Proposed sequence for review

1. **External caller adapter first, no runtime dependency.** Replace per-record
   generator field lookup with a prebuilt `operator.itemgetter`, keeping Client's
   dict/list/int result untouched. Put this in the benchmark/research adapter or
   its example, not inside FastK or a new read-as-tuples API. Retain or release
   the original dictionaries according to the existing caller's ownership.
   If the caller can directly use records, omit its extra tuple allocation.

   ```python
   from operator import itemgetter

   row = itemgetter("ts", "open", "high", "low", "close", "volume")
   rows = list(map(row, result["records"]))
   ```

   The consumed controls establish substantial mapping/release improvement;
   re-run the real research adapter plus 1,430 short queries before claiming an
   end-to-end deployed improvement. Do not alter the Client's return objects.

2. **A private decoder boundary, with a lower-memory candidate gate.** Keep one
   small `_decode_session_response(body)` helper at the Python adapter boundary;
   the existing envelope validation, serial lock, deadline and failure cleanup
   remain its caller. Preserve stdlib as default for now. Compare a lower-scratch
   native candidate against the same frozen workload before choosing a dependency.
   The measured orjson trial is a speed reference, not the recommended default.

   Proposed default-adoption gates: at least 20% bulk API median improvement on
   both shapes; no >5% short median/p95 regression; no >10% sampled tree RSS or
   >15% private-commit increase; identical built-in types/field order/i64/u64,
   all public commands/errors and failure tests on Windows and Linux. These are
   review targets, not results asserted by CI. The present candidate fails the
   memory gate. A native error must follow existing poison/reap behavior, with
   no second worker, request retry, one-shot fallback or post-failure decoder
   switching. Request dumps and CLI pretty output remain unchanged.

   Resolve out-of-range-number and malformed-input policy explicitly before
   integration. Do not merge a bare `json.loads = native.loads` swap or append a
   full Python validation walk whose cost has not been measured. If exact general
   stdlib input semantics are required, this candidate is unsuitable as-is.
   An optional acceleration extra could keep the default dependency-free package;
   choose its decoder once before requests, never in response to a failed call.
   Package/compiler-free offline installation is part of the future gate.

3. **Treat frame lifetime as a separate memory change.** The IO loop still owns
   the last Exchange until the next request or close, retaining a 54.2MB/18.8MB
   body after bulk parsing. Clear the response only after the caller finishes
   decoding/validation, using a finally ownership path; test concurrent close,
   queued timeouts and decode/allocation failure. This can target idle retained
   bytes; it does not eliminate the peak while constructing returned dictionaries
   and no latency win is established here. Measure it separately rather than
   mixing it with a parser change.

Global GC retuning, alternate public result types, a second storage API and
in-engine tuple mapping are outside this proposal. The observed parser GC pauses
are a small part of total parsing time. Keeping the engine boundary clean means
improving its adapter and research caller where this evidence places the cost.

## Reproduce and inspect

Windows 11 / Python3.12.14 / Ryzen9 9900X. OS cache/scheduling are uncontrolled;
ten bulk/control observations per group support descriptive means/medians only.
No concurrent build/install/packaging ran during authoritative timings. Linux
native decoder installation/failure behavior has not been tested in this study.
The existing delivered Windows/Linux compact builds remain unchanged.

```powershell
python -m pip install -r external-tests/requirements-json-probe.lock
$env:PYTHONPATH = (Resolve-Path clients/python/src).Path
python external-tests/prepare_session_json_probe.py --output target/python-probe-new
$env:CARGO_TARGET_DIR = "$PWD/target/python-probe-build-new"
$env:RUSTFLAGS = "-C target-feature=+crt-static"
cargo build --locked --release --manifest-path target/python-probe-new/Cargo.toml --example fastk_bridge
python external-tests/measure_compact_python.py --bridge target/release/examples/fastk_bridge.exe --probe target/python-probe-build-new/release/examples/fastk_bridge.exe --probe-source target/python-probe-new --store target/external-standard-evidence/fastk --queries docs/evidence/2026-10-20/compact-python/queries.json --output target/python-cost-new
python external-tests/benchmark_buffers.py --baseline-client clients/python/src --baseline-bridge target/release/examples/fastk_bridge.exe --client clients/python/src --bridge target/release/examples/fastk_bridge.exe --store target/external-standard-evidence/fastk --bulk-queries docs/evidence/2026-10-20/compact-python/queries.json --prior-results docs/evidence/2026-10-20/standard/raw --experimental-orjson --output target/python-native-new
```

Use a new output directory for each command. Probe source generation is private;
do not replace the production worker with the diagnostic executable. Same-source
native controls assert worker/client hashes match across routes.

[Evidence](evidence/2026-10-20/compact-python/manifest.json) includes stage/raw/control
samples, native fresh-process jobs/PIDs/memory, compatibility matrix, query hashes,
source/probe provenance, test logs and SHA256SUMS. Large raw response frames stay
under target and are identified by byte count/hash; the original prepared data
and frozen query manifests reproduce them. No new dist package was created.
