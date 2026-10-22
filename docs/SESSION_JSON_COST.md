# Large JSON after worker reuse: measurements and proposal

This is the pre-implementation study of the `b1d34929...` scoped-worker build.
The later [buffer implementation results](BUFFER_REUSE_RESULTS.md) execute the
readinto/single-Vec changes against that preserved build. Compact JSON remains
deferred. Historical measurements and their evidence below are unchanged;
reproducing them requires the matching saved sources/binary from the earlier
session-reuse wheel, rather than substituting the newer receiver.

The remaining bulk bottleneck is response handling: Python JSON parsing/object
creation, receiver allocation/copies, Rust JSON buffering and optional caller
conversion. Worker reuse solved repeated startup; it does not remove these costs.
The strongest low-dependency next step is a preallocated receiver, followed by
one Rust response buffer and compact JSON on the private session channel.

This change adds observational tools, isolated controls and a proposal only.
Production engine/CLI/client sources, timeout/no-replay/ownership semantics,
public payloads, executables, wheels and earlier evidence remain unchanged.
The experimental receiver exists only as an in-memory patch in a control tool.
No new release package or deployed speedup is claimed.

## Unchanged current Client

Each of two workers lives for the entire main experiment: the shipped bridge
and an isolated instrumented copy. Constructor/version/handshake are excluded.
Ten rounds shuffle four query shapes across both workers. Canonical input is
the same standard Arrow dataset and prepared pre-maintenance store used before.
Every timed result is checked against independent full-row hashes and expected
metadata. Four initial fixture pairs also have identical complete frame bytes.

The host is Windows 11, Python 3.12.14, Ryzen 9 9900X. OS cache/background
scheduling are uncontrolled. There are ten observations per route/shape, so
these are descriptive medians and means, with no p95/p99 claims. Small reads
are mixed with large allocations; this is not a replacement for the earlier
1,430-query short-read session benchmark.

| Shape | Rows | Fixture frame bytes | Client API median | Caller tuple conversion/release median | Combined median |
|---|---:|---:|---:|---:|---:|
| small_kline | 127 | 23,342 | 2.013ms | 0.048ms | 2.155ms |
| medium_kline | 4,077 | 742,242 | 5.999ms | 1.502ms | 7.695ms |
| bulk_kline | 437,115 | 79,555,158 | 670.413ms | 211.121ms | 889.597ms |
| bulk_scalar | 437,115 | 30,161,273 | 250.665ms | 139.412ms | 389.501ms |

Component medians are not additive. The Client already returns record dicts;
tuple conversion belongs to the benchmark/research caller, not the storage API.
The combined timer includes ordered dict-to-tuple conversion and releasing the
record dicts, while keeping the resulting tuples alive. Validation/hashing and
later tuple destruction are outside the timer.

## Where the reused-worker time goes

The following are **means from the same ten instrumented samples**, so each
column sums to its own observed combined mean. A shared Windows QPC clock
partitions the wall timeline without counting sender/receiver overlap twice.
The engine row includes request argument handling, opening/metadata/read,
response mapping and ownership release; it is not raw disk latency.
Serialization includes pretty encoding, buffered writes into Vec and its
allocation/growth. Python `json.loads` includes UTF-8 decoding, JSON parsing and
constructing dict/list/int objects; those are not separately timed internals.

| Non-overlapping wall stage | Bulk Kline mean | Share | Bulk Scalar mean | Share |
|---|---:|---:|---:|---:|
| Python request / Rust dispatch entry | 0.689ms | 0.08% | 1.645ms | 0.42% |
| Engine and response creation | 6.793ms | 0.78% | 4.953ms | 1.28% |
| Rust JSON serialization and output buffer | 83.362ms | 9.62% | 30.573ms | 7.90% |
| Rust post-encode cleanup / probe log | 0.854ms | 0.10% | 0.416ms | 0.11% |
| Rust full response envelope copy | 20.828ms | 2.40% | 8.154ms | 2.11% |
| Wire + Python frame collection | 160.370ms | 18.50% | 59.495ms | 15.37% |
| IO thread to decoding handoff | 0.091ms | 0.01% | 0.104ms | 0.03% |
| Python parse and dict/list/int creation | 388.880ms | 44.86% | 143.465ms | 37.06% |
| Python return / buffer release | 0.032ms | 0.00% | 0.031ms | 0.01% |
| Caller tuple conversion / record release | 204.942ms | 23.64% | 138.298ms | 35.72% |
| **Combined** | **866.840ms** | **100%** | **387.133ms** | **100%** |

The wire stage runs from the child starting its frame write to the Python IO
thread finishing frame collection. Child write/flush overlaps Python receiving;
it is a nested diagnostic, not another additive stage. Likewise receiver
append/final-copy timers are nested wall spans (including allocation/scheduling
and timer overhead), not pure memory bandwidth measurements.

| Nested counter, means | Bulk Kline | Bulk Scalar |
|---|---:|---:|
| Child write/flush | 146.450ms | 54.623ms |
| Python bytearray.extend spans | 126.964ms | 47.051ms |
| Final bytearray-to-bytes copy | 10.813ms | 3.842ms |

The Kline receiver uses 1,216 reads including its prefix, Scalar 463. The
existing receiver repeatedly extends a bytearray and then copies it to bytes.
The Rust session separately serializes into one Vec and copies it into an
envelope Vec. These are adapter costs; changing storage records/indexes would
not address them.

The probe has additional per-read timers and stderr logs. It uses a private
byte-bounded timing collector because the normal bounded stderr deque need not
retain an entire fragmented diagnostic line. No diagnostics are put in the
public response. Observed probe-minus-production **mean combined** differences:

- small_kline: -0.227ms.
- medium_kline: 0.511ms.
- bulk_kline: -21.531ms.
- bulk_scalar: -1.446ms.

Negative differences are observed run variation, not evidence that timers make
the engine faster. Stage shares describe the instrumented samples, not an
invented decomposition of the production median. Every instrumented timeline
closes exactly; production and probe are interleaved controls.

## What compact JSON and caller conversion can actually save

Five randomized cached-control rounds per shape parse the same result values,
without a process or pipe. Compact means removing whitespace only; field names,
nesting, integer values and metadata remain identical. All decoded objects are
compared to the canonically verified fixture. Explicit controls also preserve
Unicode, null/empty containers and signed-i64 extremes / values beyond 2^53.

| Shape | Pretty bytes | Compact bytes | Byte reduction | Cached pretty bytes parse median | Compact bytes parse median |
|---|---:|---:|---:|---:|---:|
| bulk_kline | 79,555,158 | 54,202,451 | 31.87% | 405.993ms | 381.656ms |
| bulk_scalar | 30,161,273 | 18,796,230 | 37.68% | 148.672ms | 150.499ms |

Removing whitespace shrinks bytes substantially, but parsing is still expensive:
Kline cached bytes parsing improves only about 6%; Scalar is slightly slower
in this five-sample control. It does not remove dict/list/int construction.
Direct UTF-8 decode medians are 11.603ms/4.209ms for the pretty Kline/Scalar
frames; existing-text parsing is also recorded. These separate control medians
must not be subtracted to assign exact internal parser/allocator CPU costs.

Ordered tuple conversion on already-created, retained record dicts has another
isolated control. Equivalent `list(map(itemgetter(*fields), records))` preserves
the same tuples and field order:

| Shape | Generator-comprehension median | itemgetter median |
|---|---:|---:|
| bulk_kline | 190.555ms | 75.249ms |
| bulk_scalar | 117.670ms | 40.476ms |

Those timers exclude record/tuple destruction and parse time. This is a caller
optimization, not an engine speedup. Do not change Client returns to tuples or
arrays. Any future comparative benchmark must apply the same caller conversion
to every backend and keep the earlier harness/evidence frozen.

## Isolated real-pipe receiver control

A measurement-only cached Python producer streams the same preencoded result
over real OS pipes through the existing Session machinery. It does not perform
engine reads or Rust serialization/envelope allocation. Each configuration has
ten timed observations, each preceded by a warm-up; startup/file loading are
excluded. Jobs shuffle within each round. All full result objects match the
verified fixture; every child is reaped.

The experimental receiver allocates a bytearray of the validated frame length,
fills it using bounded readinto slices and passes it directly to the unchanged
stdlib JSON decoder. It avoids repeated extend and the final full bytes copy.
It is patched into memory only for this control, not installed or shipped.

| Shape | Current receiver, pretty | Preallocated, pretty | Current receiver, compact | Preallocated, compact |
|---|---:|---:|---:|---:|
| bulk_kline | 555.788ms | 419.999ms | 492.425ms | 389.401ms |
| bulk_scalar | 212.800ms | 158.254ms | 181.798ms | 145.024ms |

These totals include parse/object creation; they are not full-engine Client
latencies. Kline's non-parse median falls from 160.149ms to 24.807ms with the
pretty receiver change; Scalar falls from 59.086ms to 10.012ms. Non-parse includes
transport, handoff and cleanup, not raw pipe bandwidth. Compact plus readinto
still leaves roughly 372ms/138ms parsing in this cached producer control.
The result supports prioritizing receiver allocation rather than expecting
whitespace removal alone to solve the remaining bulk cost.

## Proposed implementation, for review

1. **Preallocate the private receiver first.** Keep the current framed protocol,
   validated 1GiB maximum, partial-read/EOF checks and per-request deadline.
   Receive directly into one bytearray and let the existing decoder read it;
   only internal buffer types change. Public methods, dict/list/int results,
   metadata/scales, errors and one-shot CLI bytes stay unchanged. Reuse the
   current IO thread / kill-and-reap cancellation path. No new reader service,
   parser option, endpoint or engine abstraction is needed.
2. **Use one Rust session response buffer.** Write the envelope prefix into the
   response Vec, dispatch/serialize the result into that same Vec and append
   the closing brace. On application error discard any partial success buffer
   and construct the existing error envelope before writing any bytes. This
   removes the separate full result-to-envelope copy without changing dispatch
   ownership or extending read-lock lifetime. Keep one-shot serialization intact.
3. **Compact only the private session JSON.** Isolate pretty/compact selection
   inside the CLI adapter's JSON output helper, using a small private output
   wrapper if necessary. The engine/library response types do not acquire
   transport flags or serializer traits. One-shot CLI remains byte-compatible
   pretty JSON; session values, nesting, keys/order, integers and errors remain
   the same. Keep plain JSON frames; no compression, new binary schema or
   alternate public read methods are needed for this step.
4. **Keep caller conversion separate.** Where a research caller already needs
   tuples, use the measured equivalent itemgetter mapping locally. Client still
   returns dicts. If the remaining parse/object-creation cost is unacceptable,
   evaluate an optional faster decoder in a separate measured decision. Do not
   introduce a runtime dependency or change malformed-response/precision behavior
   without that evidence. Native bindings/array returns would be a different API
   and cancellation decision, outside this proposal.

Suggested acceptance gates (targets, not achieved production improvements):

- Run the same paired full-engine bulk queries for at least ten rounds; target
  >=15% lower public Client API median for both bulk shapes, and record peak RSS.
  Report caller conversion separately. Do not add medians or cached-control
  gains to manufacture an end-to-end improvement.
- Replay the existing 1,430 short queries with full canonical/metadata/type
  checks. Investigate >5% median/p95 regression using paired repeats; keep CI
  focused on correctness rather than hard machine-dependent latency assertions.
- Preserve all current lifecycle/fault/lock tests. Add fragmented prefix/body,
  zero/oversized lengths, truncated readinto, allocation failure, broken worker,
  timeout during a partial body, concurrent close and invalid-JSON tests.
- Verify no automatic request replay/reconnect, per-request lock release and
  external-write freshness. Parsing remains outside the IO timeout, as today;
  do not silently change that boundary in a buffer optimization.
- Keep signed i64, scale metadata, null/empty results, Unicode, snapshot/error
  envelopes and success dict shapes exact. Verify one-shot byte fixtures and
  offline Windows/Linux wheels before shipping any implementation.

The isolated controls make a no-extra-dependency buffer change the most useful
first reviewable patch. They do not establish a new full-research speedup or a
replacement for the earlier comparison against SQLite/DuckDB/Parquet/Arrow.

## Reproduction and evidence

```powershell
$env:PYTHONPATH = (Resolve-Path clients/python/src).Path
$env:RUSTFLAGS = '-C target-feature=+crt-static'
$env:CARGO_TARGET_DIR = "$PWD/target/session-json-current"
cargo build --locked --release --example fastk_bridge
python external-tests/prepare_session_json_probe.py --output target/session-json-probe-new
$env:CARGO_TARGET_DIR = "$PWD/target/session-json-build-new"
cargo build --locked --release --manifest-path target/session-json-probe-new/Cargo.toml --example fastk_bridge
python external-tests/measure_session_json.py --bridge target/session-json-current/release/examples/fastk_bridge.exe --probe target/session-json-build-new/release/examples/fastk_bridge.exe --probe-source target/session-json-probe-new --store target/external-standard-evidence/fastk --input target/external-standard-final/input --output target/session-json-new
python external-tests/measure_session_receiver.py --fixtures target/session-json-new --output target/session-receiver-new
```

Use the existing pinned external-test environment (Arrow oracle) and prepared
standard store. Output directories must be new. The replay tool launches cached
measurement helpers only; it never opens/writes the engine store. Large frame
fixtures remain under target and are not published as documentation evidence.

Executed checks: private release build PASS; 80 timed real-engine responses plus
eight fixture reads verified; four exact frame-byte pairs PASS; 160 isolated
cached parse/UTF-8/tuple controls PASS; 80 timed real-pipe receiver controls plus
80 warm-ups PASS; all workers reaped. Ruff/source/integrity checks and raw data
are recorded with [evidence](evidence/2026-10-20/session-json/manifest.json).
There is no production-code change requiring a new engine test-suite claim.
This performance study is Windows-only; Linux performance was not measured.

