# Python bridge cost and optimization proposal

The short-read bottleneck is Windows process startup, while large responses
are dominated by JSON, pipe transfer and Python object conversion. Optimize the
transport lifetime first; changing the storage format or adding more engine
interfaces would not address the main short-query cost.

This report records the measurement-only investigation before implementation.
Its frozen source, executable hashes and evidence remain unchanged. The scoped
worker proposed below is now implemented; see [session reuse results](SESSION_REUSE_RESULTS.md)
for the separate replay, failure checks and deployment artifacts. Bulk JSON
optimization remains a later step.
The follow-up [large JSON study](SESSION_JSON_COST.md) now measures that remaining
cost after worker reuse and proposes buffer/serialization changes for review.

## Matching the original result

The historical range median was **24.4209ms**. The full replay uses the exact
same 1,430 range requests from all five recorded rounds, the same prepared store
and the same production executable (SHA-256 begins `543d5e41f92b153c`). Returned
fields are checked against the original canonical hashes.

| Measurement | Samples | Median | p95 |
|---|---:|---:|---:|
| Historical production range | 1,430 | 24.4209ms | 29.6183ms |
| Current production replay | 1,430 | 25.5967ms | 31.1322ms |
| Paired instrumented replay | 1,430 | 25.8746ms | 31.5660ms |

The current production median is 4.8% above the historical measurement. Cache,
scheduling and host activity were uncontrolled; the historical number is not
retrospectively assigned invented component timings. Attribution uses the
current paired replay. Instrumentation adds 0.2779ms to the median and 0.1346ms
to the mean. All 1,430 paired response byte hashes match exactly.

## Short-query breakdown

These are **means from the same instrumented samples**, so the rows sum to the
26.4629ms mean. Component medians would not add up to the overall median.

| Wall-time component | Mean | Share |
|---|---:|---:|
| Before `Popen` until entering Rust `main` | 23.1511ms | 87.48% |
| Store open, metadata, range read, response mapping and close | 0.7923ms | 2.99% |
| Rust JSON encoding and buffering | 0.0191ms | 0.07% |
| Child pipe writes / flush | 0.0560ms | 0.21% |
| Python `json.loads` | 0.1666ms | 0.63% |
| Benchmark adapter dict-to-tuple conversion | 0.0657ms | 0.25% |
| End of Rust handler to Python receiving/reaping completion | 1.9237ms | 7.27% |
| Other argument handling, instrumentation and parent bookkeeping | 0.2885ms | 1.09% |
| **Total** | **26.4629ms** | **100%** |

The startup row starts immediately before `Popen`; parent argument construction
is in the final bookkeeping row. It includes process creation, image/runtime
startup and scheduling until `main`. `Popen` returning is an overlapping marker,
not another additive stage. No attribution to antivirus or a specific Windows
subsystem has been established.

Within the engine row, median store opening is 0.1390ms, metadata loading
0.1832ms and `get_kline_range` 0.1094ms (p95 0.4616ms). The read includes chunk
location and record decoding; it is not a measurement of physical disk latency.
Even eliminating all engine work would leave the main startup cost.

A diagnostic process doing no engine work has a 23.1677ms median. Production
`version --json` takes 23.0888ms. Replaying an already-encoded small response
takes 23.2709ms, including Python decoding. These controls support the startup
attribution without subtracting unrelated medians to invent JSON cost.

Reusing one Rust read-only store gives a 0.0312ms median over 7,150 reads.
This is an engine/cache lower bound, not an achievable Python latency promise:
it omits per-call ownership/opening, transport and Python conversion, and the
long-lived shared handle blocks a writer until dropped.

## Large-response breakdown

The separate size study has five samples per binary/size and reports descriptive
medians, without tail claims. The 437,115-row kline response contains 79,555,130
bytes; original and instrumented output bytes match.

| Component | Instrumented bulk median |
|---|---:|
| Engine open + metadata + read + mapping + close | 5.3878ms |
| Rust JSON encoding / buffering | 43.3417ms |
| Child pipe writes / flush | 140.1009ms |
| Python JSON decoding | 420.7069ms |
| Benchmark dict-to-tuple conversion / object release | 222.9713ms |
| Full instrumented operation | 850.9378ms |
| Full production operation | 857.1563ms |

These medians are not additive. Cached-byte decoding without any child process
still takes 407.6936ms, confirming that the bulk JSON cost is independent of
startup. Pipe-write time includes blocking/backpressure while Python receives
the bytes; it is not raw memory bandwidth. Receive work can overlap child work,
so it must not be added again as a second complete transfer stage.

The tuple conversion belongs to the external benchmark adapter, not the public
`Client` API, which already returns record dictionaries. Optimizing Client does
not automatically remove a caller's own conversion/computation costs.

## Recommended implementation sequence

### 1. Use existing bulk calls immediately

Load the required window once with `read_kline_range`, then slice or compute in
the caller. Avoid row-by-row or overlapping small-window RPC loops. This needs
no new FastK method or format. It amortizes startup but does not fix calls across
many independent symbols, nor eliminate large JSON materialization.

### 2. Add one explicitly scoped local process session

Recommended first implementation: keep all existing Client methods and return
objects; opt into reuse through a context manager:

```python
with Client(root) as store:
    first = store.read_kline_range("BTCUSDT", "1m", start_a, end_a)
    second = store.read_kline_range("BTCUSDT", "1m", start_b, end_b)
```

Calls outside a context retain the current one-process-per-operation behavior.
The existing constructor/version check remains compatible; any additional
session startup/handshake is measured as initialization, not hidden in warm
query results. Context exit closes and reaps its worker; explicit `close()` is
idempotent. No network endpoint, daemon installation or global worker pool.

The private transport owns framed request/response IO and one in-flight request
per Client. Existing JSON payloads, signed integer values, coarse error codes
and method signatures stay unchanged. A small dispatcher in the CLI adapter
calls existing bridge/library functions; no generic engine/backend trait is
added to `FastKStore`.

The worker **opens and releases the store for each request**. Do not retain a
read lock or assume an old manifest is current between requests. Existing busy,
recovery, append and snapshot rules remain authoritative. This intentionally
targets startup rather than taking the unsafe shortcut of an indefinitely
cached read-only handle.

Keep per-request timeouts: terminate and reap the worker on timeout or protocol
failure, report the failure, and never automatically replay the interrupted
operation. A write can have committed months before failure. Each explicit
subsequent operation must have documented restart behavior; no silent retry of
the failed operation. Concurrent calls on one session are serialized with a
bounded deadline; separate Clients can remain independent.

Proposed acceptance gates, not measured outcomes:

- At least 1,000 representative requests: steady-state median <=3ms and p95
  <=10ms on this workload/host; record initialization and first-result time too.
- Replay the same five-round cases and verify every field, scale and error.
- Kill the worker, stall a response, truncate a frame, fill stderr, and fail
  writes mid-month: no leaked process/thread, deadlock or automatic write replay.
- Alternate an external writer and session reads: no stale metadata, accidental
  write blocking between calls, recovery race or sealed-root mutation.
- Preserve one-shot CLI output and all existing Client behavior outside context.

The target is plausible from the measured startup floor; it requires a prototype
and must not be advertised as an achieved speedup.

### 3. Treat bulk JSON as a separate optimization

After session reuse, measure compact JSON on the private worker channel while
keeping the one-shot CLI's existing output. This can reduce bytes and encoding
work, but Python dictionaries and caller conversion remain. Re-run the full
research workload: saving about 23ms cannot by itself transform an 850ms bulk
call or guarantee a faster end-to-end research run.

Do not add `read_fast`, `read_raw`, alternate result shapes or record types just
to improve a benchmark. A columnar/array path deserves a separate use case and
API decision if bulk conversion still dominates.

PyO3 is a possible later adapter, isolated from engine dependencies. It could
remove both process startup and JSON, but adds platform wheel/ABI work, Python
object conversion and a different cancellation model. A native call cannot
inherit the existing hard timeout by killing a child. Silently substituting it
under current `Client(timeout=...)` would change semantics, so it is not the
recommended first step.

## Reproduction and evidence

| Check | Command / evidence | Exit | Result |
|---|---|---:|---|
| Private release probe | `cargo build --locked --release --manifest-path target/bridge-cost-source/Cargo.toml --example fastk_bridge` | 0 | PASS |
| Full replay | `measure_bridge_cost.py --range-cases 0 --short-only` with the paths below; 1,430 byte-identical pairs | 0 | PASS |
| Size/decode study | Same tool without those two options; canonical fields and replay bytes checked | 0 | PASS |
| Diagnostic source generation | Regenerated private CLI, library instrumentation and manifest match byte-for-byte | 0 | PASS |
| Python tools | Ruff check and format check | 0 | PASS |
| Evidence / production integrity | 17 evidence file hashes; production source and executable hashes unchanged | 0 | PASS |
| Scoped session implementation | Separate follow-up: [results](SESSION_REUSE_RESULTS.md) | — | See follow-up |

Use the pinned external-test environment, the existing verified standard input
and prepared pre-maintenance store. No engine data is modified:

```powershell
python external-tests/prepare_bridge_cost_probe.py --output target/bridge-cost-source
$env:CARGO_TARGET_DIR = "$PWD/target/bridge-cost-build"
$env:RUSTFLAGS = '-C target-feature=+crt-static'
cargo build --locked --release --manifest-path target/bridge-cost-source/Cargo.toml --example fastk_bridge
python external-tests/measure_bridge_cost.py --bridge target/release/examples/fastk_bridge.exe --probe target/bridge-cost-build/release/examples/fastk_bridge.exe --store target/external-standard-evidence/fastk --prior-results target/external-standard-buffered --input target/external-standard-final/input --output target/bridge-cost-full-replay --range-cases 0 --short-only
```

Omit the last two options for the smaller range sample plus size/decode study;
choose a new output directory each time. The diagnostic copy is private and is
never bundled into a production wheel or added to the public command contract.
Windows QPC (10MHz) provides the shared parent/child clock; each instrumented
sample's wall timeline closes exactly. Rust internal stages use `Instant`. Output validation
and hashing are outside timed operations.

See [evidence](evidence/2026-10-20/bridge-cost/manifest.json) for both summaries,
compressed raw samples, source snapshots, instrumentation diff and checksums.
The host is the same Windows 11 / Python 3.12.14 / Ryzen 9 9900X environment;
OS cache and background scheduling are uncontrolled. Production read/bridge
sources and the measured executable were unchanged during that investigation;
the follow-up records its own source and executable hashes separately.
