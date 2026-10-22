# External checks

Acceptance comparisons exercise installed artifacts and public APIs. Private
transport diagnostics are explicitly isolated in generated copies or measurement
processes; they do not modify production source or installed packages. No tool
imports `store_core`. Store fixtures live in temporary or
explicit new output directories; large data, downloads and results are not core
dependencies or packaged crate contents.

- `test_client.py`: transport failures, timeout/reaping, integer fidelity and optional bridge integration.
- `test_session.py`: real-child fault injection, blocked pipes, deadlines, cleanup, no replay, OS lock release, external-writer freshness and partial-write recovery.
- `benchmark_session.py`: exact five-round range replay against a preserved baseline, new one-shot binary and scoped worker; validates canonical rows, metadata and CLI bytes and reports startup separately. See `docs/SESSION_REUSE_RESULTS.md`.
- `benchmark_buffers.py`: paired preserved/new scoped clients in fresh Python/Rust processes, same bulk and 1,430 short queries, independent process RSS/private-commit sampling with actual Python PID reporting for Windows venv launchers. Optional `--experimental-orjson` patches only measurement children and requires identical source/binary on both routes; it does not adopt a production decoder.
- `verify_session_bytes.py`: exact session bytes, or `--compact-session` to verify identical values/field order/integer encoding and exact compact output against the earlier pretty frames; errors and three public one-shot CLI byte pairs remain identical. Includes bulk and successful continuation after an application error.
- `smoke_installed.py`: copied outside the checkout by `scripts/smoke_install.py`, checks offline wheel usage without Cargo.
- `run_benchmarks.py`: five in-process adapters, common packed NumPy int64 results, canonical input checksums, randomized rounds, full result hashes, actual-worker RSS/private memory and maintenance costs.
- `test_native.py`: installed native layout/precision, predicates, lifetimes, ownership, parallel reads, close races and truncated data.
- `test_month_consumer.py`: parallel month prefetch with chronologically carried position state.
- `backtest/`: a storage-independent RSI/SMA/momentum backtest, monthly and bulk lifecycles, five storage adapters plus a resident-data control; exact trade/equity equivalence, randomized isolated-process timing and native SVG reports. See `docs/BACKTEST_RESULTS.md`.
- `test_native_compat.py`: optional preserved v0.1 bridge reads v0.2 native writes (`FASTK_BASELINE_BRIDGE_BIN`).
- `run_read_matrix.py`: internal thread budgets 1/2/4/8 and separate outer concurrency; FastK outer workers share one native Reader, other adapters use thread-owned connections, all with one internal thread; same 1,430 short queries and frozen bulk cases. Earlier matrix evidence used one FastK handle per worker and remains unchanged.
- `test_matrix_lifecycle.py`: shared-reader ownership, worker-owned SQL connections, failure barrier abort, close-once and no query replay.
- `native_handoff/`: paired before/after native wheels, actual extension SHA checks, the same four bulk cases and 1,430 short queries, process-level distributions and RSS/private peaks; preserves initial warnings alongside fixed supplemental runs.
- `compare_rust_reads.py`: identical public Rust probe against two explicit source trees, paired rounds, canonical validation, independent 5% median regression gate. `rust-read-bench/` supplies the probe source; no engine test hook is needed.
- `compare_python_delivery.py`: separate historical compact session versus native delivery, both returning the same NumPy batches. It never enters the five-backend ranking.
- `requirements-native.lock`: pins NumPy and Maturin on top of the benchmark/dev lock; the subprocess client remains independent.
- `requirements.lock`: exact isolated benchmark/schema dependency versions; Python client itself has none.
- `test_contracts.py`: real bridge response schemas and packaged artifact hashes.
- `requirements-dev.lock`: adds the pinned formatter/linter to the isolated tooling environment.
- `probe_bulk_output.py`: repeated scalar JSON transfer measurements with output hashes; no engine internals.
- `prepare_bridge_cost_probe.py` and `measure_bridge_cost.py`: Windows parent/child timeline measurement in a private instrumented source copy; production APIs stay unchanged by the diagnostic. See `docs/PYTHON_BRIDGE_COST.md` for the historical cost decomposition.
- `prepare_session_json_probe.py` and `measure_session_json.py`: observational reused-worker JSON cost breakdown, exact frame/canonical validation, cached compact/UTF-8/tuple controls. Generated instrumentation never enters a wheel.
- `measure_session_receiver.py`: real-pipe cached-response controls for the current receiver, an in-memory readinto experiment and compact bytes. Its helper under `fixtures/` does not open the engine. See `docs/SESSION_JSON_COST.md` for measured limits and the proposal.
- `measure_compact_python.py`: Windows-only paired compact-worker observation, explicit encoding/text/JSON/object stages, retained/consumed caller mapping, record destruction and nested GC controls. Uses the private generated probe; see `docs/COMPACT_PYTHON_COST.md`. Its optional `requirements-json-probe.lock` pins the native candidate for diagnostics, separate from default client/test dependencies.

See `docs/EXTERNAL_VALIDATION.md` for runnable commands, data profiles and timing
boundaries. Small CI runs validate correctness; no CI test asserts a fragile
latency threshold. Large default runs are explicit, potentially expensive tasks.

Native Rust acceptance runners live separately under `examples/benchmarks/`.
Their cache labels do not establish an OS cold cache, and their timings cannot
be presented as Python client latency.
