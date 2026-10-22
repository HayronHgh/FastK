# Request-scoped reads

The primary experiment implements `open -> query -> close` for **every** query.
Python and imported libraries remain resident. Returned arrays are checked after
the adapter and its handles close. There is no background Reader or global file
cache kept by this harness. Each worker verifies the actual FastK extension SHA.

- `cycle-short`: all original 1,430 Kline ranges, once per fresh worker.
- `cycle-bulk`: the original four cases, five repetitions, new handle each time.
- `hot-local`: 32 warmups immediately before each measured query. This isolates
  an actual hot file hit, unlike warming an entire working set larger than a cache.
- `hot-replay`: warm all queries, then replay them in a fixed randomized order.
  FastK retains its production 64-file budget. An isolated Arrow method copy
  changes only its mmap cap to 256, covering all 240 requested month files.
- `catalog-short`: Arrow/Parquet also reload their external JSON file catalog
  inside every open. Ordinary cycles receive a caller-resident catalog and build
  the adapter's partition lists during open. This control measures our catalog
  implementation, not an unavoidable property of either file format.

All paths deliver the same packed, readonly NumPy int64 result. Canonical hashes,
full byte validation, and output destruction are outside the timers. Cycle close
includes both `adapter.close()` and adapter destruction, so request-owned state
does not leak into the next request. On failure, the worker closes the current
handle and aborts; it never retries a failed read.

The sum of each sample's open/read/close equals its total. Independently computed
phase medians need not sum to the median total. The first sample in a new worker
also permits a separate first-request view; imports are still outside that timer.
Neither a new worker nor a new handle establishes an OS cold cache.

From the repository root, with its prepared stores and isolated environment:

```powershell
$env:PYTHONUTF8="1"
$env:PYTHONPATH=(Resolve-Path external-tests).Path
$env:TEMP=(Resolve-Path target/lifecycle-temp).Path
$env:TMP=$env:TEMP
target/v02-venv/Scripts/python.exe -m read_lifecycle.run `
  --before path/to/preserved-before.whl `
  --after path/to/current.whl `
  --output target/new-lifecycle-results --rounds 3
target/v02-venv/Scripts/python.exe -m unittest read_lifecycle.test_worker
```

Output directories must be new. Adapters and process memory monitoring are shared
with the existing public benchmark. No instrumentation is added to the engine or
native API. See `docs/LAZY_READ_RESULTS.md` for measured results and limitations.
