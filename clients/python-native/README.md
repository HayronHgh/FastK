# FastK native Python module

`fastk-native` embeds the Rust engine in CPython. It is independent of the
standard-library-only `fastk-client` subprocess package. Install a wheel matching
your CPython version and platform, plus NumPy 2.3.5 or newer (below 3).

```sh
python -m pip install fastk_native-0.2.0rc1-cp312-cp312-win_amd64.whl
```

```python
import numpy as np
from fastk_native import KLINE_DTYPE, Reader, Writer

rows = np.array([(1706745600000, 100, 110, 90, 105, 200)], dtype=KLINE_DTYPE)
with Writer("market") as writer:
    writer.write_kline_range(
        "BTCUSDT", "1m", rows, timeframe_ms=60000, price_scale=100, volume_scale=1
    )

with Reader("market", threads=1) as reader:
    batch = reader.read_kline_range("BTCUSDT", "1m", 1706745600000, 1706745659999)
    prices = batch["close"]  # direct NumPy field view; no JSON or row dictionaries
    info = reader.kline_info("BTCUSDT", "1m")
```

The returned one-dimensional arrays have packed little-endian signed int64
fields. Kline fields are `ts, open, high, low, close, volume` (48 bytes/row);
Scalar fields are `ts, value` (16 bytes/row). Prices and volume remain fixed-point
integers; scales are returned by `kline_info`. Timestamps are UTC epoch
milliseconds. Results are ascending, inclusive at both range boundaries,
read-only, and own their memory independently of the reader. Empty results keep
the same dtype. Field views have the record stride; NumPy can use them directly.
Kline range/latest and Scalar range transfer the engine's original allocation
through a checked ownership cast, without another record-to-word buffer. Predicate
results still validate optional values and build their output. Disk IO still copies
data into the engine buffer.

Input must be a one-dimensional NumPy array with exactly `KLINE_DTYPE` or
`SCALAR_DTYPE`, in strictly increasing timestamp order. Noncontiguous input is
copied. Input is copied into Rust before releasing the GIL; floating point or
unsigned input is rejected. Integers outside int64 are rejected rather than
truncated. The on-disk format is unchanged.

Initialize a root using `Writer` or the CLI before opening `Reader`. Missing or
uninitialized roots are rejected, so a reader cannot silently attach later
without a lease. `Reader` holds a shared OS lease until close; other readers can coexist. Writers
fail with `STORE_BUSY` while that lease exists. `Writer` serializes its own calls
and holds exclusive OS ownership during each complete range operation. Each UTC
month commits separately. A failure reports completed months, may require
explicit recovery, and is never automatically replayed. Close is idempotent and
waits for active calls; arrays do not retain the store lease.

Rust IO runs with the GIL released. `threads=1` is the default. Larger values
create a bounded pool for that reader; reads estimated below 4 MiB stay serial,
and larger reads fill disjoint regions of one result buffer in roughly 1 MiB
tasks. Concurrent callers may share a reader. Keep internal threads at one when
running many independent queries, then measure before increasing the budget.

Native calls are synchronous and have **no hard timeout** or automatic fallback.
Use `fastk-client` when process isolation and kill/reap deadlines are required.
The CLI retains its pretty JSON and the existing client retains its dictionary
return format. Native errors are `FastKError` with an engine `code` attribute;
Python shape/type errors are `ValueError`, `TypeError`, or `OverflowError`.

Build locally with Rust and CPython headers:

```sh
python -m pip install maturin==1.15.0 numpy==2.3.5
python -m maturin build --manifest-path clients/python-native/Cargo.toml --locked --profile native
```

The `native` profile uses panic unwinding at the Python boundary. CLI binaries
keep the release profile. This is a development release for technical evaluation;
the measured adoption limits and verification evidence are in the repository.
