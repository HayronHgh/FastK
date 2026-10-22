# FastK Python client

Use a context to reuse one local bridge process across calls:

```python
from fastk_client import Client

with Client("./research-store") as store:
    first = store.read_kline_range("BTCUSDT", "1m", start_a, end_a)
    second = store.read_kline_range("BTCUSDT", "1m", start_b, end_b)
```

Import `Client` as shown below. Existing methods and return dictionaries are
unchanged. Outside a context each operation still starts a process. Context exit
or `close()` permanently closes that Client; create another Client to resume.
Timeout or a broken worker ends the session without replay or automatic restart.
A timeout while only waiting for another call does not interrupt that call.
Store ownership is acquired and released per request, so an idle session does
not retain a read lock. Session frames have a 1GiB limit; one-shot behavior is
unchanged. See `docs/SESSION_TRANSPORT.md` in the source/release documentation.

Python 3.11+; Windows x64 and Linux x64 platform wheels include `fastk_bridge`.
No Rust, Cargo, pandas, NumPy, network service or source checkout is needed.

```sh
python -m pip install /path/to/fastk_client-0.1.0rc1-py3-none-<platform>.whl
```

```python
from fastk_client import Client

store = Client("./research-store")
store.write_scalar_range(
    "BTCUSDT",
    "1m",
    "feature",
    "example",
    [{"ts": 1706745600000, "value": 9007199254740993}],
    timeframe_ms=60000,
)
print(
    store.read_scalar_range("BTCUSDT", "1m", "feature", "example", 1706745600000, 1706745600000)[
        "records"
    ]
)
```

When developing from source, install this directory and explicitly set
`Client(root, binary="/absolute/path/to/fastk_bridge")`. The client never runs
Cargo or searches PATH. Explicit binaries override bundled ones. Responses keep
integer values and scale metadata unchanged.

Writes append strictly later timestamps; overlapping writes are rejected. One
month is the commit unit. A multi-month failure or timeout can leave committed
months: inspect inventory before retrying. No automatic write retries occur.
`FastKError.code` is machine-readable; `FastKTimeout` and `FastKProtocolError`
cover transport failures. `timeout=` is a positive finite number of seconds.

For old stores without a persistent lock, run `Client(root).init()` once before
reading. Reads otherwise do not repair or migrate files. Use the admin CLI for
explicit recovery. `seal_snapshot()` freezes all series bytes and returns a
digest; pin it and use `verify_snapshot(digest)` to detect replacement/tampering.
Keep registries outside sealed data roots. Snapshot hashing is explicit and is
not included in query latency measurements.
