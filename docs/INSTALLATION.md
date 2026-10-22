# Install and integrate

Release targets are Windows x64 (MSVC or GNU) and Linux x64 static musl.
Local Windows validation used GNU and OS-provided UCRT; no MinGW DLLs are
required. The authored Windows artifact CI targets MSVC, but was not run here.
Python client wheels target Python 3.11+. This is a local
technical/research release, not a hosted or enterprise storage service.

## Python: same-process NumPy module

Install `fastk-native` using a wheel matching the exact CPython ABI and platform,
plus NumPy. It embeds Rust and needs neither a CLI nor a compiler at runtime.
See the [native quickstart](../clients/python-native/README.md) for reads/writes,
layouts and ownership. `Reader(..., threads=1)` is the default. Arrays survive
close; read-only handles exclude writers until closed. Native calls have no hard
timeout. The independent subprocess client below retains its deadlines.

```sh
python -m pip install numpy==2.3.5 /path/to/fastk_native-0.2.0rc1-cp312-cp312-win_amd64.whl
```

Linux native wheels use glibc tags determined by the actual build. Do not relabel
them as static musl or universal wheels. CPython 3.11 and 3.12 wheels are separate.
Offline installation requires the matching NumPy wheel supplied with
`--no-index --find-links /path/to/wheelhouse`.

## Python: dependency-free subprocess client

Obtain the wheel matching your platform from a built release package:

```sh
python -m pip install /path/to/fastk_client-0.2.0rc1-py3-none-win_amd64.whl
# Linux:
python -m pip install /path/to/fastk_client-0.2.0rc1-py3-none-manylinux_2_17_x86_64.whl
```

The wheel includes a standalone bridge executable. Nothing is downloaded or
compiled on first use. The client has no third-party runtime dependencies.

```python
from fastk_client import Client

store = Client("./research-store")
store.write_kline_range("BTCUSDT", "1m", [{
    "ts": 1706745600000, "open": 10000000, "high": 10020000,
    "low": 9980000, "close": 10010000, "volume": 10000,
}], timeframe_ms=60000, price_scale=100000, volume_scale=100000)
rows = store.read_kline_range("BTCUSDT", "1m", 1706745600000, 1706745600000)
print(rows["records"])
```

For a separately unpacked bridge, use `Client(root, binary="/absolute/path/to/fastk_bridge")`.
Explicit paths override the bundle; no system PATH search or Cargo fallback is
performed. Values remain integers, and callers interpret scale metadata.

Use the existing command names as client method names (hyphens become
underscores). Inventory, generic scalar writes/reads, scalar predicate queries,
indicator compatibility calls and `read_kline_latest` are supported.

For repeated calls, use `with Client(root) as store` to reuse a worker. Methods
and return objects are unchanged; store locks are released after every request.
Context exit closes the Client. A timeout or broken session is not retried or
restarted automatically. See [Session Transport](SESSION_TRANSPORT.md) for
failure, concurrency and frame-size semantics.
The [executed session replay](SESSION_REUSE_RESULTS.md) reports initialization,
steady-state latency and offline Windows/Linux wheel checks separately.

## Standalone CLI and Rust

The release archive contains `bin/fastk_bridge`, `bin/fastk_admin`, the crate,
schemas, documentation, wheel, release manifest and SHA256SUMS. Verify checksums
before using an artifact. Use `fastk_bridge version --json` to check contract v1.
The optional `validation/` directory contains the standalone public-API test and
benchmark harness plus dependency locks; it does not contain engine internals
or market datasets. It can be run from the unpacked archive without a checkout.
The bridge's default successful output is still the original JSON object.
Optional `--error-format=json` produces a structured error on stderr.

Rust callers can continue to use `FastKStore`. Use `open_read_only` for shared
read ownership; drop that handle before a writer or maintenance operation.
Rust 1.89+ is required for standard-library file locks. No background process is
started by either the library or bridge.

## Build without publishing

```sh
python scripts/package_release.py --development
```

This builds local artifacts explicitly labelled `development: true`. Formal
artifacts omit the flag and require a clean Git checkout. Output directories
must not already exist. No script commits, uploads to PyPI/crates.io, or publishes
a GitHub release. The artifact workflow builds and uploads CI artifacts only.
Official Linux builds specify `--target x86_64-unknown-linux-musl`; install that
Rust target and musl compiler tools first. Python 3.11+ builds both platforms.

To test installation from an empty directory, with Rust removed from PATH:

```sh
python scripts/smoke_install.py --wheel /path/to/client.whl --admin /path/to/fastk_admin
```

Build the native module separately:

```sh
python -m pip install -r external-tests/requirements-native.lock
python scripts/build_native_wheel.py --output dist/native
python scripts/smoke_native_install.py --wheel /path/to/native.whl --wheelhouse /path/to/numpy-wheels
```

## Troubleshooting and upgrades

| Symptom | Action |
|---|---|
| `EXECUTABLE_NOT_FOUND` | Install the platform wheel or supply an absolute bridge path |
| `PROTOCOL` / contract mismatch | Use a bridge advertising contract version 1; inspect stderr for crashes |
| Store busy | Close shared readers or wait for the active writer; do not delete the lock file |
| Legacy store has no persistent lock | Run `Client(root).init()` or `fastk_admin init --root ...` once |
| Pending recovery | Run explicit writable recovery; inspection and reads do not silently repair |
| Cannot reconstruct semantic metadata | Restore the original manifest backup; recovery will not guess kline scales |
| `TIMEOUT` | Child has been terminated and reaped; inspect/recover/inventory before retrying a write |
| Sealed snapshot rejects writes | Create a new dataset version; do not remove the seal |

Old binary data and existing successful bridge payloads remain compatible.
Stop older FastK processes before the one-time persistent-lock migration;
simultaneously running different lock protocols is not supported.
Existing low-level scalar `put` replaces a month; bridge `write` appends and now
preserves existing rows. A multi-month write can partially commit. Legacy
registry statuses stay descriptive; only `publish_dataset` establishes an
immutable verified binding. See CORE_CONTRACT.md before migrating research runs.

Node has a subprocess example at `examples/cli/node_bridge.mjs`, not an SDK.
Forward raw JSON or select a lossless integer parser: ordinary `JSON.parse`
can round signed 64-bit values beyond JavaScript's safe integer range.
