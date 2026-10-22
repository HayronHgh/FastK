# Fixed backtest, interchangeable storage

One storage-independent Python/NumPy backtest consumes the same read-only,
C-contiguous six-field int64 OHLCV arrays from FastK, Arrow IPC, Parquet,
DuckDB and SQLite. Only `fastk_bench.backends.connect` selects an adapter.
No SQL indicator pushdown, backend-specific strategy, subprocess JSON bridge,
asynchronous prefetch or proposed adaptive channel is used.

The default experiment uses the existing canonical data/stores; it never writes
to them or downloads additional data. A separate `memory` control preloads the
canonical bars before application timing. It estimates resident-data application
cost and is **not** a persistent-storage competitor.

## Fixed cases and execution

| Case | Signal evaluated at the close |
| --- | --- |
| `rsi` | Wilder RSI(14): target long at RSI <= 30, target flat at RSI >= 70, otherwise retain the target |
| `sma` | After 60 bars, target long while SMA20 > SMA60, otherwise flat |
| `momentum` | After 61 bars, target long while close > close 60 available bars ago, otherwise flat |

RSI seeds average gains/losses from the first 14 changes; subsequent values use
Wilder smoothing. An entirely flat history has RSI 50. This follows the
[Wilder calculation described in TA-Lib's RSI source](https://github.com/TA-Lib/ta-lib/blob/main/src/ta_func/ta_RSI.c);
TA-Lib is not a dependency or called by the test.

Every signal executes at the **next available bar's open**, including across a
month boundary. Start with 100,000 quote units, split equally into independent
symbol accounts. Each account is long/flat, uses fractional quantities and all
its available cash, and never transfers funds to another account. Fee is 10 bps
and adverse slippage 1 bp per fill. No leverage, shorts, stop orders, funding,
liquidity limit or forced terminal sale. Final positions are marked at the last
close; the last close's pending signal is not filled without another bar.

Signals, pending orders, cash and positions persist across batches. Missing time
is preserved; lookbacks count available bars. Portfolio inputs must have aligned
timestamps: mismatches fail instead of silently forward filling. Equity and
drawdown are measured at closes. Storage remains int64; execution accounting
uses the same float64 arithmetic for every backend.

These three cases are simple fixed workloads. They were not selected or tuned
for profitability. Momentum means independent symbol positions, not cross-asset
ranking or periodic portfolio rebalancing.

## Two lifecycles

- `monthly`: open one backend, read the selected symbols for one month, close,
  compute, release arrays; repeat chronologically with the same backtest state.
- `bulk`: open one backend, read all selected history, close, compute, release.

Both execute the same bars and fills. With two symbols and twelve months, monthly
does 12 opens and 24 queries; bulk does one open and two queries. Handles close
before computation, and returned arrays must remain usable afterwards. Read
failure closes once and propagates; there is no automatic query replay.

## Run

Use the existing benchmark environment (`external-tests/requirements-native.lock`)
with the installed native wheel built from the intended FastK version. From
`external-tests/`:

```powershell
python -m unittest backtest.test_engine -v
python -m backtest.run --stores /path/to/five-stores --source /path/to/canonical-input --output /path/to/new-results --rounds 5 --months 12
python -m backtest.report /path/to/new-results
```

`--stores` contains `fastk/`, `arrow/`, `parquet/`, `duckdb/`, `sqlite/` from the
existing common preparation runner; `--source` contains `manifest.json` and
the canonical Arrow files. See the parent [validation guide](../../docs/EXTERNAL_VALIDATION.md)
for input preparation. The output path must not exist.

To change only the storage adapter, add `--backends fastk` (or `arrow`, `parquet`,
`duckdb`, `sqlite`). The memory control is always included. Optional `--cases rsi`,
`--modes monthly`, `--symbols BTCUSDT ETHUSDT`, `--months 1`, `--rounds 1` narrow
the experiment without changing strategy/matching code.

## Measurement and evidence

Before timing, the runner verifies source file SHA256 and independently compares
every queried field/row from all selected stores with canonical input, after
closing each handle. This preflight warms OS caches. Formal settings run in
separate processes, with deterministic randomized order in each round and one
numeric-library/engine work-thread budget. No explicit query warmup is performed.

The application timer includes manifest loading, scheduling/state creation,
open/read/close, indicators, fills, close equity/drawdown, curve digest, result
summary/trade digest and input/engine release. Process launch, dependency imports,
preflight, report writing and parent memory sampling are outside it. The catalog
loads once per application run and remains caller-resident across requests.
Results include per-batch load/compute/release, first-data-ready time, setup,
finalization and the full wall time. Phase medians need not sum to the median
wall time. Curve/trade digest generation is common application work, and is
included in compute/finalization rather than claimed to be storage cost.

The memory control excludes loading and unloading its preloaded input from
application time. Its sampled memory still includes that preparation and its
Arrow dependency. It is a compute-cost control, not a minimum-RSS measurement.
Worker peak RSS/private memory is sampled every 5 ms, including backend imports
and input preparation; these are lower-bound sampled peaks, not buffer-only
allocation measurements. File-backed pages are absent from Windows private
commit, so RSS is also reported. Process thread counts include runtime helper
threads and do not measure active query parallelism.

`manifest.json` pins order, inputs, harness hashes, dependency versions and the
actual native binary hash. Every timed process's complete outcome must match
the memory reference, including every equity point and fill digest, remaining
positions, fees, drawdown and final equity. Bulk and monthly must also match.
Per-process samples, logs, PIDs, preflight checks and `summary.json` remain intact.
No test asserts a fragile latency threshold.

The optional report command writes `TABLES.md` and `report.html`. Charts use
native SVG without generated bitmap assets. Each stacked bar represents the
actual process at the middle total time for its setting, so its load/remainder
segments add exactly to that observed total; tables separately use medians.

## Historical version comparison

`compare_versions.py` reuses the same engine and measurement loop, comparing an
explicit executable built from public commit `814c566` with the installed native
module. The historical adapter invokes the original one-shot CLI and converts
its JSON records into the identical NumPy layout. It does not simulate a native
interface that the old version never provided. Startup, store open/close, JSON,
pipe transfer and Python materialization all remain inside its load time; the
old `read_ns` therefore includes its internal open/close and is not a pure IO
phase. Neither route retries failed queries.

```powershell
python -m backtest.compare_versions --binary /path/to/public-814c566-fastk_bridge --store /path/to/isolated-store --source /path/to/canonical-input --output /path/to/new-version-results --months 3 --rounds 5
```

Use an initialized **isolated copy**, including the current store's persistent
ownership-lock file, and verify its data hashes before/after. The old executable
does not provide the current read-only/locking guarantees. This Q1 comparison
has its own cohort and 262,080 rows; do not mix it with the 1,054,080-row annual
five-backend experiment. The observed ratio measures public delivery plus the
fixed backtest, not a Rust-algorithm-only speedup. Details and raw samples are
in [the version report](../../docs/VERSION_COMPARISON.md).

Five-backend results include each current adapter's conversion costs. SQLite
uses Python `fetchall` to NumPy and its existing `Path.resolve()` connection
path; DuckDB uses `fetchnumpy`; Arrow converts its column views into packed rows;
Parquet includes decompression/filtering. These are practical same-output Python
comparisons, not isolated storage-algorithm rankings. OS caches are uncontrolled;
a fresh interface/process is not proof of a physical cold-disk read. Initial
store construction, ingestion, revisions and crash recovery are outside this
read/backtest experiment.
