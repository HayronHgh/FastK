# External comparison

Profile: real. All returned fields and research checksums verified.

OS cache was uncontrolled. These are Python integration results, not Rust-only engine timings.

| Backend | Build (s, one sample) | Bytes | Range median (ms) | Research median (s) | Peak Python worker RSS (MiB) |
|---|---:|---:|---:|---:|---:|
| fastk | 2.5974 | 186878372 | 0.0215 | 0.2413 | 125.25 |
| sqlite | 8.5416 | 345583616 | 0.1277 | 3.5266 | 338.84 |
| duckdb | 11.4267 | 446705664 | 2.1042 | 0.6226 | 248.12 |
| parquet | 1.6476 | 82680692 | 1.5947 | 0.4267 | 236.68 |
| arrow | 0.6973 | 118145328 | 0.0437 | 0.2579 | 191.02 |

| Backend | Later-month append (s) | Fresh full-version rebuild (s) |
|---|---:|---:|
| fastk | 0.1117 | 2.1620 |
| sqlite | 0.0186 | 8.4035 |
| duckdb | 0.0963 | 11.1306 |
| parquet | 0.0499 | 1.6131 |
| arrow | 0.0191 | 0.6690 |

Limitations:
- OS cold cache was not established; no cold-cache claim
- All main adapters are in-process and return packed NumPy int64; legacy JSON measurements are separate
- build, extension and version rebuild each have one sample; descriptive, not distributions
- 5ms sampled memory can miss short peaks; process peaks are not added together
- research kernel is a deterministic integration workload, not a complete trading backtest
- historical corrections are measured as full new-version rebuilds for every backend; no sparse-update claim
- when prepared stores are reused, initial build times are omitted; fresh full-version rebuild costs are still measured
