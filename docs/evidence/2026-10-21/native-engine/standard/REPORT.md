# External comparison

Profile: standard. All returned fields and research checksums verified.

OS cache was uncontrolled. These are Python integration results, not Rust-only engine timings.

| Backend | Build (s, one sample) | Bytes | Range median (ms) | Research median (s) | Peak Python worker RSS (MiB) |
|---|---:|---:|---:|---:|---:|
| fastk | not measured (reused) | 1859914320 | 0.0579 | 2.0715 | 176.95 |
| sqlite | not measured (reused) | 3005304832 | 0.1317 | 34.3505 | 303.77 |
| duckdb | not measured (reused) | 4255395840 | 2.3399 | 5.3608 | 445.55 |
| parquet | not measured (reused) | 496284201 | 1.6554 | 4.1111 | 264.81 |
| arrow | not measured (reused) | 1175848800 | 0.3688 | 2.4847 | 195.35 |

| Backend | Later-month append (s) | Fresh full-version rebuild (s) |
|---|---:|---:|
| fastk | 1.4233 | 24.5877 |
| sqlite | 0.2010 | 83.4989 |
| duckdb | 0.9291 | 116.3668 |
| parquet | 0.8980 | 15.9696 |
| arrow | 0.3421 | 7.1919 |

Limitations:
- OS cold cache was not established; no cold-cache claim
- All main adapters are in-process and return packed NumPy int64; legacy JSON measurements are separate
- build, extension and version rebuild each have one sample; descriptive, not distributions
- 5ms sampled memory can miss short peaks; process peaks are not added together
- research kernel is a deterministic integration workload, not a complete trading backtest
- historical corrections are measured as full new-version rebuilds for every backend; no sparse-update claim
- when prepared stores are reused, initial build times are omitted; fresh full-version rebuild costs are still measured
