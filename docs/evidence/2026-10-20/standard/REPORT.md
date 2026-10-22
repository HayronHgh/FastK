# External comparison

Profile: standard. All returned fields and research checksums verified.

OS cache was uncontrolled. These are Python integration results, not Rust-only engine timings.

| Backend | Build (s, one sample) | Bytes | Range median (ms) | Research median (s) | Peak process-tree RSS (MiB) |
|---|---:|---:|---:|---:|---:|
| fastk | not measured (reused) | 1859914320 | 24.4209 | 60.6784 | 858.02 |
| sqlite | not measured (reused) | 3005272064 | 0.1128 | 46.3894 | 769.62 |
| duckdb | not measured (reused) | 4255395840 | 2.8905 | 32.2011 | 1023.02 |
| parquet | not measured (reused) | 496284201 | 2.4603 | 44.4724 | 774.97 |
| arrow | not measured (reused) | 1175848800 | 0.6643 | 41.3283 | 1856.68 |

| Backend | Later-month append (s) | Fresh full-version rebuild (s) |
|---|---:|---:|
| fastk | 4.8500 | 137.3270 |
| sqlite | 0.2556 | 109.9500 |
| duckdb | 1.0785 | 125.8774 |
| parquet | 1.1094 | 20.8218 |
| arrow | 0.4953 | 8.6469 |

Limitations:
- OS cold cache was not established; no cold-cache claim
- FastK uses a subprocess per request; native persistent Python bindings can be faster
- build, extension and version rebuild each have one sample; descriptive, not distributions
- sampled RSS can miss sub-10ms child peaks
- research kernel is a deterministic integration workload, not a complete trading backtest
- historical corrections are measured as full new-version rebuilds for every backend; no sparse-update claim
- when prepared stores are reused, initial build times are omitted; fresh full-version rebuild costs are still measured
