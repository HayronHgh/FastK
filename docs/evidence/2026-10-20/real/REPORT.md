# External comparison

Profile: real. All returned fields and research checksums verified.

OS cache was uncontrolled. These are Python integration results, not Rust-only engine timings.

| Backend | Build (s, one sample) | Bytes | Range median (ms) | Research median (s) | Peak process-tree RSS (MiB) |
|---|---:|---:|---:|---:|---:|
| fastk | 13.1239 | 186878372 | 23.7523 | 6.2462 | 957.46 |
| sqlite | 11.0764 | 345583616 | 0.1052 | 4.4821 | 862.07 |
| duckdb | 12.2791 | 439103488 | 2.3197 | 3.1589 | 861.36 |
| parquet | 2.0936 | 82680692 | 2.1317 | 4.6079 | 855.93 |
| arrow | 0.8354 | 118145328 | 0.6154 | 4.1804 | 911.54 |

| Backend | Later-month append (s) | Fresh full-version rebuild (s) |
|---|---:|---:|
| fastk | 0.3657 | 12.9676 |
| sqlite | 0.0205 | 11.1227 |
| duckdb | 0.1147 | 11.8814 |
| parquet | 0.0630 | 2.1005 |
| arrow | 0.0244 | 0.8901 |

Limitations:
- OS cold cache was not established; no cold-cache claim
- FastK uses a subprocess per request; native persistent Python bindings can be faster
- build, extension and version rebuild each have one sample; descriptive, not distributions
- sampled RSS can miss sub-10ms child peaks
- research kernel is a deterministic integration workload, not a complete trading backtest
- historical corrections are measured as full new-version rebuilds for every backend; no sparse-update claim
- when prepared stores are reused, initial build times are omitted; fresh full-version rebuild costs are still measured
