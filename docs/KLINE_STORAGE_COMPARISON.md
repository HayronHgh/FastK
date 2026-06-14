# Kline Storage Comparison

This report compares storage options for normalized OHLCV kline records:

```text
KlineRecord = ts, open, high, low, close, volume
```

FastK stores each kline row as six signed 64-bit integers. The raw payload is
48 bytes per row before chunk headers, manifests, sidecars, and filesystem
metadata.

## Benchmark Scope

The measured numbers below come from the Rust-only benchmark runner:

```bash
cargo run --release --example bench_kline_storage_comparison -- \
  --root target/kline-storage-comparison-10m \
  --symbol-count 24 \
  --months 10 \
  --records-per-month 41667 \
  --range-ops 100 \
  --range-rows 1024
```

Benchmark environment:

- Measured at: `2026-06-14T18:09:55.957032800+00:00`
- Host OS / arch: `windows` / `x86_64`
- Dataset: `24` symbols x `10` months x `41,667` rows per month
- Total rows: `10,000,080`
- Timeframe: `1m`
- Range workload: `100` range reads x `1,024` rows
- Source result file:
  `target/kline-storage-comparison-10m/kline-storage-comparison-results.json`

Python tools are not used for these measurements.

## Measured Results

| Backend | Rows | Size | Bytes / row | Write time | Write throughput | Full read time | Full read throughput |
|---|---:|---:|---:|---:|---:|---:|---:|
| FastK | 10,000,080 | 459.02 MiB | 48.13 B | 3.768 s | 2,653,606 rows/s | 0.150 s | 66,521,651 rows/s |
| CSV | 10,000,080 | 722.75 MiB | 75.79 B | 1.970 s | 5,075,596 rows/s | 2.317 s | 4,316,835 rows/s |
| JSONL | 10,000,080 | 1,247.27 MiB | 130.79 B | 2.226 s | 4,492,064 rows/s | 2.944 s | 3,396,683 rows/s |
| SQLite3 | 10,000,080 | 1,569.08 MiB | 164.53 B | 13.654 s | 732,418 rows/s | 2.062 s | 4,849,892 rows/s |
| TimescaleDB | Not measured | Not measured | Not measured | Not measured | Not measured | Not measured | Not measured |

TimescaleDB is not measured in this local run because it requires an external
PostgreSQL/TimescaleDB service. Docker is not available on this host and no
TimescaleDB DSN was configured, so this report does not mix estimated
TimescaleDB numbers with measured embedded-storage numbers.

## Range Read Results

The range workload reads `102,400` total rows through `100` range queries.

| Backend | Range rows read | Total range time | Average / op | Range throughput | Notes |
|---|---:|---:|---:|---:|---|
| FastK | 102,400 | 0.005234 s | 0.0523 ms | 19,562,892 rows/s | Chunk-pruned binary range reads. |
| SQLite3 | 102,400 | 0.022410 s | 0.2241 ms | 4,569,450 rows/s | Indexed range scan on `(symbol, ts)`. |
| CSV | Not reported | Not reported | Not reported | Not reported | No external index in this benchmark; range reads require full-file scan. |
| JSONL | Not reported | Not reported | Not reported | Not reported | No external index in this benchmark; range reads require full-file scan and JSON parse. |
| TimescaleDB | Not measured | Not measured | Not measured | Not measured | Requires external PostgreSQL/TimescaleDB service. |

## Operational Comparison

| Capability | FastK | CSV | JSONL | SQLite3 | TimescaleDB |
|---|---|---|---|---|---|
| Append sorted kline rows | Strong | Simple | Simple | Strong | Strong |
| Point lookup by timestamp | Strong | Weak | Weak | Strong | Strong |
| Short range query | Strong | Weak without external index | Weak without external index | Strong | Strong |
| Full scan | Strong | Strong | Moderate | Strong | Strong |
| Latest-N query | Strong | Weak unless reverse indexed | Weak unless reverse indexed | Strong | Strong |
| Embedded deployment | Strong | Strong | Strong | Strong | Weak |
| Multi-client SQL analytics | Weak | Weak | Weak | Moderate | Strong |
| Schema evolution | Controlled binary layout | Manual | Flexible but loose | Moderate | Strong |
| Operational complexity | Low | Low | Low | Low | High |

## Query Cost Model

| Workload | FastK | CSV | JSONL | SQLite3 | TimescaleDB |
|---|---:|---:|---:|---:|---:|
| Point lookup | `O(log chunks + local scan/index)` | `O(n)` | `O(n)` | `O(log n)` | `O(log n)` |
| Short range | chunk-pruned scan | full file scan | full file scan + parse | index range scan | hypertable index range scan |
| Full range | sequential binary scan | sequential text scan | sequential parse-heavy scan | table/index scan | sequential or chunk scan |
| Latest-N | manifest/chunk tail read | file tail parsing | file tail parsing | timestamp index desc | timestamp index desc |

## Practical Interpretation

FastK has the smallest measured footprint because kline rows remain fixed-width
binary records. On this dataset it stores `10,000,080` rows in `459.02 MiB`,
close to the 48-byte raw row payload.

CSV and JSONL write quickly because the benchmark performs simple buffered file
writes, but both formats pay much higher parse cost during reads and do not
provide native timestamp indexing.

SQLite3 is the strongest embedded general-purpose baseline. It supports indexed
range reads and SQL queries, but the table and primary-key index increase both
write time and storage footprint.

TimescaleDB remains the right comparison point for networked SQL analytics,
retention jobs, hypertables, compression policies, and multi-client access. It
is not an embedded local storage dependency, so it should be benchmarked against
a real PostgreSQL/TimescaleDB deployment before using numbers in capacity
planning.

## Recommendation

Use FastK for local kline and feature persistence when the hot path is
read-heavy range access over normalized fixed-record data. Keep CSV/JSONL as
import/export formats, SQLite3 as the embedded SQL baseline, and TimescaleDB
for deployments that need a server-side analytical database.
