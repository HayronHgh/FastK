# Kline Storage Comparison

This report compares kline storage options for normalized OHLCV records:

```text
KlineRecord = ts, open, high, low, close, volume
```

FastK stores this as six signed 64-bit integers, so the raw payload is 48 bytes
per row before chunk headers, sparse indexes, manifests, and filesystem
metadata.

## Scope

The numbers below are engineering sizing estimates for a 1,000,000-row kline
dataset. They are meant for architecture comparison and capacity planning, not
as a substitute for a machine-local benchmark. TimescaleDB numbers are included
as deployment estimates because this repository does not run a local TimescaleDB
server in tests.

Assumptions:

- one symbol/timeframe series
- integer-scaled OHLCV values
- rows sorted by timestamp
- no compression for JSON/CSV unless stated
- SQLite has an index on timestamp
- TimescaleDB uses a hypertable with timestamp index

## Storage Footprint

| Backend | Record shape | Estimated bytes / row | Estimated 1M-row size | Notes |
|---|---:|---:|---:|---|
| FastK | fixed binary `i64 x 6` | 48-56 B | 48-56 MB | Chunk headers and manifests are small relative to row payload. |
| CSV | text row | 70-120 B | 70-120 MB | Compact and portable, but no native index. |
| JSONL | object per row | 150-260 B | 150-260 MB | Human-readable but large due to repeated field names. |
| SQLite3 | table + timestamp index | 90-180 B | 90-180 MB | Good embedded baseline; index adds storage. |
| TimescaleDB | hypertable + index | 120-240 B | 120-240 MB | Server features add overhead; compression can reduce historical chunks. |

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

FastK is the best fit when the application wants a local, versioned,
read-heavy storage layer for already-normalized kline data and feature outputs.
It avoids SQL planner overhead and stores rows in a compact fixed-width binary
format.

CSV is acceptable for interchange and inspection, but it becomes expensive once
range reads and latest-N reads are part of the hot path.

JSONL is useful for debugging and bridge contracts, but it is inefficient as the
primary kline store because every row repeats field names and requires heavier
parsing.

SQLite3 is the strongest embedded general-purpose baseline. It is easier to
query ad hoc than FastK, but it carries row/index overhead and does not enforce
FastK's chunk lifecycle.

TimescaleDB is appropriate when the system needs server-side SQL analytics,
multi-client access, retention jobs, or operational database features. It is
heavier than FastK for an embedded research/backtest storage component.

## Recommendation

Use FastK for the local kline and feature persistence layer. Keep CSV/JSONL as
import/export formats, SQLite3 as a control-plane or general embedded baseline,
and TimescaleDB only when the project needs a networked analytical database.
