# Kline flow performance (v0.3.0-rc.1)

Measured 2026-11-07 on Windows 11, AMD Ryzen 9 9900X, 94 GiB RAM, Rust 1.97.1,
release builds. The baseline is v0.2.0-rc.1 (`origin/main` 4e5f7a2), built with
the same toolchain. Timings are local wall-clock medians, not portable promises.
Raw outputs are not committed.

## OHLCV path before/after

v0.3 compiles the kline reader and writer per row layout (generics), so the OHLCV
path keeps its own code. Two measurements:

**`bench_acceptance`** (existing runner). Old and new runs alternated: old ×2,
new ×3. Settings: `--metrics-level off --symbol-count 2 --months 3
--records-per-month 40000 --point-ops 2000 --range-ops 500 --full-scan-ops 20
--latest-n-ops 500`. The table shows the median across runs of each run's p50:

| Workload (warm) | rows/op | v0.2 µs | v0.3 µs | v0.3/v0.2 |
|---|---:|---:|---:|---:|
| write_initial | 240,000 | 81,641 | 72,758 | 0.89 |
| append_active_month | 512 | 8,731 | 7,744 | 0.89 |
| get_at_hit | 1 | 2.8 | 2.6 | 0.95 |
| get_range_short_64 | 64 | 2.8 | 2.8 | 1.00 |
| get_range_short_1024 | 1,024 | 15.8 | 14.4 | 0.91 |
| get_range_medium | 8,192 | 228 | 252 | 1.11 |
| full_scan | 120,000 | 766 | 867 | 1.13 |
| latest_n | 1,024 | 2.3 | 11.1 | see below |

Cold-ish rows (`ApproxCold`, `StricterColdish`) stayed within ±14%, and most
are within ±5%. Several warm rows are bimodal in this runner. `latest_n` has
the same minimum in both versions (2.2 µs), but its p50 fell into the slow mode
in all three new runs. `full_scan` new runs ranged 706–897 µs against 757–776 µs
for the old runs.

**Isolated A/B** (`get_kline_*` only, one process per version, alternated twice,
3 rounds each, 172,800 one-minute rows, 20,000 samples for short reads):

| Read | v0.2 p50 µs | v0.3 p50 µs |
|---|---:|---:|
| latest 1,024 | 2.4 | 2.4 |
| range 1,024 | 5.1 | 5.1–5.3 |
| range 8,192 | 40.5–41.9 | 40.6–61.2 |
| full 172,800 | 1,355–1,492 | 1,327–1,488 |
| point lookup | 2.0–2.1 | 2.0–2.1 |

Both versions show the same ~11–13 µs p90 tail on latest-1,024 in some rounds.
This run found no OHLCV read-path regression. The acceptance-runner
differences fall within its run-to-run spread.

## Cost of flow rows

One year of 1-minute rows (527,040) per series, warm page cache. The table shows
steady-state medians; the first round after writing is slower for every series
type.

| Read | OHLCV series | Flow series, flow API | Flow series, OHLCV API (projection) |
|---|---:|---:|---:|
| Full year, 1 thread | 4.8–5.2 ms | 8.5–9.9 ms | 7.4–7.8 ms |
| Full year, 4 threads | 2.1–2.6 ms | 4.0–4.2 ms | 3.2–3.8 ms |
| 64-row range | 2.0 µs | 2.1–2.2 µs | 2.2 µs |
| Latest 1,024 | 2.4 µs | 2.8 µs | 3.8 µs |
| Point lookup | 2.0 µs | 2.1 µs | — |
| Write (append, one year) | 96 ms | 116 ms | — |
| Bytes on disk | 25.4 MB | 42.2 MB | — |

Flow rows are 80/48 = 1.67× the bytes; full scans scale with bytes read. Short
reads are dominated by fixed costs and change little. The OHLCV projection of a
flow series reads every 80-byte row and copies 48 bytes. Spans up to 64 KiB
are staged on the stack with one read; larger spans reuse a bounded buffer.
The NumPy handoff of flow rows keeps the engine allocation, as for OHLCV.
