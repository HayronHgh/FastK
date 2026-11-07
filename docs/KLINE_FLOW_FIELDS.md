# Kline flow fields (schema 3, v0.3)

FastK 0.3 can keep every useful column of an exchange kline next to OHLCV, so a
research dataset written once never needs a second backfill for flow features.
It adds one kline record layout and one manifest version; existing OHLCV data is
read exactly as before and is never rewritten.

FastK still stores caller-normalized integers only. Parsing exchange payloads,
choosing scales and computing derived features stay in the caller
([Architecture Boundary](ARCHITECTURE_BOUNDARY.md)).

## Record layout

`KlineFlowRecord` is ten little-endian `i64` words (80 bytes). The first six
words are exactly the 48-byte `KlineRecord`.

| Word | Field | Unit / scale | Binance USDⓈ-M `GET /fapi/v1/klines` column |
|---:|---|---|---|
| 0 | `ts` | UTC ms | 0 open time |
| 1–4 | `open`, `high`, `low`, `close` | `price_scale` | 1–4 |
| 5 | `volume` | `volume_scale` | 5 base volume `v` |
| 6 | `quote_volume` | `quote_scale` | 7 quote volume `q` |
| 7 | `trade_count` | unscaled count | 8 number of trades |
| 8 | `taker_buy_volume` | `volume_scale` | 9 taker-buy base volume `V` |
| 9 | `taker_buy_quote_volume` | `quote_scale` | 10 taker-buy quote volume |

Not stored: column 6 (close time, always `ts + timeframe_ms - 1` for a closed
bar) and column 11 (ignored by the exchange). `quote_scale` is a new per-series
scale because quote totals can need a different magnitude from prices or base
volume. With an E8 scale an `i64` holds about 9.2×10¹⁰ quote units per bar,
which covers ETHUSDT minute to daily bars. Choose a smaller `quote_scale` for
weekly or monthly bars whose quote volume can exceed that. FastK never changes
a series' scales after registration.

## Absent values are never zero

Each flow field is either present (`>= 0`) or absent. Rust exposes absence as
`Option<i64>` through `quote_volume_opt()`, `trade_count_opt()`,
`taker_buy_volume_opt()` and `taker_buy_quote_volume_opt()`. Storage, the raw
public fields and NumPy use the sentinel `KLINE_FLOW_MISSING` (`i64::MIN`),
which is outside the valid range. JSON uses `null`. A present `0` (for example
a bar with no trades) remains `0` and is distinct from absent.

Reading an OHLCV-only series (schema 1) through any flow API returns every flow
field as absent, never as `0`.

## Validation

Every row is checked before ownership is taken or anything is written; one
invalid row rejects the whole request with `INVALID_INPUT`:

- `volume` and every present flow value are non-negative;
- `taker_buy_volume <= volume`;
- `taker_buy_quote_volume <= quote_volume`, and `taker_buy_quote_volume`
  requires `quote_volume` to be present;
- timestamps strictly increasing (bridge input is sorted first; duplicates are
  rejected), as for OHLCV;
- `quote_scale > 0`, and the series scales and schema match the existing series.

These match an executor that parses the same exchange bytes into E8 fixed
point with `None` for an absent value and checks taker-buy volume against volume.

## Series schemas never mix

A series is registered as either `ohlcv` (schema 1, 48-byte rows, manifest
version 2) or `ohlcv_flow` (schema 3, 80-byte rows, manifest version 3 with
`quote_scale`). Within a series every chunk has the same layout.

| Operation | OHLCV series | Flow series |
|---|---|---|
| `get_kline_range` / `latest_n` / `at`, sessions, `BacktestStoreView`, `read-kline-range`, native `read_kline_range` | unchanged | OHLCV projection (first 48 bytes of each row) |
| `get_kline_flow_range` / `latest_n` / `at`, `read-kline-flow-range`, native `read_kline_flow_range` | flow fields absent | full rows |
| `put_kline_chunk`, `append_kline_range`, `write-kline-range` | unchanged | rejected (schema mismatch) |
| `put_kline_flow_chunk`, `append_kline_flow_range`, `write-kline-flow-range` | rejected; old series are never rewritten in place | append |
| `replay_kline` / `replay_kline_flow` | `replay_kline` | `replay_kline_flow` |
| merge, recovery adoption, manifest rebuild, validate, scrub, snapshots | unchanged | supported |

To add flow fields for data you already hold as OHLCV, write a new flow series
in a new dataset root, for example from saved raw exchange responses, rather
than mutating the old root.

## On-disk compatibility

- Schema 1 chunks and version 2 manifests are byte-identical to FastK
  0.2.0-rc.1. A golden test compares a chunk written by 0.2.0-rc.1 with the
  same chunk written by 0.3, and checks that reads, read-only opens and writable
  `init` leave existing chunk and manifest bytes unchanged.
- Chunk layout version stays 2. Flow chunks differ only in `schema_id = 3` and
  `record_size = 80`.
- The manifest keeps magic `FKMETA02`. Version 3 is written only for flow
  series and inserts `quote_scale` after `volume_scale`. FastK 0.2 rejects it
  with `unsupported manifest version: 3` instead of misreading it.
- Recovery refuses to invent `quote_scale`, as for other scales: restore the
  original `series.meta` before rebuilding a damaged flow manifest.

## Interfaces

Rust: `register_kline_flow_series(symbol, tf, timeframe_ms, price_scale,
volume_scale, quote_scale)`, `put_kline_flow_chunk`, `append_kline_flow_range`
(registers when absent), `get_kline_flow_at`, `get_kline_flow_range`,
`get_kline_flow_latest_n`, `replay_kline_flow`. Also `SeriesMeta::quote_scale`
and `kline_schema_name(schema_id)`.

Bridge: `read-kline-flow-range`, `write-kline-flow-range`,
`read-kline-flow-latest` ([Bridge Contract](BRIDGE_CONTRACT.md)). Python
subprocess client: `read_kline_flow_range`, `write_kline_flow_range`,
`read_kline_flow_latest`.

Native NumPy (`fastk_native`): `KLINE_FLOW_DTYPE` (ten `<i8` fields in the
order above), `KLINE_FLOW_MISSING`, `Reader.read_kline_flow_range`,
`Reader.read_kline_flow_latest`, `Writer.write_kline_flow_range(...,
quote_scale=...)`. `Reader.kline_info` adds `kline_schema` and `quote_scale`.
Flow reads hand the Rust allocation to NumPy like OHLCV reads do.

## Derived features belong to the caller

With all fields stored, a caller can compute (outside FastK) for example:

- volume-weighted average price `q / v`;
- net taker flow `2V − v` and its running sum (CVD);
- taker-buy average price `Q / V` against taker-sell average `(q − Q) / (v − V)`;
- average trade size `v / trade_count`.

Check for absent values and zero denominators first. Results can be stored as
scalar features with the generic scalar APIs.

## Performance

See [Kline flow performance](KLINE_FLOW_PERFORMANCE.md) for before/after
measurements of the unchanged OHLCV path and the cost of 80-byte flow rows.
