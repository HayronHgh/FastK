# Bridge Contract

`fastk_bridge` is the stable JSON subprocess bridge for non-Rust integrations.
It reads and writes already-normalized records and inventory data. It does not
fetch market data, calculate indicators, calculate features or factors, route
records, run strategies, execute backtests, or trade.

## Invocation Model

```text
fastk_bridge <command> [options]
```

Read and inventory commands take all inputs through CLI arguments and write JSON
to stdout. Write commands take the target series through CLI arguments and read
the JSON payload from stdin or `--input-json <file>`.

Current stdout/stderr contract:

- Successful read/write/inventory commands write one JSON object to stdout.
- `--help` writes usage text and exits with code `0`.
- Operational errors write `fastk_bridge failed: ...` to stderr and exit with
  code `1`.
- Diagnostics and human-readable errors belong on stderr.
- Machine-readable success payloads belong on stdout.

The current success payloads are not wrapped in `{ "ok": true, "data": ... }`.
That wrapper is reserved for a future compatibility layer if needed. The current
contract is documented here to avoid breaking existing callers.

Recommended future error envelope, not the current CLI output:

```json
{
  "ok": false,
  "error": {
    "code": "FASTK_ERROR_CODE",
    "message": "human readable message"
  }
}
```

## Commands

### read-kline-range

Arguments:

```text
--root <path> --symbol <symbol> --timeframe <tf> --start-ts <ms> --end-ts <ms>
```

Response:

```json
{
  "symbol": "BTCUSDT",
  "timeframe": "1m",
  "timeframe_ms": 60000,
  "start_ts": 1706745600000,
  "end_ts": 1706749200000,
  "price_scale": 100000,
  "volume_scale": 100000,
  "records": [
    {
      "ts": 1706745600000,
      "open": 10000000,
      "high": 10020000,
      "low": 9980000,
      "close": 10010000,
      "volume": 10000
    }
  ]
}
```

### write-kline-range

Arguments:

```text
--root <path> --symbol <symbol> --timeframe <tf> [--input-json <file>]
```

Payload:

```json
{
  "timeframe_ms": 60000,
  "price_scale": 100000,
  "volume_scale": 100000,
  "records": [
    {
      "ts": 1706745600000,
      "open": 10000000,
      "high": 10020000,
      "low": 9980000,
      "close": 10010000,
      "volume": 10000
    }
  ]
}
```

Response:

```json
{
  "symbol": "BTCUSDT",
  "timeframe": "1m",
  "timeframe_ms": 60000,
  "price_scale": 100000,
  "volume_scale": 100000,
  "registered": true,
  "requested_record_count": 1,
  "written_record_count": 1,
  "month_batches_written": 1
}
```

### kline-inventory

Arguments:

```text
--root <path> --symbol <symbol> --timeframe <tf>
```

Response includes existence, metadata, total record count, coverage bounds, and
monthly inventory entries.

### read-indicator-range

Arguments:

```text
--root <path> --symbol <symbol> --timeframe <tf> --indicator-name <name> --start-ts <ms> --end-ts <ms>
```

Response includes `category = "indicator"`, `exists`, base price scale, coverage
bounds, total record count, and scalar records.

### write-indicator-range

Arguments:

```text
--root <path> --symbol <symbol> --timeframe <tf> --indicator-name <name> [--input-json <file>]
```

Payload:

```json
{
  "records": [
    {
      "ts": 1706745600000,
      "value": 10010000
    }
  ]
}
```

Response includes the target identity, `registered`, requested and written
record counts, and month batch count.

### indicator-inventory

Arguments:

```text
--root <path> --symbol <symbol> --timeframe <tf> --indicator-name <name>
```

Response includes existence, total record count, coverage bounds, monthly
inventory entries, and scalar query capabilities.

### read-scalar-range

Arguments:

```text
--root <path> --symbol <symbol> --timeframe <tf> --category <category> --name <name> --start-ts <ms> --end-ts <ms>
```

Response includes scalar identity, optional timeframe metadata, existence,
coverage bounds, total record count, and scalar records.

### query-scalar-predicate

Arguments:

```text
--root <path> --symbol <symbol> --timeframe <tf> --category <category> --name <name> --start-ts <ms> --end-ts <ms> --predicate <predicate>
```

Supported predicates:

- `eq --value <i64>`
- `ne --value <i64>`
- `gt --value <i64>`
- `gte --value <i64>`
- `lt --value <i64>`
- `lte --value <i64>`
- `between --min <i64> --max <i64>`
- `between-exclusive --min <i64> --max <i64>`
- `in-set --values <comma-separated-i64>`
- `not-in-set --values <comma-separated-i64>`

Add `--return-values` to include matched values. Without it, each match has a
timestamp and `value: null`.

Example:

```bash
fastk_bridge query-scalar-predicate \
  --root ./store \
  --symbol BTCUSDT \
  --timeframe 15m \
  --category feature \
  --name rsi14 \
  --start-ts 1700000000000 \
  --end-ts 1710000000000 \
  --predicate gt \
  --value 7000 \
  --return-values
```

Response:

```json
{
  "symbol": "BTCUSDT",
  "timeframe": "15m",
  "category": "feature",
  "name": "rsi14",
  "start_ts": 1700000000000,
  "end_ts": 1710000000000,
  "predicate": { "Gt": 7000 },
  "return_values": true,
  "matches": [
    {
      "ts": 1700000900000,
      "value": 7200
    }
  ],
  "stats": {
    "chunks_considered": 1,
    "chunks_scanned": 1,
    "chunks_pruned": 0,
    "blocks_considered": 4,
    "blocks_scanned": 1,
    "blocks_pruned": 3,
    "rows_checked": 256,
    "rows_matched": 1,
    "index_used": "ZoneMap",
    "fallback_scan": false
  }
}
```

This command is a storage-level predicate pushdown over `ScalarRecord.value`.
FastK does not interpret RSI, factor scores, signal states, risk flags, or any
strategy meaning.

Predicate and cost notes:

- Rust callers should use `ScalarPredicateExpr` for the full predicate surface.
  `ScalarPredicate` is retained as the legacy / compatibility shape for older
  zmap/vix helper APIs.
- `eq` and `in-set` usually fit the `.vix` value index.
- `gt`, `gte`, `lt`, `lte`, and `between` usually fit `.zmap` zone-map pruning.
- `ne` and `not-in-set` commonly require full scan fallback. Bridge callers
  should inspect `stats.fallback_scan`, `stats.index_used`, and
  `stats.rows_checked`; a full scan is reported as `index_used: "FullScan"` with
  `fallback_scan: true`.

### write-scalar-range

Arguments:

```text
--root <path> --symbol <symbol> --timeframe <tf> --category <category> --name <name> [--input-json <file>]
```

Payload:

```json
{
  "timeframe_ms": 60000,
  "records": [
    {
      "ts": 1706745600000,
      "value": 42
    }
  ]
}
```

`timeframe_ms` may be `null` only when the matching kline series exists and can
provide timeframe metadata.

### scalar-inventory

Arguments:

```text
--root <path> --symbol <symbol> --timeframe <tf> --category <category> --name <name>
```

Response includes existence, total record count, coverage bounds, monthly
inventory entries, and scalar query capabilities.

## Record Shapes

Kline JSON record:

```json
{
  "ts": 1706745600000,
  "open": 10000000,
  "high": 10020000,
  "low": 9980000,
  "close": 10010000,
  "volume": 10000
}
```

Scalar JSON record:

```json
{
  "ts": 1706745600000,
  "value": 42
}
```

All timestamps are Unix epoch milliseconds. Numeric price, volume, and scalar
values are caller-normalized integers. FastK does not infer scale or calculate
derived values.

## High-Frequency And Sequence Commands

The core crate exposes fixed-record high-frequency APIs and storage-level
sequence scan reports. `fastk_bridge` does not currently expose CLI commands for
trade, BBO, book-delta, replay, or sequence scans. Those bridge commands are
future or experimental work and must not be treated as completed release
contract.

## Schemas

Documentation schemas are stored under `schemas/`:

- `schemas/bridge_commands.json`
- `schemas/release_manifest.schema.json`

They document the current command payloads and response shapes without adding a
runtime schema validation dependency to FastK core.
