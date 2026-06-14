# Store Layout And Lifecycle

FastK lifecycle rules are storage rules. They describe how normalized records
become chunks and manifest entries; they do not describe data sourcing,
calculation, strategy execution, or trading behavior.

## Layout

Kline series:

```text
root/
  series/
    {symbol}/
      kline/
        {timeframe}/
          series.meta
          chunks/
            YYYY-MM.chunk
            YYYY-MM.delta-<generation>.chunk
            YYYY-MM.merged-<generation>.chunk
```

Scalar series:

```text
root/
  series/
    {symbol}/
      {category}/
        {timeframe@@name}/
          series.meta
          chunks/
            YYYY-MM.chunk
            YYYY-MM.merged-<generation>.chunk
            YYYY-MM.chunk.zmap
            YYYY-MM.chunk.vix
```

High-frequency fixed-record series:

```text
root/
  series/
    {symbol}/
      {trade|bbo|book_delta}/
        {channel}/
          series.meta
          chunks/
            {partition}.chunk
```

Kline and scalar series default to UTC-month partitions. Trade and book-delta
default to UTC-hour partitions. BBO defaults to UTC-day partitions.

## Chunk States

- `Active`: append/delta chunk still visible as part of the storage lifecycle
- `Sealed`: immutable stable chunk
- `Merging`: chunk involved in compaction
- `Replaced`: old chunk superseded by a merged replacement

## Kline Lifecycle

1. Caller normalizes kline rows and chooses dataset, symbol, timeframe, and root.
2. Caller registers the target series with `register_kline_series`.
3. Caller writes fixed records with `put_kline_chunk`.
4. FastK validates fixed layout, timestamp order, partition boundaries, checksum, and manifest consistency.
5. FastK updates chunk metadata and exposes range/latest/replay reads.
6. Optional storage compaction can merge safe chunks without changing caller semantics.

FastK does not fetch raw kline data or decide cleaning policy.

## Scalar Lifecycle

1. Caller calculates indicator, feature, factor, signal, portfolio, risk, or metric values outside FastK.
2. Caller chooses the scalar category and logical series key.
3. Caller registers the series with `register_scalar_series` or compatibility helpers such as `register_indicator_series`.
4. Caller writes `ScalarRecord` rows with `put_scalar_chunk` or compatibility helpers.
5. FastK stores records and builds storage sidecars such as `.zmap` and `.vix`.

The `indicator` category is a scalar namespace retained for compatibility.
FastK does not calculate indicators.

Scalar predicate queries are storage-level comparisons over
`ScalarRecord.value`. They support continuous predicates such as `>`, `>=`, `<`,
`<=`, and `between`, plus discrete predicates such as `==`, `!=`, `in-set`, and
`not-in-set`. FastK may use `.zmap` and `.vix` sidecars to reduce scanned rows,
but it does not interpret feature, factor, signal, risk, metric, or strategy
meaning.

## High-Frequency Fixed-Record Lifecycle

1. A market-data layer parses source payloads and normalizes records.
2. The caller writes `TradeRecord`, `BboRecord`, or `BookDeltaRecord` rows.
3. FastK validates fixed record layout and stores rows in the configured time partition.
4. FastK exposes range/latest/replay reads over sealed chunks.

FastK does not connect to exchanges, process WebSockets, repair exchange gaps,
or apply source-specific sequence policy.

## Recovery / Validation

FastK storage-level validation includes:

- `validate`
- `scrub`
- `repair`
- `rebuild-manifest`
- `list-orphans`
- `explain-overlap`

These tools inspect storage health:

- manifest vs filesystem consistency
- chunk checksum
- sidecar consistency
- orphan artifacts
- overlap resolution

They do not validate strategy logic or data-source correctness.

## Runtime Read Path

Kline:

- point lookup
- short range
- full range scan
- latest-N
- sealed replay

Scalar:

- point lookup
- short range
- zmap pruning
- vix direct lookup
- storage-level scalar predicate query over `ScalarRecord.value`
- raw span
- sealed replay

High-frequency fixed records:

- range scan
- latest-N
- sealed replay
- storage-level sequence scan reports for BBO, book-delta, and trade-id fields

Sequence scans report gaps, duplicates, and non-monotonic adjacent values. They
do not repair data, reconnect to sources, fetch missing rows, or apply
exchange-specific policy.

## Cache Layers

- process-wide shared manifest cache
- store-local manifest cache
- chunk header/layout cache
- chunk file handle cache
- scalar sidecar cache
- read-session logical cache reset modes

These caches are storage optimizations only. They do not imply ownership of
research, backtest, or live trading flows.

## Platform Note

Windows parent-directory `fsync` is best-effort. FastK preserves the storage
lifecycle contract, but platform durability details still depend on the host
filesystem and OS behavior.
