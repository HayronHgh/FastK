# Replay And Tail

## ReplayCursor

`ReplayCursor` provides deterministic historical replay over sealed FastK chunks.
It is a storage-level cursor, not a backtest engine or paper-trading engine.

Use it when an upper-layer system needs to feed records into:

- event-driven backtests
- historical paper-trading simulations
- post-incident market-data replay
- feature or factor rebuild jobs that consume events in order

FastK still does not calculate indicators, features, or factors. Replay only returns stored records.
Upper-layer adapters decide how to feed those records into backtests, paper-trading simulations, incident analysis, or rebuild jobs.

## Scope

Current replay scope:

- `KlineRecord`
- `ScalarRecord`
- `TradeRecord`
- `BboRecord`
- `BookDeltaRecord`
- sealed chunks only
- Month, Day, and Hour time partitions

Replay ordering is deterministic:

- chunks are ordered by `start_ts`, partition key, generation, and chunk id
- records are read in file order inside each chunk
- equal timestamps are preserved in file order
- replay does not deduplicate records

## API Shape

```rust
let mut cursor = store.replay_trade(
    "BTCUSDT",
    "binance_spot",
    start_ts,
    Some(end_ts),
)?;

loop {
    let batch = cursor.next_batch(1024)?;
    if batch.is_empty() {
        break;
    }

    for trade in batch {
        // Hand the stored record to an upper-layer adapter.
    }
}
```

`next_batch(max_records)` returns at most `max_records`. Passing `0` is an error. After the cursor is exhausted, additional calls return an empty batch.

## Backtest And Paper Usage

Backtest engines should consume replay through an upper-layer `MarketDataReplay`
or equivalent adapter:

```text
Backtest Engine
    |
    v
MarketDataReplay abstraction
    |
    v
FastK Replay Adapter
    |
    v
FastK ReplayCursor
```

FastK does not execute the backtest, own the simulation clock, generate signals,
match orders, or calculate portfolio state.

Paper-trading simulations can replay historical Trade/BBO/BookDelta channels through the same event handlers used by live systems, while keeping execution deterministic.

That does not mean FastK owns paper trading. FastK only provides stored records in deterministic order.

## Future TailCursor

`TailCursor` is future work. It will follow live data after the sealed replay boundary and will need to understand hot segments and WAL recovery state.

Replay intentionally does not follow live appends today.

## Future WAL And Hot Segment

WAL and Hot Segment are not implemented yet.

When they are added, the expected relationship is:

- `ReplayCursor`: deterministic historical scan over sealed chunks
- `TailCursor`: live follow over hot segments and newly sealed chunks
- WAL: crash-safe ingestion log used by live append/recovery

Keeping replay sealed-only first keeps the historical backtest path simple and testable before live ingestion is introduced.
