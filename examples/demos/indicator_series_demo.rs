use std::fs;
use std::path::PathBuf;

use fastk::{FastKStore, KlineRecord, ScalarRecord, ScopedScalarBinding};

fn main() {
    if let Err(err) = run() {
        eprintln!("indicator_series_demo failed: {err}");
        std::process::exit(1);
    }
}

fn run() -> fastk::Result<()> {
    let root = PathBuf::from("target/indicator-series-demo");
    if root.exists() {
        fs::remove_dir_all(&root)?;
    }

    let mut store = FastKStore::open(&root)?;
    store.init()?;
    store.register_kline_series("BTCUSDT", "1m", 60_000, 100_000, 100_000)?;
    store.put_kline_chunk("BTCUSDT", "1m", &sample_kline_records())?;

    // Integration example only: FastK stores the result, while this external materializer
    // decides coverage, reads source rows, calculates values, and writes scalar records back.
    let materializer = CloseDeltaMaterializer;
    let output = materializer.output_binding("BTCUSDT", "1m");

    if !store
        .list_indicators("BTCUSDT", "1m")?
        .iter()
        .any(|name| name == &output.name)
    {
        store.register_indicator_series("BTCUSDT", "1m", &output.name)?;

        let klines =
            store.get_kline_range("BTCUSDT", "1m", 1_706_745_600_000, 1_706_745_900_000)?;
        let derived = materializer.materialize_range(&klines);
        store.put_indicator_chunk("BTCUSDT", "1m", &output.name, &derived)?;
    }

    let rows = store.get_indicator_range(
        "BTCUSDT",
        "1m",
        &output.name,
        1_706_745_600_000,
        1_706_745_900_000,
    )?;
    println!(
        "indicator={} values={:?}",
        output.name,
        rows.iter().map(|row| row.value).collect::<Vec<_>>()
    );
    Ok(())
}

struct CloseDeltaMaterializer;

impl CloseDeltaMaterializer {
    fn output_binding(&self, symbol: &str, timeframe: &str) -> ScopedScalarBinding {
        ScopedScalarBinding::indicator(symbol, timeframe, "close_delta")
    }

    fn materialize_range(&self, klines: &[KlineRecord]) -> Vec<ScalarRecord> {
        klines
            .iter()
            .map(|row| ScalarRecord {
                ts: row.ts,
                value: row.close - row.open,
            })
            .collect()
    }
}

fn sample_kline_records() -> Vec<KlineRecord> {
    vec![
        KlineRecord {
            ts: 1_706_745_600_000,
            open: 100,
            high: 101,
            low: 99,
            close: 100,
            volume: 10,
        },
        KlineRecord {
            ts: 1_706_745_660_000,
            open: 101,
            high: 102,
            low: 100,
            close: 101,
            volume: 11,
        },
        KlineRecord {
            ts: 1_706_745_720_000,
            open: 102,
            high: 103,
            low: 101,
            close: 102,
            volume: 12,
        },
        KlineRecord {
            ts: 1_706_745_780_000,
            open: 103,
            high: 104,
            low: 102,
            close: 103,
            volume: 13,
        },
        KlineRecord {
            ts: 1_706_745_840_000,
            open: 104,
            high: 105,
            low: 103,
            close: 104,
            volume: 14,
        },
        KlineRecord {
            ts: 1_706_745_900_000,
            open: 105,
            high: 106,
            low: 104,
            close: 105,
            volume: 15,
        },
    ]
}
