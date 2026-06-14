use std::fs;
use std::path::PathBuf;

use fastk::{
    BacktestKlineBinding, BacktestPreparePlan, CompareOp, FastKStore, KlineRecord, ScalarPredicate,
    ScalarRecord, ScopedScalarBinding,
};

fn main() {
    if let Err(err) = run() {
        eprintln!("backtest_facade_demo failed: {err}");
        std::process::exit(1);
    }
}

fn run() -> fastk::Result<()> {
    // Integration example only: BacktestStoreView is a storage read facade.
    // The example does not run a strategy, simulate orders, or own portfolio logic.
    let root = PathBuf::from("target/backtest-facade-demo");
    if root.exists() {
        fs::remove_dir_all(&root)?;
    }

    let mut store = FastKStore::open(&root)?;
    store.init()?;
    store.register_kline_series("BTCUSDT", "1m", 60_000, 100_000, 100_000)?;
    store.put_kline_chunk("BTCUSDT", "1m", &sample_kline_records())?;

    let scalar_binding = ScopedScalarBinding::indicator("BTCUSDT", "1m", "rsi14");
    store.register_indicator_series("BTCUSDT", "1m", "rsi14")?;
    store.put_indicator_chunk("BTCUSDT", "1m", "rsi14", &sample_scalar_records())?;

    let mut view = store.backtest_view();
    view.initialize(&BacktestPreparePlan {
        kline: vec![BacktestKlineBinding {
            symbol: "BTCUSDT".to_string(),
            timeframe: "1m".to_string(),
        }],
        scalar: vec![scalar_binding.clone()],
        prewarm: true,
    })?;

    let snapshot = view.capability_snapshot()?;
    println!(
        "attached series: {}, scalar capability entries: {}",
        snapshot.inventory.len(),
        snapshot.scalar_capabilities.len()
    );
    println!(
        "health: clean={} issues={} orphans={}",
        snapshot.health.clean_series_count,
        snapshot.health.issue_series_count,
        snapshot.health.orphan_count
    );

    let at = view
        .get_kline_at("BTCUSDT", "1m", 1_706_745_720_000)?
        .expect("kline should exist");
    let latest = view.get_latest_n("BTCUSDT", "1m", 3)?;
    let scalar_range = view.get_indicator_range(
        "BTCUSDT",
        "1m",
        "rsi14",
        1_706_745_660_000,
        1_706_745_840_000,
    )?;
    let zmap_hits = view.find_scalar_timestamps_via_zmap(
        "BTCUSDT",
        "1m",
        "indicator",
        "rsi14",
        &ScalarPredicate {
            op: CompareOp::Between,
            value: 15,
            value2: Some(40),
        },
        1_706_745_600_000,
        1_706_745_900_000,
    )?;

    println!("get_kline_at close={}", at.close);
    println!(
        "latest_n timestamps={:?}",
        latest.iter().map(|row| row.ts).collect::<Vec<_>>()
    );
    println!(
        "scalar_range values={:?}",
        scalar_range.iter().map(|row| row.value).collect::<Vec<_>>()
    );
    println!("zmap hits={zmap_hits:?}");
    Ok(())
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

fn sample_scalar_records() -> Vec<ScalarRecord> {
    vec![
        ScalarRecord {
            ts: 1_706_745_600_000,
            value: 10,
        },
        ScalarRecord {
            ts: 1_706_745_660_000,
            value: 15,
        },
        ScalarRecord {
            ts: 1_706_745_720_000,
            value: 20,
        },
        ScalarRecord {
            ts: 1_706_745_780_000,
            value: 30,
        },
        ScalarRecord {
            ts: 1_706_745_840_000,
            value: 40,
        },
        ScalarRecord {
            ts: 1_706_745_900_000,
            value: 50,
        },
    ]
}
