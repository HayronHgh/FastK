use tempfile::TempDir;

use fastk::{
    bridge_indicator_inventory, bridge_read_indicator_range, bridge_read_kline_range,
    bridge_write_indicator_range, bridge_write_kline_range, BacktestKlineBinding,
    BacktestPreparePlan, FastKStore, KlineRecord, ScopedScalarBinding, WriteIndicatorRequest,
    WriteKlineRequest,
};

#[test]
fn release_smoke_backtest_facade_roundtrip() -> fastk::Result<()> {
    let temp_dir = TempDir::new()?;
    let mut store = FastKStore::open(temp_dir.path())?;
    store.init()?;
    store.register_kline_series("BTCUSDT", "1m", 60_000, 100_000, 100_000)?;
    store.put_kline_chunk("BTCUSDT", "1m", &sample_kline_records())?;
    store.register_indicator_series("BTCUSDT", "1m", "ma20")?;
    store.put_indicator_chunk("BTCUSDT", "1m", "ma20", &sample_indicator_records())?;

    let mut view = store.backtest_view();
    view.initialize(&BacktestPreparePlan {
        kline: vec![BacktestKlineBinding {
            symbol: "BTCUSDT".to_string(),
            timeframe: "1m".to_string(),
        }],
        scalar: vec![ScopedScalarBinding::indicator("BTCUSDT", "1m", "ma20")],
        prewarm: true,
    })?;

    let at = view.get_kline_at("BTCUSDT", "1m", 1_706_745_720_000)?;
    let range = view.get_indicator_range(
        "BTCUSDT",
        "1m",
        "ma20",
        1_706_745_720_000,
        1_706_745_900_000,
    )?;
    let snapshot = view.capability_snapshot()?;
    let cache_summary = view.cache_summary();

    assert_eq!(at.expect("kline should exist").close, 10_030_000);
    assert_eq!(range.len(), 4);
    assert_eq!(snapshot.inventory.len(), 2);
    assert!(cache_summary.attached_kline_count >= 1);
    assert!(cache_summary.attached_scalar_count >= 1);
    Ok(())
}

#[test]
fn release_smoke_bridge_roundtrip_and_auto_register() -> fastk::Result<()> {
    let temp_dir = TempDir::new()?;

    let write_kline = bridge_write_kline_range(
        temp_dir.path(),
        "BTCUSDT",
        "1m",
        WriteKlineRequest {
            timeframe_ms: 60_000,
            price_scale: 100_000,
            volume_scale: 100_000,
            records: sample_kline_records().into_iter().map(Into::into).collect(),
        },
    )?;
    assert!(write_kline.registered);
    assert_eq!(write_kline.written_record_count, 6);

    let empty_indicator = bridge_read_indicator_range(
        temp_dir.path(),
        "BTCUSDT",
        "1m",
        "ma20",
        1_706_745_720_000,
        1_706_745_900_000,
    )?;
    assert!(!empty_indicator.exists);
    assert!(empty_indicator.records.is_empty());

    let write_indicator = bridge_write_indicator_range(
        temp_dir.path(),
        "BTCUSDT",
        "1m",
        "ma20",
        WriteIndicatorRequest {
            records: sample_indicator_records()
                .into_iter()
                .map(Into::into)
                .collect(),
        },
    )?;
    assert!(write_indicator.registered);
    assert_eq!(write_indicator.written_record_count, 4);

    let kline = bridge_read_kline_range(
        temp_dir.path(),
        "BTCUSDT",
        "1m",
        1_706_745_600_000,
        1_706_745_900_000,
    )?;
    let indicator = bridge_read_indicator_range(
        temp_dir.path(),
        "BTCUSDT",
        "1m",
        "ma20",
        1_706_745_720_000,
        1_706_745_900_000,
    )?;
    let inventory = bridge_indicator_inventory(temp_dir.path(), "BTCUSDT", "1m", "ma20")?;

    assert_eq!(kline.records.len(), 6);
    assert!(indicator.exists);
    assert_eq!(indicator.records.len(), 4);
    assert!(inventory.exists);
    assert_eq!(inventory.record_count, 4);
    Ok(())
}

fn sample_kline_records() -> Vec<KlineRecord> {
    vec![
        KlineRecord {
            ts: 1_706_745_600_000,
            open: 10_000_000,
            high: 10_020_000,
            low: 9_980_000,
            close: 10_010_000,
            volume: 10_000,
        },
        KlineRecord {
            ts: 1_706_745_660_000,
            open: 10_010_000,
            high: 10_040_000,
            low: 10_000_000,
            close: 10_020_000,
            volume: 10_200,
        },
        KlineRecord {
            ts: 1_706_745_720_000,
            open: 10_020_000,
            high: 10_050_000,
            low: 10_010_000,
            close: 10_030_000,
            volume: 10_300,
        },
        KlineRecord {
            ts: 1_706_745_780_000,
            open: 10_030_000,
            high: 10_060_000,
            low: 10_020_000,
            close: 10_040_000,
            volume: 10_400,
        },
        KlineRecord {
            ts: 1_706_745_840_000,
            open: 10_040_000,
            high: 10_070_000,
            low: 10_030_000,
            close: 10_050_000,
            volume: 10_500,
        },
        KlineRecord {
            ts: 1_706_745_900_000,
            open: 10_050_000,
            high: 10_080_000,
            low: 10_040_000,
            close: 10_060_000,
            volume: 10_600,
        },
    ]
}

fn sample_indicator_records() -> Vec<fastk::ScalarRecord> {
    vec![
        fastk::ScalarRecord {
            ts: 1_706_745_720_000,
            value: 10_150_000,
        },
        fastk::ScalarRecord {
            ts: 1_706_745_780_000,
            value: 10_250_000,
        },
        fastk::ScalarRecord {
            ts: 1_706_745_840_000,
            value: 10_350_000,
        },
        fastk::ScalarRecord {
            ts: 1_706_745_900_000,
            value: 10_450_000,
        },
    ]
}
