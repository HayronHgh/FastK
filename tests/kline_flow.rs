//! Kline flow (schema 3) storage contract and OHLCV (schema 1) compatibility.
//! Public API only. Local verification file; not part of the committed core.
use std::path::{Path, PathBuf};

use fastk::{
    bridge_kline_inventory, bridge_read_kline_flow_range, bridge_read_kline_range,
    bridge_write_kline_flow_range, bridge_write_kline_range, CompactionPolicy, FastKError,
    FastKStore, KlineBridgeRecord, KlineFlowRecord, KlineRecord, WriteKlineFlowRequest,
    WriteKlineRequest, KLINE_FLOW_MISSING,
};
use sha2::{Digest, Sha256};
use tempfile::TempDir;

const SYMBOL: &str = "ETHUSDT";
const TF: &str = "1m";
const TF_MS: i64 = 60_000;
const E8: i64 = 100_000_000;
const FEB_2024: i64 = 1_706_745_600_000;
const MAR_2024: i64 = 1_709_251_200_000;
const FLOW_SCHEMA: u32 = 3;

fn ohlcv(ts: i64, step: i64) -> KlineRecord {
    KlineRecord {
        ts,
        open: 300_000 * E8 / 100 + step,
        high: 300_100 * E8 / 100 + step,
        low: 299_900 * E8 / 100 + step,
        close: 300_050 * E8 / 100 + step,
        volume: 1_000 * E8 + step,
    }
}

fn flow(ts: i64, step: i64) -> KlineFlowRecord {
    let base = ohlcv(ts, step);
    KlineFlowRecord::new(
        base,
        Some(3_000_500 * E8 + step),
        Some(10_000 + step),
        Some(base.volume / 2),
        Some(1_500_000 * E8 + step),
    )
}

fn flow_rows(start: i64, count: i64) -> Vec<KlineFlowRecord> {
    (0..count).map(|i| flow(start + i * TF_MS, i)).collect()
}

fn store() -> (TempDir, FastKStore) {
    let temp = TempDir::new().expect("temp dir");
    let mut store = FastKStore::open(temp.path()).expect("open store");
    store.init().expect("init store");
    (temp, store)
}

fn write_flow(store: &mut FastKStore, rows: &[KlineFlowRecord]) {
    store
        .append_kline_flow_range(SYMBOL, TF, rows, TF_MS, E8, E8, E8)
        .expect("flow append");
}

fn series_dir(root: &Path, symbol: &str, tf: &str) -> PathBuf {
    root.join("series").join(symbol).join("kline").join(tf)
}

fn word(bytes: &[u8], offset: usize) -> u64 {
    u64::from_le_bytes(bytes[offset..offset + 8].try_into().unwrap())
}

fn sha256(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

// ---- record ------------------------------------------------------------

#[test]
fn record_layout_and_le_roundtrip_are_stable() {
    assert_eq!(std::mem::size_of::<KlineFlowRecord>(), 80);
    assert_eq!(KlineFlowRecord::BYTE_SIZE, 80);
    let base = ohlcv(FEB_2024, 0);
    let record = KlineFlowRecord::new(base, Some(11_000), Some(42), Some(400), Some(4_400));
    let bytes = record.to_le_bytes();
    assert_eq!(&bytes[..KlineRecord::BYTE_SIZE], &base.to_le_bytes());
    assert_eq!(KlineFlowRecord::from_le_bytes(bytes), record);
    assert_eq!(record.ohlcv(), base);
}

#[test]
fn record_missing_values_are_none_not_zero() {
    let record = KlineFlowRecord::from_ohlcv(ohlcv(FEB_2024, 0));
    assert_eq!(record.quote_volume_opt(), None);
    assert_eq!(record.trade_count_opt(), None);
    assert_eq!(record.taker_buy_volume_opt(), None);
    assert_eq!(record.taker_buy_quote_volume_opt(), None);
    assert_eq!(KlineFlowRecord::default().trade_count, KLINE_FLOW_MISSING);
    record.validate_values().expect("absent values are valid");
    let zero = KlineFlowRecord::new(ohlcv(FEB_2024, 0), Some(0), Some(0), Some(0), Some(0));
    assert_eq!(zero.trade_count_opt(), Some(0));
    zero.validate_values().expect("zero is a present value");
}

#[test]
fn record_validation_rejects_invalid_values() {
    let base = KlineRecord {
        volume: 1_000,
        ..ohlcv(FEB_2024, 0)
    };
    let cases = [
        KlineFlowRecord::new(base, Some(-1), None, None, None),
        KlineFlowRecord::new(base, None, Some(-1), None, None),
        KlineFlowRecord::new(base, None, None, Some(1_001), None),
        KlineFlowRecord::new(base, Some(10), None, None, Some(11)),
        KlineFlowRecord::new(base, None, None, None, Some(1)),
        KlineFlowRecord::new(KlineRecord { volume: -1, ..base }, None, None, None, None),
    ];
    for record in cases {
        let error = record.validate_values().expect_err("invalid record");
        assert!(matches!(error, FastKError::InvalidInput(_)), "{error}");
    }
    KlineFlowRecord::new(base, Some(10), Some(1), Some(1_000), Some(10))
        .validate_values()
        .expect("boundaries are inclusive");
}

// ---- store -------------------------------------------------------------

#[test]
fn flow_rows_roundtrip_through_range_point_latest_and_replay() {
    let (_temp, mut store) = store();
    let mut rows = flow_rows(FEB_2024, 5);
    rows.extend(flow_rows(MAR_2024, 3));
    rows[1] = KlineFlowRecord::new(ohlcv(rows[1].ts, 1), None, Some(0), None, None);
    rows[2] = KlineFlowRecord::new(ohlcv(rows[2].ts, 2), Some(0), None, Some(0), Some(0));
    let summary = store
        .append_kline_flow_range(SYMBOL, TF, &rows, TF_MS, E8, E8, E8)
        .expect("flow append");
    assert!(summary.registered);
    assert_eq!(summary.month_batches_written, 2);

    let meta = store.kline_metadata(SYMBOL, TF).expect("meta");
    assert_eq!(meta.schema_id, FLOW_SCHEMA);
    assert_eq!(meta.record_size, 80);
    assert_eq!(meta.quote_scale, Some(E8));

    let all = store
        .get_kline_flow_range(SYMBOL, TF, i64::MIN, i64::MAX)
        .expect("flow range");
    assert_eq!(all, rows);
    assert_eq!(all[1].quote_volume_opt(), None);
    assert_eq!(all[1].trade_count_opt(), Some(0));
    assert_eq!(all[2].trade_count_opt(), None);
    assert_eq!(all[2].taker_buy_volume_opt(), Some(0));
    assert_eq!(
        store
            .get_kline_flow_range(SYMBOL, TF, rows[3].ts, rows[6].ts)
            .expect("cross-month range"),
        rows[3..=6]
    );
    assert_eq!(
        store.get_kline_flow_at(SYMBOL, TF, rows[4].ts).expect("at"),
        Some(rows[4])
    );
    assert_eq!(
        store
            .get_kline_flow_at(SYMBOL, TF, rows[4].ts + 1)
            .expect("absent ts"),
        None
    );
    assert_eq!(
        store
            .get_kline_flow_latest_n(SYMBOL, TF, 4)
            .expect("latest"),
        rows[4..]
    );

    let mut replay = store
        .replay_kline_flow(SYMBOL, TF, rows[0].ts, None)
        .expect("replay");
    let mut replayed = Vec::new();
    while !replay.is_exhausted() {
        replayed.extend(replay.next_batch(3).expect("replay batch"));
    }
    assert_eq!(replayed, rows);
}

#[test]
fn ohlcv_apis_project_flow_series_without_flow_fields() {
    let (_temp, mut store) = store();
    let rows = flow_rows(FEB_2024, 6);
    write_flow(&mut store, &rows);
    let expected: Vec<_> = rows.iter().map(KlineFlowRecord::ohlcv).collect();

    assert_eq!(
        store
            .get_kline_range(SYMBOL, TF, i64::MIN, i64::MAX)
            .expect("ohlcv range"),
        expected
    );
    assert_eq!(
        store
            .get_kline_range(SYMBOL, TF, rows[1].ts, rows[3].ts)
            .expect("short range"),
        expected[1..=3]
    );
    assert_eq!(
        store.get_kline_latest_n(SYMBOL, TF, 2).expect("latest"),
        expected[4..]
    );
    assert_eq!(
        store.get_kline_at(SYMBOL, TF, rows[5].ts).expect("at"),
        Some(expected[5])
    );
    let mut session = store.read_session();
    session.attach_kline_series(SYMBOL, TF).expect("attach");
    assert_eq!(
        session
            .get_kline_range(SYMBOL, TF, rows[0].ts, rows[5].ts)
            .expect("session range"),
        expected
    );
    assert_eq!(
        store
            .backtest_view()
            .get_kline_range(SYMBOL, TF, rows[0].ts, rows[5].ts)
            .expect("backtest view"),
        expected
    );
}

#[test]
fn large_planned_reads_match_for_flow_and_projected_ohlcv() {
    let (_temp, mut store) = store();
    let mut rows = flow_rows(FEB_2024, 29 * 24 * 60);
    rows.extend(flow_rows(MAR_2024, 31 * 24 * 60));
    write_flow(&mut store, &rows);
    let expected: Vec<_> = rows.iter().map(KlineFlowRecord::ohlcv).collect();
    let (first, last) = (17, rows.len() - 23);

    for threads in [1, 4] {
        store.set_read_threads(threads).expect("threads");
        assert_eq!(
            store
                .get_kline_flow_range(SYMBOL, TF, rows[first].ts, rows[last].ts)
                .expect("planned flow range"),
            rows[first..=last],
            "threads={threads}"
        );
        assert_eq!(
            store
                .get_kline_range(SYMBOL, TF, rows[first].ts, rows[last].ts)
                .expect("planned projected range"),
            expected[first..=last],
            "threads={threads}"
        );
        assert_eq!(
            store
                .get_kline_latest_n(SYMBOL, TF, 50_000)
                .expect("projected tail"),
            expected[expected.len() - 50_000..]
        );
        assert_eq!(
            store
                .get_kline_flow_latest_n(SYMBOL, TF, 50_000)
                .expect("flow tail"),
            rows[rows.len() - 50_000..]
        );
    }
}

#[test]
fn ohlcv_series_reads_flow_fields_as_absent_and_bytes_stay_unchanged() {
    let (temp, mut store) = store();
    let rows: Vec<_> = (0..4).map(|i| ohlcv(FEB_2024 + i * TF_MS, i)).collect();
    store
        .register_kline_series(SYMBOL, TF, TF_MS, E8, E8)
        .expect("register ohlcv");
    store
        .put_kline_chunk(SYMBOL, TF, &rows)
        .expect("write ohlcv");

    let dir = series_dir(temp.path(), SYMBOL, TF);
    let chunk_path = dir.join("chunks").join("2024-02.chunk");
    let meta_path = dir.join("series.meta");
    let chunk_bytes = std::fs::read(&chunk_path).expect("chunk bytes");
    let meta_bytes = std::fs::read(&meta_path).expect("manifest bytes");

    // Schema 1: version 2 header, 48-byte rows whose bytes are the OHLCV words.
    assert_eq!(word(&chunk_bytes, 8), 2, "chunk version");
    assert_eq!(word(&chunk_bytes, 24), 48, "record size");
    assert_eq!(word(&chunk_bytes, 32), 1, "schema id");
    let index_offset = word(&chunk_bytes, 104) as usize;
    let expected_body: Vec<u8> = rows.iter().flat_map(|row| row.to_le_bytes()).collect();
    assert_eq!(&chunk_bytes[128..index_offset], expected_body.as_slice());
    // Digest of the same chunk written by FastK 0.2.0-rc.1 (origin/main 4e5f7a2).
    assert_eq!(
        sha256(&chunk_bytes),
        "74924878693bbd02ed929003edd9c1a1cc972ae790e1b54737b3473579459a36"
    );
    assert_eq!(&meta_bytes[..8], b"FKMETA02");
    assert_eq!(&meta_bytes[8..12], &2u32.to_le_bytes(), "manifest version");
    assert_eq!(store.kline_metadata(SYMBOL, TF).unwrap().quote_scale, None);

    let flow_view = store
        .get_kline_flow_range(SYMBOL, TF, i64::MIN, i64::MAX)
        .expect("flow view of ohlcv series");
    assert_eq!(flow_view.len(), rows.len());
    for (view, row) in flow_view.iter().zip(&rows) {
        assert_eq!(view.ohlcv(), *row);
        assert_eq!(view.quote_volume, KLINE_FLOW_MISSING);
        assert_eq!(view.quote_volume_opt(), None);
        assert_eq!(view.trade_count_opt(), None);
        assert_eq!(view.taker_buy_volume_opt(), None);
        assert_eq!(view.taker_buy_quote_volume_opt(), None);
    }
    assert_eq!(
        store
            .get_kline_flow_at(SYMBOL, TF, rows[2].ts)
            .expect("flow at")
            .map(|row| row.trade_count_opt()),
        Some(None)
    );
    assert_eq!(
        store
            .get_kline_flow_latest_n(SYMBOL, TF, 1)
            .expect("latest")[0]
            .ohlcv(),
        rows[3]
    );
    assert_eq!(
        store
            .get_kline_range(SYMBOL, TF, i64::MIN, i64::MAX)
            .expect("ohlcv range"),
        rows
    );

    drop(store);
    let reader = FastKStore::open_read_only(temp.path()).expect("read-only");
    reader
        .get_kline_flow_range(SYMBOL, TF, i64::MIN, i64::MAX)
        .expect("read-only flow view");
    drop(reader);
    FastKStore::open(temp.path())
        .expect("reopen")
        .init()
        .expect("re-init");
    assert_eq!(std::fs::read(&chunk_path).expect("chunk"), chunk_bytes);
    assert_eq!(std::fs::read(&meta_path).expect("manifest"), meta_bytes);
}

#[test]
fn flow_series_files_use_schema_three_and_manifest_version_three() {
    let (temp, mut store) = store();
    let rows = flow_rows(FEB_2024, 3);
    write_flow(&mut store, &rows);
    let dir = series_dir(temp.path(), SYMBOL, TF);
    let manifest = std::fs::read(dir.join("series.meta")).expect("manifest");
    assert_eq!(&manifest[..8], b"FKMETA02");
    assert_eq!(&manifest[8..12], &3u32.to_le_bytes());
    let chunk = std::fs::read(dir.join("chunks").join("2024-02.chunk")).expect("chunk");
    assert_eq!(word(&chunk, 8), 3, "split chunk version");
    assert_eq!(word(&chunk, 24), 80);
    assert_eq!(word(&chunk, 32), FLOW_SCHEMA as u64);
    let index_offset = word(&chunk, 104) as usize;
    let expected: Vec<u8> = [0..8, 8..40, 40..80]
        .into_iter()
        .flat_map(|section| {
            rows.iter()
                .flat_map(move |row| row.to_le_bytes()[section.clone()].to_vec())
        })
        .collect();
    assert_eq!(&chunk[128..index_offset], expected.as_slice());

    drop(store);
    let reader = FastKStore::open_read_only(temp.path()).expect("reopen");
    let meta = reader.kline_metadata(SYMBOL, TF).expect("decoded v3");
    assert_eq!(meta.quote_scale, Some(E8));
    let mut broken = (*meta).clone();
    broken.quote_scale = None;
    assert!(broken.validate().is_err());
}

#[test]
fn invalid_flow_rows_are_rejected_before_anything_is_written() {
    let (temp, mut store) = store();
    let good = flow(FEB_2024, 0);
    let invalid = [
        KlineFlowRecord {
            taker_buy_volume: good.volume + 1,
            ..good
        },
        KlineFlowRecord {
            taker_buy_quote_volume: good.quote_volume + 1,
            ..good
        },
        KlineFlowRecord {
            quote_volume: KLINE_FLOW_MISSING,
            ..good
        },
        KlineFlowRecord {
            trade_count: -5,
            ..good
        },
        KlineFlowRecord { volume: -1, ..good },
    ];
    for row in invalid {
        let rows = [flow(FEB_2024 - TF_MS, 0), row];
        let error = store
            .append_kline_flow_range(SYMBOL, TF, &rows, TF_MS, E8, E8, E8)
            .expect_err("invalid row");
        assert!(matches!(error, FastKError::InvalidInput(_)), "{error}");
    }
    assert!(!series_dir(temp.path(), SYMBOL, TF).exists());
    assert!(store
        .append_kline_flow_range(SYMBOL, TF, &[good], TF_MS, E8, E8, 0)
        .is_err());

    write_flow(&mut store, &[good]);
    let error = store
        .put_kline_flow_chunk(
            SYMBOL,
            TF,
            &[KlineFlowRecord {
                ts: good.ts + TF_MS,
                trade_count: -1,
                ..good
            }],
        )
        .expect_err("invalid chunk row");
    assert!(matches!(error, FastKError::InvalidInput(_)));
    assert_eq!(
        store
            .get_kline_flow_range(SYMBOL, TF, i64::MIN, i64::MAX)
            .expect("unchanged"),
        [good]
    );
}

#[test]
fn schemas_never_mix_within_a_series() {
    let (_temp, mut store) = store();
    write_flow(&mut store, &flow_rows(FEB_2024, 2));
    let late = ohlcv(FEB_2024 + 10 * TF_MS, 0);
    for error in [
        store
            .put_kline_chunk(SYMBOL, TF, &[late])
            .expect_err("ohlcv into flow"),
        store
            .append_kline_range(SYMBOL, TF, &[late], TF_MS, E8, E8)
            .expect_err("ohlcv range into flow"),
        store
            .register_kline_series(SYMBOL, TF, TF_MS, E8, E8)
            .expect_err("re-register as ohlcv"),
        store
            .register_kline_flow_series(SYMBOL, TF, TF_MS, E8, E8, 100)
            .expect_err("different quote scale"),
    ] {
        assert!(matches!(error, FastKError::InvalidInput(_)), "{error}");
    }
    store
        .register_kline_flow_series(SYMBOL, TF, TF_MS, E8, E8, E8)
        .expect("idempotent flow registration");

    store
        .register_kline_series(SYMBOL, "5m", 300_000, E8, E8)
        .expect("ohlcv series");
    let error = store
        .put_kline_flow_chunk(SYMBOL, "5m", &flow_rows(FEB_2024, 1))
        .expect_err("flow into ohlcv");
    assert!(error.to_string().contains("never rewritten"), "{error}");
    assert!(store
        .append_kline_flow_range(SYMBOL, "5m", &flow_rows(FEB_2024, 1), 300_000, E8, E8, E8)
        .is_err());
}

#[test]
fn flow_month_merge_and_manifest_rebuild_preserve_every_field() {
    let (temp, mut store) = store();
    store.set_compaction_policy(CompactionPolicy {
        auto_merge: false,
        ..CompactionPolicy::default()
    });
    store
        .register_kline_flow_series(SYMBOL, TF, TF_MS, E8, E8, E8)
        .expect("register");
    let rows = flow_rows(FEB_2024, 9);
    for batch in rows.chunks(3) {
        store
            .put_kline_flow_chunk(SYMBOL, TF, batch)
            .expect("delta");
    }
    assert_eq!(store.kline_metadata(SYMBOL, TF).unwrap().chunks.len(), 3);
    store
        .merge_kline_month(SYMBOL, TF, "2024-02")
        .expect("merge flow month");
    assert_eq!(store.kline_metadata(SYMBOL, TF).unwrap().chunks.len(), 1);
    assert_eq!(
        store
            .get_kline_flow_range(SYMBOL, TF, i64::MIN, i64::MAX)
            .expect("merged rows"),
        rows
    );

    drop(store);
    fastk::rebuild_all_manifests_from_fs(temp.path()).expect("rebuild");
    let store = FastKStore::open_read_only(temp.path()).expect("reopen");
    let rebuilt = store.kline_metadata(SYMBOL, TF).expect("rebuilt meta");
    assert_eq!(rebuilt.quote_scale, Some(E8));
    assert_eq!(rebuilt.schema_id, FLOW_SCHEMA);
    assert_eq!(
        store
            .get_kline_flow_range(SYMBOL, TF, i64::MIN, i64::MAX)
            .expect("rows after rebuild"),
        rows
    );
    store.validate_store().expect("validate flow store");
}

// ---- bridge ------------------------------------------------------------

fn flow_request(records: serde_json::Value) -> WriteKlineFlowRequest {
    serde_json::from_value(serde_json::json!({
        "timeframe_ms": 60_000,
        "price_scale": 100,
        "volume_scale": 1_000,
        "quote_scale": 100_000,
        "records": records,
    }))
    .expect("flow request json")
}

#[test]
fn bridge_flow_roundtrips_nulls_and_keeps_ohlcv_contract() {
    let temp = TempDir::new().expect("tempdir");
    let request = flow_request(serde_json::json!([
        {"ts": 1_706_745_660_000_i64, "open": 1, "high": 2, "low": 1, "close": 2, "volume": 50,
         "quote_volume": 75, "trade_count": null, "taker_buy_volume": 50},
        {"ts": 1_706_745_600_000_i64, "open": 1, "high": 2, "low": 1, "close": 2, "volume": 40,
         "quote_volume": 60, "trade_count": 7, "taker_buy_volume": 10,
         "taker_buy_quote_volume": 15}
    ]));
    let written =
        bridge_write_kline_flow_range(temp.path(), SYMBOL, TF, request).expect("flow write");
    assert!(written.registered);
    assert_eq!(written.kline_schema, "ohlcv_flow");

    let read =
        bridge_read_kline_flow_range(temp.path(), SYMBOL, TF, 0, i64::MAX).expect("flow read");
    assert_eq!(read.kline_schema, "ohlcv_flow");
    assert_eq!(read.quote_scale, Some(100_000));
    assert_eq!(
        serde_json::to_value(&read.records).expect("json"),
        serde_json::json!([
            {"ts": 1_706_745_600_000_i64, "open": 1, "high": 2, "low": 1, "close": 2,
             "volume": 40, "quote_volume": 60, "trade_count": 7, "taker_buy_volume": 10,
             "taker_buy_quote_volume": 15},
            {"ts": 1_706_745_660_000_i64, "open": 1, "high": 2, "low": 1, "close": 2,
             "volume": 50, "quote_volume": 75, "trade_count": null, "taker_buy_volume": 50,
             "taker_buy_quote_volume": null}
        ])
    );

    let ohlcv = bridge_read_kline_range(temp.path(), SYMBOL, TF, 0, i64::MAX)
        .expect("ohlcv read of flow series");
    assert_eq!(
        serde_json::to_value(&ohlcv).expect("json")["records"][1],
        serde_json::json!({"ts": 1_706_745_660_000_i64, "open": 1, "high": 2, "low": 1,
                           "close": 2, "volume": 50})
    );
    let inventory =
        serde_json::to_value(bridge_kline_inventory(temp.path(), SYMBOL, TF).expect("inventory"))
            .expect("json");
    assert_eq!(inventory["kline_schema"], "ohlcv_flow");
    assert_eq!(inventory["quote_scale"], 100_000);

    let error = bridge_write_kline_range(
        temp.path(),
        SYMBOL,
        TF,
        WriteKlineRequest {
            timeframe_ms: 60_000,
            price_scale: 100,
            volume_scale: 1_000,
            records: vec![KlineBridgeRecord {
                ts: 1_706_745_720_000,
                open: 1,
                high: 1,
                low: 1,
                close: 1,
                volume: 1,
            }],
        },
    )
    .expect_err("schema mismatch");
    assert_eq!(error.code(), "INVALID_INPUT");
}

#[test]
fn bridge_flow_rejects_invalid_rows_and_reports_ohlcv_series_as_null() {
    let temp = TempDir::new().expect("tempdir");
    for bad in [
        serde_json::json!({"ts": 1, "open": 1, "high": 1, "low": 1, "close": 1, "volume": 5,
                           "taker_buy_volume": 6}),
        serde_json::json!({"ts": 1, "open": 1, "high": 1, "low": 1, "close": 1, "volume": 5,
                           "quote_volume": 5, "taker_buy_quote_volume": 6}),
        serde_json::json!({"ts": 1, "open": 1, "high": 1, "low": 1, "close": 1, "volume": 5,
                           "trade_count": -1}),
        serde_json::json!({"ts": 1, "open": 1, "high": 1, "low": 1, "close": 1, "volume": 5,
                           "quote_volume": i64::MIN}),
    ] {
        let error = bridge_write_kline_flow_range(
            temp.path(),
            SYMBOL,
            TF,
            flow_request(serde_json::json!([bad])),
        )
        .expect_err("invalid flow row");
        assert_eq!(error.code(), "INVALID_INPUT", "{error}");
    }
    let duplicate =
        serde_json::json!({"ts": 1, "open": 1, "high": 1, "low": 1, "close": 1, "volume": 5});
    assert!(bridge_write_kline_flow_range(
        temp.path(),
        SYMBOL,
        TF,
        flow_request(serde_json::json!([duplicate, duplicate])),
    )
    .is_err());

    let mut store = FastKStore::open(temp.path()).expect("open");
    store.init().expect("init");
    store
        .append_kline_range(
            "BTCUSDT",
            TF,
            &[ohlcv(FEB_2024, 0), ohlcv(FEB_2024 + TF_MS, 1)],
            TF_MS,
            E8,
            E8,
        )
        .expect("ohlcv write");
    drop(store);
    let inventory = serde_json::to_string(
        &bridge_kline_inventory(temp.path(), "BTCUSDT", TF).expect("inventory"),
    )
    .expect("json");
    assert!(!inventory.contains("kline_schema") && !inventory.contains("quote_scale"));
    let read = bridge_read_kline_flow_range(temp.path(), "BTCUSDT", TF, 0, i64::MAX)
        .expect("flow view of ohlcv series");
    assert_eq!(read.kline_schema, "ohlcv");
    assert_eq!(read.quote_scale, None);
    assert_eq!(read.records.len(), 2);
    assert!(read.records.iter().all(|row| row.quote_volume.is_none()
        && row.trade_count.is_none()
        && row.taker_buy_volume.is_none()
        && row.taker_buy_quote_volume.is_none()));
}

#[test]
fn split_flow_projects_only_requested_sections() {
    let (_temp, mut store) = store();
    let mut rows = flow_rows(FEB_2024, 29 * 24 * 60);
    rows.extend(flow_rows(MAR_2024, 31 * 24 * 60));
    write_flow(&mut store, &rows);
    store.set_read_threads(4).unwrap();
    store.set_metrics_level(fastk::MetricsLevel::Basic);
    let before = store.metrics_snapshot().bytes_read;
    let prices = store
        .read_kline_prices(SYMBOL, TF, i64::MIN, i64::MAX)
        .unwrap();
    let after_prices = store.metrics_snapshot().bytes_read;
    let activity = store
        .read_kline_activity(SYMBOL, TF, i64::MIN, i64::MAX)
        .unwrap();
    let after_activity = store.metrics_snapshot().bytes_read;
    assert_eq!(after_prices - before, rows.len() as u64 * 40);
    assert_eq!(after_activity - after_prices, rows.len() as u64 * 48);
    assert_eq!(
        prices,
        rows.iter()
            .copied()
            .map(fastk::KlinePriceRecord::from)
            .collect::<Vec<_>>()
    );
    assert_eq!(
        activity,
        rows.iter()
            .copied()
            .map(fastk::KlineActivityRecord::from)
            .collect::<Vec<_>>()
    );
}
