use tempfile::TempDir;

use crate::chunk::kline_reader;
use crate::engine::store::FastKStore;
use crate::types::{
    BboRecord, BookDeltaRecord, CompareOp, KlineRecord, PartitionPolicy, RecordType,
    ScalarIndexKind, ScalarPredicate, ScalarPredicateExpr, ScalarPredicateQuery, ScalarRecord,
    ScalarSeriesKey, TradeRecord,
};

#[test]
fn chunk_write_and_read_roundtrip() {
    let fixture = TestFixture::new().expect("fixture should be created");
    let series_dir = crate::storage::path::kline_series_dir(fixture.root(), "BTCUSDT", "1m");
    let chunk_path = crate::storage::path::chunk_path(&series_dir, "2024-02.chunk");

    let records = kline_reader::read_all(&chunk_path).expect("chunk should be readable");

    assert_eq!(records, fixture.records());
}

#[test]
fn get_kline_at_finds_exact_record() {
    let fixture = TestFixture::new().expect("fixture should be created");

    let record = fixture
        .store()
        .get_kline_at("BTCUSDT", "1m", 1_706_745_720_000)
        .expect("query should succeed");

    assert_eq!(
        record,
        Some(KlineRecord {
            ts: 1_706_745_720_000,
            open: 102,
            high: 103,
            low: 101,
            close: 102,
            volume: 12,
        })
    );
}

#[test]
fn get_kline_at_returns_none_for_missing_timestamp() {
    let fixture = TestFixture::new().expect("fixture should be created");

    let record = fixture
        .store()
        .get_kline_at("BTCUSDT", "1m", 1_706_745_750_000)
        .expect("query should succeed");

    assert_eq!(record, None);
}

#[test]
fn get_kline_range_returns_inclusive_rows() {
    let fixture = TestFixture::new().expect("fixture should be created");

    let records = fixture
        .store()
        .get_kline_range("BTCUSDT", "1m", 1_706_745_660_000, 1_706_745_780_000)
        .expect("range query should succeed");

    assert_eq!(
        records.iter().map(|record| record.ts).collect::<Vec<_>>(),
        vec![1_706_745_660_000, 1_706_745_720_000, 1_706_745_780_000]
    );
}

#[test]
fn short_range_cross_chunk_returns_only_needed_rows() {
    let fixture = TestFixture::with_two_month_chunks().expect("fixture should be created");

    let records = fixture
        .store()
        .get_kline_range("BTCUSDT", "1m", 1_706_745_840_000, 1_709_251_260_000)
        .expect("range query should succeed");

    assert_eq!(
        records.iter().map(|record| record.ts).collect::<Vec<_>>(),
        vec![
            1_706_745_840_000,
            1_706_745_900_000,
            1_709_251_200_000,
            1_709_251_260_000
        ]
    );
}

#[test]
fn repeated_point_queries_remain_correct() {
    let fixture = TestFixture::new().expect("fixture should be created");

    for _ in 0..20 {
        let record = fixture
            .store()
            .get_kline_at("BTCUSDT", "1m", 1_706_745_840_000)
            .expect("query should succeed");
        assert_eq!(record.map(|record| record.close), Some(104));
    }
}

#[test]
fn get_kline_latest_n_spans_previous_chunks() {
    let fixture = TestFixture::with_two_month_chunks().expect("fixture should be created");

    let records = fixture
        .store()
        .get_kline_latest_n("BTCUSDT", "1m", 4)
        .expect("latest_n query should succeed");

    assert_eq!(
        records.iter().map(|record| record.ts).collect::<Vec<_>>(),
        vec![
            1_706_745_900_000,
            1_709_251_200_000,
            1_709_251_260_000,
            1_709_251_320_000
        ]
    );
}

#[test]
fn put_kline_chunk_rejects_non_increasing_timestamps() {
    let temp_dir = TempDir::new().expect("temp dir should be created");
    let mut store = FastKStore::open(temp_dir.path()).expect("store should open");
    store.init().expect("store should init");
    store
        .register_kline_series("BTCUSDT", "1m", 60_000, 100_000, 100_000)
        .expect("series should register");

    let err = store
        .put_kline_chunk(
            "BTCUSDT",
            "1m",
            &[
                KlineRecord {
                    ts: 1_706_745_720_000,
                    open: 1,
                    high: 2,
                    low: 0,
                    close: 1,
                    volume: 1,
                },
                KlineRecord {
                    ts: 1_706_745_720_000,
                    open: 1,
                    high: 2,
                    low: 0,
                    close: 1,
                    volume: 1,
                },
            ],
        )
        .expect_err("put should reject non-increasing timestamps");

    assert!(matches!(err, crate::FastKError::InvalidInput(_)));
}

#[test]
fn put_kline_chunk_rejects_cross_month_records() {
    let temp_dir = TempDir::new().expect("temp dir should be created");
    let mut store = FastKStore::open(temp_dir.path()).expect("store should open");
    store.init().expect("store should init");
    store
        .register_kline_series("BTCUSDT", "1m", 60_000, 100_000, 100_000)
        .expect("series should register");

    let err = store
        .put_kline_chunk(
            "BTCUSDT",
            "1m",
            &[
                KlineRecord {
                    ts: 1_706_745_600_000,
                    open: 1,
                    high: 2,
                    low: 0,
                    close: 1,
                    volume: 1,
                },
                KlineRecord {
                    ts: 1_709_251_200_000,
                    open: 2,
                    high: 3,
                    low: 1,
                    close: 2,
                    volume: 2,
                },
            ],
        )
        .expect_err("put should reject cross-month records");

    assert!(matches!(err, crate::FastKError::InvalidInput(_)));
}

#[test]
fn append_strictly_increasing_writes_delta_and_merge_compacts_month() {
    let temp_dir = TempDir::new().expect("temp dir should be created");
    let mut store = FastKStore::open(temp_dir.path()).expect("store should open");
    store.init().expect("store should init");
    store
        .register_kline_series("BTCUSDT", "1m", 60_000, 100_000, 100_000)
        .expect("series should register");

    store
        .put_kline_chunk("BTCUSDT", "1m", &base_records())
        .expect("base chunk should write");
    store
        .put_kline_chunk(
            "BTCUSDT",
            "1m",
            &[KlineRecord {
                ts: 1_706_745_960_000,
                open: 106,
                high: 107,
                low: 105,
                close: 106,
                volume: 16,
            }],
        )
        .expect("delta append should write");

    let before_merge = store
        .get_kline_latest_n("BTCUSDT", "1m", 2)
        .expect("latest_n should succeed");
    assert_eq!(
        before_merge
            .iter()
            .map(|record| record.ts)
            .collect::<Vec<_>>(),
        vec![1_706_745_900_000, 1_706_745_960_000]
    );

    store
        .merge_kline_month("BTCUSDT", "1m", "2024-02")
        .expect("merge should succeed");

    let after_merge = store
        .get_kline_latest_n("BTCUSDT", "1m", 2)
        .expect("latest_n should succeed");
    assert_eq!(before_merge, after_merge);
}

#[test]
fn append_overlap_is_rejected() {
    let temp_dir = TempDir::new().expect("temp dir should be created");
    let mut store = FastKStore::open(temp_dir.path()).expect("store should open");
    store.init().expect("store should init");
    store
        .register_kline_series("BTCUSDT", "1m", 60_000, 100_000, 100_000)
        .expect("series should register");
    store
        .put_kline_chunk("BTCUSDT", "1m", &base_records())
        .expect("base chunk should write");

    let err = store
        .put_kline_chunk(
            "BTCUSDT",
            "1m",
            &[KlineRecord {
                ts: 1_706_745_900_000,
                open: 200,
                high: 201,
                low: 199,
                close: 200,
                volume: 20,
            }],
        )
        .expect_err("overlap append should be rejected");

    assert!(matches!(err, crate::FastKError::InvalidInput(_)));
}

#[test]
fn scalar_sidecars_attach_and_query_correctly() {
    let temp_dir = TempDir::new().expect("temp dir should be created");
    let mut store = FastKStore::open(temp_dir.path()).expect("store should open");
    store.init().expect("store should init");
    let key = sample_scalar_key();
    store
        .register_scalar_series(&key, 60_000)
        .expect("scalar series should register");
    let records = sample_scalar_records();
    store
        .put_scalar_chunk(&key, 60_000, &records, 2)
        .expect("scalar chunk should write");

    let zmap_result = store
        .find_scalar_timestamps_via_zmap(
            &key,
            &ScalarPredicate {
                op: CompareOp::Between,
                value: 20,
                value2: Some(40),
            },
            records[0].ts,
            records[records.len() - 1].ts,
        )
        .expect("zmap query should succeed");
    let vix_result = store
        .find_scalar_timestamps_via_vix(
            &key,
            &ScalarPredicate {
                op: CompareOp::Gte,
                value: 30,
                value2: None,
            },
            records[0].ts,
            records[records.len() - 1].ts,
        )
        .expect("vix query should succeed");

    let meta = store.load_scalar_meta(&key).expect("meta should load");
    assert_eq!(meta.chunks.len(), 1);
    assert_eq!(meta.chunks[0].sidecars.len(), 2);
    for sidecar in &meta.chunks[0].sidecars {
        let sidecar_path = crate::storage::path::resolve_relative_path(
            &crate::storage::path::scalar_series_dir(
                temp_dir.path(),
                &key.symbol,
                &key.category,
                &key.name,
            ),
            &sidecar.relative_path,
        );
        assert!(sidecar_path.exists());
    }

    assert_eq!(
        zmap_result,
        vec![1_706_745_720_000, 1_706_745_780_000, 1_706_745_840_000]
    );
    assert_eq!(
        vix_result,
        vec![1_706_745_780_000, 1_706_745_840_000, 1_706_745_900_000]
    );
}

#[test]
fn replacing_scalar_chunk_replaces_sidecars() {
    let temp_dir = TempDir::new().expect("temp dir should be created");
    let mut store = FastKStore::open(temp_dir.path()).expect("store should open");
    store.init().expect("store should init");
    let key = sample_scalar_key();
    store
        .register_scalar_series(&key, 60_000)
        .expect("scalar series should register");
    store
        .put_scalar_chunk(&key, 60_000, &sample_scalar_records(), 2)
        .expect("initial scalar chunk should write");

    let initial_meta = store.load_scalar_meta(&key).expect("meta should load");
    let old_sidecars = initial_meta.chunks[0].sidecars.clone();

    let replacement_records = vec![
        ScalarRecord {
            ts: 1_706_745_600_000,
            value: 5,
        },
        ScalarRecord {
            ts: 1_706_745_660_000,
            value: 10,
        },
        ScalarRecord {
            ts: 1_706_745_720_000,
            value: 15,
        },
    ];
    store
        .put_scalar_chunk(&key, 60_000, &replacement_records, 2)
        .expect("replacement scalar chunk should write");

    let updated_meta = store
        .load_scalar_meta(&key)
        .expect("updated meta should load");
    assert_eq!(updated_meta.chunks.len(), 1);
    assert!(updated_meta.chunks[0].generation > initial_meta.chunks[0].generation);

    let series_dir = crate::storage::path::scalar_series_dir(
        temp_dir.path(),
        &key.symbol,
        &key.category,
        &key.name,
    );
    for sidecar in old_sidecars {
        let sidecar_path =
            crate::storage::path::resolve_relative_path(&series_dir, &sidecar.relative_path);
        assert!(!sidecar_path.exists());
    }
    for sidecar in &updated_meta.chunks[0].sidecars {
        let sidecar_path =
            crate::storage::path::resolve_relative_path(&series_dir, &sidecar.relative_path);
        assert!(sidecar_path.exists());
    }
}

#[test]
fn sidecar_cache_hits_after_repeated_zmap_queries() {
    let temp_dir = TempDir::new().expect("temp dir should be created");
    let mut store = FastKStore::open(temp_dir.path()).expect("store should open");
    store.init().expect("store should init");
    let key = sample_scalar_key();
    let records = sample_scalar_records();
    store
        .register_scalar_series(&key, 60_000)
        .expect("scalar series should register");
    store
        .put_scalar_chunk(&key, 60_000, &records, 2)
        .expect("scalar chunk should write");

    store.reset_metrics();
    let _ = store
        .find_scalar_timestamps_via_zmap(
            &key,
            &ScalarPredicate {
                op: CompareOp::Gte,
                value: 20,
                value2: None,
            },
            records[0].ts,
            records[records.len() - 1].ts,
        )
        .expect("first zmap query should succeed");
    let first = store.metrics_snapshot();
    assert_eq!(first.sidecar_cache_hits, 0);
    assert!(first.sidecar_cache_misses >= 1);

    store.reset_metrics();
    let _ = store
        .find_scalar_timestamps_via_zmap(
            &key,
            &ScalarPredicate {
                op: CompareOp::Gte,
                value: 20,
                value2: None,
            },
            records[0].ts,
            records[records.len() - 1].ts,
        )
        .expect("second zmap query should succeed");
    let second = store.metrics_snapshot();
    assert!(second.sidecar_cache_hits >= 1);
}

#[test]
fn scalar_raw_query_matches_indexed_paths() {
    let temp_dir = TempDir::new().expect("temp dir should be created");
    let mut store = FastKStore::open(temp_dir.path()).expect("store should open");
    store.init().expect("store should init");
    let key = sample_scalar_key();
    let records = sample_scalar_records();
    let predicate = ScalarPredicate {
        op: CompareOp::Between,
        value: 15,
        value2: Some(40),
    };
    store
        .register_scalar_series(&key, 60_000)
        .expect("scalar series should register");
    store
        .put_scalar_chunk(&key, 60_000, &records, 2)
        .expect("scalar chunk should write");

    let raw = store
        .find_scalar_timestamps_raw(
            &key,
            &predicate,
            records[0].ts,
            records[records.len() - 1].ts,
        )
        .expect("raw query should succeed");
    let zmap = store
        .find_scalar_timestamps_via_zmap(
            &key,
            &predicate,
            records[0].ts,
            records[records.len() - 1].ts,
        )
        .expect("zmap query should succeed");
    let vix = store
        .find_scalar_timestamps_via_vix(
            &key,
            &predicate,
            records[0].ts,
            records[records.len() - 1].ts,
        )
        .expect("vix query should succeed");

    assert_eq!(raw, zmap);
    assert_eq!(raw, vix);
}

#[test]
fn scalar_predicate_query_continuous_gt_uses_zmap() {
    let temp_dir = TempDir::new().expect("temp dir should be created");
    let mut store = FastKStore::open(temp_dir.path()).expect("store should open");
    store.init().expect("store should init");
    let key = feature_scalar_key("rsi14");
    let records = sample_scalar_records();
    store
        .register_scalar_series(&key, 60_000)
        .expect("feature series should register");
    store
        .put_scalar_chunk(&key, 60_000, &records, 2)
        .expect("scalar chunk should write");

    let result = store
        .query_scalar_predicate(ScalarPredicateQuery {
            key,
            start_ts: records[0].ts,
            end_ts: records[records.len() - 1].ts,
            predicate: ScalarPredicateExpr::Gt(30),
            return_values: true,
        })
        .expect("predicate query should succeed");

    assert_eq!(
        scalar_matches_to_records(&result.matches),
        vec![
            ScalarRecord {
                ts: 1_706_745_840_000,
                value: 40
            },
            ScalarRecord {
                ts: 1_706_745_900_000,
                value: 50
            },
        ]
    );
    assert_eq!(result.stats.index_used, Some(ScalarIndexKind::ZoneMap));
    assert!(!result.stats.fallback_scan);
    assert_eq!(result.stats.rows_matched, 2);
    assert!(result.stats.blocks_considered > 0);
    assert!(result.stats.rows_checked > 0);
}

#[test]
fn scalar_predicate_query_between_inclusive_and_exclusive() {
    let temp_dir = TempDir::new().expect("temp dir should be created");
    let mut store = FastKStore::open(temp_dir.path()).expect("store should open");
    store.init().expect("store should init");
    let key = feature_scalar_key("ma20_gap");
    let records = sample_scalar_records();
    store
        .register_scalar_series(&key, 60_000)
        .expect("feature series should register");
    store
        .put_scalar_chunk(&key, 60_000, &records, 2)
        .expect("scalar chunk should write");

    let inclusive = store
        .find_scalar_points_by_predicate(
            &key,
            records[0].ts,
            records[records.len() - 1].ts,
            ScalarPredicateExpr::Between {
                min: 20,
                max: 40,
                inclusive: true,
            },
        )
        .expect("inclusive between should succeed");
    let exclusive = store
        .find_scalar_points_by_predicate(
            &key,
            records[0].ts,
            records[records.len() - 1].ts,
            ScalarPredicateExpr::Between {
                min: 20,
                max: 40,
                inclusive: false,
            },
        )
        .expect("exclusive between should succeed");

    assert_eq!(
        inclusive
            .iter()
            .map(|record| record.value)
            .collect::<Vec<_>>(),
        vec![20, 30, 40]
    );
    assert_eq!(
        exclusive
            .iter()
            .map(|record| record.value)
            .collect::<Vec<_>>(),
        vec![30]
    );
}

#[test]
fn scalar_predicate_query_discrete_eq_and_in_set_use_vix() {
    let temp_dir = TempDir::new().expect("temp dir should be created");
    let mut store = FastKStore::open(temp_dir.path()).expect("store should open");
    store.init().expect("store should init");
    let key = feature_scalar_key("state");
    let records = sample_scalar_records();
    store
        .register_scalar_series(&key, 60_000)
        .expect("feature series should register");
    store
        .put_scalar_chunk(&key, 60_000, &records, 2)
        .expect("scalar chunk should write");

    let eq = store
        .query_scalar_predicate(ScalarPredicateQuery {
            key: key.clone(),
            start_ts: records[0].ts,
            end_ts: records[records.len() - 1].ts,
            predicate: ScalarPredicateExpr::Eq(30),
            return_values: false,
        })
        .expect("eq query should succeed");
    let in_set = store
        .query_scalar_predicate(ScalarPredicateQuery {
            key,
            start_ts: records[0].ts,
            end_ts: records[records.len() - 1].ts,
            predicate: ScalarPredicateExpr::InSet(vec![10, 40]),
            return_values: true,
        })
        .expect("in-set query should succeed");

    assert_eq!(eq.matches.len(), 1);
    assert_eq!(eq.matches[0].ts, 1_706_745_780_000);
    assert_eq!(eq.matches[0].value, None);
    assert_eq!(eq.stats.index_used, Some(ScalarIndexKind::ValueIndex));
    assert!(!eq.stats.fallback_scan);
    assert_eq!(
        scalar_matches_to_records(&in_set.matches)
            .iter()
            .map(|record| record.value)
            .collect::<Vec<_>>(),
        vec![10, 40]
    );
    assert_eq!(in_set.stats.index_used, Some(ScalarIndexKind::ValueIndex));
}

#[test]
fn scalar_predicate_query_not_in_set_falls_back_to_scan() {
    let temp_dir = TempDir::new().expect("temp dir should be created");
    let mut store = FastKStore::open(temp_dir.path()).expect("store should open");
    store.init().expect("store should init");
    let key = feature_scalar_key("state");
    let records = sample_scalar_records();
    store
        .register_scalar_series(&key, 60_000)
        .expect("feature series should register");
    store
        .put_scalar_chunk(&key, 60_000, &records, 2)
        .expect("scalar chunk should write");

    let result = store
        .query_scalar_predicate(ScalarPredicateQuery {
            key,
            start_ts: records[0].ts,
            end_ts: records[records.len() - 1].ts,
            predicate: ScalarPredicateExpr::NotInSet(vec![10, 50]),
            return_values: true,
        })
        .expect("not-in-set query should succeed");

    assert_eq!(
        scalar_matches_to_records(&result.matches)
            .iter()
            .map(|record| record.value)
            .collect::<Vec<_>>(),
        vec![15, 20, 30, 40]
    );
    assert_eq!(result.stats.index_used, Some(ScalarIndexKind::FullScan));
    assert!(result.stats.fallback_scan);
    assert_eq!(result.stats.rows_checked, records.len() as u64);
}

#[test]
fn scalar_predicate_query_missing_sidecar_falls_back_to_scan() {
    let temp_dir = TempDir::new().expect("temp dir should be created");
    let mut store = FastKStore::open(temp_dir.path()).expect("store should open");
    store.init().expect("store should init");
    let key = factor_scalar_key("momentum_score");
    let records = sample_scalar_records();
    store
        .register_scalar_series(&key, 60_000)
        .expect("factor series should register");
    store
        .put_scalar_chunk(&key, 60_000, &records, 2)
        .expect("scalar chunk should write");

    let mut meta = (*store.load_scalar_meta(&key).expect("meta should load")).clone();
    for chunk in &mut meta.chunks {
        chunk.sidecars.clear();
    }
    store
        .catalog
        .save_scalar_meta(&meta)
        .expect("meta should save without sidecars");
    store
        .manifest_cache
        .write()
        .expect("manifest cache should lock")
        .clear();

    let result = store
        .query_scalar_predicate(ScalarPredicateQuery {
            key,
            start_ts: records[0].ts,
            end_ts: records[records.len() - 1].ts,
            predicate: ScalarPredicateExpr::Gt(30),
            return_values: false,
        })
        .expect("missing sidecar should fall back");

    assert_eq!(
        result
            .matches
            .iter()
            .map(|entry| entry.ts)
            .collect::<Vec<_>>(),
        vec![1_706_745_840_000, 1_706_745_900_000]
    );
    assert_eq!(result.stats.index_used, Some(ScalarIndexKind::FullScan));
    assert!(result.stats.fallback_scan);
}

#[test]
fn scalar_predicate_query_invalid_and_empty_ranges_are_explicit() {
    let temp_dir = TempDir::new().expect("temp dir should be created");
    let mut store = FastKStore::open(temp_dir.path()).expect("store should open");
    store.init().expect("store should init");
    let key = sample_scalar_key();
    let records = sample_scalar_records();
    store
        .register_scalar_series(&key, 60_000)
        .expect("scalar series should register");
    store
        .put_scalar_chunk(&key, 60_000, &records, 2)
        .expect("scalar chunk should write");

    let err = store
        .query_scalar_predicate(ScalarPredicateQuery {
            key: key.clone(),
            start_ts: records[records.len() - 1].ts,
            end_ts: records[0].ts,
            predicate: ScalarPredicateExpr::Gt(30),
            return_values: false,
        })
        .expect_err("invalid range should fail");
    assert!(matches!(err, crate::FastKError::InvalidInput(_)));

    let empty = store
        .query_scalar_predicate(ScalarPredicateQuery {
            key,
            start_ts: 1_800_000_000_000,
            end_ts: 1_800_000_060_000,
            predicate: ScalarPredicateExpr::Gt(30),
            return_values: false,
        })
        .expect("empty range should succeed");
    assert!(empty.matches.is_empty());
    assert_eq!(empty.stats.rows_checked, 0);
}

#[test]
fn scalar_predicate_query_indicator_compatibility_still_works() {
    let temp_dir = TempDir::new().expect("temp dir should be created");
    let mut store = FastKStore::open(temp_dir.path()).expect("store should open");
    store.init().expect("store should init");
    store
        .register_kline_series("BTCUSDT", "1m", 60_000, 100_000, 100_000)
        .expect("kline should register");
    store
        .put_kline_chunk("BTCUSDT", "1m", &base_records())
        .expect("kline should write");
    store
        .register_indicator_series("BTCUSDT", "1m", "ma20")
        .expect("indicator should register");
    store
        .put_indicator_chunk("BTCUSDT", "1m", "ma20", &sample_scalar_records())
        .expect("indicator should write");

    let key = crate::indicator_series_key("BTCUSDT", "1m", "ma20");
    let points = store
        .find_scalar_points_by_predicate(
            &key,
            1_706_745_600_000,
            1_706_745_900_000,
            ScalarPredicateExpr::Gte(40),
        )
        .expect("indicator predicate should query");
    let indicator_range = store
        .get_indicator_range(
            "BTCUSDT",
            "1m",
            "ma20",
            1_706_745_600_000,
            1_706_745_900_000,
        )
        .expect("existing indicator API should still work");

    assert_eq!(points.len(), 2);
    assert_eq!(indicator_range.len(), 6);
}

#[test]
fn inventory_stats_and_capabilities_are_reported() {
    let temp_dir = TempDir::new().expect("temp dir should be created");
    let mut store = FastKStore::open(temp_dir.path()).expect("store should open");
    store.init().expect("store should init");
    store
        .register_kline_series("BTCUSDT", "1m", 60_000, 100_000, 100_000)
        .expect("kline series should register");
    store
        .put_kline_chunk("BTCUSDT", "1m", &base_records())
        .expect("kline chunk should write");

    let key = sample_scalar_key();
    store
        .register_scalar_series(&key, 60_000)
        .expect("scalar series should register");
    store
        .put_scalar_chunk(&key, 60_000, &sample_scalar_records(), 2)
        .expect("scalar chunk should write");

    let all_series = store.list_series().expect("series list should load");
    let kline_series = store
        .list_kline_series()
        .expect("kline series list should load");
    let scalar_series = store
        .list_scalar_series()
        .expect("scalar series list should load");
    let stats = store.store_stats().expect("store stats should load");
    let capabilities = store
        .scalar_query_capabilities(&key)
        .expect("capabilities should load");

    assert_eq!(all_series.len(), 2);
    assert_eq!(kline_series.len(), 1);
    assert_eq!(scalar_series.len(), 1);
    assert_eq!(stats.series_count, 2);
    assert_eq!(stats.kline_series_count, 1);
    assert_eq!(stats.scalar_series_count, 1);
    assert_eq!(stats.chunk_count, 2);
    assert_eq!(stats.sidecar_count, 2);
    assert!(capabilities.raw_scan);
    assert!(capabilities.has_zmap);
    assert!(capabilities.has_vix);
}

#[test]
fn indicator_series_wrappers_roundtrip_records() {
    let temp_dir = TempDir::new().expect("temp dir should be created");
    let mut store = FastKStore::open(temp_dir.path()).expect("store should open");
    store.init().expect("store should init");
    store
        .register_kline_series("BTCUSDT", "1m", 60_000, 100_000, 100_000)
        .expect("kline series should register");
    store
        .put_kline_chunk("BTCUSDT", "1m", &base_records())
        .expect("kline chunk should write");

    store
        .register_indicator_series("BTCUSDT", "1m", "rsi14")
        .expect("indicator series should register");
    store
        .put_indicator_chunk("BTCUSDT", "1m", "rsi14", &sample_scalar_records())
        .expect("indicator chunk should write");

    let hit = store
        .get_indicator_at("BTCUSDT", "1m", "rsi14", 1_706_745_780_000)
        .expect("indicator point lookup should succeed")
        .expect("indicator record should exist");
    let rows = store
        .get_indicator_range(
            "BTCUSDT",
            "1m",
            "rsi14",
            1_706_745_660_000,
            1_706_745_840_000,
        )
        .expect("indicator range query should succeed");

    assert_eq!(hit.value, 30);
    assert_eq!(rows.len(), 4);
    assert_eq!(rows[0].value, 15);
    assert_eq!(rows[3].value, 40);
}

#[test]
fn indicator_inventory_and_capabilities_are_reported() {
    let temp_dir = TempDir::new().expect("temp dir should be created");
    let mut store = FastKStore::open(temp_dir.path()).expect("store should open");
    store.init().expect("store should init");
    store
        .register_kline_series("BTCUSDT", "1m", 60_000, 100_000, 100_000)
        .expect("kline series should register");
    store
        .put_kline_chunk("BTCUSDT", "1m", &base_records())
        .expect("kline chunk should write");
    store
        .put_indicator_chunk("BTCUSDT", "1m", "rsi14", &sample_scalar_records())
        .expect("indicator chunk should write");

    let indicators = store
        .list_indicators("BTCUSDT", "1m")
        .expect("indicator list should load");
    let inventory = store
        .indicator_inventory("BTCUSDT", "1m")
        .expect("indicator inventory should load");
    let capabilities = store
        .indicator_capabilities("BTCUSDT", "1m", "rsi14")
        .expect("indicator capabilities should load");

    assert_eq!(indicators, vec!["rsi14".to_string()]);
    assert_eq!(inventory.len(), 1);
    assert_eq!(inventory[0].timeframe, "1m");
    assert_eq!(inventory[0].indicator_name, "rsi14");
    assert_eq!(
        inventory[0].record_count,
        sample_scalar_records().len() as u64
    );
    assert!(capabilities.raw_scan);
    assert!(capabilities.has_zmap);
    assert!(capabilities.has_vix);
}

#[test]
fn read_session_inventory_and_health_summary_are_reported() {
    let temp_dir = TempDir::new().expect("temp dir should be created");
    let mut store = FastKStore::open(temp_dir.path()).expect("store should open");
    store.init().expect("store should init");
    store
        .register_kline_series("BTCUSDT", "1m", 60_000, 100_000, 100_000)
        .expect("kline series should register");
    store
        .put_kline_chunk("BTCUSDT", "1m", &base_records())
        .expect("kline chunk should write");

    let key = sample_scalar_key();
    store
        .register_scalar_series(&key, 60_000)
        .expect("scalar series should register");
    store
        .put_scalar_chunk(&key, 60_000, &sample_scalar_records(), 2)
        .expect("scalar chunk should write");

    let mut session = store.read_session();
    session
        .attach_kline_series("BTCUSDT", "1m")
        .expect("kline attach should succeed");
    session
        .attach_scalar_series(&key)
        .expect("scalar attach should succeed");

    let inventory = session
        .attached_inventory()
        .expect("attached inventory should load");
    let health = session
        .attached_health_summary()
        .expect("attached health should load");
    let months = store
        .kline_month_inventory("BTCUSDT", "1m")
        .expect("month inventory should load");
    let scalar_months = store
        .scalar_month_inventory(&key)
        .expect("scalar month inventory should load");
    let scalar_caps = session
        .attached_scalar_capabilities()
        .expect("scalar capabilities should load");

    assert_eq!(inventory.len(), 2);
    assert_eq!(health.series_count, 2);
    assert_eq!(health.issue_series_count, 0);
    assert_eq!(months.len(), 1);
    assert_eq!(months[0].record_count, base_records().len() as u64);
    assert_eq!(scalar_months.len(), 1);
    assert_eq!(scalar_caps.len(), 1);

    session.clear_caches();
    let record = session
        .get_kline_at("BTCUSDT", "1m", 1_706_745_720_000)
        .expect("point query should succeed")
        .expect("record should exist");
    assert_eq!(record.close, 102);
}

#[test]
fn session_cache_clear_retains_manifest_snapshot() {
    let temp_dir = TempDir::new().expect("temp dir should be created");
    let mut store = FastKStore::open(temp_dir.path()).expect("store should open");
    store.init().expect("store should init");
    store
        .register_kline_series("BTCUSDT", "1m", 60_000, 100_000, 100_000)
        .expect("kline series should register");
    store
        .put_kline_chunk("BTCUSDT", "1m", &base_records())
        .expect("kline chunk should write");

    let mut session = store.read_session();
    session
        .attach_kline_series("BTCUSDT", "1m")
        .expect("attach should succeed");

    store.reset_metrics();
    session.clear_caches();
    let _ = session
        .get_kline_at("BTCUSDT", "1m", 1_706_745_720_000)
        .expect("query should succeed");
    let snapshot = store.metrics_snapshot();

    assert_eq!(snapshot.manifest_load_ns, 0);
    assert!(snapshot.chunk_header_cache_misses >= 1);
}

#[test]
fn metrics_level_controls_runtime_overhead_and_detail() {
    let fixture = TestFixture::new().expect("fixture should build");
    let store = fixture.store();

    store.set_metrics_level(crate::MetricsLevel::Off);
    store.reset_metrics();
    let _ = store
        .get_kline_at("BTCUSDT", "1m", 1_706_745_720_000)
        .expect("point query should succeed");
    let off = store.metrics_snapshot();
    assert_eq!(off.metrics_level, crate::MetricsLevel::Off);
    assert_eq!(off.bytes_read, 0);
    assert_eq!(
        off.chunk_header_cache_hits + off.chunk_header_cache_misses,
        0
    );
    assert_eq!(off.point_record_read_ns, 0);

    store.set_metrics_level(crate::MetricsLevel::Basic);
    store.clear_read_caches();
    store.reset_metrics();
    let _ = store
        .get_kline_at("BTCUSDT", "1m", 1_706_745_720_000)
        .expect("point query should succeed");
    let basic = store.metrics_snapshot();
    assert_eq!(basic.metrics_level, crate::MetricsLevel::Basic);
    assert!(basic.bytes_read > 0);
    assert!(basic.chunk_header_cache_hits + basic.chunk_header_cache_misses >= 1);
    assert_eq!(basic.manifest_load_ns, 0);
    assert_eq!(basic.point_record_read_ns, 0);

    store.set_metrics_level(crate::MetricsLevel::Detailed);
    store.clear_read_caches();
    store.reset_metrics();
    let _ = store
        .get_kline_at("BTCUSDT", "1m", 1_706_745_720_000)
        .expect("point query should succeed");
    let detailed = store.metrics_snapshot();
    assert_eq!(detailed.metrics_level, crate::MetricsLevel::Detailed);
    assert!(detailed.chunk_lookup_ns > 0);
    assert!(detailed.point_record_read_ns > 0);
}

#[test]
fn shared_manifest_cache_shortens_reopen_like_query_bootstrap() {
    let temp_dir = TempDir::new().expect("temp dir should be created");
    let mut writer = FastKStore::open(temp_dir.path()).expect("store should open");
    writer.init().expect("store should init");
    writer
        .register_kline_series("BTCUSDT", "1m", 60_000, 100_000, 100_000)
        .expect("series should register");
    writer
        .put_kline_chunk("BTCUSDT", "1m", &base_records())
        .expect("chunk should write");

    let first_reader = FastKStore::open(temp_dir.path()).expect("reader should open");
    first_reader.set_metrics_level(crate::MetricsLevel::Detailed);
    first_reader.reset_metrics();
    let _ = first_reader
        .get_kline_at("BTCUSDT", "1m", 1_706_745_720_000)
        .expect("first query should succeed");
    let first = first_reader.metrics_snapshot();
    assert!(first.manifest_cache_hits >= 1);

    let second_reader = FastKStore::open(temp_dir.path()).expect("reader should reopen");
    second_reader.set_metrics_level(crate::MetricsLevel::Detailed);
    second_reader.reset_metrics();
    let _ = second_reader
        .get_kline_at("BTCUSDT", "1m", 1_706_745_720_000)
        .expect("second query should succeed");
    let second = second_reader.metrics_snapshot();
    assert!(second.manifest_cache_hits >= 1);
    assert_eq!(second.manifest_file_read_ns, 0);
    assert_eq!(second.manifest_decode_ns, 0);
}

#[test]
fn scalar_point_lookup_and_short_range_are_correct() {
    let temp_dir = TempDir::new().expect("temp dir should be created");
    let mut store = FastKStore::open(temp_dir.path()).expect("store should open");
    store.init().expect("store should init");
    let key = sample_scalar_key();
    let records = sample_scalar_records();
    store
        .register_scalar_series(&key, 60_000)
        .expect("scalar series should register");
    store
        .put_scalar_chunk(&key, 60_000, &records, 2)
        .expect("scalar chunk should write");

    let hit = store
        .get_scalar_at(&key, 1_706_745_780_000)
        .expect("scalar point lookup should succeed")
        .expect("scalar record should exist");
    let miss = store
        .get_scalar_at(&key, 1_706_745_750_000)
        .expect("scalar miss should succeed");
    let range = store
        .get_scalar_range(&key, 1_706_745_660_000, 1_706_745_840_000)
        .expect("scalar short range should succeed");

    assert_eq!(hit.value, 30);
    assert!(miss.is_none());
    assert_eq!(
        range.iter().map(|record| record.ts).collect::<Vec<_>>(),
        vec![
            1_706_745_660_000,
            1_706_745_720_000,
            1_706_745_780_000,
            1_706_745_840_000
        ]
    );
}

#[test]
fn high_frequency_record_types_write_read_latest_and_inventory() {
    let temp_dir = TempDir::new().expect("temp dir should be created");
    let mut store = FastKStore::open(temp_dir.path()).expect("store should open");
    store.init().expect("store should init");

    store
        .register_trade_series_with_partition("BTCUSDT", "binance_spot", 0, PartitionPolicy::hour())
        .expect("trade series should register");
    store
        .register_bbo_series("BTCUSDT", "binance_spot", 0)
        .expect("bbo series should register");
    store
        .register_book_delta_series("BTCUSDT", "binance_spot", 0)
        .expect("book delta series should register");

    store
        .put_trade_chunk("BTCUSDT", "binance_spot", &sample_trade_records())
        .expect("trade chunk should write");
    store
        .put_bbo_chunk("BTCUSDT", "binance_spot", &sample_bbo_records())
        .expect("bbo chunk should write");
    store
        .put_book_delta_chunk("BTCUSDT", "binance_spot", &sample_book_delta_records())
        .expect("book delta chunk should write");

    let trades = store
        .get_trade_range(
            "BTCUSDT",
            "binance_spot",
            1_706_745_600_000,
            1_706_749_320_000,
        )
        .expect("trade range should read");
    let bbo = store
        .get_bbo_range(
            "BTCUSDT",
            "binance_spot",
            1_706_745_600_000,
            1_706_832_060_000,
        )
        .expect("bbo range should read");
    let deltas = store
        .get_book_delta_range(
            "BTCUSDT",
            "binance_spot",
            1_706_745_600_000,
            1_706_749_320_000,
        )
        .expect("book delta range should read");

    assert_eq!(trades, sample_trade_records());
    assert_eq!(bbo, sample_bbo_records());
    assert_eq!(deltas, sample_book_delta_records());
    assert_eq!(
        store
            .get_trade_latest_n("BTCUSDT", "binance_spot", 3)
            .expect("latest trades should read")
            .iter()
            .map(|record| record.trade_id)
            .collect::<Vec<_>>(),
        vec![2, 3, 4]
    );
    assert_eq!(
        store
            .get_bbo_latest_n("BTCUSDT", "binance_spot", 1)
            .expect("latest bbo should read")[0]
            .sequence,
        2
    );
    assert_eq!(
        store
            .get_book_delta_latest_n("BTCUSDT", "binance_spot", 2)
            .expect("latest deltas should read")
            .iter()
            .map(|record| record.sequence)
            .collect::<Vec<_>>(),
        vec![102, 103]
    );

    let inventory = store.list_series().expect("inventory should load");
    assert!(inventory
        .iter()
        .any(|entry| { entry.category == "trade" && entry.record_type == RecordType::Trade }));
    assert!(inventory
        .iter()
        .any(|entry| entry.category == "bbo" && entry.record_type == RecordType::Bbo));
    assert!(inventory.iter().any(|entry| {
        entry.category == "book_delta" && entry.record_type == RecordType::BookDelta
    }));
}

#[test]
fn sequence_scan_reports_clean_book_delta_across_partitions() {
    let temp_dir = TempDir::new().expect("temp dir should be created");
    let mut store = FastKStore::open(temp_dir.path()).expect("store should open");
    store.init().expect("store should init");
    store
        .register_book_delta_series("BTCUSDT", "binance_spot", 0)
        .expect("book delta series should register");
    store
        .put_book_delta_chunk("BTCUSDT", "binance_spot", &sample_book_delta_records())
        .expect("book delta chunk should write");

    let report = store
        .scan_book_delta_sequence(
            "BTCUSDT",
            "binance_spot",
            1_706_745_600_000,
            1_706_749_320_000,
        )
        .expect("sequence scan should succeed");

    assert!(report.is_clean());
    assert_eq!(report.record_type, RecordType::BookDelta);
    assert_eq!(report.sequence_field, "sequence");
    assert_eq!(report.scanned_record_count, 4);
    assert_eq!(report.scanned_chunk_count, 2);
    assert_eq!(report.first_sequence, Some(100));
    assert_eq!(report.last_sequence, Some(103));
}

#[test]
fn sequence_scan_reports_gap_duplicate_and_non_monotonic_bbo() {
    let temp_dir = TempDir::new().expect("temp dir should be created");
    let mut store = FastKStore::open(temp_dir.path()).expect("store should open");
    store.init().expect("store should init");
    store
        .register_bbo_series("BTCUSDT", "binance_spot", 0)
        .expect("bbo series should register");
    store
        .put_bbo_chunk("BTCUSDT", "binance_spot", &sample_bbo_sequence_issues())
        .expect("bbo chunk should write");

    let report = store
        .scan_bbo_sequence(
            "BTCUSDT",
            "binance_spot",
            1_706_745_600_000,
            1_706_745_900_000,
        )
        .expect("sequence scan should succeed");

    assert!(!report.is_clean());
    assert_eq!(report.gap_count(), 1);
    assert_eq!(report.duplicate_count(), 1);
    assert_eq!(report.violation_count(), 1);
    assert_eq!(report.gaps[0].previous_sequence, 2);
    assert_eq!(report.gaps[0].expected_sequence, 3);
    assert_eq!(report.gaps[0].next_sequence, 4);
    assert_eq!(report.gaps[0].missing_count, 1);
    assert_eq!(report.duplicates[0].sequence, 4);
    assert_eq!(report.violations[0].previous_sequence, 4);
    assert_eq!(report.violations[0].next_sequence, 3);
}

#[test]
fn sequence_scan_supports_trade_id_and_empty_ranges() {
    let temp_dir = TempDir::new().expect("temp dir should be created");
    let mut store = FastKStore::open(temp_dir.path()).expect("store should open");
    store.init().expect("store should init");
    store
        .register_trade_series("BTCUSDT", "binance_spot", 0)
        .expect("trade series should register");
    store
        .put_trade_chunk("BTCUSDT", "binance_spot", &sample_trade_id_gap_records())
        .expect("trade chunk should write");

    let report = store
        .scan_trade_id_sequence(
            "BTCUSDT",
            "binance_spot",
            1_706_745_600_000,
            1_706_745_780_000,
        )
        .expect("trade id scan should succeed");
    assert_eq!(report.sequence_field, "trade_id");
    assert_eq!(report.gap_count(), 1);
    assert_eq!(report.gaps[0].expected_sequence, 3);
    assert_eq!(report.gaps[0].next_sequence, 4);

    let empty = store
        .scan_trade_id_sequence(
            "BTCUSDT",
            "binance_spot",
            1_800_000_000_000,
            1_800_000_060_000,
        )
        .expect("empty scan should succeed");
    assert!(empty.is_clean());
    assert_eq!(empty.scanned_record_count, 0);
    assert_eq!(empty.first_sequence, None);
}

#[test]
fn sequence_scan_invalid_range_is_error() {
    let temp_dir = TempDir::new().expect("temp dir should be created");
    let mut store = FastKStore::open(temp_dir.path()).expect("store should open");
    store.init().expect("store should init");
    store
        .register_bbo_series("BTCUSDT", "binance_spot", 0)
        .expect("bbo series should register");

    let err = store
        .scan_bbo_sequence(
            "BTCUSDT",
            "binance_spot",
            1_706_745_900_000,
            1_706_745_600_000,
        )
        .expect_err("invalid range should fail");

    assert!(matches!(err, crate::FastKError::InvalidInput(_)));
}

#[test]
fn replay_kline_from_middle_of_chunk() {
    let fixture = TestFixture::new().expect("fixture should be created");
    let mut cursor = fixture
        .store()
        .replay_kline("BTCUSDT", "1m", 1_706_745_710_000, None)
        .expect("replay cursor should open");

    let batch = cursor.next_batch(3).expect("batch should read");

    assert_eq!(
        batch.iter().map(|record| record.ts).collect::<Vec<_>>(),
        vec![1_706_745_720_000, 1_706_745_780_000, 1_706_745_840_000]
    );
}

#[test]
fn replay_scalar_across_month_chunks() {
    let temp_dir = TempDir::new().expect("temp dir should be created");
    let mut store = FastKStore::open(temp_dir.path()).expect("store should open");
    store.init().expect("store should init");
    let key = sample_scalar_key();
    store
        .register_scalar_series(&key, 60_000)
        .expect("scalar series should register");
    store
        .put_scalar_chunk(&key, 60_000, &sample_scalar_records(), 2)
        .expect("feb scalar chunk should write");
    store
        .put_scalar_chunk(&key, 60_000, &sample_march_scalar_records(), 2)
        .expect("mar scalar chunk should write");

    let mut cursor = store
        .replay_scalar(&key, 1_706_745_780_000, None)
        .expect("scalar replay should open");
    let first = cursor.next_batch(3).expect("first batch should read");
    let second = cursor.next_batch(10).expect("second batch should read");

    assert_eq!(
        first.iter().map(|record| record.ts).collect::<Vec<_>>(),
        vec![1_706_745_780_000, 1_706_745_840_000, 1_706_745_900_000]
    );
    assert_eq!(
        second.iter().map(|record| record.ts).collect::<Vec<_>>(),
        vec![1_709_251_200_000, 1_709_251_260_000]
    );
}

#[test]
fn replay_trade_across_hour_partitions() {
    let temp_dir = TempDir::new().expect("temp dir should be created");
    let mut store = FastKStore::open(temp_dir.path()).expect("store should open");
    store.init().expect("store should init");
    store
        .register_trade_series("BTCUSDT", "binance_spot", 0)
        .expect("trade series should register");
    store
        .put_trade_chunk("BTCUSDT", "binance_spot", &sample_trade_records())
        .expect("trade chunk should write");

    let mut cursor = store
        .replay_trade("BTCUSDT", "binance_spot", 1_706_745_600_000, None)
        .expect("trade replay should open");
    let batch = cursor.next_batch(10).expect("trade replay should read");

    assert_eq!(batch, sample_trade_records());
}

#[test]
fn replay_bbo_across_day_partitions() {
    let temp_dir = TempDir::new().expect("temp dir should be created");
    let mut store = FastKStore::open(temp_dir.path()).expect("store should open");
    store.init().expect("store should init");
    store
        .register_bbo_series("BTCUSDT", "binance_spot", 0)
        .expect("bbo series should register");
    store
        .put_bbo_chunk("BTCUSDT", "binance_spot", &sample_bbo_records())
        .expect("bbo chunk should write");

    let mut cursor = store
        .replay_bbo("BTCUSDT", "binance_spot", 1_706_745_600_000, None)
        .expect("bbo replay should open");

    assert_eq!(
        cursor.next_batch(10).expect("bbo replay should read"),
        sample_bbo_records()
    );
}

#[test]
fn replay_book_delta_batches_and_exhaustion_are_stable() {
    let temp_dir = TempDir::new().expect("temp dir should be created");
    let mut store = FastKStore::open(temp_dir.path()).expect("store should open");
    store.init().expect("store should init");
    store
        .register_book_delta_series("BTCUSDT", "binance_spot", 0)
        .expect("book delta series should register");
    store
        .put_book_delta_chunk("BTCUSDT", "binance_spot", &sample_book_delta_records())
        .expect("book delta chunk should write");

    let mut cursor = store
        .replay_book_delta("BTCUSDT", "binance_spot", 1_706_745_600_000, None)
        .expect("book delta replay should open");

    assert_eq!(
        cursor
            .next_batch(2)
            .expect("first batch should read")
            .iter()
            .map(|record| record.sequence)
            .collect::<Vec<_>>(),
        vec![100, 101]
    );
    assert_eq!(
        cursor
            .next_batch(2)
            .expect("second batch should read")
            .iter()
            .map(|record| record.sequence)
            .collect::<Vec<_>>(),
        vec![102, 103]
    );
    assert!(cursor
        .next_batch(2)
        .expect("tail batch should read")
        .is_empty());
    assert!(cursor.next_batch(2).expect("tail stays empty").is_empty());
    assert!(cursor.is_exhausted());
}

#[test]
fn replay_respects_end_ts_and_empty_ranges() {
    let temp_dir = TempDir::new().expect("temp dir should be created");
    let mut store = FastKStore::open(temp_dir.path()).expect("store should open");
    store.init().expect("store should init");
    store
        .register_trade_series("BTCUSDT", "binance_spot", 0)
        .expect("trade series should register");
    store
        .put_trade_chunk("BTCUSDT", "binance_spot", &sample_trade_records())
        .expect("trade chunk should write");

    let mut bounded = store
        .replay_trade(
            "BTCUSDT",
            "binance_spot",
            1_706_745_600_000,
            Some(1_706_745_660_000),
        )
        .expect("bounded replay should open");
    assert_eq!(
        bounded
            .next_batch(10)
            .expect("bounded batch should read")
            .iter()
            .map(|record| record.trade_id)
            .collect::<Vec<_>>(),
        vec![1, 2]
    );

    let mut empty = store
        .replay_trade("BTCUSDT", "binance_spot", 1_800_000_000_000, None)
        .expect("empty replay should open");
    assert!(empty
        .next_batch(10)
        .expect("empty batch should read")
        .is_empty());
    assert!(empty.is_exhausted());
}

#[test]
fn replay_invalid_range_and_zero_batch_are_errors() {
    let fixture = TestFixture::new().expect("fixture should be created");
    let err = fixture
        .store()
        .replay_kline("BTCUSDT", "1m", 1_706_745_900_000, Some(1_706_745_600_000))
        .expect_err("invalid range should fail");
    assert!(matches!(err, crate::FastKError::InvalidInput(_)));

    let mut cursor = fixture
        .store()
        .replay_kline("BTCUSDT", "1m", 1_706_745_600_000, None)
        .expect("cursor should open");
    let err = cursor
        .next_batch(0)
        .expect_err("zero max_records should fail");
    assert!(matches!(err, crate::FastKError::InvalidInput(_)));
}

#[test]
fn replay_does_not_break_existing_range_query() {
    let fixture = TestFixture::new().expect("fixture should be created");
    let mut cursor = fixture
        .store()
        .replay_kline("BTCUSDT", "1m", 1_706_745_600_000, None)
        .expect("cursor should open");
    let replay_rows = cursor.next_batch(100).expect("replay should read");
    let range_rows = fixture
        .store()
        .get_kline_range("BTCUSDT", "1m", 1_706_745_600_000, 1_706_745_900_000)
        .expect("range should read");

    assert_eq!(replay_rows, range_rows);
}

#[test]
fn replay_same_ts_preserves_file_order() {
    let temp_dir = TempDir::new().expect("temp dir should be created");
    let mut store = FastKStore::open(temp_dir.path()).expect("store should open");
    store.init().expect("store should init");
    store
        .register_trade_series("BTCUSDT", "binance_spot", 0)
        .expect("trade series should register");
    let records = sample_same_ts_trade_records();
    store
        .put_trade_chunk("BTCUSDT", "binance_spot", &records)
        .expect("same-ts trade chunk should write");

    let mut cursor = store
        .replay_trade("BTCUSDT", "binance_spot", 1_706_745_600_000, None)
        .expect("trade replay should open");

    assert_eq!(
        cursor
            .next_batch(10)
            .expect("same-ts replay should read")
            .iter()
            .map(|record| record.trade_id)
            .collect::<Vec<_>>(),
        vec![1, 2, 3]
    );
}

#[test]
fn partition_policy_defaults_and_cross_partition_queries_work() {
    let temp_dir = TempDir::new().expect("temp dir should be created");
    let mut store = FastKStore::open(temp_dir.path()).expect("store should open");
    store.init().expect("store should init");
    store
        .register_kline_series("BTCUSDT", "1m", 60_000, 100_000, 100_000)
        .expect("kline series should register");
    let scalar_key = sample_scalar_key();
    store
        .register_scalar_series(&scalar_key, 60_000)
        .expect("scalar series should register");
    store
        .register_trade_series_with_partition("ETHUSDT", "binance_spot", 0, PartitionPolicy::hour())
        .expect("trade series should register");
    store
        .put_trade_chunk("ETHUSDT", "binance_spot", &sample_trade_records())
        .expect("trade chunk should write");

    assert_eq!(
        store
            .load_kline_meta("BTCUSDT", "1m")
            .expect("kline meta should load")
            .chunk_unit,
        "month"
    );
    assert_eq!(
        store
            .load_scalar_meta(&scalar_key)
            .expect("scalar meta should load")
            .chunk_unit,
        "month"
    );
    let trade_meta = store
        .load_fixed_meta("ETHUSDT", "trade", "binance_spot")
        .expect("trade meta should load");
    assert_eq!(trade_meta.chunk_unit, "hour");
    assert_eq!(trade_meta.chunks.len(), 2);
    assert_eq!(
        store
            .get_trade_range(
                "ETHUSDT",
                "binance_spot",
                1_706_745_600_000,
                1_706_749_320_000,
            )
            .expect("trade range should read")
            .len(),
        4
    );
}

#[test]
fn session_prewarm_and_reset_modes_are_explicit() {
    let temp_dir = TempDir::new().expect("temp dir should be created");
    let mut store = FastKStore::open(temp_dir.path()).expect("store should open");
    store.init().expect("store should init");
    store.set_metrics_level(crate::MetricsLevel::Detailed);
    store
        .register_kline_series("BTCUSDT", "1m", 60_000, 100_000, 100_000)
        .expect("kline series should register");
    store
        .put_kline_chunk("BTCUSDT", "1m", &base_records())
        .expect("kline chunk should write");
    let key = sample_scalar_key();
    store
        .register_scalar_series(&key, 60_000)
        .expect("scalar series should register");
    store
        .put_scalar_chunk(&key, 60_000, &sample_scalar_records(), 2)
        .expect("scalar chunk should write");

    let mut session = store.read_session();
    session
        .attach_kline_series("BTCUSDT", "1m")
        .expect("attach should succeed");
    session
        .attach_scalar_series(&key)
        .expect("scalar attach should succeed");
    store.reset_metrics();
    session.prewarm().expect("prewarm should succeed");
    let warmed = store.metrics_snapshot();
    assert!(warmed.session_prewarm_ns > 0);

    session.reset(crate::SessionResetMode::QueryOnly);
    let query_only = session.cache_summary();
    assert_eq!(query_only.attached_kline_count, 1);
    assert_eq!(query_only.attached_scalar_count, 1);

    session.reset(crate::SessionResetMode::FullDetach);
    let detached = session.cache_summary();
    assert_eq!(detached.attached_kline_count, 0);
    assert_eq!(detached.attached_scalar_count, 0);
    assert!(session
        .attached_inventory()
        .expect("inventory should load")
        .is_empty());
}

struct TestFixture {
    temp_dir: TempDir,
    store: FastKStore,
    records: Vec<KlineRecord>,
}

impl TestFixture {
    fn new() -> crate::Result<Self> {
        let temp_dir = TempDir::new().map_err(crate::FastKError::from)?;
        let mut store = FastKStore::open(temp_dir.path())?;
        store.init()?;
        store.register_kline_series("BTCUSDT", "1m", 60_000, 100_000, 100_000)?;

        let records = base_records();
        store.put_kline_chunk("BTCUSDT", "1m", &records)?;

        Ok(Self {
            temp_dir,
            store,
            records,
        })
    }

    fn with_two_month_chunks() -> crate::Result<Self> {
        let temp_dir = TempDir::new().map_err(crate::FastKError::from)?;
        let mut store = FastKStore::open(temp_dir.path())?;
        store.init()?;
        store.register_kline_series("BTCUSDT", "1m", 60_000, 100_000, 100_000)?;

        let mut records = base_records();
        store.put_kline_chunk("BTCUSDT", "1m", &records)?;
        let march_records = vec![
            KlineRecord {
                ts: 1_709_251_200_000,
                open: 106,
                high: 107,
                low: 105,
                close: 106,
                volume: 16,
            },
            KlineRecord {
                ts: 1_709_251_260_000,
                open: 107,
                high: 108,
                low: 106,
                close: 107,
                volume: 17,
            },
            KlineRecord {
                ts: 1_709_251_320_000,
                open: 108,
                high: 109,
                low: 107,
                close: 108,
                volume: 18,
            },
        ];
        store.put_kline_chunk("BTCUSDT", "1m", &march_records)?;
        records.extend_from_slice(&march_records);

        Ok(Self {
            temp_dir,
            store,
            records,
        })
    }

    fn root(&self) -> &std::path::Path {
        self.temp_dir.path()
    }

    fn store(&self) -> &FastKStore {
        &self.store
    }

    fn records(&self) -> Vec<KlineRecord> {
        self.records.clone()
    }
}

fn base_records() -> Vec<KlineRecord> {
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

fn sample_scalar_key() -> ScalarSeriesKey {
    ScalarSeriesKey {
        symbol: "BTCUSDT".to_string(),
        category: "indicator".to_string(),
        name: "rsi14".to_string(),
    }
}

fn feature_scalar_key(name: &str) -> ScalarSeriesKey {
    ScalarSeriesKey {
        symbol: "BTCUSDT".to_string(),
        category: "feature".to_string(),
        name: format!("1m@@{name}"),
    }
}

fn factor_scalar_key(name: &str) -> ScalarSeriesKey {
    ScalarSeriesKey {
        symbol: "BTCUSDT".to_string(),
        category: "factor".to_string(),
        name: format!("1m@@{name}"),
    }
}

fn scalar_matches_to_records(matches: &[crate::ScalarPredicateMatch]) -> Vec<ScalarRecord> {
    matches
        .iter()
        .filter_map(|entry| {
            entry.value.map(|value| ScalarRecord {
                ts: entry.ts,
                value,
            })
        })
        .collect()
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

fn sample_trade_records() -> Vec<TradeRecord> {
    vec![
        TradeRecord {
            ts: 1_706_745_600_000,
            recv_ts: 1_706_745_600_010,
            trade_id: 1,
            price: 10_000,
            qty: 100,
            side: 1,
            flags: 0,
            _pad: [0; 6],
        },
        TradeRecord {
            ts: 1_706_745_660_000,
            recv_ts: 1_706_745_660_010,
            trade_id: 2,
            price: 10_010,
            qty: 101,
            side: -1,
            flags: 0,
            _pad: [0; 6],
        },
        TradeRecord {
            ts: 1_706_749_200_000,
            recv_ts: 1_706_749_200_010,
            trade_id: 3,
            price: 10_020,
            qty: 102,
            side: 1,
            flags: 1,
            _pad: [0; 6],
        },
        TradeRecord {
            ts: 1_706_749_260_000,
            recv_ts: 1_706_749_260_010,
            trade_id: 4,
            price: 10_030,
            qty: 103,
            side: 0,
            flags: 1,
            _pad: [0; 6],
        },
    ]
}

fn sample_trade_id_gap_records() -> Vec<TradeRecord> {
    vec![
        TradeRecord {
            ts: 1_706_745_600_000,
            recv_ts: 1_706_745_600_010,
            trade_id: 1,
            price: 10_000,
            qty: 100,
            side: 1,
            flags: 0,
            _pad: [0; 6],
        },
        TradeRecord {
            ts: 1_706_745_660_000,
            recv_ts: 1_706_745_660_010,
            trade_id: 2,
            price: 10_010,
            qty: 101,
            side: -1,
            flags: 0,
            _pad: [0; 6],
        },
        TradeRecord {
            ts: 1_706_745_720_000,
            recv_ts: 1_706_745_720_010,
            trade_id: 4,
            price: 10_020,
            qty: 102,
            side: 1,
            flags: 0,
            _pad: [0; 6],
        },
    ]
}

fn sample_same_ts_trade_records() -> Vec<TradeRecord> {
    vec![
        TradeRecord {
            ts: 1_706_745_600_000,
            recv_ts: 1_706_745_600_010,
            trade_id: 1,
            price: 10_000,
            qty: 100,
            side: 1,
            flags: 0,
            _pad: [0; 6],
        },
        TradeRecord {
            ts: 1_706_745_600_000,
            recv_ts: 1_706_745_600_011,
            trade_id: 2,
            price: 10_001,
            qty: 101,
            side: -1,
            flags: 0,
            _pad: [0; 6],
        },
        TradeRecord {
            ts: 1_706_745_660_000,
            recv_ts: 1_706_745_660_010,
            trade_id: 3,
            price: 10_010,
            qty: 102,
            side: 1,
            flags: 0,
            _pad: [0; 6],
        },
    ]
}

fn sample_bbo_records() -> Vec<BboRecord> {
    vec![
        BboRecord {
            ts: 1_706_745_600_000,
            recv_ts: 1_706_745_600_010,
            bid_price: 9_990,
            bid_qty: 100,
            ask_price: 10_010,
            ask_qty: 101,
            sequence: 1,
        },
        BboRecord {
            ts: 1_706_832_000_000,
            recv_ts: 1_706_832_000_010,
            bid_price: 10_090,
            bid_qty: 110,
            ask_price: 10_110,
            ask_qty: 111,
            sequence: 2,
        },
    ]
}

fn sample_bbo_sequence_issues() -> Vec<BboRecord> {
    vec![
        BboRecord {
            ts: 1_706_745_600_000,
            recv_ts: 1_706_745_600_010,
            bid_price: 9_990,
            bid_qty: 100,
            ask_price: 10_010,
            ask_qty: 101,
            sequence: 1,
        },
        BboRecord {
            ts: 1_706_745_660_000,
            recv_ts: 1_706_745_660_010,
            bid_price: 9_991,
            bid_qty: 100,
            ask_price: 10_011,
            ask_qty: 101,
            sequence: 2,
        },
        BboRecord {
            ts: 1_706_745_720_000,
            recv_ts: 1_706_745_720_010,
            bid_price: 9_992,
            bid_qty: 100,
            ask_price: 10_012,
            ask_qty: 101,
            sequence: 4,
        },
        BboRecord {
            ts: 1_706_745_780_000,
            recv_ts: 1_706_745_780_010,
            bid_price: 9_993,
            bid_qty: 100,
            ask_price: 10_013,
            ask_qty: 101,
            sequence: 4,
        },
        BboRecord {
            ts: 1_706_745_840_000,
            recv_ts: 1_706_745_840_010,
            bid_price: 9_994,
            bid_qty: 100,
            ask_price: 10_014,
            ask_qty: 101,
            sequence: 3,
        },
    ]
}

fn sample_book_delta_records() -> Vec<BookDeltaRecord> {
    vec![
        BookDeltaRecord {
            ts: 1_706_745_600_000,
            recv_ts: 1_706_745_600_010,
            sequence: 100,
            price: 9_990,
            qty: 10,
            side: 1,
            action: 1,
            level: 1,
            flags: 0,
        },
        BookDeltaRecord {
            ts: 1_706_745_660_000,
            recv_ts: 1_706_745_660_010,
            sequence: 101,
            price: 10_010,
            qty: 0,
            side: -1,
            action: 2,
            level: 1,
            flags: 0,
        },
        BookDeltaRecord {
            ts: 1_706_749_200_000,
            recv_ts: 1_706_749_200_010,
            sequence: 102,
            price: 10_020,
            qty: 12,
            side: 1,
            action: 1,
            level: 2,
            flags: 1,
        },
        BookDeltaRecord {
            ts: 1_706_749_260_000,
            recv_ts: 1_706_749_260_010,
            sequence: 103,
            price: 10_030,
            qty: 13,
            side: -1,
            action: 1,
            level: 2,
            flags: 1,
        },
    ]
}

fn sample_march_scalar_records() -> Vec<ScalarRecord> {
    vec![
        ScalarRecord {
            ts: 1_709_251_200_000,
            value: 60,
        },
        ScalarRecord {
            ts: 1_709_251_260_000,
            value: 70,
        },
    ]
}
