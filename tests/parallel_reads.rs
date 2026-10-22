use std::sync::{Arc, Barrier};

use fastk::{FastKStore, KlineRecord, MetricsLevel, ScalarRecord, ScalarSeriesKey};

const START: i64 = 1_704_067_200_000;

fn rows(count: usize) -> Vec<KlineRecord> {
    (0..count)
        .map(|i| KlineRecord {
            ts: START + i as i64 * 1000,
            open: i as i64,
            high: i as i64 + 1,
            low: -(i as i64),
            close: i as i64,
            volume: i64::MAX - i as i64,
        })
        .collect()
}

#[test]
fn shared_cached_and_uncached_ranges_match_across_threads() {
    let root = tempfile::tempdir().unwrap();
    let expected = Arc::new(rows(100_000));
    let key = ScalarSeriesKey {
        symbol: "A".into(),
        category: "feature".into(),
        name: "x".into(),
    };
    let scalar: Vec<_> = expected
        .iter()
        .map(|r| ScalarRecord {
            ts: r.ts,
            value: r.low,
        })
        .collect();
    let mut writer = FastKStore::open(root.path()).unwrap();
    writer
        .append_kline_range("A", "1s", &expected, 1000, 1, 1)
        .unwrap();
    writer
        .append_scalar_range(&key, &scalar, Some(1000))
        .unwrap();
    drop(writer);
    let mut store = FastKStore::open_read_only(root.path()).unwrap();
    store.set_read_threads(4).unwrap();
    store.set_metrics_level(MetricsLevel::Off);
    let store = Arc::new(store);
    let barrier = Arc::new(Barrier::new(8));
    std::thread::scope(|scope| {
        for id in 0..8 {
            let (store, expected, barrier) = (store.clone(), expected.clone(), barrier.clone());
            scope.spawn(move || {
                barrier.wait();
                for round in 0..2_000 {
                    let start = (id * 1024 + round * 7) % 20_000;
                    let end = start + 127;
                    let actual = store
                        .get_kline_range("A", "1s", expected[start].ts, expected[end - 1].ts)
                        .unwrap();
                    assert_eq!(actual, expected[start..end]);
                }
            });
        }
    });
    assert_eq!(
        store.get_kline_range("A", "1s", START, i64::MAX).unwrap(),
        *expected
    );
    assert_eq!(
        store.get_kline_latest_n("A", "1s", expected.len()).unwrap(),
        *expected
    );
    assert_eq!(
        store.get_scalar_range(&key, START, i64::MAX).unwrap(),
        scalar
    );
}

#[test]
fn rejected_inputs_leave_readable_store_and_compaction_debt_resets() {
    let root = tempfile::tempdir().unwrap();
    let expected = rows(6);
    let mut store = FastKStore::open(root.path()).unwrap();
    store
        .append_kline_range("A", "1s", &expected[..1], 1000, 1, 1)
        .unwrap();
    assert!(store
        .put_kline_chunk("A", "1s", &[expected[2], expected[1]])
        .is_err());
    assert!(store.put_kline_chunk("A", "1s", &[]).is_err());
    assert!(store.register_kline_series("A", "1s", 1000, 2, 1).is_err());
    assert!(store.register_kline_series("B", "1s", 0, 1, 1).is_err());
    for row in &expected[1..5] {
        store.put_kline_chunk("A", "1s", &[*row]).unwrap();
    }
    assert_eq!(
        store.kline_compaction_advice("A", "1s").unwrap()[0].delta_chunk_count,
        4
    );
    store.merge_kline_month("A", "1s", "2024-01").unwrap();
    let debt = store.kline_compaction_advice("A", "1s").unwrap();
    assert_eq!(
        (
            debt[0].delta_chunk_count,
            debt[0].delta_total_bytes,
            debt[0].append_count
        ),
        (0, 0, 0)
    );
    store.put_kline_chunk("A", "1s", &expected[5..]).unwrap();
    assert_eq!(
        store.kline_compaction_advice("A", "1s").unwrap()[0].delta_chunk_count,
        1
    );
    drop(store);
    assert_eq!(
        FastKStore::open_read_only(root.path())
            .unwrap()
            .get_kline_latest_n("A", "1s", 10)
            .unwrap(),
        expected
    );
}

#[test]
fn short_lookup_reads_one_sparse_window_and_bounds_capacity() {
    let root = tempfile::tempdir().unwrap();
    let expected = rows(5000);
    let mut store = FastKStore::open(root.path()).unwrap();
    store
        .append_kline_range("A", "1s", &expected, 1000, 1, 1)
        .unwrap();
    store.set_metrics_level(MetricsLevel::Detailed);
    store.get_kline_at("A", "1s", START).unwrap();
    store.reset_metrics();
    assert_eq!(
        store
            .get_kline_range("A", "1s", START + 1000, START + 1000)
            .unwrap(),
        expected[1..2]
    );
    assert_eq!(store.metrics_snapshot().bytes_read, 128 * 48);
    let out = store
        .get_kline_range("A", "1s", START, START + 2048 * 1000)
        .unwrap();
    assert_eq!(out, expected[..2049]);
    assert!(out.capacity() <= 4096);
}

#[test]
fn adjacent_sparse_blocks_are_read_once_and_trimmed_exactly() {
    let root = tempfile::tempdir().unwrap();
    let expected: Vec<_> = rows(5000).into_iter().filter(|r| r.open != 150).collect();
    let mut store = FastKStore::open(root.path()).unwrap();
    store
        .append_kline_range("A", "1s", &expected, 1000, 1, 1)
        .unwrap();
    store.set_metrics_level(MetricsLevel::Detailed);
    store.get_kline_at("A", "1s", START).unwrap();
    store.reset_metrics();
    let out = store
        .get_kline_range("A", "1s", START + 100_000, START + 226_000)
        .unwrap();
    let wanted: Vec<_> = expected
        .iter()
        .copied()
        .filter(|r| (START + 100_000..=START + 226_000).contains(&r.ts))
        .collect();
    assert_eq!(out, wanted);
    assert_eq!(store.metrics_snapshot().bytes_read, 256 * 48);
    assert!(out.capacity() <= 512);
}

#[test]
fn coalesced_short_read_preserves_rows_from_the_previous_month() {
    let root = tempfile::tempdir().unwrap();
    let boundary = 1_706_745_600_000;
    let mut expected = rows(512);
    for (index, row) in expected.iter_mut().enumerate() {
        row.ts = boundary - 256_000 + index as i64 * 1000;
    }
    let mut store = FastKStore::open(root.path()).unwrap();
    store
        .append_kline_range("A", "1s", &expected, 1000, 1, 1)
        .unwrap();
    assert_eq!(store.kline_chunk_inventory("A", "1s").unwrap().len(), 2);
    store.set_metrics_level(MetricsLevel::Detailed);
    store.reset_metrics();
    let actual = store
        .get_kline_range("A", "1s", boundary - 100_000, boundary + 99_000)
        .unwrap();
    assert_eq!(actual, expected[156..356]);
    assert_eq!(store.metrics_snapshot().bytes_read, 256 * 48);
}

#[test]
fn reversed_sparse_windows_fail_without_panicking_or_partial_rows() {
    let root = tempfile::tempdir().unwrap();
    let expected = rows(5000);
    let mut store = FastKStore::open(root.path()).unwrap();
    store
        .append_kline_range("A", "1s", &expected, 1000, 1, 1)
        .unwrap();
    let chunk = store.kline_chunk_inventory("A", "1s").unwrap().remove(0);
    drop(store);
    let path = root
        .path()
        .join("series/A/kline/1s")
        .join(&chunk.relative_path);
    let mut bytes = std::fs::read(&path).unwrap();
    for (index, position) in [200u64, 256, 0, 128].into_iter().enumerate() {
        let offset = chunk.sparse_index_offset as usize + index * 16 + 8;
        bytes[offset..offset + 8].copy_from_slice(&position.to_le_bytes());
    }
    std::fs::write(path, bytes).unwrap();
    let reader = FastKStore::open_read_only(root.path()).unwrap();
    let error = reader
        .get_kline_range("A", "1s", START + 100_000, START + 300_000)
        .unwrap_err();
    assert_eq!(error.code(), "INVALID_DATA");
    assert!(error
        .to_string()
        .contains("sparse windows are out of order"));
}

#[test]
fn shared_bulk_ranges_and_latest_match_for_all_worker_budgets() {
    let root = tempfile::tempdir().unwrap();
    let expected = rows(600_000);
    let key = ScalarSeriesKey {
        symbol: "A".into(),
        category: "feature".into(),
        name: "x".into(),
    };
    let scalar: Vec<_> = expected
        .iter()
        .map(|r| ScalarRecord {
            ts: r.ts,
            value: r.low,
        })
        .collect();
    let mut writer = FastKStore::open(root.path()).unwrap();
    writer
        .append_kline_range("A", "1s", &expected, 1000, 1, 1)
        .unwrap();
    writer
        .append_scalar_range(&key, &scalar, Some(1000))
        .unwrap();
    drop(writer);
    for workers in [1, 2, 4, 8] {
        let mut store = FastKStore::open_read_only(root.path()).unwrap();
        store.set_read_threads(workers).unwrap();
        store.set_metrics_level(MetricsLevel::Off);
        std::thread::scope(|scope| {
            for _ in 0..4 {
                let (store, expected, scalar, key) = (&store, &expected, &scalar, &key);
                scope.spawn(move || {
                    assert_eq!(
                        store
                            .get_kline_range(
                                "A",
                                "1s",
                                expected[7].ts,
                                expected[expected.len() - 7].ts
                            )
                            .unwrap(),
                        expected[7..expected.len() - 6]
                    );
                    assert_eq!(
                        store
                            .get_kline_latest_n("A", "1s", expected.len() - 3)
                            .unwrap(),
                        expected[3..]
                    );
                    assert_eq!(
                        store
                            .get_scalar_range(key, scalar[3].ts, scalar[scalar.len() - 4].ts)
                            .unwrap(),
                        scalar[3..scalar.len() - 3]
                    );
                });
            }
        });
    }
}

#[test]
fn truncated_planned_read_returns_error_for_every_worker_budget() {
    let root = tempfile::tempdir().unwrap();
    let expected = rows(300_000);
    let mut writer = FastKStore::open(root.path()).unwrap();
    writer
        .append_kline_range("A", "1s", &expected, 1000, 1, 1)
        .unwrap();
    drop(writer);
    let mut store = FastKStore::open_read_only(root.path()).unwrap();
    assert_eq!(
        store.get_kline_at("A", "1s", START).unwrap(),
        Some(expected[0])
    );
    let meta = store.kline_metadata("A", "1s").unwrap();
    let chunk = root
        .path()
        .join("series/A/kline/1s")
        .join(&meta.chunks[0].relative_path);
    // Simulate an external disk fault after the header was cached. No partial
    // initialized result may escape, even when other tasks finished successfully.
    std::fs::OpenOptions::new()
        .write(true)
        .open(chunk)
        .unwrap()
        .set_len(6 * 1024 * 1024)
        .unwrap();
    for workers in [1, 2, 4, 8] {
        store.set_read_threads(workers).unwrap();
        assert!(store.get_kline_range("A", "1s", START, i64::MAX).is_err());
        assert!(store.get_kline_latest_n("A", "1s", expected.len()).is_err());
    }
}
