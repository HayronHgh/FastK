use fastk::{
    bridge_read_scalar_range, bridge_write_scalar_range, ScalarBridgeRecord, WriteScalarRequest,
};
use fastk::{FastKStore, ScalarRecord, ScalarSeriesKey};

const START: i64 = 1_706_745_600_000;

#[test]
fn recovery_never_resurrects_older_orphans_over_committed_replacement() {
    let root = tempfile::tempdir().unwrap();
    let mut store = fixture(root.path());
    let series = root.path().join("series/BTCUSDT/feature/test");
    let old = store.scalar_chunk_inventory(&key()).unwrap()[0].clone();
    let mut leftovers = vec![(
        old.relative_path.clone(),
        std::fs::read(series.join(&old.relative_path)).unwrap(),
    )];
    for sidecar in old.sidecars {
        leftovers.push((
            sidecar.relative_path.clone(),
            std::fs::read(series.join(&sidecar.relative_path)).unwrap(),
        ));
    }
    let replacement = vec![ScalarRecord {
        ts: START + 60000,
        value: 999,
    }];
    store
        .put_scalar_chunk(&key(), 60000, &replacement, 2)
        .unwrap();
    // Simulate failed/deferred physical deletion after a successful commit.
    for (path, bytes) in leftovers {
        std::fs::write(series.join(path), bytes).unwrap();
    }
    store.repair_store().unwrap();
    assert_eq!(
        store
            .get_scalar_range(&key(), START, START + 60000)
            .unwrap(),
        replacement
    );
}

#[test]
fn rebuilding_preserves_scales_and_missing_kline_metadata_fails_closed() {
    let root = tempfile::tempdir().unwrap();
    let mut store = FastKStore::open(root.path()).unwrap();
    store.init().unwrap();
    store
        .register_kline_series("BTCUSDT", "1m", 60000, 100_000_000, 1_000_000)
        .unwrap();
    let row = fastk::KlineRecord {
        ts: START,
        open: 10,
        high: 12,
        low: 9,
        close: 11,
        volume: 1,
    };
    store.put_kline_chunk("BTCUSDT", "1m", &[row]).unwrap();
    drop(store);
    fastk::rebuild_all_manifests_from_fs(root.path()).unwrap();
    let response =
        fastk::bridge_read_kline_range(root.path(), "BTCUSDT", "1m", START, START).unwrap();
    assert_eq!(
        (response.price_scale, response.volume_scale),
        (100_000_000, 1_000_000)
    );
    let path = root.path().join("series/BTCUSDT/kline/1m/series.meta");
    let backup = std::fs::read(&path).unwrap();
    std::fs::remove_file(&path).unwrap();
    std::fs::write(root.path().join(".fastk.recovery.pending"), b"pending").unwrap();
    let error = FastKStore::open(root.path()).unwrap_err();
    assert!(error.to_string().contains("semantic metadata"), "{error}");
    assert!(!path.exists(), "recovery must not fabricate scales");
    std::fs::write(&path, backup).unwrap();
    FastKStore::open(root.path()).unwrap();
}

#[test]
fn explicit_repair_reports_the_work_it_performs() {
    let root = tempfile::tempdir().unwrap();
    let mut store = fixture(root.path());
    std::fs::write(root.path().join("unfinished.tmp"), b"partial").unwrap();
    std::fs::write(root.path().join(".fastk.recovery.pending"), b"pending").unwrap();
    let report = store.repair_store().unwrap();
    assert_eq!(report.removed_temp_files, 1);
    assert_eq!(store.repair_store().unwrap().removed_temp_files, 0);
    std::fs::write(root.path().join("unfinished.tmp"), b"partial").unwrap();
    std::fs::write(root.path().join(".fastk.recovery.pending"), b"pending").unwrap();
    assert_eq!(
        fastk::recover_store(root.path())
            .unwrap()
            .removed_temp_files,
        1
    );
}

#[test]
fn publishing_snapshot_finalizes_kline_deltas_for_complete_replay() {
    let root = tempfile::tempdir().unwrap();
    let mut store = FastKStore::open(root.path()).unwrap();
    store.init().unwrap();
    store
        .register_kline_series("BTCUSDT", "1m", 60000, 1, 1)
        .unwrap();
    let first = fastk::KlineRecord {
        ts: START,
        open: 10,
        high: 12,
        low: 9,
        close: 11,
        volume: 1,
    };
    let second = fastk::KlineRecord {
        ts: START + 60000,
        ..first
    };
    store.put_kline_chunk("BTCUSDT", "1m", &[first]).unwrap();
    store.put_kline_chunk("BTCUSDT", "1m", &[second]).unwrap();
    drop(store);
    fastk::seal_snapshot(root.path()).unwrap();
    let reader = FastKStore::open_read_only(root.path()).unwrap();
    let mut replay = reader
        .replay_kline("BTCUSDT", "1m", START, Some(START + 60000))
        .unwrap();
    assert_eq!(replay.next_batch(10).unwrap(), vec![first, second]);
}

fn key() -> ScalarSeriesKey {
    ScalarSeriesKey {
        symbol: "BTCUSDT".into(),
        category: "feature".into(),
        name: "test".into(),
    }
}
fn rows() -> Vec<ScalarRecord> {
    vec![
        ScalarRecord {
            ts: START,
            value: 10,
        },
        ScalarRecord {
            ts: START + 60_000,
            value: 20,
        },
    ]
}
fn fixture(root: &std::path::Path) -> FastKStore {
    let mut store = FastKStore::open(root).unwrap();
    store.init().unwrap();
    store.put_scalar_chunk(&key(), 60_000, &rows(), 2).unwrap();
    store
}

#[test]
fn scalar_bridge_append_preserves_earlier_rows_in_same_month() {
    let root = tempfile::tempdir().unwrap();
    let start = 1_706_745_600_000;
    for offset in [0, 1] {
        bridge_write_scalar_range(
            root.path(),
            "BTCUSDT",
            "1m",
            "feature",
            "test",
            WriteScalarRequest {
                timeframe_ms: Some(60_000),
                records: vec![ScalarBridgeRecord {
                    ts: start + offset * 60_000,
                    value: offset + 10,
                }],
            },
        )
        .unwrap();
    }
    let result = bridge_read_scalar_range(
        root.path(),
        "BTCUSDT",
        "1m",
        "feature",
        "test",
        start,
        start + 60_000,
    )
    .unwrap();
    assert_eq!(
        result.records.len(),
        2,
        "appending must not replace the old month"
    );
    assert_eq!(result.records[0].value, 10);
}

#[test]
fn scalar_append_and_replace_are_distinct_contracts() {
    let root = tempfile::tempdir().unwrap();
    let mut store = fixture(root.path());
    let later = ScalarRecord {
        ts: START + 120_000,
        value: 30,
    };
    store
        .append_scalar_chunk(&key(), 60_000, &[later], 2)
        .unwrap();
    assert_eq!(
        store
            .get_scalar_range(&key(), START, i64::MAX)
            .unwrap()
            .len(),
        3
    );
    assert!(store
        .append_scalar_chunk(&key(), 60_000, &[later], 2)
        .is_err());
    store.put_scalar_chunk(&key(), 60_000, &[later], 2).unwrap();
    assert_eq!(
        store.get_scalar_range(&key(), START, i64::MAX).unwrap(),
        vec![later]
    );
}

#[test]
fn alternating_writers_reload_metadata_after_locking() {
    let root = tempfile::tempdir().unwrap();
    let mut first = fixture(root.path());
    let mut second = FastKStore::open(root.path()).unwrap();
    second.get_scalar_range(&key(), START, i64::MAX).unwrap();
    first
        .append_scalar_chunk(
            &key(),
            60_000,
            &[ScalarRecord {
                ts: START + 120_000,
                value: 30,
            }],
            2,
        )
        .unwrap();
    second
        .append_scalar_chunk(
            &key(),
            60_000,
            &[ScalarRecord {
                ts: START + 180_000,
                value: 40,
            }],
            2,
        )
        .unwrap();
    assert_eq!(
        second
            .get_scalar_range(&key(), START, i64::MAX)
            .unwrap()
            .iter()
            .map(|row| row.value)
            .collect::<Vec<_>>(),
        vec![10, 20, 30, 40]
    );
}

#[test]
fn readonly_handles_share_and_exclude_writers_until_dropped() {
    let root = tempfile::tempdir().unwrap();
    let mut writer = fixture(root.path());
    let mut reader = FastKStore::open_read_only(root.path()).unwrap();
    let other_reader = FastKStore::open_read_only(root.path()).unwrap();
    assert!(reader.init().is_err());
    assert!(writer.put_scalar_chunk(&key(), 60_000, &rows(), 2).is_err());
    drop(reader);
    drop(other_reader);
    writer.put_scalar_chunk(&key(), 60_000, &rows(), 2).unwrap();
}

#[test]
fn recovery_cannot_run_while_another_writer_owns_store() {
    use std::fs::{self, OpenOptions};
    let root = tempfile::tempdir().unwrap();
    fixture(root.path());
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(root.path().join(".fastk.write.lock"))
        .unwrap();
    file.try_lock().unwrap();
    fs::write(root.path().join(".fastk.recovery.pending"), "pending").unwrap();
    fs::write(root.path().join("writer-owned.tmp"), "unfinished").unwrap();
    assert!(FastKStore::open(root.path()).is_err());
    assert!(root.path().join("writer-owned.tmp").exists());
    file.unlock().unwrap();
    drop(file);
    FastKStore::open(root.path()).unwrap();
    assert!(!root.path().join("writer-owned.tmp").exists());
}

#[test]
fn stale_legacy_lock_contents_do_not_block_writer() {
    let root = tempfile::tempdir().unwrap();
    std::fs::write(root.path().join(".fastk.write.lock"), "pid=999999,ts_ms=0").unwrap();
    fixture(root.path());
}

#[test]
fn snapshot_freezes_writes_and_verifies_relocation_and_tampering() {
    let parent = tempfile::tempdir().unwrap();
    let root = parent.path().join("original");
    let mut writer = fixture(&root);
    let sealed = fastk::seal_snapshot(&root).unwrap();
    assert_eq!(sealed, fastk::seal_snapshot(&root).unwrap());
    assert!(writer.put_scalar_chunk(&key(), 60_000, &rows(), 2).is_err());
    assert!(writer.init().is_err());
    assert!(writer.repair_store().is_err());
    assert!(fastk::rebuild_all_manifests_from_fs(&root).is_err());
    drop(writer);
    let moved = parent.path().join("moved");
    std::fs::rename(&root, &moved).unwrap();
    fastk::verify_snapshot(&moved, Some(&sealed.digest)).unwrap();
    assert!(fastk::verify_snapshot(&moved, Some("wrong")).is_err());
    let file = &sealed.files[0];
    let path = moved.join(&file.path);
    let mut bytes = std::fs::read(&path).unwrap();
    bytes[0] ^= 1;
    std::fs::write(path, bytes).unwrap();
    assert!(fastk::verify_snapshot(&moved, Some(&sealed.digest)).is_err());
}

#[test]
fn publishing_registry_binding_is_immutable_but_legacy_status_stays_compatible() {
    let parent = tempfile::tempdir().unwrap();
    let root = parent.path().join("store");
    fixture(&root);
    let registry = fastk::DatasetRegistry::open(parent.path().join("catalog.sqlite")).unwrap();
    let draft = fastk::DatasetManifestRecord {
        dataset_id: "test".into(),
        version: "v1".into(),
        fastk_root: root.to_str().unwrap().into(),
        source: "fixture".into(),
        market: "test".into(),
        start_ts: START,
        end_ts: START + 60_000,
        schema_version: "1".into(),
        checksum: None,
        status: "draft".into(),
        created_at: 1,
    };
    registry.upsert_dataset(&draft).unwrap();
    let published = registry.publish_dataset(&draft).unwrap();
    assert_eq!(registry.publish_dataset(&draft).unwrap(), published);
    registry.upsert_dataset(&published).unwrap();
    let mut changed = published.clone();
    changed.fastk_root = "elsewhere".into();
    assert!(registry.upsert_dataset(&changed).is_err());
    assert!(registry.upsert_dataset(&draft).is_err());
    registry
        .resolve_dataset_ref(&published.dataset_ref())
        .unwrap();
}

#[test]
fn sidecar_same_length_corruption_is_rejected_on_reopen() {
    let root = tempfile::tempdir().unwrap();
    let store = fixture(root.path());
    let chunk = &store.scalar_chunk_inventory(&key()).unwrap()[0];
    let path = root.path().join("series/BTCUSDT/feature/test").join(
        &chunk
            .sidecars
            .iter()
            .find(|sidecar| sidecar.kind == "vix")
            .unwrap()
            .relative_path,
    );
    drop(store);
    let mut bytes = std::fs::read(&path).unwrap();
    bytes[0] ^= 1;
    std::fs::write(path, bytes).unwrap();
    let store = FastKStore::open_read_only(root.path()).unwrap();
    let result = store.query_scalar_predicate(fastk::ScalarPredicateQuery {
        key: key(),
        start_ts: START,
        end_ts: START + 60_000,
        predicate: fastk::ScalarPredicateExpr::Eq(10),
        return_values: true,
    });
    assert!(result.is_err());
}

#[test]
fn cross_month_failure_reports_committed_month_and_preserves_it() {
    let root = tempfile::tempdir().unwrap();
    let mut store = FastKStore::open(root.path()).unwrap();
    store.init().unwrap();
    std::fs::create_dir_all(
        root.path()
            .join("series/BTCUSDT/feature/1m@@test/chunks/2024-03.chunk"),
    )
    .unwrap();
    let error = bridge_write_scalar_range(
        root.path(),
        "BTCUSDT",
        "1m",
        "feature",
        "test",
        WriteScalarRequest {
            timeframe_ms: Some(60000),
            records: vec![
                ScalarBridgeRecord {
                    ts: START,
                    value: 10,
                },
                ScalarBridgeRecord {
                    ts: 1709251200000,
                    value: 20,
                },
            ],
        },
    )
    .unwrap_err();
    assert!(
        error.to_string().contains("completed_months=[\"2024-02\"]"),
        "{error}"
    );
    assert!(
        FastKStore::open_read_only(root.path()).is_err(),
        "unfinished operation requires explicit recovery"
    );
    let _recovered = FastKStore::open(root.path()).unwrap();
    let read = bridge_read_scalar_range(
        root.path(),
        "BTCUSDT",
        "1m",
        "feature",
        "test",
        START,
        START,
    )
    .unwrap();
    assert_eq!(
        read.records,
        vec![ScalarBridgeRecord {
            ts: START,
            value: 10
        }]
    );
}

#[test]
fn process_kill_releases_store_os_lock() {
    use std::io::{BufRead, BufReader};
    use std::process::{Command, Stdio};
    let root = tempfile::tempdir().unwrap();
    let mut store = fixture(root.path());
    let mut child = Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "lock_worker", "--nocapture"])
        .env("FASTK_LOCK_WORKER_ROOT", root.path())
        .stdout(Stdio::piped())
        .spawn()
        .unwrap();
    let output = BufReader::new(child.stdout.take().unwrap());
    let (ready_tx, ready_rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        for line in output.lines() {
            if line.is_ok_and(|line| line.contains("LOCK_READY")) {
                let _ = ready_tx.send(());
                break;
            }
        }
    });
    if let Err(error) = ready_rx.recv_timeout(std::time::Duration::from_secs(10)) {
        let _ = child.kill();
        let _ = child.wait();
        panic!("lock worker not ready: {error}");
    }
    assert!(store.put_scalar_chunk(&key(), 60000, &rows(), 2).is_err());
    child.kill().unwrap();
    child.wait().unwrap();
    store.put_scalar_chunk(&key(), 60000, &rows(), 2).unwrap();
}

#[test]
fn lock_worker() {
    use std::io::Write;
    if let Ok(root) = std::env::var("FASTK_LOCK_WORKER_ROOT") {
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(std::path::Path::new(&root).join(".fastk.write.lock"))
            .unwrap();
        file.try_lock().unwrap();
        println!("LOCK_READY");
        std::io::stdout().flush().unwrap();
        loop {
            std::thread::park();
        }
    }
}

#[cfg(feature = "fault-injection")]
#[test]
fn crash_worker() {
    if let Ok(root) = std::env::var("FASTK_WORKER_ROOT") {
        let mut store = FastKStore::open(root).unwrap();
        let result = store.put_scalar_chunk(
            &key(),
            60_000,
            &[
                ScalarRecord {
                    ts: START,
                    value: 99,
                },
                ScalarRecord {
                    ts: START + 60_000,
                    value: 100,
                },
            ],
            2,
        );
        if std::env::var("FASTK_TEST_IO_AT").is_ok() {
            assert!(result.is_err());
        } else {
            result.unwrap();
        }
    }
}

#[cfg(feature = "fault-injection")]
#[test]
fn injected_flush_sync_and_rename_failures_leave_old_manifest_recoverable() {
    for stage in ["flush:.vix", "sync:.vix", "rename:series.meta"] {
        let root = tempfile::tempdir().unwrap();
        fixture(root.path());
        let old_manifest = root.path().join("series/BTCUSDT/feature/test/series.meta");
        let old_bytes = std::fs::read(&old_manifest).unwrap();
        let status = std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "crash_worker", "--nocapture"])
            .env("FASTK_WORKER_ROOT", root.path())
            .env("FASTK_TEST_IO_AT", stage)
            .status()
            .unwrap();
        assert!(status.success(), "{stage}");
        assert_eq!(
            std::fs::read(&old_manifest).unwrap(),
            old_bytes,
            "old manifest was changed after failed {stage}"
        );
        let store = FastKStore::open(root.path()).unwrap();
        let values: Vec<_> = store
            .get_scalar_range(&key(), START, START + 60000)
            .unwrap()
            .iter()
            .map(|r| r.value)
            .collect();
        assert!(values == vec![10, 20] || values == vec![99, 100]);
    }
}

#[cfg(feature = "fault-injection")]
#[test]
fn abrupt_exit_at_each_scalar_commit_stage_never_mixes_versions() {
    for point in ["scalar_chunk", "scalar_sidecars", "scalar_manifest"] {
        let root = tempfile::tempdir().unwrap();
        fixture(root.path());
        let status = std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "crash_worker", "--nocapture"])
            .env("FASTK_WORKER_ROOT", root.path())
            .env("FASTK_TEST_CRASH_AT", point)
            .status()
            .unwrap();
        assert_eq!(
            status.code(),
            Some(86),
            "checkpoint {point} was not reached"
        );
        let mut reopened = FastKStore::open(root.path()).unwrap();
        let recovered = reopened
            .get_scalar_range(&key(), START, START + 60_000)
            .unwrap();
        let values: Vec<_> = recovered.iter().map(|row| row.value).collect();
        assert!(
            values == vec![10, 20] || values == vec![99, 100],
            "mixed snapshot at {point}: {values:?}"
        );
        reopened.repair_store().unwrap();
        assert_eq!(
            reopened
                .get_scalar_range(&key(), START, START + 60_000)
                .unwrap(),
            recovered
        );
    }
}
