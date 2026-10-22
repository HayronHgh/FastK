//! Public API consumer: first-touch concurrency, repeated reads and last-owner lease.
use fastk::{FastKStore, KlineRecord};
use std::path::Path;
use std::sync::{Arc, Barrier};
use std::time::Instant;

#[cfg(target_os = "linux")]
fn store_fds(root: &Path) -> Option<usize> {
    Some(
        std::fs::read_dir("/proc/self/fd")
            .unwrap()
            .filter_map(|entry| std::fs::read_link(entry.ok()?.path()).ok())
            .filter(|path| path.starts_with(root))
            .count(),
    )
}

#[cfg(not(target_os = "linux"))]
fn store_fds(_: &Path) -> Option<usize> {
    None
}

#[test]
fn one_store_survives_first_touch_reuse_and_releases_its_last_lease() {
    let waves: usize = std::env::var("FASTK_LIFETIME_WAVES")
        .map(|value| value.parse().unwrap())
        .unwrap_or(12);
    assert!(waves > 0);
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().canonicalize().unwrap();
    let rows: Vec<_> = (0..4)
        .map(|i| KlineRecord {
            ts: 1_704_067_200_000 + i * 60_000,
            open: i64::MIN + i,
            high: i64::MAX - i,
            low: -i,
            close: (1 << 54) + i,
            volume: i,
        })
        .collect();
    {
        let mut writer = FastKStore::open(&root).unwrap();
        for symbol in 0..240 {
            writer
                .append_kline_range(&format!("S{symbol}"), "1m", &rows, 60_000, 1, 1)
                .unwrap();
        }
    }
    assert_eq!(store_fds(&root).unwrap_or(0), 0);
    for workers in [1, 4, 8] {
        let store = Arc::new(FastKStore::open_read_only(&root).unwrap());
        let mut fd_waves = Vec::new();
        let mut first_wave_ns = 0;
        // First wave is completely un-warmed. Subsequent waves churn >64 files.
        for wave in 0..waves {
            let start = Instant::now();
            let barrier = Barrier::new(workers);
            std::thread::scope(|scope| {
                for worker in 0..workers {
                    let store = store.clone();
                    let (rows, barrier) = (&rows, &barrier);
                    scope.spawn(move || {
                        barrier.wait();
                        for _ in 0..20 {
                            for symbol in (worker..240).step_by(workers) {
                                let result = store
                                    .get_kline_range(&format!("S{symbol}"), "1m", 0, i64::MAX)
                                    .unwrap();
                                assert_eq!(&result, rows);
                            }
                        }
                    });
                }
            });
            if wave == 0 {
                first_wave_ns = start.elapsed().as_nanos();
            }
            fd_waves.push(store_fds(&root));
        }
        // Single-store capacity stays bounded after each quiescent wave.
        if cfg!(target_os = "linux") {
            assert!(fd_waves.iter().all(|fds| *fds == Some(65)), "{fd_waves:?}");
        }
        let retained = store.get_kline_range("S0", "1m", 0, i64::MAX).unwrap();
        let last_owner = store.clone();
        drop(store);
        let mut writer = FastKStore::open(&root).unwrap();
        let symbol = format!("AFTER{workers}");
        let error = writer
            .append_kline_range(&symbol, "1m", &rows, 60_000, 1, 1)
            .unwrap_err();
        assert!(error.to_string().contains("store is busy"), "{error}");
        drop(writer);
        assert_eq!(Arc::strong_count(&last_owner), 1);
        drop(last_owner);
        assert_eq!(store_fds(&root).unwrap_or(0), 0);
        let mut writer = FastKStore::open(&root).unwrap();
        writer
            .append_kline_range(&symbol, "1m", &rows, 60_000, 1, 1)
            .unwrap();
        assert_eq!(retained, rows); // Owned data outlives its store.
        drop(writer);
        println!(
            "{}",
            serde_json::json!({"workers": workers, "queries": waves*20*240,
                "first_wave_ns": first_wave_ns, "store_fds_after_waves": fd_waves,
                "fds_after_last_owner": store_fds(&root), "passed": true})
        );
    }
}
