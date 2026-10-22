//! Public Rust API only. Hashing, result comparison and JSON are outside timers.
use fastk::{scoped_scalar_series_key, FastKStore, KlineRecord, MetricsLevel};
use serde::Deserialize;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Barrier};
use std::time::Instant;

#[derive(Deserialize)]
struct Expected {
    rows: usize,
    sha256: String,
}
#[derive(Deserialize)]
struct Query {
    symbol: String,
    start_ts: i64,
    end_ts: i64,
    round: usize,
    expected: Expected,
}

fn verify<const N: usize>(rows: impl Iterator<Item = [i64; N]>, expected: &Value) {
    let mut digest = Sha256::new();
    let mut buffer = Vec::with_capacity(128);
    let mut count = 0;
    for row in rows {
        buffer.clear();
        serde_json::to_writer(&mut buffer, &row[..]).unwrap();
        digest.update(&buffer);
        digest.update(b"\n");
        count += 1;
    }
    assert_eq!(expected["rows"], count);
    assert_eq!(expected["sha256"], format!("{:x}", digest.finalize()));
}

fn fd_count() -> Option<usize> {
    std::fs::read_dir("/proc/self/fd").ok().map(|it| it.count())
}

fn read(store: &FastKStore, q: &Query) -> Vec<KlineRecord> {
    store
        .get_kline_range(&q.symbol, "1m", q.start_ts, q.end_ts)
        .unwrap()
}

fn sample<T: PartialEq>(mut read: impl FnMut() -> Vec<T>) -> (Vec<T>, Vec<u64>, Vec<u64>) {
    let mut warm = Vec::new();
    let mut times = Vec::new();
    let start = Instant::now();
    let expected = read();
    warm.push(start.elapsed().as_nanos() as u64);
    for i in 0..61 {
        let start = Instant::now();
        let rows = read();
        let ns = start.elapsed().as_nanos() as u64;
        if i < 31 {
            warm.push(ns);
        } else {
            times.push(ns);
        }
        assert!(rows == expected);
    }
    (expected, warm, times)
}

fn main() {
    let args: Vec<_> = std::env::args().collect();
    let started = Instant::now();
    let input = std::fs::read(&args[3]).unwrap();
    let workers: usize = args[4].parse().unwrap();
    if args[1] == "bulk" {
        let mut store = FastKStore::open_read_only(&args[2]).unwrap();
        store.set_metrics_level(MetricsLevel::Off);
        store.set_read_threads(workers).unwrap();
        let queries: Vec<Value> = serde_json::from_slice(&input).unwrap();
        let mut results = Vec::new();
        for q in queries {
            let symbol = q["symbol"].as_str().unwrap();
            let start = q["start_ts"].as_i64().unwrap();
            let end = q["end_ts"].as_i64().unwrap();
            let (warm, times) = if let Some(feature) = q["feature"].as_str() {
                let key = scoped_scalar_series_key(symbol, "1m", "feature", feature);
                let (rows, warm, times) =
                    sample(|| store.get_scalar_range(&key, start, end).unwrap());
                verify(rows.iter().map(|r| [r.ts, r.value]), &q["expected"]);
                (warm, times)
            } else {
                let (rows, warm, times) = sample(|| {
                    if q["kind"] == "latest" {
                        store
                            .get_kline_latest_n(symbol, "1m", q["limit"].as_u64().unwrap() as usize)
                            .unwrap()
                    } else {
                        store.get_kline_range(symbol, "1m", start, end).unwrap()
                    }
                });
                verify(
                    rows.iter()
                        .map(|r| [r.ts, r.open, r.high, r.low, r.close, r.volume]),
                    &q["expected"],
                );
                (warm, times)
            };
            results.push(json!({"case":q["case"],"warm_ns":warm,"times_ns":times}));
        }
        println!(
            "{}",
            json!({"workers":workers,"samples":results,"fds":fd_count(),"total_ns":started.elapsed().as_nanos() as u64,"all_results_verified":true})
        );
        return;
    }
    let queries: Vec<Query> = serde_json::from_slice(&input).unwrap();
    assert_eq!(queries.len(), 1430);
    let shared = args[5] == "shared";
    let stores: Vec<_> = (0..if shared { 1 } else { workers })
        .map(|_| {
            let store = FastKStore::open_read_only(&args[2]).unwrap();
            store.set_metrics_level(MetricsLevel::Off);
            Arc::new(store)
        })
        .collect();
    let open_ns = started.elapsed().as_nanos() as u64;
    let barrier = Barrier::new(workers + 1);
    let completed: Vec<_> = (0..10).map(|_| AtomicUsize::new(0)).collect();
    let ended: Vec<_> = (0..10).map(|_| AtomicU64::new(0)).collect();
    let mut fd_samples = Vec::new();
    let (walls, samples) = std::thread::scope(|scope| {
        let mut handles = Vec::new();
        for id in 0..workers {
            let (queries, barrier, completed, ended) = (&queries, &barrier, &completed, &ended);
            let store = stores[if shared { 0 } else { id }].clone();
            handles.push(scope.spawn(move || {
                // Identical one-query bootstrap; remaining first-touch cost stays timed.
                drop(read(&store, &queries[id]));
                let mut samples = Vec::new();
                for group in 0..10 {
                    let assigned: Vec<_> = queries
                        .iter()
                        .enumerate()
                        .filter(|(_, q)| q.round == group % 5)
                        .enumerate()
                        .filter(|(i, _)| i % workers == id)
                        .map(|(_, q)| q)
                        .collect();
                    let mut retained = Vec::with_capacity(assigned.len());
                    barrier.wait();
                    barrier.wait();
                    for (index, q) in assigned {
                        let start = Instant::now();
                        let rows = read(&store, q);
                        samples.push((group, index, start.elapsed().as_nanos() as u64));
                        retained.push((rows, q));
                    }
                    if completed[group].fetch_add(1, Ordering::AcqRel) + 1 == workers {
                        ended[group].store(started.elapsed().as_nanos() as u64, Ordering::Release);
                    }
                    barrier.wait();
                    for (rows, q) in retained {
                        verify(
                            rows.iter()
                                .map(|r| [r.ts, r.open, r.high, r.low, r.close, r.volume]),
                            &json!({"rows":q.expected.rows,"sha256":q.expected.sha256}),
                        );
                    }
                }
                samples
            }));
        }
        let mut walls = Vec::new();
        for group in 0..10 {
            barrier.wait();
            let start = started.elapsed().as_nanos() as u64;
            barrier.wait();
            barrier.wait();
            walls.push(ended[group].load(Ordering::Acquire) - start);
            fd_samples.push(fd_count());
        }
        let samples: Vec<_> = handles
            .into_iter()
            .flat_map(|h| h.join().unwrap())
            .collect();
        (walls, samples)
    });
    assert_eq!(samples.len(), 2860);
    println!(
        "{}",
        json!({"workers":workers,"shared":shared,"open_ns":open_ns,"walls_ns":walls,"samples":samples,"fds":fd_samples,"total_ns":started.elapsed().as_nanos() as u64,"all_results_verified":true})
    );
}
