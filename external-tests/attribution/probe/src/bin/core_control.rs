//! Independent public-API control: repeated handles, no Python per read.
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

fn verified(rows: &[KlineRecord], q: &Query) -> bool {
    let mut digest = Sha256::new();
    let mut buffer = Vec::with_capacity(128);
    for r in rows {
        buffer.clear();
        serde_json::to_writer(
            &mut buffer,
            &[r.ts, r.open, r.high, r.low, r.close, r.volume],
        )
        .unwrap();
        digest.update(&buffer);
        digest.update(b"\n");
    }
    rows.len() == q.expected.rows && format!("{:x}", digest.finalize()) == q.expected.sha256
}

fn read(store: &FastKStore, q: &Query) -> Vec<KlineRecord> {
    store
        .get_kline_range(&q.symbol, "1m", q.start_ts, q.end_ts)
        .unwrap()
}

fn main() {
    let args: Vec<_> = std::env::args().collect();
    let prepare = Instant::now();
    if args.iter().any(|arg| arg == "--prepare-fds") {
        // Experimental control only: grow the table on the main thread, then
        // close every dummy descriptor before any store/query worker exists.
        let files: Vec<_> = (0..1024)
            .map(|_| std::fs::File::open("/dev/null").unwrap())
            .collect();
        drop(files);
    }
    let preparation_ns = prepare.elapsed().as_nanos() as u64;
    let root = &args[2];
    let input = std::fs::read(&args[3]).unwrap();
    if args[1] == "layout" {
        let store = FastKStore::open_read_only(root).unwrap();
        let queries: Vec<Value> = serde_json::from_slice(&input).unwrap();
        let mut observations = Vec::new();
        for q in queries {
            let symbol = q["symbol"].as_str().unwrap();
            let start = q["start_ts"].as_i64().unwrap();
            let end = q["end_ts"].as_i64().unwrap();
            let (pointer, capacity, words) = if let Some(feature) = q["feature"].as_str() {
                let key = scoped_scalar_series_key(symbol, "1m", "feature", feature);
                let rows = store.get_scalar_range(&key, start, end).unwrap();
                let pointer = rows.as_ptr() as usize;
                let capacity = rows.capacity();
                (
                    pointer,
                    capacity,
                    rows.into_iter()
                        .flat_map(|r| [r.ts, r.value])
                        .collect::<Vec<i64>>(),
                )
            } else {
                let rows = store.get_kline_range(symbol, "1m", start, end).unwrap();
                let pointer = rows.as_ptr() as usize;
                let capacity = rows.capacity();
                (
                    pointer,
                    capacity,
                    rows.into_iter()
                        .flat_map(|r| [r.ts, r.open, r.high, r.low, r.close, r.volume])
                        .collect::<Vec<i64>>(),
                )
            };
            observations.push(json!({"case":q["case"],"record_capacity":capacity,"word_capacity":words.capacity(),"word_len":words.len(),"record_to_word_same_pointer":pointer==words.as_ptr() as usize}));
        }
        println!(
            "{}",
            json!({"observations":observations,"kline_size":std::mem::size_of::<KlineRecord>(),"kline_align":std::mem::align_of::<KlineRecord>(),"scalar_size":std::mem::size_of::<fastk::ScalarRecord>(),"scalar_align":std::mem::align_of::<fastk::ScalarRecord>()})
        );
        return;
    }
    let queries: Vec<Query> = serde_json::from_slice(&input).unwrap();
    assert_eq!(queries.len(), 1430);
    if args[1] == "trace" {
        let store = FastKStore::open_read_only(root).unwrap();
        store.set_metrics_level(MetricsLevel::Detailed);
        let mut samples = Vec::new();
        for pass in 0..2 {
            for (index, q) in queries.iter().enumerate() {
                store.reset_metrics();
                let start = Instant::now();
                let rows = read(&store, q);
                let elapsed = start.elapsed().as_nanos() as u64;
                let snapshot = store.metrics_snapshot();
                assert!(verified(&rows, q));
                samples.push(json!({"pass":pass,"index":index,"ns":elapsed,"metrics":snapshot}));
            }
        }
        println!("{}", json!({"samples":samples,"all_results_verified":true}));
        return;
    }
    let workers: usize = args[4].parse().unwrap();
    let prewarm = args[5] == "all";
    let stores: Vec<_> = (0..workers)
        .map(|_| {
            let store = FastKStore::open_read_only(root).unwrap();
            store.set_metrics_level(MetricsLevel::Off);
            Arc::new(store)
        })
        .collect();
    let barrier = Barrier::new(workers + 1);
    let origin = Instant::now();
    let completed: Vec<_> = (0..10).map(|_| AtomicUsize::new(0)).collect();
    let ended: Vec<_> = (0..10).map(|_| AtomicU64::new(0)).collect();
    let result = std::thread::scope(|scope| {
        let mut futures = Vec::new();
        for worker in 0..workers {
            let (barrier, completed, ended, queries) = (&barrier, &completed, &ended, &queries);
            let store = stores[worker].clone();
            futures.push(scope.spawn(move || {
                if prewarm {
                    for q in queries {
                        drop(read(&store, q));
                    }
                } else {
                    drop(read(&store, &queries[worker]));
                }
                let mut samples = Vec::new();
                for round in 0..10 {
                    let chunk: Vec<_> = queries
                        .iter()
                        .enumerate()
                        .filter(|(_, q)| q.round == round % 5)
                        .enumerate()
                        .filter(|(i, _)| i % workers == worker)
                        .map(|(_, q)| q)
                        .collect();
                    let mut retained = Vec::with_capacity(chunk.len());
                    barrier.wait();
                    barrier.wait();
                    for (index, q) in chunk {
                        let start = Instant::now();
                        let rows = read(&store, q);
                        samples.push((round, index, start.elapsed().as_nanos() as u64));
                        retained.push((rows, q));
                    }
                    if completed[round].fetch_add(1, Ordering::AcqRel) + 1 == workers {
                        ended[round].store(origin.elapsed().as_nanos() as u64, Ordering::Release);
                    }
                    barrier.wait();
                    for (rows, q) in retained {
                        assert!(verified(&rows, q));
                    }
                }
                samples
            }));
        }
        let mut walls = Vec::new();
        for round in 0..10 {
            barrier.wait();
            let start = origin.elapsed().as_nanos() as u64;
            barrier.wait();
            barrier.wait();
            walls.push(ended[round].load(Ordering::Acquire) - start);
        }
        let samples: Vec<_> = futures
            .into_iter()
            .flat_map(|f| f.join().unwrap())
            .collect();
        json!({"workers":workers,"prewarm_all":prewarm,"walls_ns":walls,"samples":samples,"all_results_verified":samples.len()==2860})
    });
    let mut result = result;
    result["fd_preparation_ns"] = json!(preparation_ns);
    println!("{result}");
}
