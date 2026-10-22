//! Counters only. Basic instrumentation changes contention; do not time this run.
use fastk::{FastKStore, MetricsLevel};
use serde_json::{json, Value};
use std::collections::BTreeSet;
use std::sync::{Arc, Barrier};

fn main() {
    let args: Vec<_> = std::env::args().collect();
    let queries: Vec<Value> = serde_json::from_slice(&std::fs::read(&args[2]).unwrap()).unwrap();
    let threads: usize = args[3].parse().unwrap();
    let shared = args[4] == "shared";
    let cycles: usize = args[5].parse().unwrap();
    let stores: Vec<_> = (0..if shared { 1 } else { threads })
        .map(|_| {
            let store = FastKStore::open_read_only(&args[1]).unwrap();
            store.set_metrics_level(MetricsLevel::Basic);
            Arc::new(store)
        })
        .collect();
    let read = |store: &FastKStore, q: &Value| {
        let rows = store
            .get_kline_range(
                q["symbol"].as_str().unwrap(),
                "1m",
                q["start_ts"].as_i64().unwrap(),
                q["end_ts"].as_i64().unwrap(),
            )
            .unwrap();
        assert_eq!(rows.len(), q["expected"]["rows"].as_u64().unwrap() as usize);
    };
    let mut working_sets = Vec::new();
    for worker in 0..threads {
        let mut files = BTreeSet::new();
        for (id, q) in queries.iter().enumerate() {
            if id % threads != worker {
                continue;
            }
            let symbol = q["symbol"].as_str().unwrap();
            let meta = stores[if shared { 0 } else { worker }]
                .kline_metadata(symbol, "1m")
                .unwrap();
            let chunks = meta.find_chunks_for_range(
                q["start_ts"].as_i64().unwrap(),
                q["end_ts"].as_i64().unwrap(),
            );
            for chunk in &meta.chunks[chunks] {
                files.insert(format!("{symbol}/{}", chunk.relative_path));
            }
        }
        working_sets.push(files);
    }
    let union: BTreeSet<_> = working_sets.iter().flatten().cloned().collect();
    let barrier = Barrier::new(threads + 1);
    let mut fds = Vec::new();
    std::thread::scope(|scope| {
        for worker in 0..threads {
            let store = &stores[if shared { 0 } else { worker }];
            let barrier = &barrier;
            let queries = &queries;
            let read = &read;
            scope.spawn(move || {
                let assigned: Vec<_> = queries
                    .iter()
                    .enumerate()
                    .filter(|(id, _)| id % threads == worker)
                    .map(|(_, q)| q)
                    .collect();
                for q in &assigned {
                    read(store, q);
                }
                barrier.wait();
                barrier.wait();
                for _ in 0..cycles {
                    for q in &assigned {
                        read(store, q);
                    }
                }
                barrier.wait();
            });
        }
        barrier.wait();
        for store in &stores {
            store.reset_metrics();
        }
        fds.push(std::fs::read_dir("/proc/self/fd").ok().map(|r| r.count()));
        barrier.wait();
        barrier.wait();
        fds.push(std::fs::read_dir("/proc/self/fd").ok().map(|r| r.count()));
    });
    println!(
        "{}",
        json!({"shared":shared,"threads":threads,"cycles":cycles,"queries":queries.len()*cycles,"unique_files":union.len(),"per_worker_unique_files":working_sets.iter().map(BTreeSet::len).collect::<Vec<_>>(),"metrics":stores.iter().map(|s|s.metrics_snapshot()).collect::<Vec<_>>(),"fds":fds,"timings_not_recorded":true,"passed":true})
    );
}
