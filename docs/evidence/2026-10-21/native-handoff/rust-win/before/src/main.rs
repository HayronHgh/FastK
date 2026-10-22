//! Public API only; identical source compiles against either frozen or current engine.
use fastk::{scoped_scalar_series_key, FastKStore, MetricsLevel};
use serde::Serialize;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::{hint::black_box, path::PathBuf, time::Instant};

#[derive(Serialize)]
struct RowHash {
    rows: usize,
    sha256: String,
}

#[derive(Serialize)]
struct Sample<'a> {
    case: &'a str,
    times_ns: Vec<u64>,
    warmup_ns: Vec<u64>,
    result: RowHash,
}

fn sample<T: PartialEq>(
    mut read: impl FnMut() -> Vec<T>,
    repeats: usize,
) -> (Vec<T>, Vec<u64>, Vec<u64>) {
    let start = Instant::now();
    let expected = black_box(read());
    let mut warmup = Vec::with_capacity(32);
    warmup.push(start.elapsed().as_nanos() as u64);
    let mut times = Vec::with_capacity(repeats);
    let mut last = Vec::new();
    for index in 0..(31 + repeats) {
        let start = Instant::now();
        let rows = black_box(read());
        let elapsed = start.elapsed().as_nanos() as u64;
        black_box(&rows);
        if index < 31 {
            warmup.push(elapsed);
        } else {
            times.push(elapsed);
        }
        assert!(
            rows == expected,
            "repeated read differs from canonical-checked result"
        );
        last = rows;
    }
    (last, times, warmup)
}

fn hash<const N: usize>(rows: impl Iterator<Item = [i64; N]>, count: usize) -> RowHash {
    let mut digest = Sha256::new();
    let mut encoded = Vec::with_capacity(128);
    for row in rows {
        encoded.clear();
        serde_json::to_writer(&mut encoded, row.as_slice()).unwrap();
        digest.update(&encoded);
        digest.update(b"\n");
    }
    RowHash {
        rows: count,
        sha256: format!("{:x}", digest.finalize()),
    }
}

fn main() {
    let args: Vec<_> = std::env::args().collect();
    let store = FastKStore::open_read_only(PathBuf::from(&args[1])).unwrap();
    store.set_metrics_level(MetricsLevel::Off);
    let queries: Vec<Value> = serde_json::from_slice(&std::fs::read(&args[2]).unwrap()).unwrap();
    let mut samples = Vec::with_capacity(queries.len());
    for query in &queries {
        let symbol = query["symbol"].as_str().unwrap();
        let start = query["start_ts"].as_i64().unwrap();
        let end = query["end_ts"].as_i64().unwrap();
        let case = query["case"].as_str().unwrap();
        let repeats = if matches!(case, "short" | "latest") {
            32
        } else {
            10
        };
        let (observed, times, warmup) = if let Some(feature) = query["feature"].as_str() {
            let key = scoped_scalar_series_key(symbol, "1m", "feature", feature);
            let (rows, times, warmup) = sample(
                || store.get_scalar_range(&key, start, end).unwrap(),
                repeats,
            );
            (
                hash(rows.iter().map(|r| [r.ts, r.value]), rows.len()),
                times,
                warmup,
            )
        } else {
            let (rows, times, warmup) = sample(
                || {
                    if query["kind"] == "latest" {
                        store
                            .get_kline_latest_n(
                                symbol,
                                "1m",
                                query["limit"].as_u64().unwrap() as usize,
                            )
                            .unwrap()
                    } else {
                        store.get_kline_range(symbol, "1m", start, end).unwrap()
                    }
                },
                repeats,
            );
            (
                hash(
                    rows.iter()
                        .map(|r| [r.ts, r.open, r.high, r.low, r.close, r.volume]),
                    rows.len(),
                ),
                times,
                warmup,
            )
        };
        assert_eq!(
            observed.rows as u64,
            query["expected"]["rows"].as_u64().unwrap()
        );
        assert_eq!(
            observed.sha256,
            query["expected"]["sha256"].as_str().unwrap()
        );
        samples.push(Sample {
            case,
            times_ns: times,
            warmup_ns: warmup,
            result: observed,
        });
    }
    println!(
        "{}",
        json!({"samples":samples,"all_results_verified":true,
        "scope":"single-thread public Rust reads only; metrics off; reused prewarmed handle; OS cache uncontrolled"})
    );
}
