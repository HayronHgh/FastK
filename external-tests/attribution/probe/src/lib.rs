//! Measurement-only mirror. Never linked into fastk-native or the engine.
use fastk::{scoped_scalar_series_key, FastKStore, KlineRecord, MetricsLevel};
use numpy::{IntoPyArray, PyArrayMethods, PyReadonlyArray1};
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use serde::Deserialize;
use serde_json::json;
use sha2::{Digest, Sha256};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Barrier, RwLock};
use std::time::Instant;

fn ns(start: Instant) -> u64 {
    start.elapsed().as_nanos() as u64
}

fn error(value: impl std::fmt::Display) -> PyErr {
    PyValueError::new_err(value.to_string())
}

// Same production array steps, timed separately. NumPy gets ownership of Vec.
fn wrap(
    py: Python<'_>,
    words: Vec<i64>,
    dtype: &str,
    mut stats: Vec<u64>,
    started: Instant,
) -> PyResult<(Py<PyAny>, Vec<u64>)> {
    let pointer = words.as_ptr();
    let step = Instant::now();
    let dtype = py.import("fastk_native._native")?.getattr(dtype)?;
    stats[4] = ns(step);
    let step = Instant::now();
    let base = words.into_pyarray(py);
    stats[5] = ns(step);
    stats[13] = (pointer == base.data()) as u64;
    let step = Instant::now();
    base.call_method1("setflags", (false,))?;
    stats[6] = ns(step);
    let step = Instant::now();
    let result = base.call_method1("view", (dtype,))?;
    stats[7] = ns(step);
    let step = Instant::now();
    result.call_method1("setflags", (false,))?;
    stats[8] = ns(step);
    stats[9] = ns(started);
    Ok((result.unbind(), stats))
}

#[pyclass]
struct ProbeReader {
    store: RwLock<FastKStore>,
}

#[pymethods]
impl ProbeReader {
    #[new]
    #[pyo3(signature = (root, threads=1))]
    fn new(py: Python<'_>, root: PathBuf, threads: usize) -> PyResult<Self> {
        let store = py
            .detach(|| {
                if !root.join(".fastk.write.lock").try_exists()? {
                    return Err(fastk::FastKError::InvalidInput(
                        "uninitialized store".into(),
                    ));
                }
                let mut store = FastKStore::open_read_only(root)?;
                store.set_read_threads(threads)?;
                store.set_metrics_level(MetricsLevel::Off);
                Ok::<_, fastk::FastKError>(store)
            })
            .map_err(error)?;
        Ok(Self {
            store: RwLock::new(store),
        })
    }

    #[pyo3(signature = (symbol, start, end, feature=None))]
    fn read(
        &self,
        py: Python<'_>,
        symbol: &str,
        start: i64,
        end: i64,
        feature: Option<&str>,
    ) -> PyResult<(Py<PyAny>, Vec<u64>)> {
        let started = Instant::now();
        let (words, mut stats, finished) = py
            .detach(|| {
                let mut stats = vec![0u64; 15];
                stats[14] = ns(started);
                let step = Instant::now();
                let store = self.store.read().unwrap();
                stats[0] = ns(step);
                let (words, capacity) = if let Some(feature) = feature {
                    // Scoped key construction is part of the detached diagnostic core phase.
                    let step = Instant::now();
                    let key = scoped_scalar_series_key(symbol, "1m", "feature", feature);
                    let rows = store.get_scalar_range(&key, start, end)?;
                    stats[1] = ns(step);
                    let capacity = rows.capacity();
                    let step = Instant::now();
                    let words: Vec<i64> = rows.into_iter().flat_map(|r| [r.ts, r.value]).collect();
                    stats[2] = ns(step);
                    (words, capacity)
                } else {
                    let step = Instant::now();
                    let rows = store.get_kline_range(symbol, "1m", start, end)?;
                    stats[1] = ns(step);
                    let capacity = rows.capacity();
                    let step = Instant::now();
                    let words: Vec<i64> = rows
                        .into_iter()
                        .flat_map(|r| [r.ts, r.open, r.high, r.low, r.close, r.volume])
                        .collect();
                    stats[2] = ns(step);
                    (words, capacity)
                };
                stats[10] = capacity as u64;
                stats[11] = words.capacity() as u64;
                stats[12] = words.len() as u64;
                drop(store);
                Ok::<_, fastk::FastKError>((words, stats, Instant::now()))
            })
            .map_err(error)?;
        stats[3] = ns(finished);
        wrap(
            py,
            words,
            if feature.is_some() {
                "SCALAR_DTYPE"
            } else {
                "KLINE_DTYPE"
            },
            stats,
            started,
        )
    }
}

#[pyfunction]
fn copy_batch(
    py: Python<'_>,
    input: PyReadonlyArray1<'_, i64>,
    dtype: &str,
) -> PyResult<(Py<PyAny>, Vec<u64>)> {
    let input = input.as_slice().map_err(error)?;
    let started = Instant::now();
    let (words, copied, finished) = py.detach(|| {
        let step = Instant::now();
        let words = std::hint::black_box(input).to_vec();
        (words, ns(step), Instant::now())
    });
    let mut stats = vec![0u64; 15];
    stats[2] = copied;
    stats[3] = ns(finished);
    stats[11] = words.capacity() as u64;
    stats[12] = words.len() as u64;
    wrap(py, words, dtype, stats, started)
}

#[pyfunction]
fn gil_roundtrip(py: Python<'_>) -> u64 {
    let finished = py.detach(Instant::now);
    ns(finished)
}

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

enum Rows {
    Records(Vec<KlineRecord>),
    Words(Vec<i64>),
}

fn validate(rows: Rows, query: &Query) -> bool {
    let mut digest = Sha256::new();
    let mut buffer = Vec::with_capacity(128);
    let mut update = |values: &[i64]| {
        buffer.clear();
        serde_json::to_writer(&mut buffer, values).unwrap();
        digest.update(&buffer);
        digest.update(b"\n");
    };
    let count = match rows {
        Rows::Records(rows) => {
            let count = rows.len();
            for r in rows {
                update(&[r.ts, r.open, r.high, r.low, r.close, r.volume]);
            }
            count
        }
        Rows::Words(words) => {
            let count = words.len() / 6;
            for row in words.chunks_exact(6) {
                update(row);
            }
            count
        }
    };
    count == query.expected.rows && format!("{:x}", digest.finalize()) == query.expected.sha256
}

#[pyfunction]
fn core_parallel(
    py: Python<'_>,
    root: PathBuf,
    queries: &str,
    workers: usize,
    shared: bool,
    flatten: bool,
) -> PyResult<String> {
    if workers == 0 {
        return Err(error("positive workers required"));
    }
    let queries: Vec<Query> = serde_json::from_str(queries).map_err(error)?;
    if queries.len() != 1430 {
        return Err(error("expected frozen 1430 ranges"));
    }
    let result = py.detach(|| {
        let stores: Vec<_> = (0..if shared {1} else {workers}).map(|_| {
            let store = FastKStore::open_read_only(root.clone()).unwrap();
            store.set_metrics_level(MetricsLevel::Off);
            Arc::new(store)
        }).collect();
        let barrier = Barrier::new(workers+1);
        let origin = Instant::now();
        let completed: Vec<_> = (0..5).map(|_| AtomicUsize::new(0)).collect();
        let finished: Vec<_> = (0..5).map(|_| AtomicU64::new(0)).collect();
        std::thread::scope(|scope| {
            let mut futures = Vec::new();
            for worker in 0..workers {
                let store = stores[if shared {0} else {worker}].clone();
                let queries = &queries;
                let barrier = &barrier;
                let completed = &completed;
                let finished = &finished;
                futures.push(scope.spawn(move || {
                    let first=&queries[worker];
                    drop(store.get_kline_range(&first.symbol,"1m",first.start_ts,first.end_ts).unwrap());
                    let mut samples=Vec::with_capacity(queries.len()/workers+5);
                    let mut verified=0;
                    for round in 0..5 {
                        let chunk: Vec<_> = queries.iter().enumerate().filter(|(_,q)|q.round==round).enumerate().filter(|(i,_)|i%workers==worker).map(|(_,item)|item).collect();
                        let mut retained=Vec::with_capacity(chunk.len());
                        // Ready barrier excludes prior validation and prewarm.
                        barrier.wait();
                        barrier.wait();
                        for (index,q) in chunk {
                            let step=Instant::now();
                            let records=store.get_kline_range(&q.symbol,"1m",q.start_ts,q.end_ts).unwrap();
                            let rows=if flatten {Rows::Words(records.into_iter().flat_map(|r|[r.ts,r.open,r.high,r.low,r.close,r.volume]).collect())} else {Rows::Records(records)};
                            samples.push((index,ns(step)));
                            retained.push((rows,q));
                        }
                        if completed[round].fetch_add(1, Ordering::AcqRel)+1 == workers {
                            finished[round].store(ns(origin), Ordering::Release);
                        }
                        barrier.wait();
                        for (rows,q) in retained { assert!(validate(rows,q),"canonical mismatch");verified+=1; }
                    }
                    (samples,verified)
                }));
            }
            let mut walls=Vec::new();
            for round in 0..5 {
                barrier.wait();
                let start=ns(origin);
                barrier.wait();
                barrier.wait();
                walls.push(finished[round].load(Ordering::Acquire)-start);
            }
            let mut samples=Vec::new();let mut verified=0;
            for future in futures { let (s,n)=future.join().unwrap();samples.extend(s);verified+=n; }
            json!({"walls_ns":walls,"samples":samples,"verified":verified,"all_results_verified":verified==1430,"timing_boundary":"ready barrier excludes prewarm and previous validation; endpoint is last worker before validation"})
        })
    });
    Ok(result.to_string())
}

#[pymodule]
fn _fastk_attribution(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<ProbeReader>()?;
    module.add_function(wrap_pyfunction!(copy_batch, module)?)?;
    module.add_function(wrap_pyfunction!(gil_roundtrip, module)?)?;
    module.add_function(wrap_pyfunction!(core_parallel, module)?)?;
    Ok(())
}
