//! Diagnostic executable only: actual engine plus isolated dispatch/copy/IO controls.
//! Perf counters cover repeated query cycles (including result destruction), not validation.
use fastk::{FastKStore, KlineRecord, MetricsLevel};
use rayon::prelude::*;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, Read, Seek, SeekFrom, Write};
use std::sync::{Arc, Barrier};
use std::time::Instant;

// Reuse the unchanged positional IO implementation without production hooks.
#[path = "../../../src/store_core/chunk/file.rs"]
#[allow(dead_code)]
mod file;

struct Control(Option<(File, BufReader<File>)>);
impl Control {
    fn new() -> Self {
        Self(std::env::var("PROFILE_CONTROL").ok().map(|path| {
            let writer = OpenOptions::new().write(true).open(path).unwrap();
            let reader = File::open(std::env::var("PROFILE_ACK").unwrap()).unwrap();
            (writer, BufReader::new(reader))
        }))
    }
    fn command(&mut self, command: &str) {
        if let Some((writer, reader)) = &mut self.0 {
            writeln!(writer, "{command}").unwrap();
            writer.flush().unwrap();
            let mut ack = String::new();
            reader.read_line(&mut ack).unwrap();
            assert!(ack.contains("ack"), "perf control: {ack:?}");
        }
    }
}

fn checked(rows: &[KlineRecord], q: &Value) {
    let mut digest = Sha256::new();
    let mut encoded = Vec::with_capacity(128);
    for r in rows {
        encoded.clear();
        serde_json::to_writer(
            &mut encoded,
            &[r.ts, r.open, r.high, r.low, r.close, r.volume],
        )
        .unwrap();
        digest.update(&encoded);
        digest.update(b"\n");
    }
    assert_eq!(rows.len(), q["expected"]["rows"].as_u64().unwrap() as usize);
    assert_eq!(
        format!("{:x}", digest.finalize()),
        q["expected"]["sha256"].as_str().unwrap()
    );
}

fn read(store: &FastKStore, q: &Value) -> Vec<KlineRecord> {
    store
        .get_kline_range(
            q["symbol"].as_str().unwrap(),
            "1m",
            q["start_ts"].as_i64().unwrap(),
            q["end_ts"].as_i64().unwrap(),
        )
        .unwrap()
}

fn open(root: &str, threads: usize) -> FastKStore {
    let mut store = FastKStore::open_read_only(root).unwrap();
    store.set_metrics_level(MetricsLevel::Off);
    store.set_read_threads(threads).unwrap();
    store
}

fn main() {
    let args: Vec<_> = std::env::args().collect();
    let (mode, root) = (args[1].as_str(), args[2].as_str());
    let queries: Vec<Value> = serde_json::from_slice(&std::fs::read(&args[3]).unwrap()).unwrap();
    let threads: usize = args[4].parse().unwrap();
    let groups: usize = args[5].parse().unwrap();
    let cycles: usize = args[6].parse().unwrap();
    assert!(threads > 0 && groups > 0 && cycles > 0);
    let store = Arc::new(open(root, if mode == "engine" { threads } else { 1 }));
    let mut ctl = Control::new();
    let mut times = Vec::new();
    let phase_ns;
    let bytes;
    if matches!(mode, "outer" | "private") {
        assert_eq!(queries.len(), 1430);
        let stores: Vec<_> = if mode == "private" {
            std::iter::once(store.clone())
                .chain((1..threads).map(|_| Arc::new(open(root, 1))))
                .collect()
        } else {
            vec![store.clone(); threads]
        };
        // All handles fully warmed before enabling counters: no FD-table bootstrap.
        for s in &stores {
            for q in &queries {
                checked(&read(s, q), q);
            }
        }
        let barrier = Barrier::new(threads + 1);
        let (totals, elapsed) = std::thread::scope(|scope| {
            let mut workers = Vec::new();
            for (id, s) in stores.iter().enumerate() {
                let assigned: Vec<_> = queries
                    .iter()
                    .enumerate()
                    .filter(|(i, _)| i % threads == id)
                    .map(|(_, q)| q)
                    .collect();
                let barrier = &barrier;
                workers.push(scope.spawn(move || {
                    let mut total = 0usize;
                    for q in &assigned {
                        checked(&read(s, q), q);
                    }
                    barrier.wait();
                    for _ in 0..cycles {
                        for q in &assigned {
                            let rows = read(s, q);
                            assert_eq!(
                                rows.len(),
                                q["expected"]["rows"].as_u64().unwrap() as usize
                            );
                            total = total.wrapping_add(std::hint::black_box(rows.len()));
                        }
                    }
                    barrier.wait();
                    total
                }));
            }
            ctl.command("enable");
            let start = Instant::now();
            barrier.wait();
            barrier.wait();
            let elapsed = start.elapsed().as_nanos() as u64;
            ctl.command("disable");
            (
                workers
                    .into_iter()
                    .map(|w| w.join().unwrap())
                    .sum::<usize>(),
                elapsed,
            )
        });
        phase_ns = elapsed;
        for q in &queries {
            checked(&read(&store, q), q);
        }
        bytes = totals * 48;
    } else {
        let q = queries.iter().find(|q| q["case"] == "bulk_kline").unwrap();
        let reference = read(&store, q);
        checked(&reference, q);
        bytes = reference.len() * 48;
        if mode == "engine" {
            let mut last = Vec::new();
            for _ in 0..32 {
                last = read(&store, q);
                assert_eq!(last, reference);
            }
            ctl.command("enable");
            let phase = Instant::now();
            for _ in 0..cycles {
                let start = Instant::now();
                let rows = read(&store, q);
                times.push(start.elapsed().as_nanos() as u64);
                assert_eq!(rows.len(), reference.len());
                // Exclude destructor from per-read timer, include it in perf's query-cycle counters.
                last = rows;
            }
            phase_ns = phase.elapsed().as_nanos() as u64;
            ctl.command("disable");
            assert_eq!(last, reference);
            checked(&last, q);
        } else {
            let source: Vec<u8> = reference
                .iter()
                .flat_map(|r| [r.ts, r.open, r.high, r.low, r.close, r.volume])
                .flat_map(i64::to_le_bytes)
                .collect();
            let mut output = vec![0u8; bytes];
            let raw_path = std::path::Path::new(root)
                .parent()
                .unwrap()
                .join("scaling-flat-control.bin");
            std::fs::write(&raw_path, &source).unwrap();
            let file = file::ChunkFile::open(&raw_path).unwrap();
            let pool = rayon::ThreadPoolBuilder::new()
                .num_threads(threads)
                .build()
                .unwrap();
            let chunk_size = bytes.div_ceil(groups);
            let run = |output: &mut [u8]| {
                pool.install(|| {
                    output
                        .par_chunks_mut(chunk_size)
                        .enumerate()
                        .for_each(|(id, dst)| {
                            let offset = id * chunk_size;
                            match mode {
                                "copy" => dst.copy_from_slice(&source[offset..offset + dst.len()]),
                                "pread" => {
                                    let mut reader = file.clone();
                                    reader.seek(SeekFrom::Start(offset as u64)).unwrap();
                                    reader.read_exact(dst).unwrap();
                                }
                                "schedule" => {
                                    std::hint::black_box(id);
                                }
                                _ => panic!("unknown mode {mode}"),
                            }
                        })
                });
            };
            for _ in 0..32 {
                run(&mut output);
            }
            ctl.command("enable");
            let phase = Instant::now();
            for _ in 0..cycles {
                let start = Instant::now();
                run(&mut output);
                times.push(start.elapsed().as_nanos() as u64);
            }
            phase_ns = phase.elapsed().as_nanos() as u64;
            ctl.command("disable");
            if mode != "schedule" {
                assert_eq!(output, source);
            }
        }
    }
    println!(
        "{}",
        json!({"mode":mode,"threads":threads,"groups_control_only":groups,"cycles":cycles,"bytes":bytes,"phase_ns":phase_ns,"read_times_ns":times,"validation":"canonical warm/final; hot-loop length check (schedule is negative control)","fds":std::fs::read_dir("/proc/self/fd").ok().map(|r|r.count()),"passed":true})
    );
}
