//! Reused-buffer memcpy size curve; no IO, allocation, hashing or engine locks in timers.
use rayon::prelude::*;
use serde_json::json;
use std::time::Instant;

fn main() {
    let args: Vec<_> = std::env::args().collect();
    let bytes: usize = args[1].parse().unwrap();
    let threads: usize = args[2].parse().unwrap();
    let groups: usize = args[3].parse().unwrap();
    let source: Vec<_> = (0..bytes).map(|i| (i % 251) as u8).collect();
    let mut output = vec![0u8; bytes];
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(threads)
        .build()
        .unwrap();
    let stride = bytes.div_ceil(groups);
    let copy = |out: &mut [u8]| {
        pool.install(|| {
            out.par_chunks_mut(stride).enumerate().for_each(|(i, dst)| {
                dst.copy_from_slice(&source[i * stride..i * stride + dst.len()])
            })
        })
    };
    let mut samples = Vec::with_capacity(128);
    for i in 0..160 {
        let start = Instant::now();
        copy(&mut output);
        let ns = start.elapsed().as_nanos() as u64;
        if i >= 32 {
            samples.push(ns);
        }
    }
    assert_eq!(output, source);
    println!(
        "{}",
        json!({"bytes":bytes,"threads":threads,"groups":groups,"times_ns":samples,"warmups":32,"passed":true,"scope":"effective memcpy throughput, not a physical DRAM bandwidth counter"})
    );
}
