use std::fs;
use std::path::PathBuf;

use fastk::{FastKStore, KlineRecord};

fn main() {
    if let Err(err) = run() {
        eprintln!("prepare_python_demo_store failed: {err}");
        std::process::exit(1);
    }
}

fn run() -> fastk::Result<()> {
    let root = parse_root();
    if root.exists() {
        fs::remove_dir_all(&root)?;
    }

    let mut store = FastKStore::open(&root)?;
    store.init()?;
    store.register_kline_series("BTCUSDT", "1m", 60_000, 100_000, 100_000)?;
    store.put_kline_chunk("BTCUSDT", "1m", &sample_kline_records())?;

    println!(
        "seeded demo store at {} with {} kline rows for BTCUSDT/1m",
        root.display(),
        sample_kline_records().len()
    );
    Ok(())
}

fn parse_root() -> PathBuf {
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        if arg == "--root" {
            if let Some(root) = args.next() {
                return PathBuf::from(root);
            }
        }
    }
    PathBuf::from("target/python-ma20-demo-store")
}

fn sample_kline_records() -> Vec<KlineRecord> {
    let start_ts = 1_706_745_600_000i64;
    let mut records = Vec::with_capacity(120);
    for index in 0..120i64 {
        let ts = start_ts + index * 60_000;
        let drift = index * 1_750;
        let oscillation = match index % 6 {
            0 => -9_000,
            1 => -3_000,
            2 => 4_000,
            3 => 9_500,
            4 => 6_000,
            _ => -1_500,
        };
        let open = 5_000_000_000 + drift + oscillation;
        let close = open
            + match index % 4 {
                0 => 6_500,
                1 => -2_500,
                2 => 9_000,
                _ => -5_000,
            };
        let high = open.max(close) + 11_000;
        let low = open.min(close) - 10_500;
        let volume = 250_000 + index * 750;
        records.push(KlineRecord {
            ts,
            open,
            high,
            low,
            close,
            volume,
        });
    }
    records
}
