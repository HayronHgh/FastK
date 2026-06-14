use std::env;
use std::fs::{self, File};
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::time::Instant;

use chrono::{TimeZone, Utc};
use fastk::{FastKError, FastKStore, KlineRecord, Result};
use rusqlite::{params, Connection};
use serde::{Deserialize, Serialize};

const DEFAULT_SYMBOL_COUNT: usize = 24;
const DEFAULT_MONTHS: usize = 10;
const DEFAULT_RECORDS_PER_MONTH: usize = 41_667;
const DEFAULT_RANGE_OPS: usize = 100;
const DEFAULT_RANGE_ROWS: usize = 1_024;
const TIMEFRAME: &str = "1m";
const TIMEFRAME_MS: i64 = 60_000;
const PRICE_SCALE: i64 = 100_000;
const VOLUME_SCALE: i64 = 100_000;

fn main() {
    if let Err(err) = run() {
        eprintln!("bench_kline_storage_comparison failed: {err}");
        std::process::exit(1);
    }
}

fn run() -> Result<()> {
    let config = Config::parse(env::args().skip(1))?;
    if config.root.exists() {
        fs::remove_dir_all(&config.root)?;
    }
    fs::create_dir_all(&config.root)?;

    let mut backends = Vec::new();
    backends.push(bench_fastk(&config)?);
    backends.push(bench_csv(&config)?);
    backends.push(bench_jsonl(&config)?);
    backends.push(bench_sqlite(&config)?);

    let summary = Summary {
        measured_at_utc: Utc::now().to_rfc3339(),
        os: env::consts::OS.to_string(),
        arch: env::consts::ARCH.to_string(),
        config: config.summary(),
        total_rows: config.total_rows(),
        backends,
        notes: vec![
            "CSV and JSONL range reads are not reported because they require full-file scans without an external index.".to_string(),
            "TimescaleDB is not measured by this local Rust benchmark because it requires an external PostgreSQL/Timescale service.".to_string(),
        ],
    };

    let output_path = config.root.join("kline-storage-comparison-results.json");
    let mut writer = BufWriter::new(File::create(&output_path)?);
    serde_json::to_writer_pretty(&mut writer, &summary)?;
    writer.write_all(b"\n")?;
    writer.flush()?;

    println!("{}", serde_json::to_string_pretty(&summary)?);
    println!("results saved: {}", output_path.display());
    Ok(())
}

fn bench_fastk(config: &Config) -> Result<BackendResult> {
    let root = config.root.join("fastk");
    let started = Instant::now();
    let mut store = FastKStore::open(&root)?;
    store.init()?;
    for symbol_idx in 0..config.symbol_count {
        let symbol = symbol_name(symbol_idx);
        store.register_kline_series(&symbol, TIMEFRAME, TIMEFRAME_MS, PRICE_SCALE, VOLUME_SCALE)?;
        for month_idx in 0..config.months {
            let rows = month_records(symbol_idx, month_idx, config.records_per_month)?;
            store.put_kline_chunk(&symbol, TIMEFRAME, &rows)?;
        }
    }
    let write_seconds = started.elapsed().as_secs_f64();
    let bytes_on_disk = dir_size(&root)?;

    let read_started = Instant::now();
    let mut checksum = ReadChecksum::default();
    for symbol_idx in 0..config.symbol_count {
        let symbol = symbol_name(symbol_idx);
        for month_idx in 0..config.months {
            let start_ts = month_start_ts(month_idx)?;
            let end_ts = start_ts + (config.records_per_month as i64 - 1) * TIMEFRAME_MS;
            let rows = store.get_kline_range(&symbol, TIMEFRAME, start_ts, end_ts)?;
            checksum.observe_many(&rows);
        }
    }
    let full_read_seconds = read_started.elapsed().as_secs_f64();

    let range = measure_fastk_range_reads(&store, config)?;
    Ok(BackendResult {
        backend: "FastK".to_string(),
        rows_written: config.total_rows(),
        bytes_on_disk,
        write_seconds,
        full_read_seconds,
        full_read_rows: checksum.rows,
        checksum: checksum.checksum,
        range_read: Some(range),
    })
}

fn bench_csv(config: &Config) -> Result<BackendResult> {
    let dir = config.root.join("csv");
    fs::create_dir_all(&dir)?;
    let path = dir.join("klines.csv");

    let started = Instant::now();
    {
        let mut writer = BufWriter::new(File::create(&path)?);
        writer.write_all(b"symbol,ts,open,high,low,close,volume\n")?;
        for_each_record(config, |symbol, record| {
            writeln!(
                writer,
                "{},{},{},{},{},{},{}",
                symbol,
                record.ts,
                record.open,
                record.high,
                record.low,
                record.close,
                record.volume
            )?;
            Ok(())
        })?;
        writer.flush()?;
    }
    let write_seconds = started.elapsed().as_secs_f64();
    let bytes_on_disk = dir_size(&dir)?;

    let read_started = Instant::now();
    let mut checksum = ReadChecksum::default();
    let reader = BufReader::new(File::open(&path)?);
    for (line_idx, line) in reader.lines().enumerate() {
        let line = line?;
        if line_idx == 0 {
            continue;
        }
        let record = parse_csv_record(&line)?;
        checksum.observe(&record);
    }
    let full_read_seconds = read_started.elapsed().as_secs_f64();

    Ok(BackendResult {
        backend: "CSV".to_string(),
        rows_written: config.total_rows(),
        bytes_on_disk,
        write_seconds,
        full_read_seconds,
        full_read_rows: checksum.rows,
        checksum: checksum.checksum,
        range_read: None,
    })
}

fn bench_jsonl(config: &Config) -> Result<BackendResult> {
    let dir = config.root.join("jsonl");
    fs::create_dir_all(&dir)?;
    let path = dir.join("klines.jsonl");

    let started = Instant::now();
    {
        let mut writer = BufWriter::new(File::create(&path)?);
        for_each_record(config, |symbol, record| {
            let row = JsonKlineRecord {
                symbol: symbol.to_string(),
                ts: record.ts,
                open: record.open,
                high: record.high,
                low: record.low,
                close: record.close,
                volume: record.volume,
            };
            serde_json::to_writer(&mut writer, &row)?;
            writer.write_all(b"\n")?;
            Ok(())
        })?;
        writer.flush()?;
    }
    let write_seconds = started.elapsed().as_secs_f64();
    let bytes_on_disk = dir_size(&dir)?;

    let read_started = Instant::now();
    let mut checksum = ReadChecksum::default();
    let reader = BufReader::new(File::open(&path)?);
    for line in reader.lines() {
        let line = line?;
        let row: JsonKlineRecord = serde_json::from_str(&line)?;
        checksum.observe(&row.to_kline_record());
    }
    let full_read_seconds = read_started.elapsed().as_secs_f64();

    Ok(BackendResult {
        backend: "JSONL".to_string(),
        rows_written: config.total_rows(),
        bytes_on_disk,
        write_seconds,
        full_read_seconds,
        full_read_rows: checksum.rows,
        checksum: checksum.checksum,
        range_read: None,
    })
}

fn bench_sqlite(config: &Config) -> Result<BackendResult> {
    let dir = config.root.join("sqlite");
    fs::create_dir_all(&dir)?;
    let path = dir.join("klines.sqlite3");

    let started = Instant::now();
    let mut connection = open_sqlite(&path)?;
    connection
        .execute_batch(
            "CREATE TABLE klines (
                symbol TEXT NOT NULL,
                ts INTEGER NOT NULL,
                open INTEGER NOT NULL,
                high INTEGER NOT NULL,
                low INTEGER NOT NULL,
                close INTEGER NOT NULL,
                volume INTEGER NOT NULL,
                PRIMARY KEY(symbol, ts)
            );",
        )
        .map_err(sqlite_err)?;
    let tx = connection.transaction().map_err(sqlite_err)?;
    {
        let mut stmt = tx
            .prepare(
                "INSERT INTO klines (symbol, ts, open, high, low, close, volume)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            )
            .map_err(sqlite_err)?;
        for_each_record(config, |symbol, record| {
            stmt.execute(params![
                symbol,
                record.ts,
                record.open,
                record.high,
                record.low,
                record.close,
                record.volume
            ])
            .map_err(sqlite_err)?;
            Ok(())
        })?;
    }
    tx.commit().map_err(sqlite_err)?;
    let write_seconds = started.elapsed().as_secs_f64();
    let bytes_on_disk = dir_size(&dir)?;

    let read_started = Instant::now();
    let mut checksum = ReadChecksum::default();
    let mut stmt = connection
        .prepare("SELECT ts, open, high, low, close, volume FROM klines ORDER BY symbol, ts")
        .map_err(sqlite_err)?;
    let rows = stmt
        .query_map([], |row| {
            Ok(KlineRecord {
                ts: row.get(0)?,
                open: row.get(1)?,
                high: row.get(2)?,
                low: row.get(3)?,
                close: row.get(4)?,
                volume: row.get(5)?,
            })
        })
        .map_err(sqlite_err)?;
    for row in rows {
        checksum.observe(&row.map_err(sqlite_err)?);
    }
    let full_read_seconds = read_started.elapsed().as_secs_f64();

    let range = measure_sqlite_range_reads(&connection, config)?;
    Ok(BackendResult {
        backend: "SQLite3".to_string(),
        rows_written: config.total_rows(),
        bytes_on_disk,
        write_seconds,
        full_read_seconds,
        full_read_rows: checksum.rows,
        checksum: checksum.checksum,
        range_read: Some(range),
    })
}

fn measure_fastk_range_reads(store: &FastKStore, config: &Config) -> Result<RangeReadResult> {
    let started = Instant::now();
    let mut rows_read = 0usize;
    let mut checksum = 0i128;
    for query in range_queries(config)? {
        let rows = store.get_kline_range(&query.symbol, TIMEFRAME, query.start_ts, query.end_ts)?;
        rows_read += rows.len();
        checksum += rows.iter().map(|record| record.close as i128).sum::<i128>();
    }
    Ok(RangeReadResult {
        ops: config.range_ops,
        rows_per_op: config.range_rows,
        rows_read,
        seconds: started.elapsed().as_secs_f64(),
        checksum,
    })
}

fn measure_sqlite_range_reads(connection: &Connection, config: &Config) -> Result<RangeReadResult> {
    let queries = range_queries(config)?;
    let started = Instant::now();
    let mut rows_read = 0usize;
    let mut checksum = 0i128;
    let mut stmt = connection
        .prepare(
            "SELECT ts, open, high, low, close, volume
             FROM klines
             WHERE symbol = ?1 AND ts >= ?2 AND ts <= ?3
             ORDER BY ts",
        )
        .map_err(sqlite_err)?;
    for query in queries {
        let rows = stmt
            .query_map(params![query.symbol, query.start_ts, query.end_ts], |row| {
                Ok(KlineRecord {
                    ts: row.get(0)?,
                    open: row.get(1)?,
                    high: row.get(2)?,
                    low: row.get(3)?,
                    close: row.get(4)?,
                    volume: row.get(5)?,
                })
            })
            .map_err(sqlite_err)?;
        for row in rows {
            let record = row.map_err(sqlite_err)?;
            rows_read += 1;
            checksum += record.close as i128;
        }
    }
    Ok(RangeReadResult {
        ops: config.range_ops,
        rows_per_op: config.range_rows,
        rows_read,
        seconds: started.elapsed().as_secs_f64(),
        checksum,
    })
}

fn for_each_record<F>(config: &Config, mut f: F) -> Result<()>
where
    F: FnMut(&str, KlineRecord) -> Result<()>,
{
    for symbol_idx in 0..config.symbol_count {
        let symbol = symbol_name(symbol_idx);
        for month_idx in 0..config.months {
            for record in month_records(symbol_idx, month_idx, config.records_per_month)? {
                f(&symbol, record)?;
            }
        }
    }
    Ok(())
}

fn month_records(symbol_idx: usize, month_idx: usize, rows: usize) -> Result<Vec<KlineRecord>> {
    let start_ts = month_start_ts(month_idx)?;
    let mut out = Vec::with_capacity(rows);
    let mut close = (50_000 + symbol_idx as i64 * 100) * PRICE_SCALE;
    for index in 0..rows {
        close += (((index + month_idx) % 17) as i64 - 8) * 3;
        out.push(KlineRecord {
            ts: start_ts + index as i64 * TIMEFRAME_MS,
            open: close - 5,
            high: close + 10,
            low: close - 15,
            close,
            volume: (1_000 + month_idx as i64 * 10 + index as i64) * VOLUME_SCALE,
        });
    }
    Ok(out)
}

fn range_queries(config: &Config) -> Result<Vec<RangeQuery>> {
    if config.range_rows == 0 || config.range_rows > config.records_per_month {
        return Err(FastKError::InvalidInput(
            "range rows must be between 1 and records_per_month".to_string(),
        ));
    }
    let max_offset = config.records_per_month - config.range_rows;
    let mut queries = Vec::with_capacity(config.range_ops);
    for index in 0..config.range_ops {
        let symbol = symbol_name(index % config.symbol_count);
        let month_idx = index % config.months;
        let offset = if max_offset == 0 {
            0
        } else {
            index.wrapping_mul(997) % (max_offset + 1)
        };
        let start_ts = month_start_ts(month_idx)? + offset as i64 * TIMEFRAME_MS;
        let end_ts = start_ts + (config.range_rows as i64 - 1) * TIMEFRAME_MS;
        queries.push(RangeQuery {
            symbol,
            start_ts,
            end_ts,
        });
    }
    Ok(queries)
}

fn parse_csv_record(line: &str) -> Result<KlineRecord> {
    let mut parts = line.split(',');
    let _symbol = parts
        .next()
        .ok_or_else(|| FastKError::InvalidData("missing csv symbol".to_string()))?;
    let ts = parse_csv_i64(parts.next(), "ts")?;
    let open = parse_csv_i64(parts.next(), "open")?;
    let high = parse_csv_i64(parts.next(), "high")?;
    let low = parse_csv_i64(parts.next(), "low")?;
    let close = parse_csv_i64(parts.next(), "close")?;
    let volume = parse_csv_i64(parts.next(), "volume")?;
    Ok(KlineRecord {
        ts,
        open,
        high,
        low,
        close,
        volume,
    })
}

fn parse_csv_i64(value: Option<&str>, name: &str) -> Result<i64> {
    value
        .ok_or_else(|| FastKError::InvalidData(format!("missing csv {name}")))?
        .parse::<i64>()
        .map_err(|err| FastKError::InvalidData(format!("invalid csv {name}: {err}")))
}

fn open_sqlite(path: &Path) -> Result<Connection> {
    let connection = Connection::open(path).map_err(sqlite_err)?;
    connection
        .execute_batch(
            "PRAGMA journal_mode=WAL;
             PRAGMA synchronous=NORMAL;
             PRAGMA temp_store=MEMORY;
             PRAGMA cache_size=-262144;",
        )
        .map_err(sqlite_err)?;
    Ok(connection)
}

fn sqlite_err(err: rusqlite::Error) -> FastKError {
    FastKError::InvalidData(format!("sqlite error: {err}"))
}

fn month_start_ts(month_idx: usize) -> Result<i64> {
    let year = 2024 + (month_idx / 12) as i32;
    let month = (month_idx % 12 + 1) as u32;
    Utc.with_ymd_and_hms(year, month, 1, 0, 0, 0)
        .single()
        .map(|dt| dt.timestamp_millis())
        .ok_or_else(|| FastKError::InvalidInput(format!("invalid month index: {month_idx}")))
}

fn symbol_name(symbol_idx: usize) -> String {
    format!("SYM{symbol_idx:03}")
}

fn dir_size(path: &Path) -> Result<u64> {
    let mut total = 0u64;
    if path.is_file() {
        return Ok(path.metadata()?.len());
    }
    for entry in fs::read_dir(path)? {
        let entry = entry?;
        let metadata = entry.metadata()?;
        if metadata.is_dir() {
            total = total.saturating_add(dir_size(&entry.path())?);
        } else {
            total = total.saturating_add(metadata.len());
        }
    }
    Ok(total)
}

#[derive(Debug, Clone)]
struct Config {
    root: PathBuf,
    symbol_count: usize,
    months: usize,
    records_per_month: usize,
    range_ops: usize,
    range_rows: usize,
}

impl Config {
    fn parse<I>(args: I) -> Result<Self>
    where
        I: IntoIterator<Item = String>,
    {
        let mut config = Self {
            root: PathBuf::from("target/kline-storage-comparison"),
            symbol_count: DEFAULT_SYMBOL_COUNT,
            months: DEFAULT_MONTHS,
            records_per_month: DEFAULT_RECORDS_PER_MONTH,
            range_ops: DEFAULT_RANGE_OPS,
            range_rows: DEFAULT_RANGE_ROWS,
        };
        let mut args = args.into_iter();
        while let Some(arg) = args.next() {
            match arg.as_str() {
                "--root" => config.root = PathBuf::from(next_value(&mut args, "--root")?),
                "--symbol-count" => {
                    config.symbol_count = parse_usize(next_value(&mut args, "--symbol-count")?)?
                }
                "--months" => config.months = parse_usize(next_value(&mut args, "--months")?)?,
                "--records-per-month" => {
                    config.records_per_month =
                        parse_usize(next_value(&mut args, "--records-per-month")?)?
                }
                "--range-ops" => {
                    config.range_ops = parse_usize(next_value(&mut args, "--range-ops")?)?
                }
                "--range-rows" => {
                    config.range_rows = parse_usize(next_value(&mut args, "--range-rows")?)?
                }
                "--help" | "-h" => {
                    print_help();
                    std::process::exit(0);
                }
                other => {
                    return Err(FastKError::InvalidInput(format!(
                        "unknown argument: {other}"
                    )))
                }
            }
        }
        Ok(config)
    }

    fn total_rows(&self) -> usize {
        self.symbol_count * self.months * self.records_per_month
    }

    fn summary(&self) -> ConfigSummary {
        ConfigSummary {
            root: self.root.display().to_string(),
            symbol_count: self.symbol_count,
            months: self.months,
            records_per_month: self.records_per_month,
            total_rows: self.total_rows(),
            timeframe: TIMEFRAME.to_string(),
            timeframe_ms: TIMEFRAME_MS,
            range_ops: self.range_ops,
            range_rows: self.range_rows,
        }
    }
}

fn next_value<I>(args: &mut I, flag: &str) -> Result<String>
where
    I: Iterator<Item = String>,
{
    args.next()
        .ok_or_else(|| FastKError::InvalidInput(format!("missing value for {flag}")))
}

fn parse_usize(value: String) -> Result<usize> {
    value
        .parse::<usize>()
        .map_err(|err| FastKError::InvalidInput(format!("invalid usize '{value}': {err}")))
}

fn print_help() {
    println!("bench_kline_storage_comparison [options]");
    println!("  --root <path>");
    println!("  --symbol-count <n>          default {DEFAULT_SYMBOL_COUNT}");
    println!("  --months <n>                default {DEFAULT_MONTHS}");
    println!("  --records-per-month <n>     default {DEFAULT_RECORDS_PER_MONTH}");
    println!("  --range-ops <n>             default {DEFAULT_RANGE_OPS}");
    println!("  --range-rows <n>            default {DEFAULT_RANGE_ROWS}");
}

#[derive(Debug, Default)]
struct ReadChecksum {
    rows: usize,
    checksum: i128,
}

impl ReadChecksum {
    fn observe(&mut self, record: &KlineRecord) {
        self.rows += 1;
        self.checksum += record.close as i128;
    }

    fn observe_many(&mut self, records: &[KlineRecord]) {
        self.rows += records.len();
        self.checksum += records
            .iter()
            .map(|record| record.close as i128)
            .sum::<i128>();
    }
}

#[derive(Debug)]
struct RangeQuery {
    symbol: String,
    start_ts: i64,
    end_ts: i64,
}

#[derive(Debug, Serialize, Deserialize)]
struct JsonKlineRecord {
    symbol: String,
    ts: i64,
    open: i64,
    high: i64,
    low: i64,
    close: i64,
    volume: i64,
}

impl JsonKlineRecord {
    fn to_kline_record(&self) -> KlineRecord {
        KlineRecord {
            ts: self.ts,
            open: self.open,
            high: self.high,
            low: self.low,
            close: self.close,
            volume: self.volume,
        }
    }
}

#[derive(Debug, Serialize)]
struct Summary {
    measured_at_utc: String,
    os: String,
    arch: String,
    config: ConfigSummary,
    total_rows: usize,
    backends: Vec<BackendResult>,
    notes: Vec<String>,
}

#[derive(Debug, Serialize)]
struct ConfigSummary {
    root: String,
    symbol_count: usize,
    months: usize,
    records_per_month: usize,
    total_rows: usize,
    timeframe: String,
    timeframe_ms: i64,
    range_ops: usize,
    range_rows: usize,
}

#[derive(Debug, Serialize)]
struct BackendResult {
    backend: String,
    rows_written: usize,
    bytes_on_disk: u64,
    write_seconds: f64,
    full_read_seconds: f64,
    full_read_rows: usize,
    checksum: i128,
    range_read: Option<RangeReadResult>,
}

#[derive(Debug, Serialize)]
struct RangeReadResult {
    ops: usize,
    rows_per_op: usize,
    rows_read: usize,
    seconds: f64,
    checksum: i128,
}
