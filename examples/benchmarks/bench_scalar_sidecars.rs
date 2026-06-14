use std::env;
use std::fs::{self, File};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::time::Instant;

use chrono::{TimeZone, Utc};
use fastk::{
    BenchmarkMetrics, CompareOp, FastKError, FastKReadSession, FastKStore, LatencySummary,
    MetricsAccumulator, MetricsLevel, Result, ScalarPredicate, ScalarRecord, ScalarSeriesKey,
    DEFAULT_SCALAR_ZMAP_BLOCK_SIZE,
};
use serde::Serialize;

fn main() {
    if let Err(err) = run() {
        eprintln!("bench_scalar_sidecars failed: {err}");
        std::process::exit(1);
    }
}

fn run() -> Result<()> {
    let config = Config::parse(env::args().skip(1))?;
    if config.root.exists() {
        fs::remove_dir_all(&config.root)?;
    }
    fs::create_dir_all(&config.root)?;
    let records = build_records(config.rows, config.timeframe_ms)?;
    let predicate = ScalarPredicate {
        op: CompareOp::Between,
        value: -50,
        value2: Some(50),
    };

    let mut results = Vec::new();
    for &block_size in &config.block_sizes {
        results.push(run_block_size_case(
            &config, &records, &predicate, block_size,
        )?);
    }

    let summary = Summary {
        config: config.describe(),
        recommended_block_size: DEFAULT_SCALAR_ZMAP_BLOCK_SIZE as u32,
        results,
    };
    let output_path = config.root.join("scalar-sidecar-results.json");
    let mut writer = BufWriter::new(File::create(&output_path)?);
    serde_json::to_writer_pretty(&mut writer, &summary).map_err(FastKError::from)?;
    writer.write_all(b"\n")?;
    writer.flush()?;

    println!(
        "{}",
        serde_json::to_string_pretty(&summary).map_err(FastKError::from)?
    );
    println!("results saved  : {}", output_path.display());
    Ok(())
}

fn run_block_size_case(
    config: &Config,
    records: &[ScalarRecord],
    predicate: &ScalarPredicate,
    block_size: usize,
) -> Result<BlockSizeResult> {
    let root = config.root.join(format!("block-{block_size}"));
    let key = ScalarSeriesKey {
        symbol: "BTCUSDT".to_string(),
        category: "indicator".to_string(),
        name: format!("rsi14-b{block_size}"),
    };

    let write_started = Instant::now();
    let mut store = open_fastk_with_level(&root, config.metrics_level)?;
    store.init()?;
    store.register_scalar_series(&key, config.timeframe_ms)?;
    for chunk in split_scalar_by_month(records)? {
        store.put_scalar_chunk(&key, config.timeframe_ms, &chunk, block_size)?;
    }
    let write_seconds = write_started.elapsed().as_secs_f64();
    let write_metrics = store.metrics_snapshot();

    let point_ts = records[records.len() / 2].ts;
    let range_start_index =
        (records.len() / 3).min(records.len().saturating_sub(config.range_rows.max(1)));
    let range_end_index =
        (range_start_index + config.range_rows.saturating_sub(1)).min(records.len() - 1);
    let range_start = records[range_start_index].ts;
    let range_end = records[range_end_index].ts;
    let query_start = records[0].ts;
    let query_end = records[records.len() - 1].ts;

    let point_lookup = measure_scalar_session_query(
        &root,
        &key,
        config.metrics_level,
        config.query_ops,
        |session| {
            let row = session.get_scalar_at(&key, point_ts)?;
            debug_assert_eq!(row.map(|record| record.ts), Some(point_ts));
            Ok(())
        },
    )?;
    let short_range = measure_scalar_session_query(
        &root,
        &key,
        config.metrics_level,
        config.query_ops,
        |session| {
            let rows = session.get_scalar_range(&key, range_start, range_end)?;
            debug_assert!(!rows.is_empty());
            Ok(())
        },
    )?;
    let zmap_query = measure_scalar_session_query(
        &root,
        &key,
        config.metrics_level,
        config.query_ops,
        |session| {
            let rows =
                session.find_scalar_timestamps_via_zmap(&key, predicate, query_start, query_end)?;
            debug_assert!(!rows.is_empty());
            Ok(())
        },
    )?;
    let vix_query = measure_scalar_session_query(
        &root,
        &key,
        config.metrics_level,
        config.query_ops,
        |session| {
            let rows =
                session.find_scalar_timestamps_via_vix(&key, predicate, query_start, query_end)?;
            debug_assert!(!rows.is_empty());
            Ok(())
        },
    )?;

    Ok(BlockSizeResult {
        block_size: block_size as u32,
        write_seconds,
        write_metrics: BenchmarkMetrics::from_snapshot(write_metrics),
        point_lookup,
        short_range,
        zmap_query,
        vix_query,
    })
}

fn measure_scalar_session_query<F>(
    root: &Path,
    key: &ScalarSeriesKey,
    metrics_level: MetricsLevel,
    ops: usize,
    mut query: F,
) -> Result<QueryBench>
where
    F: FnMut(&FastKReadSession<'_>) -> Result<()>,
{
    let cold_store = open_fastk_with_level(root, metrics_level)?;
    cold_store.reset_metrics();
    let mut cold_session = cold_store.read_session();
    let first_load_started = Instant::now();
    cold_session.attach_scalar_series(key)?;
    query(&cold_session)?;
    let first_load_seconds = first_load_started.elapsed().as_secs_f64();

    let store = open_fastk_with_level(root, metrics_level)?;
    let mut session = store.read_session();
    session.attach_scalar_series(key)?;
    session.prewarm()?;
    store.reset_metrics();

    let mut samples = Vec::with_capacity(ops);
    for _ in 0..ops {
        let started = Instant::now();
        query(&session)?;
        samples.push(started.elapsed());
    }

    let mut metrics = MetricsAccumulator::default();
    metrics.add_snapshot(store.metrics_snapshot());
    Ok(QueryBench {
        first_load_seconds,
        latency: LatencySummary::from_durations(&samples, ops),
        metrics: metrics.finish(),
    })
}

fn build_records(rows: usize, timeframe_ms: i64) -> Result<Vec<ScalarRecord>> {
    let start_ts = Utc
        .with_ymd_and_hms(2024, 1, 1, 0, 0, 0)
        .single()
        .ok_or_else(|| FastKError::InvalidInput("invalid scalar benchmark start date".to_string()))?
        .timestamp_millis();
    let mut records = Vec::with_capacity(rows);
    for index in 0..rows {
        let wave = ((index % 200) as i64) - 100;
        records.push(ScalarRecord {
            ts: start_ts + index as i64 * timeframe_ms,
            value: wave * wave.signum(),
        });
    }
    Ok(records)
}

fn split_scalar_by_month(records: &[ScalarRecord]) -> Result<Vec<Vec<ScalarRecord>>> {
    let mut groups = Vec::new();
    let mut current = Vec::new();
    let mut current_month = None;

    for record in records {
        let month_key = chrono::DateTime::<Utc>::from_timestamp_millis(record.ts)
            .ok_or_else(|| FastKError::InvalidInput(format!("invalid scalar ts: {}", record.ts)))?
            .format("%Y-%m")
            .to_string();
        if current_month.as_deref() != Some(month_key.as_str()) && !current.is_empty() {
            groups.push(current);
            current = Vec::new();
        }
        current_month = Some(month_key);
        current.push(*record);
    }

    if !current.is_empty() {
        groups.push(current);
    }
    Ok(groups)
}

fn open_fastk_with_level(path: &Path, level: MetricsLevel) -> Result<FastKStore> {
    let store = FastKStore::open(path)?;
    store.set_metrics_level(level);
    Ok(store)
}

#[derive(Debug, Clone)]
struct Config {
    root: PathBuf,
    rows: usize,
    timeframe_ms: i64,
    query_ops: usize,
    range_rows: usize,
    block_sizes: Vec<usize>,
    metrics_level: MetricsLevel,
}

impl Config {
    fn parse<I>(args: I) -> Result<Self>
    where
        I: IntoIterator<Item = String>,
    {
        let mut config = Self {
            root: PathBuf::from("target/scalar-sidecars"),
            rows: 200_000,
            timeframe_ms: 60_000,
            query_ops: 128,
            range_rows: 256,
            block_sizes: vec![128, 256, 512, 1024],
            metrics_level: MetricsLevel::Basic,
        };

        let mut args = args.into_iter();
        while let Some(arg) = args.next() {
            match arg.as_str() {
                "--root" => config.root = PathBuf::from(next_value(&mut args, "--root")?),
                "--rows" => config.rows = parse_usize(next_value(&mut args, "--rows")?)?,
                "--timeframe-ms" => {
                    config.timeframe_ms = parse_i64(next_value(&mut args, "--timeframe-ms")?)?
                }
                "--query-ops" => {
                    config.query_ops = parse_usize(next_value(&mut args, "--query-ops")?)?
                }
                "--range-rows" => {
                    config.range_rows = parse_usize(next_value(&mut args, "--range-rows")?)?
                }
                "--metrics-level" => {
                    config.metrics_level =
                        parse_metrics_level(next_value(&mut args, "--metrics-level")?)?
                }
                "--block-sizes" => {
                    config.block_sizes = next_value(&mut args, "--block-sizes")?
                        .split(',')
                        .map(|value| {
                            value.trim().parse::<usize>().map_err(|err| {
                                FastKError::InvalidInput(format!(
                                    "invalid block size '{value}': {err}",
                                ))
                            })
                        })
                        .collect::<Result<Vec<_>>>()?;
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

    fn describe(&self) -> ConfigSummary {
        ConfigSummary {
            root: self.root.display().to_string(),
            rows: self.rows,
            timeframe_ms: self.timeframe_ms,
            query_ops: self.query_ops,
            range_rows: self.range_rows,
            block_sizes: self.block_sizes.clone(),
            metrics_level: self.metrics_level.as_str().to_string(),
        }
    }
}

#[derive(Debug, Serialize)]
struct ConfigSummary {
    root: String,
    rows: usize,
    timeframe_ms: i64,
    query_ops: usize,
    range_rows: usize,
    block_sizes: Vec<usize>,
    metrics_level: String,
}

#[derive(Debug, Serialize)]
struct Summary {
    config: ConfigSummary,
    recommended_block_size: u32,
    results: Vec<BlockSizeResult>,
}

#[derive(Debug, Serialize)]
struct BlockSizeResult {
    block_size: u32,
    write_seconds: f64,
    write_metrics: BenchmarkMetrics,
    point_lookup: QueryBench,
    short_range: QueryBench,
    zmap_query: QueryBench,
    vix_query: QueryBench,
}

#[derive(Debug, Serialize)]
struct QueryBench {
    first_load_seconds: f64,
    latency: LatencySummary,
    metrics: BenchmarkMetrics,
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

fn parse_i64(value: String) -> Result<i64> {
    value
        .parse::<i64>()
        .map_err(|err| FastKError::InvalidInput(format!("invalid i64 '{value}': {err}")))
}

fn parse_metrics_level(value: String) -> Result<MetricsLevel> {
    match value.to_ascii_lowercase().as_str() {
        "off" => Ok(MetricsLevel::Off),
        "basic" => Ok(MetricsLevel::Basic),
        "detailed" => Ok(MetricsLevel::Detailed),
        other => Err(FastKError::InvalidInput(format!(
            "invalid metrics level '{other}', expected off/basic/detailed",
        ))),
    }
}

fn print_help() {
    println!("bench_scalar_sidecars [options]");
    println!("  --root <path>");
    println!("  --rows <count>");
    println!("  --timeframe-ms <ms>");
    println!("  --query-ops <count>");
    println!("  --range-rows <count>");
    println!("  --metrics-level <off|basic|detailed>");
    println!("  --block-sizes <csv>");
}
