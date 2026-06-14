use std::env;
use std::fs::{self, File};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::time::Instant;

use chrono::{TimeZone, Utc};
use fastk::{
    BacktestKlineBinding, BacktestPreparePlan, BenchmarkMetrics, CompareOp, FastKError, FastKStore,
    KlineRecord, LatencySummary, MetricsAccumulator, MetricsLevel, Result, ScalarPredicate,
    ScalarRecord, ScopedScalarBinding, INDICATOR_CATEGORY,
};
use serde::Serialize;

fn main() {
    if let Err(err) = run() {
        eprintln!("bench_backtest_facade failed: {err}");
        std::process::exit(1);
    }
}

fn run() -> Result<()> {
    let config = Config::parse(env::args().skip(1))?;
    if config.root.exists() {
        fs::remove_dir_all(&config.root)?;
    }
    fs::create_dir_all(&config.root)?;

    let dataset = Dataset::build(&config)?;
    let store_root = config.root.join("fastk");
    populate_store(&store_root, &dataset, &config)?;

    let summary = Summary {
        config: config.describe(),
        results: vec![
            measure_attach_single(&config, &store_root, &dataset)?,
            measure_attach_many(&config, &store_root, &dataset)?,
            measure_first_query_direct_store(&config, &store_root, &dataset)?,
            measure_first_query_after_attach_session(&config, &store_root, &dataset)?,
            measure_first_query_after_attach_facade(&config, &store_root, &dataset)?,
            measure_first_range_direct_store(&config, &store_root, &dataset)?,
            measure_first_range_after_attach_session(&config, &store_root, &dataset)?,
            measure_first_range_after_attach_facade(&config, &store_root, &dataset)?,
            measure_repeated_query_after_prewarm_session(&config, &store_root, &dataset)?,
            measure_repeated_query_after_prewarm_facade(&config, &store_root, &dataset)?,
            measure_scalar_point_first_touch(&config, &store_root, &dataset)?,
            measure_scalar_point_repeated(&config, &store_root, &dataset)?,
            measure_scalar_short_range_first_touch(&config, &store_root, &dataset)?,
            measure_scalar_short_range_repeated(&config, &store_root, &dataset)?,
            measure_scalar_zmap_first_touch(&config, &store_root, &dataset)?,
            measure_scalar_zmap_repeated(&config, &store_root, &dataset)?,
            measure_scalar_vix_first_touch(&config, &store_root, &dataset)?,
            measure_scalar_vix_repeated(&config, &store_root, &dataset)?,
        ],
    };

    let output_path = config.root.join("backtest-facade-bench.json");
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

fn measure_attach_single(
    config: &Config,
    store_root: &Path,
    dataset: &Dataset,
) -> Result<WorkloadResult> {
    let mut samples = Vec::with_capacity(config.attach_samples);
    let mut metrics = MetricsAccumulator::default();
    for _ in 0..config.attach_samples {
        let store = open_store(store_root, config.metrics_level)?;
        store.reset_metrics();
        let mut session = store.read_session();
        let started = Instant::now();
        session.attach_kline_series(&dataset.symbols[0], &config.timeframe)?;
        samples.push(started.elapsed());
        metrics.add_snapshot(store.metrics_snapshot());
    }
    Ok(WorkloadResult::new(
        "attach_single",
        LatencySummary::from_durations(&samples, config.attach_samples),
        metrics.finish(),
        "approx_cold_reopen_like_attach",
    ))
}

fn measure_attach_many(
    config: &Config,
    store_root: &Path,
    dataset: &Dataset,
) -> Result<WorkloadResult> {
    let bindings: Vec<_> = dataset
        .symbols
        .iter()
        .map(|symbol| (symbol.as_str(), config.timeframe.as_str()))
        .collect();
    let mut samples = Vec::with_capacity(config.attach_samples);
    let mut metrics = MetricsAccumulator::default();
    for _ in 0..config.attach_samples {
        let store = open_store(store_root, config.metrics_level)?;
        store.reset_metrics();
        let mut session = store.read_session();
        let started = Instant::now();
        session.attach_kline_many(bindings.iter().copied())?;
        samples.push(started.elapsed());
        metrics.add_snapshot(store.metrics_snapshot());
    }
    Ok(WorkloadResult::new(
        "attach_many",
        LatencySummary::from_durations(&samples, config.attach_samples * bindings.len()),
        metrics.finish(),
        "approx_cold_reopen_like_attach_many",
    ))
}

fn measure_first_query_direct_store(
    config: &Config,
    store_root: &Path,
    dataset: &Dataset,
) -> Result<WorkloadResult> {
    let ts = dataset.kline_point_ts;
    let mut samples = Vec::with_capacity(config.attach_samples);
    let mut metrics = MetricsAccumulator::default();
    for _ in 0..config.attach_samples {
        let store = open_store(store_root, config.metrics_level)?;
        store.reset_metrics();
        let started = Instant::now();
        let row = store.get_kline_at(&dataset.symbols[0], &config.timeframe, ts)?;
        debug_assert!(row.is_some());
        samples.push(started.elapsed());
        metrics.add_snapshot(store.metrics_snapshot());
    }
    Ok(WorkloadResult::new(
        "first_query_after_open_kline_at",
        LatencySummary::from_durations(&samples, config.attach_samples),
        metrics.finish(),
        "approx_cold_direct_query_without_attach",
    ))
}

fn measure_first_query_after_attach_session(
    config: &Config,
    store_root: &Path,
    dataset: &Dataset,
) -> Result<WorkloadResult> {
    let ts = dataset.kline_point_ts;
    let mut samples = Vec::with_capacity(config.attach_samples);
    let mut metrics = MetricsAccumulator::default();
    for _ in 0..config.attach_samples {
        let store = open_store(store_root, config.metrics_level)?;
        store.reset_metrics();
        let mut session = store.read_session();
        let started = Instant::now();
        session.attach_kline_series(&dataset.symbols[0], &config.timeframe)?;
        let row = session.get_kline_at(&dataset.symbols[0], &config.timeframe, ts)?;
        debug_assert!(row.is_some());
        samples.push(started.elapsed());
        metrics.add_snapshot(store.metrics_snapshot());
    }
    Ok(WorkloadResult::new(
        "first_query_after_attach_kline_at_session",
        LatencySummary::from_durations(&samples, config.attach_samples),
        metrics.finish(),
        "approx_cold_attach_then_first_query",
    ))
}

fn measure_first_query_after_attach_facade(
    config: &Config,
    store_root: &Path,
    dataset: &Dataset,
) -> Result<WorkloadResult> {
    let ts = dataset.kline_point_ts;
    let mut samples = Vec::with_capacity(config.attach_samples);
    let mut metrics = MetricsAccumulator::default();
    for _ in 0..config.attach_samples {
        let store = open_store(store_root, config.metrics_level)?;
        store.reset_metrics();
        let mut view = store.backtest_view();
        let started = Instant::now();
        view.initialize(&BacktestPreparePlan {
            kline: vec![BacktestKlineBinding {
                symbol: dataset.symbols[0].clone(),
                timeframe: config.timeframe.clone(),
            }],
            scalar: Vec::new(),
            prewarm: false,
        })?;
        let row = view.get_kline_at(&dataset.symbols[0], &config.timeframe, ts)?;
        debug_assert!(row.is_some());
        samples.push(started.elapsed());
        metrics.add_snapshot(store.metrics_snapshot());
    }
    Ok(WorkloadResult::new(
        "first_query_after_attach_kline_at_facade",
        LatencySummary::from_durations(&samples, config.attach_samples),
        metrics.finish(),
        "approx_cold_backtest_facade_initialize_then_query",
    ))
}

fn measure_first_range_direct_store(
    config: &Config,
    store_root: &Path,
    dataset: &Dataset,
) -> Result<WorkloadResult> {
    let mut samples = Vec::with_capacity(config.attach_samples);
    let mut metrics = MetricsAccumulator::default();
    for _ in 0..config.attach_samples {
        let store = open_store(store_root, config.metrics_level)?;
        store.reset_metrics();
        let started = Instant::now();
        let rows = store.get_kline_range(
            &dataset.symbols[0],
            &config.timeframe,
            dataset.kline_range_start,
            dataset.kline_range_end,
        )?;
        debug_assert_eq!(rows.len(), config.range_rows);
        samples.push(started.elapsed());
        metrics.add_snapshot(store.metrics_snapshot());
    }
    Ok(WorkloadResult::new(
        "first_query_after_open_kline_range",
        LatencySummary::from_durations(&samples, config.attach_samples),
        metrics.finish(),
        "approx_cold_direct_range_without_attach",
    ))
}

fn measure_first_range_after_attach_session(
    config: &Config,
    store_root: &Path,
    dataset: &Dataset,
) -> Result<WorkloadResult> {
    let mut samples = Vec::with_capacity(config.attach_samples);
    let mut metrics = MetricsAccumulator::default();
    for _ in 0..config.attach_samples {
        let store = open_store(store_root, config.metrics_level)?;
        store.reset_metrics();
        let mut session = store.read_session();
        let started = Instant::now();
        session.attach_kline_series(&dataset.symbols[0], &config.timeframe)?;
        let rows = session.get_kline_range(
            &dataset.symbols[0],
            &config.timeframe,
            dataset.kline_range_start,
            dataset.kline_range_end,
        )?;
        debug_assert_eq!(rows.len(), config.range_rows);
        samples.push(started.elapsed());
        metrics.add_snapshot(store.metrics_snapshot());
    }
    Ok(WorkloadResult::new(
        "first_query_after_attach_kline_range_session",
        LatencySummary::from_durations(&samples, config.attach_samples),
        metrics.finish(),
        "approx_cold_attach_then_first_range_query",
    ))
}

fn measure_first_range_after_attach_facade(
    config: &Config,
    store_root: &Path,
    dataset: &Dataset,
) -> Result<WorkloadResult> {
    let mut samples = Vec::with_capacity(config.attach_samples);
    let mut metrics = MetricsAccumulator::default();
    for _ in 0..config.attach_samples {
        let store = open_store(store_root, config.metrics_level)?;
        store.reset_metrics();
        let mut view = store.backtest_view();
        let started = Instant::now();
        view.initialize(&BacktestPreparePlan {
            kline: vec![BacktestKlineBinding {
                symbol: dataset.symbols[0].clone(),
                timeframe: config.timeframe.clone(),
            }],
            scalar: Vec::new(),
            prewarm: false,
        })?;
        let rows = view.get_kline_range(
            &dataset.symbols[0],
            &config.timeframe,
            dataset.kline_range_start,
            dataset.kline_range_end,
        )?;
        debug_assert_eq!(rows.len(), config.range_rows);
        samples.push(started.elapsed());
        metrics.add_snapshot(store.metrics_snapshot());
    }
    Ok(WorkloadResult::new(
        "first_query_after_attach_kline_range_facade",
        LatencySummary::from_durations(&samples, config.attach_samples),
        metrics.finish(),
        "approx_cold_backtest_facade_initialize_then_range_query",
    ))
}

fn measure_repeated_query_after_prewarm_session(
    config: &Config,
    store_root: &Path,
    dataset: &Dataset,
) -> Result<WorkloadResult> {
    let ts = dataset.kline_point_ts;
    let store = open_store(store_root, config.metrics_level)?;
    let mut session = store.read_session();
    session.attach_kline_many(
        dataset
            .symbols
            .iter()
            .map(|symbol| (symbol.as_str(), config.timeframe.as_str())),
    )?;
    session.prewarm()?;
    store.reset_metrics();

    let mut samples = Vec::with_capacity(config.query_ops);
    for _ in 0..config.query_ops {
        let started = Instant::now();
        let row = session.get_kline_at(&dataset.symbols[0], &config.timeframe, ts)?;
        debug_assert!(row.is_some());
        samples.push(started.elapsed());
    }

    Ok(WorkloadResult::new(
        "repeated_query_after_prewarm_kline_at_session",
        LatencySummary::from_durations(&samples, config.query_ops),
        BenchmarkMetrics::from_snapshot(store.metrics_snapshot()),
        "warm_session_after_prewarm",
    ))
}

fn measure_repeated_query_after_prewarm_facade(
    config: &Config,
    store_root: &Path,
    dataset: &Dataset,
) -> Result<WorkloadResult> {
    let ts = dataset.kline_point_ts;
    let store = open_store(store_root, config.metrics_level)?;
    let mut view = store.backtest_view();
    view.initialize(&BacktestPreparePlan {
        kline: dataset
            .symbols
            .iter()
            .map(|symbol| BacktestKlineBinding {
                symbol: symbol.clone(),
                timeframe: config.timeframe.clone(),
            })
            .collect(),
        scalar: vec![dataset.scalar_binding.clone()],
        prewarm: true,
    })?;
    store.reset_metrics();

    let mut samples = Vec::with_capacity(config.query_ops);
    for _ in 0..config.query_ops {
        let started = Instant::now();
        let row = view.get_kline_at(&dataset.symbols[0], &config.timeframe, ts)?;
        debug_assert!(row.is_some());
        samples.push(started.elapsed());
    }

    Ok(WorkloadResult::new(
        "repeated_query_after_prewarm_kline_at_facade",
        LatencySummary::from_durations(&samples, config.query_ops),
        BenchmarkMetrics::from_snapshot(store.metrics_snapshot()),
        "warm_backtest_facade_after_prewarm",
    ))
}

fn measure_scalar_point_first_touch(
    config: &Config,
    store_root: &Path,
    dataset: &Dataset,
) -> Result<WorkloadResult> {
    scalar_first_touch(
        config,
        store_root,
        dataset,
        "scalar_point_first_touch",
        |view, data| {
            let row = view.get_scalar_at(
                &data.scalar_binding.symbol,
                &data.scalar_binding.timeframe,
                &data.scalar_binding.category,
                &data.scalar_binding.name,
                data.scalar_point_ts,
            )?;
            debug_assert!(row.is_some());
            Ok(())
        },
    )
}

fn measure_scalar_short_range_first_touch(
    config: &Config,
    store_root: &Path,
    dataset: &Dataset,
) -> Result<WorkloadResult> {
    scalar_first_touch(
        config,
        store_root,
        dataset,
        "scalar_short_range_first_touch",
        |view, data| {
            let rows = view.get_scalar_range(
                &data.scalar_binding.symbol,
                &data.scalar_binding.timeframe,
                &data.scalar_binding.category,
                &data.scalar_binding.name,
                data.scalar_range_start,
                data.scalar_range_end,
            )?;
            debug_assert!(!rows.is_empty());
            Ok(())
        },
    )
}

fn measure_scalar_zmap_first_touch(
    config: &Config,
    store_root: &Path,
    dataset: &Dataset,
) -> Result<WorkloadResult> {
    scalar_first_touch(
        config,
        store_root,
        dataset,
        "scalar_zmap_first_touch",
        |view, data| {
            let rows = view.find_scalar_timestamps_via_zmap(
                &data.scalar_binding.symbol,
                &data.scalar_binding.timeframe,
                &data.scalar_binding.category,
                &data.scalar_binding.name,
                &data.scalar_predicate,
                data.scalar_query_start,
                data.scalar_query_end,
            )?;
            debug_assert!(!rows.is_empty());
            Ok(())
        },
    )
}

fn measure_scalar_vix_first_touch(
    config: &Config,
    store_root: &Path,
    dataset: &Dataset,
) -> Result<WorkloadResult> {
    scalar_first_touch(
        config,
        store_root,
        dataset,
        "scalar_vix_first_touch",
        |view, data| {
            let rows = view.find_scalar_timestamps_via_vix(
                &data.scalar_binding.symbol,
                &data.scalar_binding.timeframe,
                &data.scalar_binding.category,
                &data.scalar_binding.name,
                &data.scalar_predicate,
                data.scalar_query_start,
                data.scalar_query_end,
            )?;
            debug_assert!(!rows.is_empty());
            Ok(())
        },
    )
}

fn measure_scalar_point_repeated(
    config: &Config,
    store_root: &Path,
    dataset: &Dataset,
) -> Result<WorkloadResult> {
    scalar_repeated(
        config,
        store_root,
        dataset,
        "scalar_point_repeated",
        |view, data| {
            let row = view.get_scalar_at(
                &data.scalar_binding.symbol,
                &data.scalar_binding.timeframe,
                &data.scalar_binding.category,
                &data.scalar_binding.name,
                data.scalar_point_ts,
            )?;
            debug_assert!(row.is_some());
            Ok(())
        },
    )
}

fn measure_scalar_short_range_repeated(
    config: &Config,
    store_root: &Path,
    dataset: &Dataset,
) -> Result<WorkloadResult> {
    scalar_repeated(
        config,
        store_root,
        dataset,
        "scalar_short_range_repeated",
        |view, data| {
            let rows = view.get_scalar_range(
                &data.scalar_binding.symbol,
                &data.scalar_binding.timeframe,
                &data.scalar_binding.category,
                &data.scalar_binding.name,
                data.scalar_range_start,
                data.scalar_range_end,
            )?;
            debug_assert!(!rows.is_empty());
            Ok(())
        },
    )
}

fn measure_scalar_zmap_repeated(
    config: &Config,
    store_root: &Path,
    dataset: &Dataset,
) -> Result<WorkloadResult> {
    scalar_repeated(
        config,
        store_root,
        dataset,
        "scalar_zmap_repeated",
        |view, data| {
            let rows = view.find_scalar_timestamps_via_zmap(
                &data.scalar_binding.symbol,
                &data.scalar_binding.timeframe,
                &data.scalar_binding.category,
                &data.scalar_binding.name,
                &data.scalar_predicate,
                data.scalar_query_start,
                data.scalar_query_end,
            )?;
            debug_assert!(!rows.is_empty());
            Ok(())
        },
    )
}

fn measure_scalar_vix_repeated(
    config: &Config,
    store_root: &Path,
    dataset: &Dataset,
) -> Result<WorkloadResult> {
    scalar_repeated(
        config,
        store_root,
        dataset,
        "scalar_vix_repeated",
        |view, data| {
            let rows = view.find_scalar_timestamps_via_vix(
                &data.scalar_binding.symbol,
                &data.scalar_binding.timeframe,
                &data.scalar_binding.category,
                &data.scalar_binding.name,
                &data.scalar_predicate,
                data.scalar_query_start,
                data.scalar_query_end,
            )?;
            debug_assert!(!rows.is_empty());
            Ok(())
        },
    )
}

fn scalar_first_touch<F>(
    config: &Config,
    store_root: &Path,
    dataset: &Dataset,
    workload: &str,
    mut query: F,
) -> Result<WorkloadResult>
where
    F: FnMut(&fastk::BacktestStoreView<'_>, &Dataset) -> Result<()>,
{
    let mut samples = Vec::with_capacity(config.attach_samples);
    let mut metrics = MetricsAccumulator::default();
    for _ in 0..config.attach_samples {
        let store = open_store(store_root, config.metrics_level)?;
        store.reset_metrics();
        let mut view = store.backtest_view();
        view.initialize(&BacktestPreparePlan {
            kline: Vec::new(),
            scalar: vec![dataset.scalar_binding.clone()],
            prewarm: false,
        })?;
        let started = Instant::now();
        query(&view, dataset)?;
        samples.push(started.elapsed());
        metrics.add_snapshot(store.metrics_snapshot());
    }
    Ok(WorkloadResult::new(
        workload,
        LatencySummary::from_durations(&samples, config.attach_samples),
        metrics.finish(),
        "approx_cold_scalar_first_touch_after_attach",
    ))
}

fn scalar_repeated<F>(
    config: &Config,
    store_root: &Path,
    dataset: &Dataset,
    workload: &str,
    mut query: F,
) -> Result<WorkloadResult>
where
    F: FnMut(&fastk::BacktestStoreView<'_>, &Dataset) -> Result<()>,
{
    let store = open_store(store_root, config.metrics_level)?;
    let mut view = store.backtest_view();
    view.initialize(&BacktestPreparePlan {
        kline: Vec::new(),
        scalar: vec![dataset.scalar_binding.clone()],
        prewarm: true,
    })?;
    store.reset_metrics();

    let mut samples = Vec::with_capacity(config.query_ops);
    for _ in 0..config.query_ops {
        let started = Instant::now();
        query(&view, dataset)?;
        samples.push(started.elapsed());
    }

    Ok(WorkloadResult::new(
        workload,
        LatencySummary::from_durations(&samples, config.query_ops),
        BenchmarkMetrics::from_snapshot(store.metrics_snapshot()),
        "warm_scalar_query_after_prewarm",
    ))
}

fn populate_store(root: &Path, dataset: &Dataset, config: &Config) -> Result<()> {
    let mut store = open_store(root, config.metrics_level)?;
    store.init()?;
    for symbol in &dataset.symbols {
        store.register_kline_series(
            symbol,
            &config.timeframe,
            config.timeframe_ms,
            config.price_scale,
            config.volume_scale,
        )?;
    }
    for (symbol, months) in dataset.kline_months.iter().enumerate() {
        for chunk in months {
            store.put_kline_chunk(&dataset.symbols[symbol], &config.timeframe, chunk)?;
        }
    }

    if dataset.scalar_binding.category == INDICATOR_CATEGORY {
        store.register_indicator_series(
            &dataset.scalar_binding.symbol,
            &dataset.scalar_binding.timeframe,
            &dataset.scalar_binding.name,
        )?;
        for chunk in &dataset.scalar_months {
            store.put_indicator_chunk(
                &dataset.scalar_binding.symbol,
                &dataset.scalar_binding.timeframe,
                &dataset.scalar_binding.name,
                chunk,
            )?;
        }
    } else {
        let scalar_key = dataset.scalar_binding.to_series_key();
        store.register_scalar_series(&scalar_key, config.timeframe_ms)?;
        for chunk in &dataset.scalar_months {
            store.put_scalar_chunk(
                &scalar_key,
                config.timeframe_ms,
                chunk,
                config.zmap_block_size,
            )?;
        }
    }
    Ok(())
}

fn open_store(root: &Path, metrics_level: MetricsLevel) -> Result<FastKStore> {
    let store = FastKStore::open(root)?;
    store.set_metrics_level(metrics_level);
    Ok(store)
}

#[derive(Debug, Clone)]
struct Config {
    root: PathBuf,
    timeframe: String,
    timeframe_ms: i64,
    price_scale: i64,
    volume_scale: i64,
    metrics_level: MetricsLevel,
    symbol_count: usize,
    months: usize,
    records_per_month: usize,
    range_rows: usize,
    attach_samples: usize,
    query_ops: usize,
    zmap_block_size: usize,
}

impl Config {
    fn parse<I>(args: I) -> Result<Self>
    where
        I: IntoIterator<Item = String>,
    {
        let mut config = Self {
            root: PathBuf::from("target/backtest-facade-bench"),
            timeframe: "1m".to_string(),
            timeframe_ms: 60_000,
            price_scale: 100_000,
            volume_scale: 100_000,
            metrics_level: MetricsLevel::Basic,
            symbol_count: 4,
            months: 4,
            records_per_month: 10_000,
            range_rows: 256,
            attach_samples: 32,
            query_ops: 256,
            zmap_block_size: 256,
        };

        let mut args = args.into_iter();
        while let Some(arg) = args.next() {
            match arg.as_str() {
                "--root" => config.root = PathBuf::from(next_value(&mut args, "--root")?),
                "--metrics-level" => {
                    config.metrics_level =
                        parse_metrics_level(next_value(&mut args, "--metrics-level")?)?
                }
                "--symbol-count" => {
                    config.symbol_count = parse_usize(next_value(&mut args, "--symbol-count")?)?
                }
                "--months" => config.months = parse_usize(next_value(&mut args, "--months")?)?,
                "--records-per-month" => {
                    config.records_per_month =
                        parse_usize(next_value(&mut args, "--records-per-month")?)?
                }
                "--range-rows" => {
                    config.range_rows = parse_usize(next_value(&mut args, "--range-rows")?)?
                }
                "--attach-samples" => {
                    config.attach_samples = parse_usize(next_value(&mut args, "--attach-samples")?)?
                }
                "--query-ops" => {
                    config.query_ops = parse_usize(next_value(&mut args, "--query-ops")?)?
                }
                "--zmap-block-size" => {
                    config.zmap_block_size =
                        parse_usize(next_value(&mut args, "--zmap-block-size")?)?
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
            timeframe: self.timeframe.clone(),
            timeframe_ms: self.timeframe_ms,
            metrics_level: self.metrics_level.as_str().to_string(),
            symbol_count: self.symbol_count,
            months: self.months,
            records_per_month: self.records_per_month,
            range_rows: self.range_rows,
            attach_samples: self.attach_samples,
            query_ops: self.query_ops,
            zmap_block_size: self.zmap_block_size,
        }
    }
}

#[derive(Debug, Serialize)]
struct ConfigSummary {
    root: String,
    timeframe: String,
    timeframe_ms: i64,
    metrics_level: String,
    symbol_count: usize,
    months: usize,
    records_per_month: usize,
    range_rows: usize,
    attach_samples: usize,
    query_ops: usize,
    zmap_block_size: usize,
}

#[derive(Debug, Serialize)]
struct Summary {
    config: ConfigSummary,
    results: Vec<WorkloadResult>,
}

#[derive(Debug, Serialize)]
struct WorkloadResult {
    workload: String,
    latency: LatencySummary,
    metrics: BenchmarkMetrics,
    note: String,
}

impl WorkloadResult {
    fn new(workload: &str, latency: LatencySummary, metrics: BenchmarkMetrics, note: &str) -> Self {
        Self {
            workload: workload.to_string(),
            latency,
            metrics,
            note: note.to_string(),
        }
    }
}

#[derive(Debug, Clone)]
struct Dataset {
    symbols: Vec<String>,
    kline_months: Vec<Vec<Vec<KlineRecord>>>,
    scalar_binding: ScopedScalarBinding,
    scalar_months: Vec<Vec<ScalarRecord>>,
    kline_point_ts: i64,
    kline_range_start: i64,
    kline_range_end: i64,
    scalar_point_ts: i64,
    scalar_range_start: i64,
    scalar_range_end: i64,
    scalar_query_start: i64,
    scalar_query_end: i64,
    scalar_predicate: ScalarPredicate,
}

impl Dataset {
    fn build(config: &Config) -> Result<Self> {
        let mut symbols = Vec::with_capacity(config.symbol_count);
        let mut kline_months = Vec::with_capacity(config.symbol_count);
        for symbol_idx in 0..config.symbol_count {
            let symbol = format!("SYM{symbol_idx:03}");
            symbols.push(symbol);
            let mut months = Vec::with_capacity(config.months);
            for month_idx in 0..config.months {
                months.push(build_kline_records(
                    symbol_idx,
                    month_idx,
                    config.records_per_month,
                    config.timeframe_ms,
                    config.price_scale,
                    config.volume_scale,
                )?);
            }
            kline_months.push(months);
        }

        let scalar_binding = ScopedScalarBinding::indicator(
            &symbols[0],
            &config.timeframe,
            &format!("rsi14-b{}", config.zmap_block_size),
        );
        let scalar_months =
            build_scalar_months(config.months, config.records_per_month, config.timeframe_ms)?;
        let flattened: Vec<_> = scalar_months
            .iter()
            .flat_map(|chunk| chunk.iter().copied())
            .collect();

        let kline_point_ts = kline_months[0][0][config.records_per_month / 2].ts;
        let kline_range_anchor = config.records_per_month / 3;
        let kline_range_end_idx =
            (kline_range_anchor + config.range_rows - 1).min(config.records_per_month - 1);
        let kline_range_start = kline_months[0][0][kline_range_anchor].ts;
        let kline_range_end = kline_months[0][0][kline_range_end_idx].ts;

        Ok(Self {
            symbols,
            kline_months,
            scalar_binding,
            kline_point_ts,
            kline_range_start,
            kline_range_end,
            scalar_point_ts: flattened[flattened.len() / 2].ts,
            scalar_range_start: flattened[flattened.len() / 3].ts,
            scalar_range_end: flattened[(flattened.len() / 3 + 255).min(flattened.len() - 1)].ts,
            scalar_query_start: flattened[0].ts,
            scalar_query_end: flattened[flattened.len() - 1].ts,
            scalar_predicate: ScalarPredicate {
                op: CompareOp::Between,
                value: -50,
                value2: Some(50),
            },
            scalar_months,
        })
    }
}

fn build_kline_records(
    symbol_idx: usize,
    month_idx: usize,
    rows: usize,
    timeframe_ms: i64,
    price_scale: i64,
    volume_scale: i64,
) -> Result<Vec<KlineRecord>> {
    let start_ts = month_start_ts(month_idx)?;
    let mut out = Vec::with_capacity(rows);
    let mut close = (50_000 + symbol_idx as i64 * 100) * price_scale;
    for index in 0..rows {
        close += (((index + month_idx) % 17) as i64 - 8) * 3;
        out.push(KlineRecord {
            ts: start_ts + index as i64 * timeframe_ms,
            open: close - 5,
            high: close + 10,
            low: close - 15,
            close,
            volume: (1_000 + month_idx as i64 * 10 + index as i64) * volume_scale,
        });
    }
    Ok(out)
}

fn build_scalar_months(
    months: usize,
    rows: usize,
    timeframe_ms: i64,
) -> Result<Vec<Vec<ScalarRecord>>> {
    let mut out = Vec::with_capacity(months);
    for month_idx in 0..months {
        let start_ts = month_start_ts(month_idx)?;
        let mut chunk = Vec::with_capacity(rows);
        for index in 0..rows {
            let wave = ((index % 200) as i64) - 100;
            chunk.push(ScalarRecord {
                ts: start_ts + index as i64 * timeframe_ms,
                value: wave * wave.signum(),
            });
        }
        out.push(chunk);
    }
    Ok(out)
}

fn month_start_ts(month_idx: usize) -> Result<i64> {
    let year = 2024 + (month_idx / 12) as i32;
    let month = (month_idx % 12 + 1) as u32;
    Utc.with_ymd_and_hms(year, month, 1, 0, 0, 0)
        .single()
        .map(|dt| dt.timestamp_millis())
        .ok_or_else(|| FastKError::InvalidInput(format!("invalid month index: {month_idx}")))
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
    println!("bench_backtest_facade [options]");
    println!("  --root <path>");
    println!("  --metrics-level <off|basic|detailed>");
    println!("  --symbol-count <n>");
    println!("  --months <n>");
    println!("  --records-per-month <n>");
    println!("  --range-rows <n>");
    println!("  --attach-samples <n>");
    println!("  --query-ops <n>");
    println!("  --zmap-block-size <n>");
}
