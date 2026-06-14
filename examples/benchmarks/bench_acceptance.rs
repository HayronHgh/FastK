use std::env;
use std::fs::{self, File};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::time::Instant;

use chrono::{TimeZone, Utc};
use fastk::{
    build_acceptance_matrix, BenchmarkMetrics, CompactionPolicy, FastKError, FastKStore,
    KlineRecord, LatencySummary, MetricsAccumulator, MetricsLevel, Result, Temperature,
    WorkloadDescriptor,
};
use rusqlite::{params, Connection, OptionalExtension};
use serde::Serialize;

const DEFAULT_SHORT_RANGE_SIZES: &[usize] = &[16, 64, 256, 1024];

fn main() {
    if let Err(err) = run() {
        eprintln!("bench_acceptance failed: {err}");
        std::process::exit(1);
    }
}

fn run() -> Result<()> {
    let config = Config::parse(env::args().skip(1))?;
    if config.root.exists() {
        fs::remove_dir_all(&config.root)?;
    }
    fs::create_dir_all(&config.root)?;
    let cold_cache_path = prepare_cold_cache_file(&config.root, config.cold_cache_mib)?;
    let dataset = Dataset::build(&config)?;

    let mut results = Vec::new();
    results.extend(bench_fastk(&config, &dataset, &cold_cache_path)?);
    results.extend(bench_sqlite_tuned(&config, &dataset, &cold_cache_path)?);

    let summary = Summary {
        config: config.describe(),
        matrix: build_acceptance_matrix(&config.short_range_sizes),
        results,
    };
    let output_path = config.root.join("acceptance-results.json");
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

fn bench_fastk(
    config: &Config,
    dataset: &Dataset,
    cold_cache_path: &Path,
) -> Result<Vec<WorkloadResult>> {
    let store_root = config.root.join("fastk");
    populate_fastk_store(&store_root, dataset, config)?;
    let mut results = Vec::new();

    results.push(measure_fastk_write(config, dataset)?);
    results.push(measure_fastk_append(config)?);
    results.push(measure_fastk_merge(config)?);
    results.push(measure_fastk_session_attach(
        config,
        dataset,
        &store_root,
        Temperature::ApproxCold,
        false,
    )?);
    results.push(measure_fastk_session_attach(
        config,
        dataset,
        &store_root,
        Temperature::ApproxCold,
        true,
    )?);
    results.push(run_fastk_point_workload(
        "get_at_hit",
        "single_chunk_single_series",
        Temperature::Warm,
        &store_root,
        config.metrics_level,
        &config.timeframe,
        dataset,
        cold_cache_path,
        config.point_ops,
        false,
        false,
    )?);
    results.push(run_fastk_point_workload(
        "get_at_hit",
        "single_chunk_single_series",
        Temperature::ApproxCold,
        &store_root,
        config.metrics_level,
        &config.timeframe,
        dataset,
        cold_cache_path,
        config.point_ops.min(128),
        false,
        false,
    )?);
    results.push(run_fastk_point_workload(
        "get_at_hit",
        "single_chunk_single_series",
        Temperature::StricterColdish,
        &store_root,
        config.metrics_level,
        &config.timeframe,
        dataset,
        cold_cache_path,
        config.point_ops.min(128),
        false,
        false,
    )?);
    results.push(run_fastk_point_workload(
        "get_at_miss",
        "multi_chunk_multi_series",
        Temperature::Warm,
        &store_root,
        config.metrics_level,
        &config.timeframe,
        dataset,
        cold_cache_path,
        config.point_ops,
        true,
        true,
    )?);
    results.push(run_fastk_point_workload(
        "get_at_miss",
        "multi_chunk_multi_series",
        Temperature::ApproxCold,
        &store_root,
        config.metrics_level,
        &config.timeframe,
        dataset,
        cold_cache_path,
        config.point_ops.min(128),
        true,
        true,
    )?);
    results.push(run_fastk_point_workload(
        "get_at_miss",
        "multi_chunk_multi_series",
        Temperature::StricterColdish,
        &store_root,
        config.metrics_level,
        &config.timeframe,
        dataset,
        cold_cache_path,
        config.point_ops.min(128),
        true,
        true,
    )?);

    for &range_rows in &config.short_range_sizes {
        results.push(run_fastk_range_workload(
            &format!("get_range_short_{range_rows}"),
            "single_chunk_single_series",
            Temperature::Warm,
            &store_root,
            config.metrics_level,
            &config.timeframe,
            dataset,
            cold_cache_path,
            config.range_ops,
            range_rows,
            false,
        )?);
        results.push(run_fastk_range_workload(
            &format!("get_range_short_{range_rows}"),
            "single_chunk_single_series",
            Temperature::ApproxCold,
            &store_root,
            config.metrics_level,
            &config.timeframe,
            dataset,
            cold_cache_path,
            config.range_ops.min(64),
            range_rows,
            false,
        )?);
        results.push(run_fastk_range_workload(
            &format!("get_range_short_{range_rows}"),
            "single_chunk_single_series",
            Temperature::StricterColdish,
            &store_root,
            config.metrics_level,
            &config.timeframe,
            dataset,
            cold_cache_path,
            config.range_ops.min(64),
            range_rows,
            false,
        )?);
    }

    results.push(run_fastk_range_workload(
        "get_range_medium",
        "multi_chunk_single_series",
        Temperature::Warm,
        &store_root,
        config.metrics_level,
        &config.timeframe,
        dataset,
        cold_cache_path,
        config.range_ops,
        config.medium_range_rows,
        true,
    )?);
    results.push(run_fastk_range_workload(
        "get_range_medium",
        "multi_chunk_single_series",
        Temperature::ApproxCold,
        &store_root,
        config.metrics_level,
        &config.timeframe,
        dataset,
        cold_cache_path,
        config.range_ops.min(64),
        config.medium_range_rows,
        true,
    )?);
    results.push(run_fastk_range_workload(
        "get_range_medium",
        "multi_chunk_single_series",
        Temperature::StricterColdish,
        &store_root,
        config.metrics_level,
        &config.timeframe,
        dataset,
        cold_cache_path,
        config.range_ops.min(64),
        config.medium_range_rows,
        true,
    )?);
    results.push(run_fastk_full_scan_workload(
        Temperature::Warm,
        &store_root,
        config.metrics_level,
        &config.timeframe,
        dataset,
        cold_cache_path,
        config.full_scan_ops,
    )?);
    results.push(run_fastk_full_scan_workload(
        Temperature::ApproxCold,
        &store_root,
        config.metrics_level,
        &config.timeframe,
        dataset,
        cold_cache_path,
        config.full_scan_ops.min(4),
    )?);
    results.push(run_fastk_full_scan_workload(
        Temperature::StricterColdish,
        &store_root,
        config.metrics_level,
        &config.timeframe,
        dataset,
        cold_cache_path,
        config.full_scan_ops.min(4),
    )?);
    results.push(run_fastk_latest_n_workload(
        Temperature::Warm,
        &store_root,
        config.metrics_level,
        &config.timeframe,
        dataset,
        cold_cache_path,
        config.latest_n_ops,
        config.latest_n_rows,
    )?);
    results.push(run_fastk_latest_n_workload(
        Temperature::ApproxCold,
        &store_root,
        config.metrics_level,
        &config.timeframe,
        dataset,
        cold_cache_path,
        config.latest_n_ops.min(64),
        config.latest_n_rows,
    )?);
    results.push(run_fastk_latest_n_workload(
        Temperature::StricterColdish,
        &store_root,
        config.metrics_level,
        &config.timeframe,
        dataset,
        cold_cache_path,
        config.latest_n_ops.min(64),
        config.latest_n_rows,
    )?);
    Ok(results)
}

fn bench_sqlite_tuned(
    config: &Config,
    dataset: &Dataset,
    cold_cache_path: &Path,
) -> Result<Vec<WorkloadResult>> {
    let db_path = config
        .root
        .join("sqlite_tuned")
        .join("fastk_acceptance.sqlite3");
    populate_sqlite_tuned(&db_path, dataset)?;
    let mut results = vec![
        measure_sqlite_write(config, dataset)?,
        run_sqlite_point_workload(
            "get_at_hit",
            "single_chunk_single_series",
            Temperature::Warm,
            &db_path,
            dataset,
            cold_cache_path,
            config.point_ops,
            false,
        )?,
        run_sqlite_point_workload(
            "get_at_hit",
            "single_chunk_single_series",
            Temperature::ApproxCold,
            &db_path,
            dataset,
            cold_cache_path,
            config.point_ops.min(128),
            false,
        )?,
        run_sqlite_point_workload(
            "get_at_hit",
            "single_chunk_single_series",
            Temperature::StricterColdish,
            &db_path,
            dataset,
            cold_cache_path,
            config.point_ops.min(128),
            false,
        )?,
        run_sqlite_point_workload(
            "get_at_miss",
            "multi_chunk_multi_series",
            Temperature::Warm,
            &db_path,
            dataset,
            cold_cache_path,
            config.point_ops,
            true,
        )?,
        run_sqlite_point_workload(
            "get_at_miss",
            "multi_chunk_multi_series",
            Temperature::ApproxCold,
            &db_path,
            dataset,
            cold_cache_path,
            config.point_ops.min(128),
            true,
        )?,
        run_sqlite_point_workload(
            "get_at_miss",
            "multi_chunk_multi_series",
            Temperature::StricterColdish,
            &db_path,
            dataset,
            cold_cache_path,
            config.point_ops.min(128),
            true,
        )?,
    ];

    for &range_rows in &config.short_range_sizes {
        let workload = format!("get_range_short_{range_rows}");
        results.push(run_sqlite_range_workload(
            &workload,
            "single_chunk_single_series",
            Temperature::Warm,
            &db_path,
            dataset,
            cold_cache_path,
            config.range_ops,
            range_rows,
            false,
        )?);
        results.push(run_sqlite_range_workload(
            &workload,
            "single_chunk_single_series",
            Temperature::ApproxCold,
            &db_path,
            dataset,
            cold_cache_path,
            config.range_ops.min(64),
            range_rows,
            false,
        )?);
        results.push(run_sqlite_range_workload(
            &workload,
            "single_chunk_single_series",
            Temperature::StricterColdish,
            &db_path,
            dataset,
            cold_cache_path,
            config.range_ops.min(64),
            range_rows,
            false,
        )?);
    }

    results.extend([
        run_sqlite_range_workload(
            "get_range_medium",
            "multi_chunk_single_series",
            Temperature::Warm,
            &db_path,
            dataset,
            cold_cache_path,
            config.range_ops,
            config.medium_range_rows,
            true,
        )?,
        run_sqlite_range_workload(
            "get_range_medium",
            "multi_chunk_single_series",
            Temperature::ApproxCold,
            &db_path,
            dataset,
            cold_cache_path,
            config.range_ops.min(64),
            config.medium_range_rows,
            true,
        )?,
        run_sqlite_range_workload(
            "get_range_medium",
            "multi_chunk_single_series",
            Temperature::StricterColdish,
            &db_path,
            dataset,
            cold_cache_path,
            config.range_ops.min(64),
            config.medium_range_rows,
            true,
        )?,
        run_sqlite_full_scan_workload(
            Temperature::Warm,
            &db_path,
            dataset,
            cold_cache_path,
            config.full_scan_ops,
        )?,
        run_sqlite_full_scan_workload(
            Temperature::ApproxCold,
            &db_path,
            dataset,
            cold_cache_path,
            config.full_scan_ops.min(4),
        )?,
        run_sqlite_full_scan_workload(
            Temperature::StricterColdish,
            &db_path,
            dataset,
            cold_cache_path,
            config.full_scan_ops.min(4),
        )?,
        run_sqlite_latest_n_workload(
            Temperature::Warm,
            &db_path,
            dataset,
            cold_cache_path,
            config.latest_n_ops,
            config.latest_n_rows,
        )?,
        run_sqlite_latest_n_workload(
            Temperature::ApproxCold,
            &db_path,
            dataset,
            cold_cache_path,
            config.latest_n_ops.min(64),
            config.latest_n_rows,
        )?,
        run_sqlite_latest_n_workload(
            Temperature::StricterColdish,
            &db_path,
            dataset,
            cold_cache_path,
            config.latest_n_ops.min(64),
            config.latest_n_rows,
        )?,
    ]);

    Ok(results)
}

fn measure_fastk_write(config: &Config, dataset: &Dataset) -> Result<WorkloadResult> {
    let mut samples = Vec::new();
    let mut metrics = MetricsAccumulator::default();
    for sample_idx in 0..config.write_samples {
        let root = config.root.join(format!("fastk-write-{sample_idx}"));
        if root.exists() {
            fs::remove_dir_all(&root)?;
        }
        let started = Instant::now();
        let mut store = open_fastk_with_level(&root, config.metrics_level)?;
        store.init()?;
        for series in &dataset.series {
            store.register_kline_series(
                &series.symbol,
                &config.timeframe,
                config.timeframe_ms,
                config.price_scale,
                config.volume_scale,
            )?;
            for month in &series.month_chunks {
                store.put_kline_chunk(&series.symbol, &config.timeframe, month)?;
            }
        }
        samples.push(started.elapsed());
        metrics.add_snapshot(store.metrics_snapshot());
    }
    Ok(WorkloadResult::new(
        "fastk",
        "write_initial",
        Temperature::Warm,
        "multi_series_multi_month",
        dataset.total_rows(),
        LatencySummary::from_durations(&samples, dataset.total_rows() * config.write_samples),
        Some(metrics.finish()),
        "",
    ))
}

fn measure_fastk_append(config: &Config) -> Result<WorkloadResult> {
    let root = config.root.join("fastk-append");
    if root.exists() {
        fs::remove_dir_all(&root)?;
    }
    let mut store = open_fastk_with_level(&root, config.metrics_level)?;
    store.init()?;
    store.set_compaction_policy(CompactionPolicy {
        auto_merge: false,
        ..CompactionPolicy::default()
    });
    store.register_kline_series(
        "APPEND",
        &config.timeframe,
        config.timeframe_ms,
        config.price_scale,
        config.volume_scale,
    )?;
    let base = build_month_records(
        0,
        0,
        1_024,
        config.timeframe_ms,
        config.price_scale,
        config.volume_scale,
    )?;
    store.put_kline_chunk("APPEND", &config.timeframe, &base)?;

    let mut samples = Vec::new();
    for batch_idx in 0..config.append_batches {
        let start = 1_024 + batch_idx * config.append_batch_rows;
        let batch = build_append_batch(start, config.append_batch_rows, config)?;
        let started = Instant::now();
        store.put_kline_chunk("APPEND", &config.timeframe, &batch)?;
        samples.push(started.elapsed());
    }

    Ok(WorkloadResult::new(
        "fastk",
        "append_active_month",
        Temperature::Warm,
        "single_series_single_month",
        config.append_batches * config.append_batch_rows,
        LatencySummary::from_durations(&samples, config.append_batches * config.append_batch_rows),
        Some(BenchmarkMetrics::from_snapshot(store.metrics_snapshot())),
        "",
    ))
}

fn measure_fastk_merge(config: &Config) -> Result<WorkloadResult> {
    let mut samples = Vec::new();
    let mut metrics = MetricsAccumulator::default();
    for sample_idx in 0..config.merge_samples {
        let root = config.root.join(format!("fastk-merge-{sample_idx}"));
        if root.exists() {
            fs::remove_dir_all(&root)?;
        }
        let mut store = open_fastk_with_level(&root, config.metrics_level)?;
        store.init()?;
        store.register_kline_series(
            "MERGE",
            &config.timeframe,
            config.timeframe_ms,
            config.price_scale,
            config.volume_scale,
        )?;
        let base = build_month_records(
            0,
            0,
            1_024,
            config.timeframe_ms,
            config.price_scale,
            config.volume_scale,
        )?;
        store.put_kline_chunk("MERGE", &config.timeframe, &base)?;
        for batch_idx in 0..config.append_batches {
            let start = 1_024 + batch_idx * config.append_batch_rows;
            let batch = build_append_batch(start, config.append_batch_rows, config)?;
            store.put_kline_chunk("MERGE", &config.timeframe, &batch)?;
        }
        store.reset_metrics();
        let started = Instant::now();
        store.merge_kline_month("MERGE", &config.timeframe, "2024-01")?;
        samples.push(started.elapsed());
        metrics.add_snapshot(store.metrics_snapshot());
    }

    Ok(WorkloadResult::new(
        "fastk",
        "merge_active_month",
        Temperature::Warm,
        "single_series_single_month",
        config.merge_samples,
        LatencySummary::from_durations(&samples, config.merge_samples),
        Some(metrics.finish()),
        "",
    ))
}

fn measure_fastk_session_attach(
    config: &Config,
    dataset: &Dataset,
    store_root: &Path,
    temperature: Temperature,
    multi_series: bool,
) -> Result<WorkloadResult> {
    let attach_count = if multi_series {
        dataset.series.len()
    } else {
        1
    };
    let mut samples = Vec::new();
    let mut metrics = MetricsAccumulator::default();

    match temperature {
        Temperature::ApproxCold => {
            for _ in 0..config.attach_samples {
                let store = open_fastk_with_level(store_root, config.metrics_level)?;
                store.reset_metrics();
                let mut session = store.read_session();
                let started = Instant::now();
                if multi_series {
                    for series in &dataset.series {
                        session.attach_kline_series(&series.symbol, &config.timeframe)?;
                    }
                } else {
                    session.attach_kline_series(&dataset.series[0].symbol, &config.timeframe)?;
                }
                samples.push(started.elapsed());
                metrics.add_snapshot(store.metrics_snapshot());
            }
        }
        Temperature::Warm | Temperature::StricterColdish => {
            let store = open_fastk_with_level(store_root, config.metrics_level)?;
            for _ in 0..config.attach_samples {
                store.reset_metrics();
                let mut session = store.read_session();
                let started = Instant::now();
                if multi_series {
                    for series in &dataset.series {
                        session.attach_kline_series(&series.symbol, &config.timeframe)?;
                    }
                } else {
                    session.attach_kline_series(&dataset.series[0].symbol, &config.timeframe)?;
                }
                samples.push(started.elapsed());
                metrics.add_snapshot(store.metrics_snapshot());
            }
        }
    }

    Ok(WorkloadResult::new(
        "fastk",
        if multi_series {
            "session_attach_multi"
        } else {
            "session_attach_single"
        },
        temperature,
        if multi_series {
            "multi_chunk_multi_series"
        } else {
            "single_chunk_single_series"
        },
        attach_count,
        LatencySummary::from_durations(&samples, attach_count * samples.len()),
        Some(metrics.finish()),
        "",
    ))
}

fn run_fastk_point_workload(
    workload: &str,
    scenario: &str,
    temperature: Temperature,
    store_root: &Path,
    metrics_level: MetricsLevel,
    timeframe: &str,
    dataset: &Dataset,
    cold_cache_path: &Path,
    ops: usize,
    miss: bool,
    multi_series: bool,
) -> Result<WorkloadResult> {
    let mut samples = Vec::new();
    let mut metrics = MetricsAccumulator::default();
    let target_series = &dataset.series[0];
    let hits = build_point_targets(target_series, ops, miss);
    match temperature {
        Temperature::Warm => {
            let store = open_fastk_with_level(store_root, metrics_level)?;
            for (index, ts) in hits.into_iter().enumerate() {
                let symbol = if multi_series {
                    &dataset.series[index % dataset.series.len()].symbol
                } else {
                    &target_series.symbol
                };
                let started = Instant::now();
                let _ = store.get_kline_at(symbol, timeframe, ts)?;
                samples.push(started.elapsed());
            }
            metrics.add_snapshot(store.metrics_snapshot());
        }
        Temperature::ApproxCold => {
            for (index, ts) in hits.into_iter().take(ops.min(128)).enumerate() {
                thrash_cache(cold_cache_path)?;
                let store = open_fastk_with_level(store_root, metrics_level)?;
                let symbol = if multi_series {
                    &dataset.series[index % dataset.series.len()].symbol
                } else {
                    &target_series.symbol
                };
                let started = Instant::now();
                let _ = store.get_kline_at(symbol, timeframe, ts)?;
                samples.push(started.elapsed());
                metrics.add_snapshot(store.metrics_snapshot());
            }
        }
        Temperature::StricterColdish => {
            let store = open_fastk_with_level(store_root, metrics_level)?;
            let mut session = store.read_session();
            if multi_series {
                for series in &dataset.series {
                    session.attach_kline_series(&series.symbol, timeframe)?;
                }
            } else {
                session.attach_kline_series(&target_series.symbol, timeframe)?;
            }
            for (index, ts) in hits.into_iter().take(ops.min(128)).enumerate() {
                thrash_cache(cold_cache_path)?;
                session.clear_caches();
                let symbol = if multi_series {
                    &dataset.series[index % dataset.series.len()].symbol
                } else {
                    &target_series.symbol
                };
                let started = Instant::now();
                let _ = session.get_kline_at(symbol, timeframe, ts)?;
                samples.push(started.elapsed());
                metrics.add_snapshot(store.metrics_snapshot());
                store.reset_metrics();
            }
        }
    }
    Ok(WorkloadResult::new(
        "fastk",
        workload,
        temperature,
        scenario,
        1,
        LatencySummary::from_durations(&samples, samples.len()),
        Some(metrics.finish()),
        "",
    ))
}

fn run_fastk_range_workload(
    workload: &str,
    scenario: &str,
    temperature: Temperature,
    store_root: &Path,
    metrics_level: MetricsLevel,
    timeframe: &str,
    dataset: &Dataset,
    cold_cache_path: &Path,
    ops: usize,
    range_rows: usize,
    cross_month: bool,
) -> Result<WorkloadResult> {
    let cases = build_range_cases(&dataset.series[0], ops, range_rows, cross_month);
    let mut samples = Vec::new();
    let mut metrics = MetricsAccumulator::default();
    match temperature {
        Temperature::Warm => {
            let store = open_fastk_with_level(store_root, metrics_level)?;
            for (start_ts, end_ts) in &cases {
                let started = Instant::now();
                let rows = store.get_kline_range(
                    &dataset.series[0].symbol,
                    timeframe,
                    *start_ts,
                    *end_ts,
                )?;
                debug_assert!(!rows.is_empty());
                samples.push(started.elapsed());
            }
            metrics.add_snapshot(store.metrics_snapshot());
        }
        Temperature::ApproxCold => {
            for (start_ts, end_ts) in &cases[..cases.len().min(64)] {
                thrash_cache(cold_cache_path)?;
                let store = open_fastk_with_level(store_root, metrics_level)?;
                let started = Instant::now();
                let rows = store.get_kline_range(
                    &dataset.series[0].symbol,
                    timeframe,
                    *start_ts,
                    *end_ts,
                )?;
                debug_assert!(!rows.is_empty());
                samples.push(started.elapsed());
                metrics.add_snapshot(store.metrics_snapshot());
            }
        }
        Temperature::StricterColdish => {
            let store = open_fastk_with_level(store_root, metrics_level)?;
            let mut session = store.read_session();
            session.attach_kline_series(&dataset.series[0].symbol, timeframe)?;
            for (start_ts, end_ts) in &cases[..cases.len().min(64)] {
                thrash_cache(cold_cache_path)?;
                session.clear_caches();
                let started = Instant::now();
                let rows = session.get_kline_range(
                    &dataset.series[0].symbol,
                    timeframe,
                    *start_ts,
                    *end_ts,
                )?;
                debug_assert!(!rows.is_empty());
                samples.push(started.elapsed());
                metrics.add_snapshot(store.metrics_snapshot());
                store.reset_metrics();
            }
        }
    }
    let total_ops = samples.len();
    Ok(WorkloadResult::new(
        "fastk",
        workload,
        temperature,
        scenario,
        range_rows,
        LatencySummary::from_durations(&samples, total_ops),
        Some(metrics.finish()),
        "",
    ))
}

fn run_fastk_full_scan_workload(
    temperature: Temperature,
    store_root: &Path,
    metrics_level: MetricsLevel,
    timeframe: &str,
    dataset: &Dataset,
    cold_cache_path: &Path,
    ops: usize,
) -> Result<WorkloadResult> {
    let series = &dataset.series[0];
    let mut samples = Vec::new();
    let mut metrics = MetricsAccumulator::default();
    match temperature {
        Temperature::Warm => {
            let store = open_fastk_with_level(store_root, metrics_level)?;
            for _ in 0..ops {
                let started = Instant::now();
                let rows = store.get_kline_range(
                    &series.symbol,
                    timeframe,
                    series.all_records[0].ts,
                    series.all_records[series.all_records.len() - 1].ts,
                )?;
                debug_assert_eq!(rows.len(), series.all_records.len());
                samples.push(started.elapsed());
            }
            metrics.add_snapshot(store.metrics_snapshot());
        }
        Temperature::ApproxCold => {
            for _ in 0..ops {
                thrash_cache(cold_cache_path)?;
                let store = open_fastk_with_level(store_root, metrics_level)?;
                let started = Instant::now();
                let rows = store.get_kline_range(
                    &series.symbol,
                    timeframe,
                    series.all_records[0].ts,
                    series.all_records[series.all_records.len() - 1].ts,
                )?;
                debug_assert_eq!(rows.len(), series.all_records.len());
                samples.push(started.elapsed());
                metrics.add_snapshot(store.metrics_snapshot());
            }
        }
        Temperature::StricterColdish => {
            let store = open_fastk_with_level(store_root, metrics_level)?;
            let mut session = store.read_session();
            session.attach_kline_series(&series.symbol, timeframe)?;
            for _ in 0..ops {
                thrash_cache(cold_cache_path)?;
                session.clear_caches();
                let started = Instant::now();
                let rows = session.get_kline_range(
                    &series.symbol,
                    timeframe,
                    series.all_records[0].ts,
                    series.all_records[series.all_records.len() - 1].ts,
                )?;
                debug_assert_eq!(rows.len(), series.all_records.len());
                samples.push(started.elapsed());
                metrics.add_snapshot(store.metrics_snapshot());
                store.reset_metrics();
            }
        }
    }
    Ok(WorkloadResult::new(
        "fastk",
        "full_scan",
        temperature,
        "multi_chunk_single_series",
        series.all_records.len(),
        LatencySummary::from_durations(&samples, samples.len()),
        Some(metrics.finish()),
        "",
    ))
}

fn run_fastk_latest_n_workload(
    temperature: Temperature,
    store_root: &Path,
    metrics_level: MetricsLevel,
    timeframe: &str,
    dataset: &Dataset,
    cold_cache_path: &Path,
    ops: usize,
    latest_n_rows: usize,
) -> Result<WorkloadResult> {
    let series = &dataset.series[0];
    let mut samples = Vec::new();
    let mut metrics = MetricsAccumulator::default();
    match temperature {
        Temperature::Warm => {
            let store = open_fastk_with_level(store_root, metrics_level)?;
            for _ in 0..ops {
                let started = Instant::now();
                let rows = store.get_kline_latest_n(&series.symbol, timeframe, latest_n_rows)?;
                debug_assert_eq!(rows.len(), latest_n_rows);
                samples.push(started.elapsed());
            }
            metrics.add_snapshot(store.metrics_snapshot());
        }
        Temperature::ApproxCold => {
            for _ in 0..ops {
                thrash_cache(cold_cache_path)?;
                let store = open_fastk_with_level(store_root, metrics_level)?;
                let started = Instant::now();
                let rows = store.get_kline_latest_n(&series.symbol, timeframe, latest_n_rows)?;
                debug_assert_eq!(rows.len(), latest_n_rows);
                samples.push(started.elapsed());
                metrics.add_snapshot(store.metrics_snapshot());
            }
        }
        Temperature::StricterColdish => {
            let store = open_fastk_with_level(store_root, metrics_level)?;
            let mut session = store.read_session();
            session.attach_kline_series(&series.symbol, timeframe)?;
            for _ in 0..ops {
                thrash_cache(cold_cache_path)?;
                session.clear_caches();
                let started = Instant::now();
                let rows = session.get_kline_latest_n(&series.symbol, timeframe, latest_n_rows)?;
                debug_assert_eq!(rows.len(), latest_n_rows);
                samples.push(started.elapsed());
                metrics.add_snapshot(store.metrics_snapshot());
                store.reset_metrics();
            }
        }
    }
    Ok(WorkloadResult::new(
        "fastk",
        "latest_n",
        temperature,
        "multi_chunk_single_series",
        latest_n_rows,
        LatencySummary::from_durations(&samples, samples.len()),
        Some(metrics.finish()),
        "",
    ))
}

fn run_sqlite_point_workload(
    workload: &str,
    scenario: &str,
    temperature: Temperature,
    db_path: &Path,
    dataset: &Dataset,
    cold_cache_path: &Path,
    ops: usize,
    miss: bool,
) -> Result<WorkloadResult> {
    let target_series = &dataset.series[0];
    let point_targets = build_point_targets(target_series, ops, miss);
    let mut samples = Vec::new();
    match temperature {
        Temperature::Warm => {
            let connection = open_sqlite(db_path)?;
            for ts in point_targets {
                let started = Instant::now();
                let _ = connection
                    .query_row(
                        "SELECT ts FROM klines WHERE symbol = ?1 AND ts = ?2",
                        params![&target_series.symbol, ts],
                        |row| row.get::<_, i64>(0),
                    )
                    .optional()
                    .map_err(sqlite_err)?;
                samples.push(started.elapsed());
            }
        }
        Temperature::ApproxCold => {
            for ts in point_targets.into_iter().take(128) {
                thrash_cache(cold_cache_path)?;
                let connection = open_sqlite(db_path)?;
                let started = Instant::now();
                let _ = connection
                    .query_row(
                        "SELECT ts FROM klines WHERE symbol = ?1 AND ts = ?2",
                        params![&target_series.symbol, ts],
                        |row| row.get::<_, i64>(0),
                    )
                    .optional()
                    .map_err(sqlite_err)?;
                samples.push(started.elapsed());
            }
        }
        Temperature::StricterColdish => {
            let connection = open_sqlite(db_path)?;
            for ts in point_targets.into_iter().take(128) {
                thrash_cache(cold_cache_path)?;
                connection
                    .execute_batch("PRAGMA shrink_memory;")
                    .map_err(sqlite_err)?;
                let started = Instant::now();
                let _ = connection
                    .query_row(
                        "SELECT ts FROM klines WHERE symbol = ?1 AND ts = ?2",
                        params![&target_series.symbol, ts],
                        |row| row.get::<_, i64>(0),
                    )
                    .optional()
                    .map_err(sqlite_err)?;
                samples.push(started.elapsed());
            }
        }
    }
    Ok(WorkloadResult::new(
        "sqlite3_tuned",
        workload,
        temperature,
        scenario,
        1,
        LatencySummary::from_durations(&samples, samples.len()),
        None,
        "",
    ))
}

fn run_sqlite_range_workload(
    workload: &str,
    scenario: &str,
    temperature: Temperature,
    db_path: &Path,
    dataset: &Dataset,
    cold_cache_path: &Path,
    ops: usize,
    range_rows: usize,
    cross_month: bool,
) -> Result<WorkloadResult> {
    let cases = build_range_cases(&dataset.series[0], ops, range_rows, cross_month);
    let mut samples = Vec::new();
    match temperature {
        Temperature::Warm => {
            let connection = open_sqlite(db_path)?;
            for (start_ts, end_ts) in &cases {
                let started = Instant::now();
                let rows = query_sqlite_window(
                    &connection,
                    &dataset.series[0].symbol,
                    *start_ts,
                    *end_ts,
                )?;
                debug_assert!(!rows.is_empty());
                samples.push(started.elapsed());
            }
        }
        Temperature::ApproxCold => {
            for (start_ts, end_ts) in &cases[..cases.len().min(64)] {
                thrash_cache(cold_cache_path)?;
                let connection = open_sqlite(db_path)?;
                let started = Instant::now();
                let rows = query_sqlite_window(
                    &connection,
                    &dataset.series[0].symbol,
                    *start_ts,
                    *end_ts,
                )?;
                debug_assert!(!rows.is_empty());
                samples.push(started.elapsed());
            }
        }
        Temperature::StricterColdish => {
            let connection = open_sqlite(db_path)?;
            for (start_ts, end_ts) in &cases[..cases.len().min(64)] {
                thrash_cache(cold_cache_path)?;
                connection
                    .execute_batch("PRAGMA shrink_memory;")
                    .map_err(sqlite_err)?;
                let started = Instant::now();
                let rows = query_sqlite_window(
                    &connection,
                    &dataset.series[0].symbol,
                    *start_ts,
                    *end_ts,
                )?;
                debug_assert!(!rows.is_empty());
                samples.push(started.elapsed());
            }
        }
    }
    Ok(WorkloadResult::new(
        "sqlite3_tuned",
        workload,
        temperature,
        scenario,
        range_rows,
        LatencySummary::from_durations(&samples, samples.len()),
        None,
        "",
    ))
}

fn run_sqlite_full_scan_workload(
    temperature: Temperature,
    db_path: &Path,
    dataset: &Dataset,
    cold_cache_path: &Path,
    ops: usize,
) -> Result<WorkloadResult> {
    let series = &dataset.series[0];
    let mut samples = Vec::new();
    match temperature {
        Temperature::Warm => {
            let connection = open_sqlite(db_path)?;
            for _ in 0..ops {
                let started = Instant::now();
                let rows = query_sqlite_window(
                    &connection,
                    &series.symbol,
                    series.all_records[0].ts,
                    series.all_records[series.all_records.len() - 1].ts,
                )?;
                debug_assert_eq!(rows.len(), series.all_records.len());
                samples.push(started.elapsed());
            }
        }
        Temperature::ApproxCold => {
            for _ in 0..ops {
                thrash_cache(cold_cache_path)?;
                let connection = open_sqlite(db_path)?;
                let started = Instant::now();
                let rows = query_sqlite_window(
                    &connection,
                    &series.symbol,
                    series.all_records[0].ts,
                    series.all_records[series.all_records.len() - 1].ts,
                )?;
                debug_assert_eq!(rows.len(), series.all_records.len());
                samples.push(started.elapsed());
            }
        }
        Temperature::StricterColdish => {
            let connection = open_sqlite(db_path)?;
            for _ in 0..ops {
                thrash_cache(cold_cache_path)?;
                connection
                    .execute_batch("PRAGMA shrink_memory;")
                    .map_err(sqlite_err)?;
                let started = Instant::now();
                let rows = query_sqlite_window(
                    &connection,
                    &series.symbol,
                    series.all_records[0].ts,
                    series.all_records[series.all_records.len() - 1].ts,
                )?;
                debug_assert_eq!(rows.len(), series.all_records.len());
                samples.push(started.elapsed());
            }
        }
    }
    Ok(WorkloadResult::new(
        "sqlite3_tuned",
        "full_scan",
        temperature,
        "multi_chunk_single_series",
        series.all_records.len(),
        LatencySummary::from_durations(&samples, samples.len()),
        None,
        "",
    ))
}

fn run_sqlite_latest_n_workload(
    temperature: Temperature,
    db_path: &Path,
    dataset: &Dataset,
    cold_cache_path: &Path,
    ops: usize,
    latest_n_rows: usize,
) -> Result<WorkloadResult> {
    let series = &dataset.series[0];
    let mut samples = Vec::new();
    match temperature {
        Temperature::Warm => {
            let connection = open_sqlite(db_path)?;
            for _ in 0..ops {
                let started = Instant::now();
                let rows = query_sqlite_latest_n(&connection, &series.symbol, latest_n_rows)?;
                debug_assert_eq!(rows.len(), latest_n_rows);
                samples.push(started.elapsed());
            }
        }
        Temperature::ApproxCold => {
            for _ in 0..ops {
                thrash_cache(cold_cache_path)?;
                let connection = open_sqlite(db_path)?;
                let started = Instant::now();
                let rows = query_sqlite_latest_n(&connection, &series.symbol, latest_n_rows)?;
                debug_assert_eq!(rows.len(), latest_n_rows);
                samples.push(started.elapsed());
            }
        }
        Temperature::StricterColdish => {
            let connection = open_sqlite(db_path)?;
            for _ in 0..ops {
                thrash_cache(cold_cache_path)?;
                connection
                    .execute_batch("PRAGMA shrink_memory;")
                    .map_err(sqlite_err)?;
                let started = Instant::now();
                let rows = query_sqlite_latest_n(&connection, &series.symbol, latest_n_rows)?;
                debug_assert_eq!(rows.len(), latest_n_rows);
                samples.push(started.elapsed());
            }
        }
    }
    Ok(WorkloadResult::new(
        "sqlite3_tuned",
        "latest_n",
        temperature,
        "multi_chunk_single_series",
        latest_n_rows,
        LatencySummary::from_durations(&samples, samples.len()),
        None,
        "",
    ))
}

fn measure_sqlite_write(config: &Config, dataset: &Dataset) -> Result<WorkloadResult> {
    let mut samples = Vec::new();
    for sample_idx in 0..config.write_samples {
        let db_path = config
            .root
            .join(format!("sqlite-write-{sample_idx}.sqlite3"));
        if db_path.exists() {
            fs::remove_file(&db_path)?;
        }
        let started = Instant::now();
        populate_sqlite_tuned(&db_path, dataset)?;
        samples.push(started.elapsed());
    }
    Ok(WorkloadResult::new(
        "sqlite3_tuned",
        "write_initial",
        Temperature::Warm,
        "multi_series_multi_month",
        dataset.total_rows(),
        LatencySummary::from_durations(&samples, dataset.total_rows() * config.write_samples),
        None,
        "",
    ))
}

fn populate_fastk_store(root: &Path, dataset: &Dataset, config: &Config) -> Result<()> {
    if root.exists() {
        fs::remove_dir_all(root)?;
    }
    let mut store = open_fastk_with_level(root, config.metrics_level)?;
    store.init()?;
    for series in &dataset.series {
        store.register_kline_series(
            &series.symbol,
            &config.timeframe,
            config.timeframe_ms,
            config.price_scale,
            config.volume_scale,
        )?;
        for month in &series.month_chunks {
            store.put_kline_chunk(&series.symbol, &config.timeframe, month)?;
        }
    }
    Ok(())
}

fn populate_sqlite_tuned(path: &Path, dataset: &Dataset) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    if path.exists() {
        fs::remove_file(path)?;
    }
    let mut connection = open_sqlite(path)?;
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
        for series in &dataset.series {
            for record in &series.all_records {
                stmt.execute(params![
                    &series.symbol,
                    record.ts,
                    record.open,
                    record.high,
                    record.low,
                    record.close,
                    record.volume
                ])
                .map_err(sqlite_err)?;
            }
        }
    }
    tx.commit().map_err(sqlite_err)?;
    Ok(())
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

fn query_sqlite_window(
    connection: &Connection,
    symbol: &str,
    start_ts: i64,
    end_ts: i64,
) -> Result<Vec<KlineRecord>> {
    let mut stmt = connection
        .prepare(
            "SELECT ts, open, high, low, close, volume
             FROM klines
             WHERE symbol = ?1 AND ts >= ?2 AND ts <= ?3
             ORDER BY ts",
        )
        .map_err(sqlite_err)?;
    let rows = stmt
        .query_map(params![symbol, start_ts, end_ts], |row| {
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
    let mut out = Vec::new();
    for row in rows {
        out.push(row.map_err(sqlite_err)?);
    }
    Ok(out)
}

fn query_sqlite_latest_n(
    connection: &Connection,
    symbol: &str,
    n: usize,
) -> Result<Vec<KlineRecord>> {
    let mut stmt = connection
        .prepare(
            "SELECT ts, open, high, low, close, volume
             FROM klines
             WHERE symbol = ?1
             ORDER BY ts DESC
             LIMIT ?2",
        )
        .map_err(sqlite_err)?;
    let rows = stmt
        .query_map(params![symbol, n as i64], |row| {
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
    let mut out = Vec::new();
    for row in rows {
        out.push(row.map_err(sqlite_err)?);
    }
    out.reverse();
    Ok(out)
}

fn build_point_targets(series: &SeriesData, count: usize, miss: bool) -> Vec<i64> {
    let len = series.all_records.len();
    let mut out = Vec::with_capacity(count);
    for index in 0..count {
        let position = index * (len.saturating_sub(1)) / count.max(1);
        let ts = series.all_records[position].ts;
        out.push(if miss { ts + 30_000 } else { ts });
    }
    out
}

fn build_range_cases(
    series: &SeriesData,
    count: usize,
    range_rows: usize,
    cross_month: bool,
) -> Vec<(i64, i64)> {
    if cross_month {
        let first = series.month_chunks[0]
            [series.month_chunks[0].len() - range_rows.min(series.month_chunks[0].len())]
        .ts;
        let last_month = &series.month_chunks[1.min(series.month_chunks.len() - 1)];
        let last = last_month[range_rows.min(last_month.len()) - 1].ts;
        return vec![(first, last); count.min(32).max(1)];
    }

    let chunk = &series.month_chunks[0];
    let safe_rows = range_rows.min(chunk.len()).max(1);
    let max_start = chunk.len().saturating_sub(safe_rows);
    let mut out = Vec::with_capacity(count);
    for index in 0..count {
        let start = if max_start == 0 {
            0
        } else {
            index * max_start / count.max(1)
        };
        out.push((chunk[start].ts, chunk[start + safe_rows - 1].ts));
    }
    out
}

fn build_month_records(
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

fn build_append_batch(
    start_index: usize,
    rows: usize,
    config: &Config,
) -> Result<Vec<KlineRecord>> {
    let month_start = month_start_ts(0)?;
    let mut out = Vec::with_capacity(rows);
    let mut close = 60_000 * config.price_scale + start_index as i64;
    for offset in 0..rows {
        close += ((offset % 11) as i64) - 5;
        out.push(KlineRecord {
            ts: month_start + (start_index + offset) as i64 * config.timeframe_ms,
            open: close - 2,
            high: close + 8,
            low: close - 10,
            close,
            volume: (2_000 + start_index as i64 + offset as i64) * config.volume_scale,
        });
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

fn prepare_cold_cache_file(root: &Path, mib: usize) -> Result<PathBuf> {
    let path = root.join("cold-cache.bin");
    let bytes = mib.max(1) * 1024 * 1024;
    let mut writer = BufWriter::new(File::create(&path)?);
    let buffer = vec![0x5Au8; 8 * 1024 * 1024];
    let mut written = 0usize;
    while written < bytes {
        let remaining = bytes - written;
        let chunk = remaining.min(buffer.len());
        writer.write_all(&buffer[..chunk])?;
        written += chunk;
    }
    writer.flush()?;
    Ok(path)
}

fn thrash_cache(path: &Path) -> Result<()> {
    let bytes = fs::read(path)?;
    if bytes.is_empty() {
        return Err(FastKError::InvalidData(
            "cold cache file is empty".to_string(),
        ));
    }
    Ok(())
}

fn sqlite_err(err: rusqlite::Error) -> FastKError {
    FastKError::InvalidData(format!("sqlite error: {err}"))
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
    point_ops: usize,
    range_ops: usize,
    full_scan_ops: usize,
    latest_n_ops: usize,
    latest_n_rows: usize,
    medium_range_rows: usize,
    append_batches: usize,
    append_batch_rows: usize,
    write_samples: usize,
    merge_samples: usize,
    attach_samples: usize,
    cold_cache_mib: usize,
    short_range_sizes: Vec<usize>,
}

impl Config {
    fn parse<I>(args: I) -> Result<Self>
    where
        I: IntoIterator<Item = String>,
    {
        let mut config = Self {
            root: PathBuf::from("target/acceptance-bench"),
            timeframe: "1m".to_string(),
            timeframe_ms: 60_000,
            price_scale: 100_000,
            volume_scale: 100_000,
            metrics_level: MetricsLevel::Basic,
            symbol_count: 4,
            months: 4,
            records_per_month: 10_000,
            point_ops: 256,
            range_ops: 128,
            full_scan_ops: 8,
            latest_n_ops: 128,
            latest_n_rows: 1_024,
            medium_range_rows: 8_192,
            append_batches: 8,
            append_batch_rows: 64,
            write_samples: 5,
            merge_samples: 8,
            attach_samples: 16,
            cold_cache_mib: 256,
            short_range_sizes: DEFAULT_SHORT_RANGE_SIZES.to_vec(),
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
                "--point-ops" => {
                    config.point_ops = parse_usize(next_value(&mut args, "--point-ops")?)?
                }
                "--range-ops" => {
                    config.range_ops = parse_usize(next_value(&mut args, "--range-ops")?)?
                }
                "--full-scan-ops" => {
                    config.full_scan_ops = parse_usize(next_value(&mut args, "--full-scan-ops")?)?
                }
                "--latest-n-ops" => {
                    config.latest_n_ops = parse_usize(next_value(&mut args, "--latest-n-ops")?)?
                }
                "--latest-n-rows" => {
                    config.latest_n_rows = parse_usize(next_value(&mut args, "--latest-n-rows")?)?
                }
                "--medium-range-rows" => {
                    config.medium_range_rows =
                        parse_usize(next_value(&mut args, "--medium-range-rows")?)?
                }
                "--append-batches" => {
                    config.append_batches = parse_usize(next_value(&mut args, "--append-batches")?)?
                }
                "--append-batch-rows" => {
                    config.append_batch_rows =
                        parse_usize(next_value(&mut args, "--append-batch-rows")?)?
                }
                "--write-samples" => {
                    config.write_samples = parse_usize(next_value(&mut args, "--write-samples")?)?
                }
                "--merge-samples" => {
                    config.merge_samples = parse_usize(next_value(&mut args, "--merge-samples")?)?
                }
                "--attach-samples" => {
                    config.attach_samples = parse_usize(next_value(&mut args, "--attach-samples")?)?
                }
                "--cold-cache-mib" => {
                    config.cold_cache_mib = parse_usize(next_value(&mut args, "--cold-cache-mib")?)?
                }
                "--short-range-sizes" => {
                    config.short_range_sizes = next_value(&mut args, "--short-range-sizes")?
                        .split(',')
                        .map(|value| {
                            value.trim().parse::<usize>().map_err(|err| {
                                FastKError::InvalidInput(format!(
                                    "invalid short range size '{value}': {err}"
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
            timeframe: self.timeframe.clone(),
            timeframe_ms: self.timeframe_ms,
            metrics_level: self.metrics_level.as_str().to_string(),
            symbol_count: self.symbol_count,
            months: self.months,
            records_per_month: self.records_per_month,
            point_ops: self.point_ops,
            range_ops: self.range_ops,
            full_scan_ops: self.full_scan_ops,
            latest_n_ops: self.latest_n_ops,
            latest_n_rows: self.latest_n_rows,
            medium_range_rows: self.medium_range_rows,
            append_batches: self.append_batches,
            append_batch_rows: self.append_batch_rows,
            write_samples: self.write_samples,
            merge_samples: self.merge_samples,
            attach_samples: self.attach_samples,
            cold_cache_mib: self.cold_cache_mib,
            short_range_sizes: self.short_range_sizes.clone(),
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
    point_ops: usize,
    range_ops: usize,
    full_scan_ops: usize,
    latest_n_ops: usize,
    latest_n_rows: usize,
    medium_range_rows: usize,
    append_batches: usize,
    append_batch_rows: usize,
    write_samples: usize,
    merge_samples: usize,
    attach_samples: usize,
    cold_cache_mib: usize,
    short_range_sizes: Vec<usize>,
}

#[derive(Debug, Serialize)]
struct Summary {
    config: ConfigSummary,
    matrix: Vec<WorkloadDescriptor>,
    results: Vec<WorkloadResult>,
}

#[derive(Debug, Serialize)]
struct WorkloadResult {
    backend: String,
    workload: String,
    temperature: Temperature,
    scenario: String,
    rows_per_op: usize,
    latency: LatencySummary,
    metrics: Option<BenchmarkMetrics>,
    note: String,
}

impl WorkloadResult {
    fn new(
        backend: &str,
        workload: &str,
        temperature: Temperature,
        scenario: &str,
        rows_per_op: usize,
        latency: LatencySummary,
        metrics: Option<BenchmarkMetrics>,
        note: &str,
    ) -> Self {
        Self {
            backend: backend.to_string(),
            workload: workload.to_string(),
            temperature,
            scenario: scenario.to_string(),
            rows_per_op,
            latency,
            metrics,
            note: if note.is_empty() {
                match temperature {
                    Temperature::Warm => String::new(),
                    Temperature::ApproxCold => {
                        "reopen + cache-thrash approximation; includes backend reopen cost".to_string()
                    }
                    Temperature::StricterColdish => {
                        "cache-thrash plus logical cache reset/shrink; keeps long-lived session state and is not OS true cold cache"
                            .to_string()
                    }
                }
            } else {
                note.to_string()
            },
        }
    }
}

#[derive(Debug, Clone)]
struct Dataset {
    series: Vec<SeriesData>,
}

#[derive(Debug, Clone)]
struct SeriesData {
    symbol: String,
    month_chunks: Vec<Vec<KlineRecord>>,
    all_records: Vec<KlineRecord>,
}

impl Dataset {
    fn build(config: &Config) -> Result<Self> {
        let mut series = Vec::with_capacity(config.symbol_count);
        for symbol_idx in 0..config.symbol_count {
            let symbol = format!("SYM{symbol_idx:03}");
            let mut month_chunks = Vec::with_capacity(config.months);
            let mut all_records = Vec::new();
            for month_idx in 0..config.months {
                let chunk = build_month_records(
                    symbol_idx,
                    month_idx,
                    config.records_per_month,
                    config.timeframe_ms,
                    config.price_scale,
                    config.volume_scale,
                )?;
                all_records.extend_from_slice(&chunk);
                month_chunks.push(chunk);
            }
            series.push(SeriesData {
                symbol,
                month_chunks,
                all_records,
            });
        }
        Ok(Self { series })
    }

    fn total_rows(&self) -> usize {
        self.series
            .iter()
            .map(|series| series.all_records.len())
            .sum()
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

fn open_fastk_with_level(path: &Path, level: MetricsLevel) -> Result<FastKStore> {
    let store = FastKStore::open(path)?;
    store.set_metrics_level(level);
    Ok(store)
}

fn print_help() {
    println!("bench_acceptance [options]");
    println!("  --root <path>");
    println!("  --metrics-level <off|basic|detailed>");
    println!("  --symbol-count <n>");
    println!("  --months <n>");
    println!("  --records-per-month <n>");
    println!("  --point-ops <n>");
    println!("  --range-ops <n>");
    println!("  --full-scan-ops <n>");
    println!("  --latest-n-ops <n>");
    println!("  --latest-n-rows <n>");
    println!("  --medium-range-rows <n>");
    println!("  --append-batches <n>");
    println!("  --append-batch-rows <n>");
    println!("  --write-samples <n>");
    println!("  --merge-samples <n>");
    println!("  --attach-samples <n>");
    println!("  --cold-cache-mib <MiB>");
    println!("  --short-range-sizes <csv>");
}
