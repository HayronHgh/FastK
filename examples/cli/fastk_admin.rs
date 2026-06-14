use std::env;
use std::path::PathBuf;

use fastk::{
    explain_overlaps, rebuild_all_manifests_from_fs, FastKError, FastKStore, Result,
    ScalarSeriesKey,
};

fn main() {
    if let Err(err) = run() {
        eprintln!("fastk_admin failed: {err}");
        std::process::exit(1);
    }
}

fn run() -> Result<()> {
    let config = Config::parse(env::args().skip(1))?;
    match config.command {
        Command::Validate => {
            let store = FastKStore::open(&config.root)?;
            let reports = if config.verbose {
                store.validate_manifest_vs_fs_verbose()?
            } else {
                store.validate_manifest_vs_fs()?
            };
            if reports.iter().any(|report| !report.is_clean()) {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&reports).map_err(FastKError::from)?
                );
                return Err(FastKError::InvalidData(
                    "validation reported issues".to_string(),
                ));
            }
            println!(
                "{}",
                serde_json::to_string_pretty(&reports).map_err(FastKError::from)?
            );
        }
        Command::Scrub => {
            let store = FastKStore::open(&config.root)?;
            let reports = store.scrub_store(config.verbose)?;
            if reports.iter().any(|report| !report.validation.is_clean()) {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&reports).map_err(FastKError::from)?
                );
                return Err(FastKError::InvalidData("scrub reported issues".to_string()));
            }
            println!(
                "{}",
                serde_json::to_string_pretty(&reports).map_err(FastKError::from)?
            );
        }
        Command::Repair => {
            let report = if config.dry_run {
                let store = FastKStore::open(&config.root)?;
                store.repair_store_dry_run()?
            } else {
                let mut store = FastKStore::open(&config.root)?;
                store.repair_store()?
            };
            println!(
                "{}",
                serde_json::to_string_pretty(&report).map_err(FastKError::from)?
            );
        }
        Command::RebuildManifest => {
            let rebuilt = rebuild_all_manifests_from_fs(&config.root)?;
            println!(
                "{}",
                serde_json::to_string_pretty(&rebuilt).map_err(FastKError::from)?
            );
        }
        Command::ListOrphans => {
            let store = FastKStore::open(&config.root)?;
            let artifacts = store.list_orphans()?;
            println!(
                "{}",
                serde_json::to_string_pretty(&artifacts).map_err(FastKError::from)?
            );
        }
        Command::ExplainOverlap => {
            let overlaps = explain_overlaps(&config.root)?;
            println!(
                "{}",
                serde_json::to_string_pretty(&overlaps).map_err(FastKError::from)?
            );
        }
        Command::ListSeries => {
            let store = FastKStore::open(&config.root)?;
            let series = store.list_series()?;
            println!(
                "{}",
                serde_json::to_string_pretty(&series).map_err(FastKError::from)?
            );
        }
        Command::Stats => {
            let store = FastKStore::open(&config.root)?;
            let stats = store.store_stats()?;
            println!(
                "{}",
                serde_json::to_string_pretty(&stats).map_err(FastKError::from)?
            );
        }
        Command::Health => {
            let store = FastKStore::open(&config.root)?;
            let health = store.health_summary()?;
            println!(
                "{}",
                serde_json::to_string_pretty(&health).map_err(FastKError::from)?
            );
        }
        Command::Inventory => {
            let store = FastKStore::open(&config.root)?;
            match (&config.symbol, &config.timeframe, &config.category, &config.name) {
                (Some(symbol), Some(timeframe), _, _) => {
                    let inventory = store.kline_month_inventory(symbol, timeframe)?;
                    println!(
                        "{}",
                        serde_json::to_string_pretty(&inventory).map_err(FastKError::from)?
                    );
                }
                (Some(symbol), _, Some(category), Some(name)) => {
                    let key = ScalarSeriesKey {
                        symbol: symbol.clone(),
                        category: category.clone(),
                        name: name.clone(),
                    };
                    let inventory = store.scalar_month_inventory(&key)?;
                    println!(
                        "{}",
                        serde_json::to_string_pretty(&inventory).map_err(FastKError::from)?
                    );
                }
                _ => {
                    return Err(FastKError::InvalidInput(
                        "inventory requires either --symbol + --timeframe, or --symbol + --category + --name"
                            .to_string(),
                    ))
                }
            }
        }
        Command::Capabilities => {
            let store = FastKStore::open(&config.root)?;
            let symbol = config.symbol.ok_or_else(|| {
                FastKError::InvalidInput("capabilities requires --symbol".to_string())
            })?;
            let category = config.category.ok_or_else(|| {
                FastKError::InvalidInput("capabilities requires --category".to_string())
            })?;
            let name = config.name.ok_or_else(|| {
                FastKError::InvalidInput("capabilities requires --name".to_string())
            })?;
            let key = ScalarSeriesKey {
                symbol,
                category,
                name,
            };
            let capabilities = store.scalar_query_capabilities(&key)?;
            println!(
                "{}",
                serde_json::to_string_pretty(&capabilities).map_err(FastKError::from)?
            );
        }
    }
    Ok(())
}

#[derive(Debug, Clone, Copy)]
enum Command {
    Validate,
    Scrub,
    Repair,
    RebuildManifest,
    ListOrphans,
    ExplainOverlap,
    ListSeries,
    Stats,
    Health,
    Inventory,
    Capabilities,
}

#[derive(Debug)]
struct Config {
    command: Command,
    root: PathBuf,
    dry_run: bool,
    verbose: bool,
    symbol: Option<String>,
    timeframe: Option<String>,
    category: Option<String>,
    name: Option<String>,
}

impl Config {
    fn parse<I>(args: I) -> Result<Self>
    where
        I: IntoIterator<Item = String>,
    {
        let mut command = None;
        let mut root = None;
        let mut dry_run = false;
        let mut verbose = false;
        let mut symbol = None;
        let mut timeframe = None;
        let mut category = None;
        let mut name = None;

        let mut args = args.into_iter();
        while let Some(arg) = args.next() {
            match arg.as_str() {
                "validate" => command = Some(Command::Validate),
                "scrub" => command = Some(Command::Scrub),
                "repair" => command = Some(Command::Repair),
                "rebuild-manifest" => command = Some(Command::RebuildManifest),
                "list-orphans" => command = Some(Command::ListOrphans),
                "explain-overlap" => command = Some(Command::ExplainOverlap),
                "list-series" => command = Some(Command::ListSeries),
                "stats" => command = Some(Command::Stats),
                "health" => command = Some(Command::Health),
                "inventory" => command = Some(Command::Inventory),
                "capabilities" => command = Some(Command::Capabilities),
                "--root" => root = Some(PathBuf::from(next_value(&mut args, "--root")?)),
                "--dry-run" => dry_run = true,
                "--verbose" => verbose = true,
                "--symbol" => symbol = Some(next_value(&mut args, "--symbol")?),
                "--timeframe" => timeframe = Some(next_value(&mut args, "--timeframe")?),
                "--category" => category = Some(next_value(&mut args, "--category")?),
                "--name" => name = Some(next_value(&mut args, "--name")?),
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

        Ok(Self {
            command: command
                .ok_or_else(|| FastKError::InvalidInput("missing command".to_string()))?,
            root: root.ok_or_else(|| FastKError::InvalidInput("missing --root".to_string()))?,
            dry_run,
            verbose,
            symbol,
            timeframe,
            category,
            name,
        })
    }
}

fn next_value<I>(args: &mut I, flag: &str) -> Result<String>
where
    I: Iterator<Item = String>,
{
    args.next()
        .ok_or_else(|| FastKError::InvalidInput(format!("missing value for {flag}")))
}

fn print_help() {
    println!("fastk_admin <command> --root <path>");
    println!("commands:");
    println!("  validate");
    println!("  scrub");
    println!("  repair");
    println!("  rebuild-manifest");
    println!("  list-orphans");
    println!("  explain-overlap");
    println!("  list-series");
    println!("  stats");
    println!("  health");
    println!("  inventory");
    println!("  capabilities");
    println!("flags:");
    println!("  --dry-run");
    println!("  --verbose");
    println!("  --symbol <symbol>");
    println!("  --timeframe <timeframe>");
    println!("  --category <category>");
    println!("  --name <name>");
}
