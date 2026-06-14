use std::collections::HashMap;
use std::fs;
use std::io::{self, Read, Write};
use std::path::PathBuf;

use fastk::{
    bridge_indicator_inventory, bridge_kline_inventory, bridge_query_scalar_predicate,
    bridge_read_indicator_range, bridge_read_kline_range, bridge_read_scalar_range,
    bridge_scalar_inventory, bridge_write_indicator_range, bridge_write_kline_range,
    bridge_write_scalar_range, FastKError, Result, ScalarPredicateExpr, WriteIndicatorRequest,
    WriteKlineRequest, WriteScalarRequest,
};

fn main() {
    if let Err(err) = run() {
        eprintln!("fastk_bridge failed: {err}");
        std::process::exit(1);
    }
}

fn run() -> Result<()> {
    let mut args = std::env::args().skip(1);
    let Some(command) = args.next() else {
        print_usage();
        return Ok(());
    };

    if command == "--help" || command == "-h" {
        print_usage();
        return Ok(());
    }

    let options = parse_options(args.collect())?;
    match command.as_str() {
        "read-kline-range" => {
            let response = bridge_read_kline_range(
                require_path(&options, "root")?,
                require_str(&options, "symbol")?,
                require_str(&options, "timeframe")?,
                require_i64(&options, "start-ts")?,
                require_i64(&options, "end-ts")?,
            )?;
            write_json(&response)?;
        }
        "read-indicator-range" => {
            let response = bridge_read_indicator_range(
                require_path(&options, "root")?,
                require_str(&options, "symbol")?,
                require_str(&options, "timeframe")?,
                require_str(&options, "indicator-name")?,
                require_i64(&options, "start-ts")?,
                require_i64(&options, "end-ts")?,
            )?;
            write_json(&response)?;
        }
        "read-scalar-range" => {
            let response = bridge_read_scalar_range(
                require_path(&options, "root")?,
                require_str(&options, "symbol")?,
                require_str(&options, "timeframe")?,
                require_str(&options, "category")?,
                require_str(&options, "name")?,
                require_i64(&options, "start-ts")?,
                require_i64(&options, "end-ts")?,
            )?;
            write_json(&response)?;
        }
        "query-scalar-predicate" => {
            let response = bridge_query_scalar_predicate(
                require_path(&options, "root")?,
                require_str(&options, "symbol")?,
                require_str(&options, "timeframe")?,
                require_str(&options, "category")?,
                require_str(&options, "name")?,
                require_i64(&options, "start-ts")?,
                require_i64(&options, "end-ts")?,
                parse_scalar_predicate(&options)?,
                options.contains_key("return-values"),
            )?;
            write_json(&response)?;
        }
        "write-kline-range" => {
            let payload = read_write_request(opt_str(&options, "input-json"))?;
            let payload: WriteKlineRequest = serde_json::from_value(payload)?;
            let response = bridge_write_kline_range(
                require_path(&options, "root")?,
                require_str(&options, "symbol")?,
                require_str(&options, "timeframe")?,
                payload,
            )?;
            write_json(&response)?;
        }
        "kline-inventory" => {
            let response = bridge_kline_inventory(
                require_path(&options, "root")?,
                require_str(&options, "symbol")?,
                require_str(&options, "timeframe")?,
            )?;
            write_json(&response)?;
        }
        "write-indicator-range" => {
            let payload = read_write_request(opt_str(&options, "input-json"))?;
            let payload: WriteIndicatorRequest = serde_json::from_value(payload)?;
            let response = bridge_write_indicator_range(
                require_path(&options, "root")?,
                require_str(&options, "symbol")?,
                require_str(&options, "timeframe")?,
                require_str(&options, "indicator-name")?,
                payload,
            )?;
            write_json(&response)?;
        }
        "write-scalar-range" => {
            let payload = read_write_request(opt_str(&options, "input-json"))?;
            let payload: WriteScalarRequest = serde_json::from_value(payload)?;
            let response = bridge_write_scalar_range(
                require_path(&options, "root")?,
                require_str(&options, "symbol")?,
                require_str(&options, "timeframe")?,
                require_str(&options, "category")?,
                require_str(&options, "name")?,
                payload,
            )?;
            write_json(&response)?;
        }
        "indicator-inventory" => {
            let response = bridge_indicator_inventory(
                require_path(&options, "root")?,
                require_str(&options, "symbol")?,
                require_str(&options, "timeframe")?,
                require_str(&options, "indicator-name")?,
            )?;
            write_json(&response)?;
        }
        "scalar-inventory" => {
            let response = bridge_scalar_inventory(
                require_path(&options, "root")?,
                require_str(&options, "symbol")?,
                require_str(&options, "timeframe")?,
                require_str(&options, "category")?,
                require_str(&options, "name")?,
            )?;
            write_json(&response)?;
        }
        other => {
            return Err(FastKError::InvalidInput(format!(
                "unsupported bridge command: {other}",
            )));
        }
    }

    Ok(())
}

fn parse_options(args: Vec<String>) -> Result<HashMap<String, String>> {
    let mut options = HashMap::new();
    let mut index = 0usize;
    while index < args.len() {
        let key = &args[index];
        if !key.starts_with("--") {
            return Err(FastKError::InvalidInput(format!(
                "expected option starting with --, got {key}",
            )));
        }
        let Some(value) = args.get(index + 1) else {
            if key == "--return-values" {
                options.insert(key.trim_start_matches("--").to_string(), "true".to_string());
                index += 1;
                continue;
            }
            return Err(FastKError::InvalidInput(format!(
                "missing value for option {key}",
            )));
        };
        if value.starts_with("--") {
            if key == "--return-values" {
                options.insert(key.trim_start_matches("--").to_string(), "true".to_string());
                index += 1;
                continue;
            }
            return Err(FastKError::InvalidInput(format!(
                "missing value for option {key}",
            )));
        }
        options.insert(key.trim_start_matches("--").to_string(), value.clone());
        index += 2;
    }
    Ok(options)
}

fn require_str<'a>(options: &'a HashMap<String, String>, key: &str) -> Result<&'a str> {
    options
        .get(key)
        .map(String::as_str)
        .ok_or_else(|| FastKError::InvalidInput(format!("missing --{key}")))
}

fn opt_str<'a>(options: &'a HashMap<String, String>, key: &str) -> Option<&'a str> {
    options.get(key).map(String::as_str)
}

fn require_i64(options: &HashMap<String, String>, key: &str) -> Result<i64> {
    require_str(options, key)?
        .parse::<i64>()
        .map_err(|err| FastKError::InvalidInput(format!("invalid --{key} value: {err}")))
}

fn require_path(options: &HashMap<String, String>, key: &str) -> Result<PathBuf> {
    Ok(PathBuf::from(require_str(options, key)?))
}

fn parse_scalar_predicate(options: &HashMap<String, String>) -> Result<ScalarPredicateExpr> {
    let predicate = require_str(options, "predicate")?;
    match predicate {
        "eq" => Ok(ScalarPredicateExpr::Eq(require_i64(options, "value")?)),
        "ne" => Ok(ScalarPredicateExpr::Ne(require_i64(options, "value")?)),
        "gt" => Ok(ScalarPredicateExpr::Gt(require_i64(options, "value")?)),
        "gte" => Ok(ScalarPredicateExpr::Gte(require_i64(options, "value")?)),
        "lt" => Ok(ScalarPredicateExpr::Lt(require_i64(options, "value")?)),
        "lte" => Ok(ScalarPredicateExpr::Lte(require_i64(options, "value")?)),
        "between" => Ok(ScalarPredicateExpr::Between {
            min: require_i64(options, "min")?,
            max: require_i64(options, "max")?,
            inclusive: true,
        }),
        "between-exclusive" => Ok(ScalarPredicateExpr::Between {
            min: require_i64(options, "min")?,
            max: require_i64(options, "max")?,
            inclusive: false,
        }),
        "in-set" => Ok(ScalarPredicateExpr::InSet(parse_i64_list(require_str(
            options, "values",
        )?)?)),
        "not-in-set" => Ok(ScalarPredicateExpr::NotInSet(parse_i64_list(require_str(
            options, "values",
        )?)?)),
        other => Err(FastKError::InvalidInput(format!(
            "unsupported scalar predicate: {other}"
        ))),
    }
}

fn parse_i64_list(raw: &str) -> Result<Vec<i64>> {
    if raw.trim().is_empty() {
        return Ok(Vec::new());
    }
    raw.split(',')
        .map(|value| {
            value.trim().parse::<i64>().map_err(|err| {
                FastKError::InvalidInput(format!("invalid integer in --values: {err}"))
            })
        })
        .collect()
}

fn read_write_request(input_json: Option<&str>) -> Result<serde_json::Value> {
    let raw = if let Some(path) = input_json {
        fs::read_to_string(path)?
    } else {
        let mut raw = String::new();
        io::stdin().read_to_string(&mut raw)?;
        raw
    };
    if raw.trim().is_empty() {
        return Err(FastKError::InvalidInput(
            "write-indicator-range expected JSON on stdin or via --input-json".to_string(),
        ));
    }
    Ok(serde_json::from_str(&raw)?)
}

fn write_json<T: serde::Serialize>(value: &T) -> Result<()> {
    let stdout = io::stdout();
    let mut handle = stdout.lock();
    serde_json::to_writer_pretty(&mut handle, value)?;
    handle.write_all(b"\n")?;
    Ok(())
}

fn print_usage() {
    eprintln!(
        "\
fastk_bridge <command> [options]

Commands:
  read-kline-range
    --root <path> --symbol <symbol> --timeframe <tf> --start-ts <ms> --end-ts <ms>
  write-kline-range
    --root <path> --symbol <symbol> --timeframe <tf> [--input-json <file>]
    If --input-json is omitted, JSON is read from stdin.
  kline-inventory
    --root <path> --symbol <symbol> --timeframe <tf>
  read-indicator-range
    --root <path> --symbol <symbol> --timeframe <tf> --indicator-name <name> --start-ts <ms> --end-ts <ms>
  read-scalar-range
    --root <path> --symbol <symbol> --timeframe <tf> --category <category> --name <name> --start-ts <ms> --end-ts <ms>
  query-scalar-predicate
    --root <path> --symbol <symbol> --timeframe <tf> --category <category> --name <name> --start-ts <ms> --end-ts <ms>
    --predicate <eq|ne|gt|gte|lt|lte|between|between-exclusive|in-set|not-in-set>
    Use --value <i64> for eq/ne/gt/gte/lt/lte, --min <i64> --max <i64> for between,
    --values <comma-separated-i64> for in-set/not-in-set, and optional --return-values.
  write-indicator-range
    --root <path> --symbol <symbol> --timeframe <tf> --indicator-name <name> [--input-json <file>]
    If --input-json is omitted, JSON is read from stdin.
  write-scalar-range
    --root <path> --symbol <symbol> --timeframe <tf> --category <category> --name <name> [--input-json <file>]
    If --input-json is omitted, JSON is read from stdin.
  indicator-inventory
    --root <path> --symbol <symbol> --timeframe <tf> --indicator-name <name>
  scalar-inventory
    --root <path> --symbol <symbol> --timeframe <tf> --category <category> --name <name>"
    );
}
