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

#[path = "session_transport.rs"]
mod session_transport;

fn main() {
    if let Err(err) = run() {
        if std::env::args().any(|arg| arg == "--error-format=json") {
            eprintln!(
                "{}",
                serde_json::json!({"error": {"code": err.code(), "message": err.to_string()}})
            );
        } else {
            eprintln!("fastk_bridge failed: {err}");
        }
        std::process::exit(1);
    }
}

fn run() -> Result<()> {
    let args: Vec<String> = std::env::args()
        .skip(1)
        .filter(|arg| arg != "--error-format=json")
        .collect();
    if args == ["--session-stdio"] {
        return session_transport::run();
    }
    if args.is_empty() || matches!(args[0].as_str(), "--help" | "-h") {
        print_usage();
        return Ok(());
    }
    dispatch(
        args,
        Input::Stdin,
        &mut JsonOutput::Pretty(io::stdout().lock()),
    )
}

// Presentation belongs to the CLI adapter, never to engine response types.
enum JsonOutput<W> {
    Pretty(W),
    Compact(W),
}

enum Input {
    Stdin,
    Session(Option<serde_json::Value>),
}
impl Input {
    fn read(self, input_json: Option<&str>) -> Result<serde_json::Value> {
        match self {
            Self::Stdin => read_write_request(input_json),
            Self::Session(Some(payload)) => Ok(payload),
            Self::Session(None) => Err(FastKError::InvalidInput(
                "write requires a session payload".into(),
            )),
        }
    }
}

fn dispatch(args: Vec<String>, input: Input, output: &mut JsonOutput<impl Write>) -> Result<()> {
    let mut args = args.into_iter().filter(|arg| arg != "--error-format=json");
    let Some(command) = args.next() else {
        return Err(FastKError::InvalidInput("expected a bridge command".into()));
    };

    if command == "version" {
        let remaining: Vec<_> = args.collect();
        if remaining != ["--json"] {
            return Err(FastKError::InvalidInput("use version --json".into()));
        }
        return write_json(
            output,
            &serde_json::json!({"name": "fastk", "version": env!("CARGO_PKG_VERSION"), "contract_version": 1}),
        );
    }

    let options = parse_options(args.collect())?;
    match command.as_str() {
        "read-kline-latest" => {
            let store = fastk::FastKStore::open_read_only(require_path(&options, "root")?)?;
            let limit = require_i64(&options, "limit")?;
            let limit = usize::try_from(limit)
                .map_err(|_| FastKError::InvalidInput("limit must be nonnegative".into()))?;
            let rows: Vec<fastk::KlineBridgeRecord> = store
                .get_kline_latest_n(
                    require_str(&options, "symbol")?,
                    require_str(&options, "timeframe")?,
                    limit,
                )?
                .into_iter()
                .map(Into::into)
                .collect();
            write_json(output, &serde_json::json!({"records": rows}))?;
        }
        "init" => {
            let mut store = fastk::FastKStore::open(require_path(&options, "root")?)?;
            store.init()?;
            write_json(output, &serde_json::json!({"initialized": true}))?;
        }
        "seal-snapshot" => {
            write_json(
                output,
                &fastk::seal_snapshot(require_path(&options, "root")?)?,
            )?;
        }
        "verify-snapshot" => {
            write_json(
                output,
                &fastk::verify_snapshot(
                    require_path(&options, "root")?,
                    opt_str(&options, "digest"),
                )?,
            )?;
        }
        "read-kline-range" => {
            let response = bridge_read_kline_range(
                require_path(&options, "root")?,
                require_str(&options, "symbol")?,
                require_str(&options, "timeframe")?,
                require_i64(&options, "start-ts")?,
                require_i64(&options, "end-ts")?,
            )?;
            write_json(output, &response)?;
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
            write_json(output, &response)?;
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
            write_json(output, &response)?;
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
            write_json(output, &response)?;
        }
        "write-kline-range" => {
            let payload = input.read(opt_str(&options, "input-json"))?;
            let payload: WriteKlineRequest = serde_json::from_value(payload)?;
            let response = bridge_write_kline_range(
                require_path(&options, "root")?,
                require_str(&options, "symbol")?,
                require_str(&options, "timeframe")?,
                payload,
            )?;
            write_json(output, &response)?;
        }
        "kline-inventory" => {
            let response = bridge_kline_inventory(
                require_path(&options, "root")?,
                require_str(&options, "symbol")?,
                require_str(&options, "timeframe")?,
            )?;
            write_json(output, &response)?;
        }
        "write-indicator-range" => {
            let payload = input.read(opt_str(&options, "input-json"))?;
            let payload: WriteIndicatorRequest = serde_json::from_value(payload)?;
            let response = bridge_write_indicator_range(
                require_path(&options, "root")?,
                require_str(&options, "symbol")?,
                require_str(&options, "timeframe")?,
                require_str(&options, "indicator-name")?,
                payload,
            )?;
            write_json(output, &response)?;
        }
        "write-scalar-range" => {
            let payload = input.read(opt_str(&options, "input-json"))?;
            let payload: WriteScalarRequest = serde_json::from_value(payload)?;
            let response = bridge_write_scalar_range(
                require_path(&options, "root")?,
                require_str(&options, "symbol")?,
                require_str(&options, "timeframe")?,
                require_str(&options, "category")?,
                require_str(&options, "name")?,
                payload,
            )?;
            write_json(output, &response)?;
        }
        "indicator-inventory" => {
            let response = bridge_indicator_inventory(
                require_path(&options, "root")?,
                require_str(&options, "symbol")?,
                require_str(&options, "timeframe")?,
                require_str(&options, "indicator-name")?,
            )?;
            write_json(output, &response)?;
        }
        "scalar-inventory" => {
            let response = bridge_scalar_inventory(
                require_path(&options, "root")?,
                require_str(&options, "symbol")?,
                require_str(&options, "timeframe")?,
                require_str(&options, "category")?,
                require_str(&options, "name")?,
            )?;
            write_json(output, &response)?;
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

fn write_json<T: serde::Serialize>(output: &mut JsonOutput<impl Write>, value: &T) -> Result<()> {
    match output {
        JsonOutput::Compact(writer) => serde_json::to_writer(writer, value)?,
        JsonOutput::Pretty(writer) => {
            // Buffer above stdout's line writer to avoid one pipe write per line.
            let mut handle = io::BufWriter::with_capacity(64 * 1024, writer);
            serde_json::to_writer_pretty(&mut handle, value)?;
            handle.write_all(b"\n")?;
            handle.flush()?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod output_tests {
    use super::*;

    #[test]
    fn private_compact_preserves_typed_values_and_public_pretty_bytes() {
        let value = serde_json::json!({
            "symbol": "中文\n\"quoted\"",
            "records": [{"ts": i64::MIN, "value": i64::MAX}],
            "empty": [], "optional": null
        });
        let mut pretty = Vec::new();
        write_json(&mut JsonOutput::Pretty(&mut pretty), &value).unwrap();
        let mut expected = serde_json::to_vec_pretty(&value).unwrap();
        expected.push(b'\n');
        assert_eq!(pretty, expected);

        let mut compact = Vec::new();
        write_json(&mut JsonOutput::Compact(&mut compact), &value).unwrap();
        assert_eq!(compact, serde_json::to_vec(&value).unwrap());
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&compact).unwrap(),
            value
        );
        assert!(compact.len() < pretty.len());
    }

    #[test]
    fn compact_propagates_serialization_errors() {
        struct Broken;
        impl serde::Serialize for Broken {
            fn serialize<S: serde::Serializer>(
                &self,
                _: S,
            ) -> std::result::Result<S::Ok, S::Error> {
                Err(serde::ser::Error::custom("failed serialization"))
            }
        }
        let mut bytes = Vec::new();
        assert!(write_json(&mut JsonOutput::Compact(&mut bytes), &Broken).is_err());
        assert!(bytes.is_empty());
    }
}

fn print_usage() {
    eprintln!(
        "\
fastk_bridge <command> [options]

Commands:
  version --json
  init --root <path>
  seal-snapshot --root <path>
  verify-snapshot --root <path> [--digest <sha256>]
  read-kline-latest --root <path> --symbol <symbol> --timeframe <tf> --limit <n>
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
