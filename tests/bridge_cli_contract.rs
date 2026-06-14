use std::io::Write;
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::sync::Mutex;

use serde_json::Value;
use tempfile::TempDir;

static CLI_LOCK: Mutex<()> = Mutex::new(());

#[test]
fn bridge_help_is_stable() {
    let _guard = CLI_LOCK.lock().expect("cli lock should not be poisoned");
    let output = run_bridge_raw(&["--help"], None);
    assert!(
        output.status.success(),
        "help should succeed\nstdout:\n{}\nstderr:\n{}",
        output.stdout,
        output.stderr
    );

    let combined = format!("{}\n{}", output.stdout, output.stderr);
    for command in [
        "read-kline-range",
        "write-kline-range",
        "kline-inventory",
        "read-indicator-range",
        "write-indicator-range",
        "indicator-inventory",
        "read-scalar-range",
        "query-scalar-predicate",
        "write-scalar-range",
        "scalar-inventory",
    ] {
        assert!(
            combined.contains(command),
            "bridge help should list {command}"
        );
    }
}

#[test]
fn bridge_help_does_not_advertise_fetch_compute_or_service_behavior() {
    let _guard = CLI_LOCK.lock().expect("cli lock should not be poisoned");
    let output = run_bridge_raw(&["--help"], None);
    assert!(output.status.success(), "help should succeed");

    let combined = format!("{}\n{}", output.stdout, output.stderr).to_lowercase();
    for forbidden in [
        "fetch",
        "compute",
        "route",
        "strategy",
        "websocket",
        "server",
        "daemon",
    ] {
        assert!(
            !combined.contains(forbidden),
            "bridge help must not advertise {forbidden}"
        );
    }
}

#[test]
fn bridge_kline_roundtrip_smoke() {
    let _guard = CLI_LOCK.lock().expect("cli lock should not be poisoned");
    let temp_dir = TempDir::new().expect("temp dir should be created");
    let root = temp_dir.path().to_string_lossy().into_owned();

    let write = run_bridge_json(
        &[
            "write-kline-range",
            "--root",
            &root,
            "--symbol",
            "BTCUSDT",
            "--timeframe",
            "1m",
        ],
        Some(
            r#"{
              "timeframe_ms": 60000,
              "price_scale": 100000,
              "volume_scale": 100000,
              "records": [
                { "ts": 1706745600000, "open": 10000000, "high": 10020000, "low": 9980000, "close": 10010000, "volume": 10000 },
                { "ts": 1706745660000, "open": 10010000, "high": 10030000, "low": 10000000, "close": 10020000, "volume": 10020 }
              ]
            }"#,
        ),
    );
    assert_eq!(write["written_record_count"], 2);

    let read = run_bridge_json(
        &[
            "read-kline-range",
            "--root",
            &root,
            "--symbol",
            "BTCUSDT",
            "--timeframe",
            "1m",
            "--start-ts",
            "1706745600000",
            "--end-ts",
            "1706745660000",
        ],
        None,
    );
    assert_eq!(read["records"].as_array().expect("records array").len(), 2);
    assert_eq!(read["records"][1]["close"], 10020000);
}

#[test]
fn bridge_scalar_roundtrip_smoke() {
    let _guard = CLI_LOCK.lock().expect("cli lock should not be poisoned");
    let temp_dir = TempDir::new().expect("temp dir should be created");
    let root = temp_dir.path().to_string_lossy().into_owned();

    let write = run_bridge_json(
        &[
            "write-scalar-range",
            "--root",
            &root,
            "--symbol",
            "BTCUSDT",
            "--timeframe",
            "1m",
            "--category",
            "feature",
            "--name",
            "rsi_14",
        ],
        Some(
            r#"{
              "timeframe_ms": 60000,
              "records": [
                { "ts": 1706745600000, "value": 42 },
                { "ts": 1706745660000, "value": 43 }
              ]
            }"#,
        ),
    );
    assert_eq!(write["category"], "feature");
    assert_eq!(write["written_record_count"], 2);

    let read = run_bridge_json(
        &[
            "read-scalar-range",
            "--root",
            &root,
            "--symbol",
            "BTCUSDT",
            "--timeframe",
            "1m",
            "--category",
            "feature",
            "--name",
            "rsi_14",
            "--start-ts",
            "1706745600000",
            "--end-ts",
            "1706745660000",
        ],
        None,
    );
    assert!(read["exists"].as_bool().expect("exists bool"));
    assert_eq!(read["records"].as_array().expect("records array").len(), 2);
    assert_eq!(read["records"][1]["value"], 43);
}

#[test]
fn bridge_query_scalar_predicate_gt() {
    let _guard = CLI_LOCK.lock().expect("cli lock should not be poisoned");
    let temp_dir = TempDir::new().expect("temp dir should be created");
    let root = temp_dir.path().to_string_lossy().into_owned();
    seed_scalar_feature(&root);

    let response = run_bridge_json(
        &[
            "query-scalar-predicate",
            "--root",
            &root,
            "--symbol",
            "BTCUSDT",
            "--timeframe",
            "1m",
            "--category",
            "feature",
            "--name",
            "rsi_14",
            "--start-ts",
            "1706745600000",
            "--end-ts",
            "1706745720000",
            "--predicate",
            "gt",
            "--value",
            "42",
            "--return-values",
        ],
        None,
    );

    assert_eq!(response["matches"].as_array().expect("matches").len(), 2);
    assert_eq!(response["matches"][0]["value"], 43);
    assert_eq!(response["matches"][1]["value"], 44);
    assert_eq!(response["stats"]["rows_matched"], 2);
}

#[test]
fn bridge_query_scalar_predicate_in_set() {
    let _guard = CLI_LOCK.lock().expect("cli lock should not be poisoned");
    let temp_dir = TempDir::new().expect("temp dir should be created");
    let root = temp_dir.path().to_string_lossy().into_owned();
    seed_scalar_feature(&root);

    let response = run_bridge_json(
        &[
            "query-scalar-predicate",
            "--root",
            &root,
            "--symbol",
            "BTCUSDT",
            "--timeframe",
            "1m",
            "--category",
            "feature",
            "--name",
            "rsi_14",
            "--start-ts",
            "1706745600000",
            "--end-ts",
            "1706745720000",
            "--predicate",
            "in-set",
            "--values",
            "42,44",
        ],
        None,
    );

    assert_eq!(response["matches"].as_array().expect("matches").len(), 2);
    assert!(response["matches"][0]["value"].is_null());
    assert_eq!(response["matches"][0]["ts"], 1_706_745_600_000i64);
    assert_eq!(response["matches"][1]["ts"], 1_706_745_720_000i64);
}

#[test]
fn release_manifest_schema_smoke() {
    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let bridge_schema: Value = serde_json::from_str(
        &std::fs::read_to_string(manifest_dir.join("schemas/bridge_commands.json"))
            .expect("bridge schema should read"),
    )
    .expect("bridge schema should parse");
    let manifest_schema: Value = serde_json::from_str(
        &std::fs::read_to_string(manifest_dir.join("schemas/release_manifest.schema.json"))
            .expect("release manifest schema should read"),
    )
    .expect("release manifest schema should parse");

    assert_eq!(
        bridge_schema["properties"]["contract_version"]["const"],
        "0.1.0-rc.1"
    );
    assert!(bridge_schema["properties"]["commands"]["items"]["enum"]
        .as_array()
        .expect("commands enum")
        .iter()
        .any(|command| command == "write-scalar-range"));
    assert!(manifest_schema["required"]
        .as_array()
        .expect("required array")
        .iter()
        .any(|field| field == "artifacts"));
    assert!(manifest_schema["required"]
        .as_array()
        .expect("required array")
        .iter()
        .any(|field| field == "target"));
}

fn seed_scalar_feature(root: &str) {
    let write = run_bridge_json(
        &[
            "write-scalar-range",
            "--root",
            root,
            "--symbol",
            "BTCUSDT",
            "--timeframe",
            "1m",
            "--category",
            "feature",
            "--name",
            "rsi_14",
        ],
        Some(
            r#"{
              "timeframe_ms": 60000,
              "records": [
                { "ts": 1706745600000, "value": 42 },
                { "ts": 1706745660000, "value": 43 },
                { "ts": 1706745720000, "value": 44 }
              ]
            }"#,
        ),
    );
    assert_eq!(write["written_record_count"], 3);
}

fn run_bridge_json(args: &[&str], input: Option<&str>) -> Value {
    let output = run_bridge_raw(args, input);
    assert!(
        output.status.success(),
        "bridge command should succeed\nargs: {:?}\nstdout:\n{}\nstderr:\n{}",
        args,
        output.stdout,
        output.stderr
    );
    serde_json::from_str(&output.stdout).expect("bridge stdout should be JSON")
}

fn run_bridge_raw(args: &[&str], input: Option<&str>) -> BridgeOutput {
    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let cargo = std::env::var("CARGO").unwrap_or_else(|_| "cargo".to_string());
    let mut command = Command::new(cargo);
    command
        .current_dir(manifest_dir)
        .args(["run", "--quiet", "--example", "fastk_bridge", "--"])
        .args(args)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());

    if input.is_some() {
        command.stdin(Stdio::piped());
    }

    let mut child = command.spawn().expect("cargo run should start");
    if let Some(input) = input {
        child
            .stdin
            .as_mut()
            .expect("stdin should be piped")
            .write_all(input.as_bytes())
            .expect("stdin should write");
    }
    let output = child.wait_with_output().expect("cargo run should finish");
    BridgeOutput {
        status: output.status,
        stdout: String::from_utf8_lossy(&output.stdout).into_owned(),
        stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
    }
}

struct BridgeOutput {
    status: std::process::ExitStatus,
    stdout: String,
    stderr: String,
}
