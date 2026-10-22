"""Build source for a PRIVATE instrumented copy; never edit the shipped bridge."""

import argparse
import hashlib
import json
import shutil
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

ENGINE = r"""
    let started = std::time::Instant::now();
    let root = root.as_ref();
    let store = FastKStore::open_read_only(root.to_path_buf())?;
    let opened = started.elapsed().as_nanos();
    let catalog = Catalog::read_only(root.to_path_buf());
    let meta = catalog.load_kline_meta(symbol, timeframe)?;
    let metadata = started.elapsed().as_nanos();
    let records = store.get_kline_range(symbol, timeframe, start_ts, end_ts)?;
    let read = started.elapsed().as_nanos();
    let response = KlineRangeResponse {
        symbol: symbol.to_string(), timeframe: timeframe.to_string(),
        timeframe_ms: meta.timeframe_ms, start_ts, end_ts,
        price_scale: meta.price_scale, volume_scale: meta.volume_scale,
        records: records.into_iter().map(Into::into).collect(),
    };
    let converted = started.elapsed().as_nanos();
    drop(meta); drop(catalog); drop(store);
    let closed = started.elapsed().as_nanos();
    eprintln!("FASTK_COST_ENGINE {}", serde_json::json!({
        "open_ns": opened, "metadata_ns": metadata-opened,
        "read_ns": read-metadata, "response_ns": converted-read,
        "close_ns": closed-converted, "total_ns": closed
    }));
    Ok(response)
"""

WRITER = r"""
struct CostWriter<W> { inner: W, ns: u128, calls: usize }
impl<W: Write> Write for CostWriter<W> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let t = std::time::Instant::now();
        let result = self.inner.write(bytes);
        self.ns += t.elapsed().as_nanos(); self.calls += 1; result
    }
    fn flush(&mut self) -> io::Result<()> {
        let t = std::time::Instant::now();
        let result = self.inner.flush(); self.ns += t.elapsed().as_nanos(); result
    }
}
fn write_json<T: serde::Serialize>(value: &T) -> Result<()> {
    let stdout = io::stdout();
    let writer = CostWriter { inner: stdout.lock(), ns: 0, calls: 0 };
    let mut handle = io::BufWriter::with_capacity(64 * 1024, writer);
    let started = std::time::Instant::now();
    serde_json::to_writer_pretty(&mut handle, value)?;
    handle.write_all(b"\n")?; handle.flush()?;
    let total = started.elapsed().as_nanos();
    eprintln!("FASTK_COST_JSON {}", serde_json::json!({
        "encode_and_buffer_ns": total-handle.get_ref().ns,
        "pipe_write_ns": handle.get_ref().ns, "total_ns": total,
        "write_calls": handle.get_ref().calls
    }));
    Ok(())
}
"""

HELPERS = r"""
#[link(name = "kernel32")]
unsafe extern "system" { fn QueryPerformanceCounter(value: *mut i64) -> i32; }
fn cost_ticks() -> i64 {
    let mut value = 0;
    assert_ne!(unsafe { QueryPerformanceCounter(&mut value) }, 0); value
}
fn cost_loop(options: &HashMap<String, String>) -> Result<()> {
    use sha2::{Digest, Sha256};
    let root = require_path(options, "root")?;
    let requests: Vec<serde_json::Value> = serde_json::from_slice(
        &fs::read(require_path(options, "requests")?)?)?;
    let repeat = require_i64(options, "repeat")?;
    let t = std::time::Instant::now();
    let store = fastk::FastKStore::open_read_only(root)?;
    let open_ns = t.elapsed().as_nanos();
    let mut samples = Vec::new();
    for round in 0..repeat {
        for q in &requests {
            let t = std::time::Instant::now();
            let records = store.get_kline_range(q["symbol"].as_str().unwrap(), "1m",
                q["start_ts"].as_i64().unwrap(), q["end_ts"].as_i64().unwrap())?;
            let elapsed = t.elapsed().as_nanos();
            let mut hash = Sha256::new();
            for r in &records {
                hash.update(serde_json::to_vec(&[r.ts,r.open,r.high,r.low,r.close,r.volume])?);
                hash.update(b"\n");
            }
            samples.push(serde_json::json!({"round":round,"read_ns":elapsed,
                "rows":records.len(),"sha256":format!("{:x}",hash.finalize())}));
        }
    }
    write_json(&serde_json::json!({"open_ns":open_ns,"samples":samples}))
}
"""


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    shutil.copytree(ROOT / "src", output / "src")
    shutil.copy2(ROOT / "Cargo.lock", output / "Cargo.lock")
    shutil.copy2(ROOT / "README.md", output / "README.md")
    manifest = (ROOT / "Cargo.toml").read_text()
    manifest = (
        manifest.split("[[example]]")[0]
        + """[[example]]
name = "fastk_bridge"
path = "bridge.rs"

"""
        + "[profile.release]"
        + manifest.split("[profile.release]", 1)[1]
    )
    (output / "Cargo.toml").write_text(manifest)
    engine = (ROOT / "src/bridge.rs").read_text()
    begin = engine.index("    let root = root.as_ref();", engine.index("pub fn read_kline_range("))
    end = engine.index("\n}\n", begin)
    (output / "src/bridge.rs").write_text(engine[:begin] + ENGINE + engine[end:])
    cli = (ROOT / "examples/cli/fastk_bridge.rs").read_text()
    cli = cli.replace(
        "fn main() {\n    if let Err(err) = run() {",
        """fn main() {
    let main_tick = cost_ticks();
    let result = run();
    let end_tick = cost_ticks();
    eprintln!("FASTK_COST_LIFECYCLE {}", serde_json::json!({"main_tick": main_tick, "end_tick": end_tick}));
    if let Err(err) = result {""",
        1,
    )
    cli = cli.replace(
        '    if command == "version" {',
        """    if command == "cost-null" { return Ok(()); }
    if command == "cost-replay" {
        let path = args.next().expect("fixture path");
        let t = std::time::Instant::now();
        let bytes = fs::read(path)?;
        let read_ns = t.elapsed().as_nanos();
        let mut writer = CostWriter { inner: io::stdout().lock(), ns: 0, calls: 0 };
        writer.write_all(&bytes)?; writer.flush()?;
        eprintln!("FASTK_COST_REPLAY {}", serde_json::json!({"fixture_read_ns":read_ns,
            "pipe_write_ns":writer.ns,"write_calls":writer.calls})); return Ok(());
    }
    if command == "version" {""",
        1,
    )
    cli = cli.replace(
        "    match command.as_str() {",
        """    if command == "cost-loop" { return cost_loop(&options); }
    match command.as_str() {""",
        1,
    )
    begin = cli.index("fn write_json<")
    end = cli.index("\nfn ", begin + 1)
    cli = cli[:begin] + WRITER + "\n" + cli[end:] + "\n" + HELPERS
    (output / "bridge.rs").write_text(cli)
    hashes = {
        str(p.relative_to(ROOT)): hashlib.sha256(p.read_bytes()).hexdigest()
        for p in [
            ROOT / "src/bridge.rs",
            ROOT / "examples/cli/fastk_bridge.rs",
            ROOT / "Cargo.toml",
        ]
    }
    (output / "probe-source.json").write_text(json.dumps(hashes, indent=2) + "\n")
    print(output)


if __name__ == "__main__":
    main()
