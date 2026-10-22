"""Generate a private observational session probe; never edit production code."""

import argparse
import hashlib
import json
import shutil
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

RUST_CLOCK = r"""
#[link(name = "kernel32")]
unsafe extern "system" { fn QueryPerformanceCounter(value: *mut i64) -> i32; }
fn cost_ticks() -> i64 {
    let mut value = 0;
    assert_ne!(unsafe { QueryPerformanceCounter(&mut value) }, 0);
    value
}
"""

MEASURE = '''"""Private probe state, used sequentially by the external measurement tool."""
import ctypes

TRACE = {}
KERNEL = ctypes.WinDLL("kernel32", use_last_error=True)
FREQUENCY = ctypes.c_longlong()
assert KERNEL.QueryPerformanceFrequency(ctypes.byref(FREQUENCY))

def ticks():
    value = ctypes.c_longlong()
    assert KERNEL.QueryPerformanceCounter(ctypes.byref(value))
    return value.value
'''

READ_EXACT = """def _read_exact(stream, size):
    started = ticks()
    chunks = bytearray()
    read_ns = append_ns = calls = 0
    while len(chunks) < size:
        before = time.perf_counter_ns()
        chunk = stream.read(min(size - len(chunks), 64 * 1024))
        after = time.perf_counter_ns()
        if not chunk:
            raise SessionProtocolError("worker closed a session frame")
        chunks.extend(chunk)
        appended = time.perf_counter_ns()
        read_ns += after - before
        append_ns += appended - after
        calls += 1
    before = time.perf_counter_ns()
    result = bytes(chunks)
    copied = time.perf_counter_ns()
    TRACE.setdefault("reads", []).append({
        "bytes": size, "calls": calls, "syscall_and_wait_ns": read_ns,
        "append_ns": append_ns, "final_bytes_copy_ns": copied-before,
        "start_tick": started, "end_tick": ticks(),
    })
    return result
"""


def replace_once(source, old, new):
    if source.count(old) != 1:
        raise ValueError(f"source changed; expected one occurrence: {old[:100]!r}")
    return source.replace(old, new, 1)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    shutil.copytree(ROOT / "src", output / "src")
    for name in ["Cargo.lock", "README.md"]:
        shutil.copy2(ROOT / name, output / name)
    manifest = (ROOT / "Cargo.toml").read_text()
    manifest = (
        manifest.split("[[example]]")[0]
        + '[[example]]\nname = "fastk_bridge"\npath = "bridge.rs"\n\n[profile.release]'
        + manifest.split("[profile.release]", 1)[1]
    )
    (output / "Cargo.toml").write_text(manifest)
    cli = (ROOT / "examples/cli/fastk_bridge.rs").read_text()
    cli = replace_once(
        cli,
        "    serde_json::to_writer_pretty(&mut handle, value)?;",
        "    let cost_json_start = cost_ticks();\n    serde_json::to_writer_pretty(&mut handle, value)?;",
    )
    # Bound the instrumentation to write_json, not other command output.
    start = cli.index("fn write_json<")
    before, writer = cli[:start], cli[start:]
    writer = replace_once(
        writer,
        "    handle.flush()?;",
        """    handle.flush()?;
    let cost_json_end = cost_ticks();
    eprintln!("FASTK_BULK_JSON {}", serde_json::json!({
        "start_tick":cost_json_start, "end_tick":cost_json_end
    }));""",
    )
    (output / "bridge.rs").write_text(before + writer + RUST_CLOCK)
    transport = (ROOT / "examples/cli/session_transport.rs").read_text()
    transport = replace_once(
        transport, "use super::{dispatch, Input};", "use super::{cost_ticks, dispatch, Input};"
    )
    transport = replace_once(
        transport,
        "        last_id = request.id;",
        "        last_id = request.id;\n        let cost_dispatch_start = cost_ticks();",
    )
    transport = replace_once(
        transport,
        "        let response = match dispatch(request.args, Input::Session(request.payload), &mut output) {",
        """        let cost_result = dispatch(request.args, Input::Session(request.payload), &mut output);
        let cost_dispatch_end = cost_ticks();
        let response = match cost_result {""",
    )
    transport = replace_once(
        transport,
        "        write_frame(&mut writer, &response)?;",
        """        let cost_write_start = cost_ticks();
        write_frame(&mut writer, &response)?;
        let cost_write_end = cost_ticks();
        eprintln!("FASTK_BULK_SESSION {}", serde_json::json!({
            "id":request.id, "dispatch_start_tick":cost_dispatch_start,
            "dispatch_end_tick":cost_dispatch_end, "write_start_tick":cost_write_start,
            "write_end_tick":cost_write_end, "result_bytes":output.len(),
            "frame_bytes":response.len()
        }));""",
    )
    (output / "session_transport.rs").write_text(transport)
    package = output / "python/fastk_cost_client"
    shutil.copytree(
        ROOT / "clients/python/src/fastk_client",
        package,
        ignore=shutil.ignore_patterns("__pycache__", "_bin"),
    )
    session = (package / "_session.py").read_text()
    session = replace_once(
        session, "MAX_FRAME_BYTES =", "from ._measure import TRACE, ticks\n\nMAX_FRAME_BYTES ="
    )
    session = replace_once(
        session,
        "        self._stderr: deque[bytes] = deque(maxlen=8)",
        "        self._stderr: deque[bytes] = deque(maxlen=8)\n        self._cost_stderr = bytearray()",
    )
    session = replace_once(
        session,
        "                self._stderr.append(chunk)",
        "                self._stderr.append(chunk)\n                self._cost_stderr.extend(chunk)\n                if len(self._cost_stderr) > 65536:\n                    del self._cost_stderr[:-65536]",
    )
    begin, end = session.index("def _read_exact("), session.index("\ndef _read_frame(")
    session = session[:begin] + READ_EXACT + "\n" + session[end:]
    session = replace_once(
        session,
        "                current.response = _read_frame(self.process.stdout)\n                current.done.set()",
        '                current.response = _read_frame(self.process.stdout)\n                TRACE["io_return_tick"] = ticks()\n                current.done.set()',
    )
    (package / "_session.py").write_text(session)
    (package / "_measure.py").write_text(MEASURE)
    sources = [
        "Cargo.toml",
        "Cargo.lock",
        "src/bridge.rs",
        "examples/cli/fastk_bridge.rs",
        "examples/cli/session_transport.rs",
        "clients/python/src/fastk_client/client.py",
        "clients/python/src/fastk_client/_session.py",
        "clients/python/src/fastk_client/__init__.py",
    ]
    hashes = {name: hashlib.sha256((ROOT / name).read_bytes()).hexdigest() for name in sources}
    (output / "production-hashes.json").write_text(json.dumps(hashes, indent=2) + "\n")
    print(output)


if __name__ == "__main__":
    main()
