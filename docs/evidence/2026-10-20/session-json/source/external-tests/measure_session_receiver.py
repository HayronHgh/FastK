"""Isolated cached-pipe control for receiver copies; never changes installed code."""

import argparse
import json
import random
import subprocess
import sys
import time
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from fastk_client import Client, _session


def read_into(stream, size):
    data = bytearray(size)
    view = memoryview(data)
    offset = 0
    while offset < size:
        amount = stream.readinto(view[offset : offset + 64 * 1024])
        if not amount:
            raise _session.SessionProtocolError("worker closed a session frame")
        offset += amount
    return data


def stats(values):
    values = sorted(values)
    return {
        "samples": len(values),
        "mean_ns": sum(values) / len(values),
        "median_ns": values[(len(values) - 1) // 2],
        "min_ns": values[0],
        "max_ns": values[-1],
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--fixtures", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--rounds", type=int, default=10)
    args = parser.parse_args()
    if args.rounds < 1:
        parser.error("rounds must be positive")
    args.output.mkdir(parents=True, exist_ok=False)
    original_read, original_json, popen = _session._read_exact, _session.json, subprocess.Popen
    fake = args.output.resolve() / "measurement-worker.exe"
    fake.touch()
    worker = Path(__file__).parent / "fixtures/json_replay_worker.py"
    samples, process_counts = [], {}
    rng = random.Random(20261022)
    for label in ["bulk_kline", "bulk_scalar"]:
        pretty = (args.fixtures / f"{label}-frame.json").read_bytes()
        reference = json.loads(pretty)["result"]
        compact = json.dumps(json.loads(pretty), separators=(",", ":"), allow_nan=False).encode()
        files = {}
        for style, body in [("pretty", pretty), ("compact", compact)]:
            path = args.output.resolve() / f"{label}-{style}.json"
            path.write_bytes(body)
            files[style] = path
        for repeat in range(args.rounds):
            jobs = [(style, mode) for style in files for mode in ["original", "readinto"]]
            rng.shuffle(jobs)
            for style, mode in jobs:
                trace, children = {}, []

                def launch(command, **kwargs):
                    child = popen(
                        [sys.executable, str(worker.resolve()), str(files[style]), *command[1:]],
                        **kwargs,
                    )
                    children.append(child)
                    return child

                def loads(data):
                    before = time.perf_counter_ns()
                    value = json.loads(data)
                    trace.update(parse_ns=time.perf_counter_ns() - before, body_bytes=len(data))
                    return value

                _session._read_exact = original_read if mode == "original" else read_into
                _session.json = SimpleNamespace(loads=loads, dumps=json.dumps)
                try:
                    with patch.object(subprocess, "Popen", launch):
                        with Client(args.output, binary=fake, timeout=120) as client:
                            # Same cached producer and frame twice; first is warm-up.
                            first = client._run("cached-read")
                            assert first == reference
                            del first
                            trace.clear()
                            before = time.perf_counter_ns()
                            value = client._run("cached-read")
                            after = time.perf_counter_ns()
                            assert value == reference
                            del value
                        assert len(children) == 2 and all(p.poll() is not None for p in children)
                        process_counts[f"{label}-{repeat}-{style}-{mode}"] = len(children)
                    samples.append(
                        {
                            "case": label,
                            "round": repeat,
                            "style": style,
                            "receiver": mode,
                            "total_ns": after - before,
                            "parse_ns": trace["parse_ns"],
                            "non_parse_ns": after - before - trace["parse_ns"],
                            "body_bytes": trace["body_bytes"],
                        }
                    )
                finally:
                    _session._read_exact, _session.json = original_read, original_json
                print(
                    f"case={label} round={repeat} style={style} receiver={mode} verified",
                    flush=True,
                )
        del reference, pretty, compact
    groups = {}
    for label in ["bulk_kline", "bulk_scalar"]:
        groups[label] = {}
        for style in ["pretty", "compact"]:
            for mode in ["original", "readinto"]:
                rows = [
                    s
                    for s in samples
                    if s["case"] == label and s["style"] == style and s["receiver"] == mode
                ]
                groups[label][style + "_" + mode] = {
                    key: stats([s[key] for s in rows])
                    for key in ["total_ns", "parse_ns", "non_parse_ns"]
                }
    result = {
        "all_results_verified": True,
        "all_children_reaped": True,
        "groups": groups,
        "timed_responses": len(samples),
        "process_counts": process_counts,
        "scope": "Cached preencoded Python producer on real OS pipes + existing Session, startup and warm-up excluded; no engine/serialization/caller tuple conversion; experimental readinto is patched in memory only",
    }
    for name, value in [("summary", result), ("samples", samples)]:
        (args.output / f"{name}.json").write_text(json.dumps(value, indent=2) + "\n")
    print(args.output / "summary.json")


if __name__ == "__main__":
    main()
