"""Measure complete scalar JSON pipe transfer; parsing stays outside the timer."""

import argparse
import hashlib
import json
import statistics
import subprocess
import time
from pathlib import Path


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bridge", type=Path, required=True)
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--symbol", required=True)
    parser.add_argument("--timeframe", default="1m")
    parser.add_argument("--category", default="feature")
    parser.add_argument("--name", default="ma20")
    parser.add_argument("--start-ts", required=True, type=int)
    parser.add_argument("--end-ts", required=True, type=int)
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.repeats < 1:
        parser.error("repeats must be positive")
    command = [str(args.bridge.resolve()), "read-scalar-range", "--root", str(args.root.resolve())]
    for name in ["symbol", "timeframe", "category", "name", "start_ts", "end_ts"]:
        command.extend(["--" + name.replace("_", "-"), str(getattr(args, name))])
    samples = []
    for _ in range(args.repeats):
        start = time.perf_counter_ns()
        response = subprocess.run(command, capture_output=True, check=True, timeout=180)
        elapsed = time.perf_counter_ns() - start
        samples.append(
            {
                "elapsed_ns": elapsed,
                "rows": len(json.loads(response.stdout)["records"]),
                "stdout_bytes": len(response.stdout),
                "stdout_sha256": hashlib.sha256(response.stdout).hexdigest(),
            }
        )
    if len({sample["stdout_sha256"] for sample in samples}) != 1:
        raise RuntimeError("output changed between samples")
    result = {
        "bridge_sha256": hashlib.sha256(args.bridge.read_bytes()).hexdigest(),
        "samples": samples,
        "median_ns": statistics.median(sample["elapsed_ns"] for sample in samples),
        "boundary": "process startup and complete pipe transfer; parsing outside timer; OS cache uncontrolled",
    }
    args.output.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
