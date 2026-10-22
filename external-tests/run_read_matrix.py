"""Compare five in-process NumPy adapters without modifying prepared stores."""

import argparse
import hashlib
import importlib.metadata
import json
import platform
import random
import sys
from pathlib import Path

from fastk_bench.backends import NAMES
from fastk_bench.process import run
from run_benchmarks import summarize


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--stores", type=Path, required=True)
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--prior-queries", type=Path, required=True)
    parser.add_argument("--bulk-queries", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--budgets", nargs="+", type=int, default=[1, 2, 4, 8])
    parser.add_argument("--rounds", type=int, default=10)
    parser.add_argument("--timeout", type=float, default=600)
    args = parser.parse_args()
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    bulk = [{**q, "case": case} for case, q in json.loads(args.bulk_queries.read_text()).items()]
    shorts = [
        {**q, "case": "short", "round": i}
        for i in range(5)
        for q in json.loads((args.prior_queries / f"queries-{i}.json").read_text())
        if q["kind"] == "range"
    ]
    if len(shorts) != 1430:
        raise AssertionError("the frozen short-query workload must contain 1430 queries")
    (output / "bulk.json").write_text(json.dumps(bulk, indent=2) + "\n")
    (output / "short.json").write_text(json.dumps(shorts, indent=2) + "\n")
    jobs = [
        (name, mode, budget)
        for budget in args.budgets
        for name in NAMES
        for mode in ["internal", "outer"]
    ]
    random.Random(20261021).shuffle(jobs)
    results = []
    for name, mode, budget in jobs:
        stem = f"{name}-{mode}-{budget}"
        config = {
            "backend": name,
            "mode": mode,
            "budget": budget,
            "manifest": str((args.input / "manifest.json").resolve()),
            "store": str((args.stores / name).resolve()),
            "queries": str(output / ("bulk.json" if mode == "internal" else "short.json")),
            "output": str(output / f"{stem}.json"),
            "rounds": args.rounds,
            "timeout": args.timeout,
        }
        path = output / f"{stem}-config.json"
        path.write_text(json.dumps(config) + "\n")
        result = run("fastk_bench.matrix_worker", path, output / f"{stem}.json", args.timeout)
        cases = sorted({s["case"] for s in result["samples"]})
        result["latencies"] = {
            case: summarize(
                [s["ns"] for s in result["samples"] if s["case"] == case], case == "short"
            )
            for case in cases
        }
        results.append(result)
        print("verified", stem, flush=True)
    environment = {
        "python": sys.version,
        "platform": platform.platform(),
        "dependencies": {
            name: importlib.metadata.version(name)
            for name in ["fastk-native", "numpy", "duckdb", "pyarrow", "psutil"]
        },
        "native_module_sha256": hashlib.sha256(
            Path(__import__("fastk_native._native", fromlist=["__file__"]).__file__).read_bytes()
        ).hexdigest(),
        "input_manifest_sha256": hashlib.sha256(
            (args.input / "manifest.json").read_bytes()
        ).hexdigest(),
        "harness_sha256": hashlib.sha256(
            b"".join(
                path.read_bytes()
                for path in sorted(
                    [Path(__file__), *(Path(__file__).parent / "fastk_bench").glob("*.py")]
                )
            )
        ).hexdigest(),
    }
    summary = {
        "environment": environment,
        "job_order": jobs,
        "all_results_verified": True,
        "results": results,
        "limits": [
            "OS cache uncontrolled; each internal case has 32 untimed warmups; outer workers prewarm one query",
            "internal: one query, per-handle pool budget; SQLite stays serial",
            "outer: FastK shares one native Reader; other adapters use thread-owned handles; internal budget one; wall excludes validation",
            "outer memory retains each 286-query round for validation after the timed group",
            "5ms actual PID sampling can miss short peaks; bulk distributions descriptive",
        ],
    }
    (output / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")


if __name__ == "__main__":
    main()
