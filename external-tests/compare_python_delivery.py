"""Separate legacy session/NumPy delivery comparison with frozen queries."""

import argparse
import json
import random
from pathlib import Path

from fastk_bench.process import run
from run_benchmarks import summarize


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--store", type=Path, required=True)
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--prior-queries", type=Path, required=True)
    parser.add_argument("--bulk-queries", type=Path, required=True)
    parser.add_argument("--legacy-bridge", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    bulk = [{**q, "case": case} for case, q in json.loads(args.bulk_queries.read_text()).items()]
    shorts = [
        {**q, "case": "short"}
        for i in range(5)
        for q in json.loads((args.prior_queries / f"queries-{i}.json").read_text())
        if q["kind"] == "range"
    ]
    assert len(shorts) == 1430
    for case, queries in [("bulk", bulk), ("short", shorts)]:
        (output / f"{case}.json").write_text(json.dumps(queries, indent=2) + "\n")
    jobs = [(route, case) for route in ["legacy", "native"] for case in ["bulk", "short"]]
    random.Random(20261021).shuffle(jobs)
    results = []
    for route, case in jobs:
        stem = f"{route}-{case}"
        config = {
            "backend": "fastk",
            "mode": "internal",
            "budget": 1,
            "manifest": str((args.input / "manifest.json").resolve()),
            "store": str(args.store.resolve()),
            "queries": str(output / f"{case}.json"),
            "output": str(output / f"{stem}.json"),
            "rounds": 10 if case == "bulk" else 1,
            "legacy_bridge": str(args.legacy_bridge.resolve()) if route == "legacy" else None,
            "monitor_children": True,
        }
        path = output / f"{stem}-config.json"
        path.write_text(json.dumps(config) + "\n")
        result = run("fastk_bench.matrix_worker", path, output / f"{stem}.json", 600)
        result["route"] = route
        result["latencies"] = {
            kind: summarize(
                [s["ns"] for s in result["samples"] if s["case"] == kind], kind == "short"
            )
            for kind in {s["case"] for s in result["samples"]}
        }
        results.append(result)
        print("verified", stem, flush=True)
    summary = {
        "all_results_verified": True,
        "results": results,
        "job_order": jobs,
        "limits": [
            "separate from five-backend native ranking",
            "legacy client includes itemgetter/fromiter conversion to the same NumPy result",
            "historical compact bridge versus current native core; not a pure serializer microbenchmark",
            "steady-state prewarmed handles; startup recorded separately",
            "RSS sampled per actual worker/child PID; simultaneous tree peak is not a sum of individual peaks",
        ],
    }
    (output / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")


if __name__ == "__main__":
    main()
