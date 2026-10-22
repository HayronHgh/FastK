"""Same Q1 backtest: public initial CLI/JSON versus current native delivery."""

import argparse
import hashlib
import json
import random
import statistics
from pathlib import Path

from fastk_bench.process import run

from .engine import CASES


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("binary", "store", "source", "output"):
        parser.add_argument("--" + name, type=Path, required=True)
    parser.add_argument("--rounds", type=int, default=5)
    parser.add_argument("--months", type=int, default=3)
    args = parser.parse_args()
    if args.rounds < 1 or args.months < 1:
        parser.error("rounds and months must be positive")
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    import fastk_native._native as native

    def sha(p):
        return hashlib.sha256(p.read_bytes()).hexdigest()

    common = {
        "backend": "fastk",
        "source": str(args.source.resolve()),
        "manifest": str((args.source / "manifest.json").resolve()),
        "store": str(args.store.resolve()),
        "symbols": ["BTCUSDT", "ETHUSDT"],
        "months": args.months,
        "mode": "monthly",
        "monitor_children": True,
        "native_sha256": sha(Path(native.__file__)),
        "binary": str(args.binary.resolve()),
        "binary_sha256": sha(args.binary),
        "initial_commit": "814c5667aa25c89373bffc0582cfbb5f49c9d066",
    }
    versions = ["public-814c566", "current-native"]

    def execute(job, stem):
        dest = output / (stem + ".json")
        config = {**common, **job, "output": str(dest)}
        path = output / (stem + "-config.json")
        path.write_text(json.dumps(config) + "\n")
        result = run("backtest.version_worker", path, dest, 600)
        print("verified", stem, flush=True)
        return result

    for version in versions:
        result = execute({"version": version, "preflight": True}, "preflight-" + version)
        assert result["all_results_verified"]
    jobs = []
    rng = random.Random(20261022)
    for index in range(args.rounds):
        pairs = [[{"version": v, "case": case, "round": index} for v in versions] for case in CASES]
        rng.shuffle(pairs)
        for pair in pairs:
            rng.shuffle(pair)
            jobs.extend(pair)
    for i, job in enumerate(jobs):
        job["output"] = f"{i:02}-{job['version']}-{job['case']}-r{job['round']}.json"
    source = Path(__file__).parent
    manifest = {
        "common": common,
        "rounds": args.rounds,
        "jobs": jobs,
        "harness_sha256": {p.name: sha(p) for p in source.glob("*.py")},
    }
    (output / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    results = [execute(j, Path(j["output"]).stem) for j in jobs]
    rows = []
    for case in CASES:
        case_runs = [r for r in results if r["case"] == case]
        assert len({json.dumps(r["outcome"], sort_keys=True) for r in case_runs}) == 1
        for version in versions:
            runs = [r for r in case_runs if r["version"] == version]
            rows.append(
                {
                    "case": case,
                    "version": version,
                    "total_ns": statistics.median(r["total_ns"] for r in runs),
                    "load_ns": statistics.median(
                        sum(s["load_ns"] for s in r["samples"]) for r in runs
                    ),
                    "total_samples_ns": [r["total_ns"] for r in runs],
                    "rss_bytes": statistics.median(r["memory"]["peak_rss_bytes"] for r in runs),
                    "tree_rss_bytes": statistics.median(
                        r["memory"]["peak_simultaneous_tree_rss_bytes"] for r in runs
                    ),
                    "outcome": runs[0]["outcome"],
                }
            )
    (output / "summary.json").write_text(
        json.dumps({"all_outcomes_identical": True, "results": rows}, indent=2) + "\n"
    )


if __name__ == "__main__":
    main()
