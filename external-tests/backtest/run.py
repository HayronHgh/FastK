"""Run fixed strategies across five stores and an explicitly resident-data control."""

import argparse
import hashlib
import importlib.metadata
import json
import platform
import random
import shutil
import statistics
import sys
from pathlib import Path

import numpy as np
from fastk_bench.backends import NAMES
from fastk_bench.process import run

from .engine import CASES, PARAMETERS


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def summarize(output):
    manifest = json.loads((output / "manifest.json").read_text())
    results = [json.loads((output / j["output"]).read_text()) for j in manifest["jobs"]]
    rows = []
    for mode in manifest["modes"]:
        for case in manifest["cases"]:
            reference = [
                r["outcome"]
                for r in results
                if r["backend"] == "memory" and r["mode"] == mode and r["case"] == case
            ][0]
            for r in (r for r in results if r["mode"] == mode and r["case"] == case):
                assert r["outcome"] == reference, (r["backend"], mode, case, "backtest mismatch")
            for backend in [*manifest["backends"], "memory"]:
                runs = [
                    r
                    for r in results
                    if r["mode"] == mode and r["case"] == case and r["backend"] == backend
                ]
                assert len(runs) == manifest["rounds"]
                phases = {
                    p: [sum(s[p] for s in r["samples"]) for r in runs]
                    for p in (
                        "open_ns",
                        "read_ns",
                        "close_ns",
                        "load_ns",
                        "compute_ns",
                        "release_ns",
                    )
                }
                totals = [r["total_ns"] for r in runs]
                rows.append(
                    {
                        "mode": mode,
                        "case": case,
                        "backend": backend,
                        "total_ns": statistics.median(totals),
                        "total_samples_ns": totals,
                        "phase_medians_ns": {p: statistics.median(v) for p, v in phases.items()},
                        "load_fraction": statistics.median(
                            phases["load_ns"][i] / totals[i] for i in range(len(runs))
                        ),
                        "batch_load_p95_ns": statistics.median(
                            float(np.percentile([s["load_ns"] for s in r["samples"]], 95))
                            for r in runs
                        ),
                        "rss_bytes": statistics.median(r["memory"]["peak_rss_bytes"] for r in runs),
                        "private_bytes": statistics.median(
                            r["memory"]["peak_private_bytes"] for r in runs
                        ),
                        "first_data_ns": statistics.median(r["first_data_ns"] for r in runs),
                        "outcome": reference,
                    }
                )
    # Changing batching must preserve every fill and every equity point as well.
    for case in manifest["cases"]:
        assert (
            len({json.dumps(r["outcome"], sort_keys=True) for r in rows if r["case"] == case}) == 1
        )
    summary = {"all_outcomes_identical": True, "processes": len(results), "results": rows}
    (output / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
    return summary


def main():
    p = argparse.ArgumentParser(description=__doc__)
    for name in ("stores", "source", "output"):
        p.add_argument("--" + name, type=Path, required=True)
    p.add_argument("--rounds", type=int, default=5)
    p.add_argument("--months", type=int, default=12)
    p.add_argument("--symbols", nargs="+")
    p.add_argument("--backends", nargs="+", choices=NAMES, default=NAMES)
    p.add_argument("--cases", nargs="+", choices=CASES, default=list(CASES))
    p.add_argument("--modes", nargs="+", choices=("monthly", "bulk"), default=["monthly", "bulk"])
    args = p.parse_args()
    if args.rounds < 1 or args.months < 1:
        p.error("rounds and months must be positive")
    if any(len(set(values)) != len(values) for values in (args.backends, args.cases, args.modes)):
        p.error("duplicate backend, case or mode")
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    source = args.source.resolve()
    manifest = json.loads((source / "manifest.json").read_text())
    symbols = sorted(
        args.symbols or {p["symbol"] for p in manifest["parts"] if p["kind"] == "bars"}
    )
    if len(set(symbols)) != len(symbols):
        p.error("duplicate symbols")
    months = sorted(
        {p["month"] for p in manifest["parts"] if p["symbol"] in symbols and p["kind"] == "bars"}
    )[: args.months]
    if len(months) != args.months:
        p.error("not enough months in source")
    # Verify canonical bytes before producing the independent oracle. No new download.
    source_hashes = {}
    for part in manifest["parts"]:
        if part["kind"] == "bars" and part["symbol"] in symbols and part["month"] in months:
            actual = sha(source / part["path"])
            assert actual == part["sha256"], part["path"]
            source_hashes[part["path"]] = actual
    shutil.copyfile(source / "manifest.json", output / "input-manifest.json")
    import fastk_native._native as native

    native_sha = sha(Path(native.__file__))
    common = {
        "manifest": str(output / "input-manifest.json"),
        "source": str(source),
        "symbols": symbols,
        "months": args.months,
        "native_sha256": native_sha,
    }
    environment = {
        "python": sys.version,
        "platform": platform.platform(),
        "native_sha256": native_sha,
        "native_path": str(native.__file__),
        "versions": {
            n: importlib.metadata.version(n)
            for n in ("numpy", "pyarrow", "duckdb", "psutil", "fastk-native")
        },
        "source_sha256": source_hashes,
        "harness_sha256": {
            str(f.relative_to(Path(__file__).parents[1])): sha(f)
            for f in [
                *sorted(Path(__file__).parent.glob("*.py")),
                *sorted((Path(__file__).parents[1] / "fastk_bench").glob("*.py")),
            ]
        },
    }
    preflights = []
    for backend in args.backends:
        name = "preflight-" + backend
        dest = output / (name + ".json")
        config = {
            **common,
            "backend": backend,
            "store": str((args.stores / backend).resolve()),
            "preflight": True,
            "output": str(dest),
        }
        config_path = output / (name + "-config.json")
        config_path.write_text(json.dumps(config) + "\n")
        result = run("backtest.worker", config_path, dest, 600)
        assert result["all_results_verified"]
        preflights.append(dest.name)
        print("verified", name, flush=True)
    jobs = []
    rng = random.Random(20261022)
    for round_index in range(args.rounds):
        group = [
            {"round": round_index, "backend": backend, "case": case, "mode": mode}
            for backend in [*args.backends, "memory"]
            for case in args.cases
            for mode in args.modes
        ]
        rng.shuffle(group)
        jobs.extend(group)
    for i, job in enumerate(jobs):
        job["output"] = f"{i:03}-{job['backend']}-{job['case']}-{job['mode']}-r{job['round']}.json"
    frozen = {
        "environment": environment,
        "common": common,
        "parameters": PARAMETERS,
        "rounds": args.rounds,
        "backends": args.backends,
        "cases": args.cases,
        "modes": args.modes,
        "preflights": preflights,
        "jobs": jobs,
    }
    (output / "manifest.json").write_text(json.dumps(frozen, indent=2) + "\n")
    for job in jobs:
        dest = output / job["output"]
        config = {
            **common,
            **job,
            "store": str((args.stores / job["backend"]).resolve()),
            "output": str(dest),
        }
        config_path = dest.with_name(dest.stem + "-config.json")
        config_path.write_text(json.dumps(config) + "\n")
        run("backtest.worker", config_path, dest, 600)
        print("completed", dest.stem, flush=True)
    summarize(output)
    print("All backtest outcomes identical across stores, rounds and batch sizes.", flush=True)


if __name__ == "__main__":
    main()
