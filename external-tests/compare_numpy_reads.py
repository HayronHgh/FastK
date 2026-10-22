"""Repeat five-backend reads in isolated processes using the frozen NumPy contract."""

import argparse
import hashlib
import importlib.metadata
import json
import platform
import random
import shutil
import sqlite3
import statistics
import sys
from pathlib import Path

import numpy as np
from fastk_bench.backends import NAMES
from fastk_bench.process import run


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def summarize(output):
    manifest = json.loads((output / "manifest.json").read_text())
    rows = []
    verified = 0
    for backend in NAMES:
        for workload in ("bulk", "short"):
            runs = []
            for job in manifest["jobs"]:
                if job["backend"] == backend and job["workload"] == workload:
                    result = json.loads((output / job["output"]).read_text())
                    assert result["all_results_verified"]
                    assert len(result["samples"]) == (120 if workload == "bulk" else 1430)
                    verified += len(result["samples"])
                    runs.append(result)
            assert len(runs) == manifest["process_rounds"]
            for case in sorted({s["case"] for s in runs[0]["samples"]}):
                values = [[s["ns"] for s in r["samples"] if s["case"] == case] for r in runs]
                medians = [statistics.median(v) for v in values]
                rows.append(
                    dict(
                        backend=backend,
                        workload=workload,
                        case=case,
                        process_medians_ns=medians,
                        median_ns=statistics.median(medians),
                        process_p95_ns=[float(np.percentile(v, 95)) for v in values],
                        process_p99_ns=[float(np.percentile(v, 99)) for v in values],
                        median_p95_ns=statistics.median(
                            float(np.percentile(v, 95)) for v in values
                        ),
                        median_p99_ns=statistics.median(
                            float(np.percentile(v, 99)) for v in values
                        ),
                        query_time_sum_ns=[sum(v) for v in values],
                        peak_private_bytes=[r["memory"]["peak_private_bytes"] for r in runs],
                        peak_rss_bytes=[r["memory"]["peak_rss_bytes"] for r in runs],
                        open_ns=[r["open_ns"] for r in runs],
                    )
                )
    summary = dict(
        environment=manifest["environment"],
        process_rounds=manifest["process_rounds"],
        verified_timed_results=verified,
        results=rows,
        limits=[
            "Windows GNU native extension; not native Linux or Windows MSVC evidence",
            "One backend handle and one query at a time; budget=1 for every backend",
            "Each query has 32 untimed warmups; OS cache uncontrolled, not a cold-cache test",
            "Timer includes Python call and materialization of packed readonly NumPy int64 output",
            "Canonical row validation and full output SHA256 are outside query timing",
            f"Each backend/workload runs in {manifest['process_rounds']} fresh processes; backend order randomized per round",
            "Bulk 30 timed repetitions per query; short 1430 distinct frozen queries once per process",
            "Memory is whole-worker sampled peak including imports, warmup and validation; not allocation delta",
            "Windows private commit excludes mmap file pages; report RSS alongside private commit",
            "Memory sampled every 5ms; peaks are lower bounds, bulk memory covers all four cases",
            "Short p95/p99 mix query shapes and symbols; they are not repeat distributions of one query",
            "SQL includes query planning and native binding conversion; no SQL prepared-statement redesign",
        ],
    )
    (output / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
    return summary


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("stores", "manifest", "bulk", "short", "output"):
        parser.add_argument("--" + name, type=Path, required=True)
    parser.add_argument("--process-rounds", type=int, default=5)
    parser.add_argument("--timeout", type=float, default=600)
    args = parser.parse_args()
    if args.process_rounds < 1:
        parser.error("process rounds must be positive")
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    for name in ("bulk", "short"):
        shutil.copy2(getattr(args, name), output / (name + ".json"))
    shutil.copy2(args.manifest, output / "input-manifest.json")
    assert len(json.loads((output / "bulk.json").read_text())) == 4
    assert len(json.loads((output / "short.json").read_text())) == 1430
    import fastk_native._native as native

    environment = dict(
        python=sys.version,
        platform=platform.platform(),
        sqlite=sqlite3.sqlite_version,
        dependencies={
            n: importlib.metadata.version(n)
            for n in ("numpy", "duckdb", "pyarrow", "psutil", "fastk-native")
        },
        native_module=str(native.__file__),
        native_sha256=sha(Path(native.__file__)),
        inputs={n: sha(output / (n + ".json")) for n in ("bulk", "short", "input-manifest")},
        harness={
            str(p.relative_to(Path(__file__).parent)): sha(p)
            for p in [
                Path(__file__),
                Path(__file__).parent / "native_handoff/worker.py",
                *sorted((Path(__file__).parent / "fastk_bench").glob("*.py")),
            ]
        },
    )
    jobs = []
    rng = random.Random(20261021)
    for index in range(args.process_rounds):
        batch = [
            dict(backend=n, workload=w, process_round=index)
            for n in NAMES
            for w in ("bulk", "short")
        ]
        rng.shuffle(batch)
        jobs.extend(batch)
    for index, job in enumerate(jobs):
        job["output"] = (
            f"{index:02}-{job['backend']}-{job['workload']}-r{job['process_round']}.json"
        )
    manifest = dict(environment=environment, jobs=jobs, process_rounds=args.process_rounds)
    (output / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    for job in jobs:
        result_path = output / job["output"]
        config = dict(
            backend=job["backend"],
            mode="internal",
            budget=1,
            manifest=str(output / "input-manifest.json"),
            store=str((args.stores / job["backend"]).resolve()),
            queries=str(output / (job["workload"] + ".json")),
            output=str(result_path),
            rounds=30 if job["workload"] == "bulk" else 1,
        )
        if job["backend"] == "fastk":
            config["expected_native_sha256"] = environment["native_sha256"]
        config_path = result_path.with_name(result_path.stem + "-config.json")
        config_path.write_text(json.dumps(config) + "\n")
        if job["backend"] == "fastk":
            assert sha(Path(native.__file__)) == environment["native_sha256"]
        module = (
            "native_handoff.worker" if job["backend"] == "fastk" else "fastk_bench.matrix_worker"
        )
        result = run(module, config_path, result_path, args.timeout)
        assert result["all_results_verified"]
        print("verified", result_path.name, flush=True)
    summarize(output)


if __name__ == "__main__":
    main()
