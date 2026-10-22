"""Run isolated attribution jobs against frozen queries and unchanged native wheels."""

import argparse
import hashlib
import importlib.metadata
import json
import os
import platform
import random
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from fastk_bench.process import run


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--stores", type=Path, required=True)
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--bulk", type=Path, required=True)
    parser.add_argument("--short", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--rounds", type=int, default=3)
    parser.add_argument("--parallel", action="store_true")
    parser.add_argument("--repetitions", type=int, default=30)
    args = parser.parse_args()
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    jobs = []
    for round_ in range(args.rounds):
        for route in ["native", "probe", "arrow", "arrow-probe", "copy-batch", "numpy-copy"]:
            jobs.append({"mode": "bulk", "route": route, "round": round_})
        if args.parallel:
            for internal in [2, 4, 8]:
                for route in ["native", "probe"]:
                    jobs.append(
                        {"mode": "bulk", "route": route, "round": round_, "internal": internal}
                    )
            for workers in [1, 2, 4, 8]:
                for route in ["native", "direct", "probe", "rust-core", "rust-words", "gil-only"]:
                    jobs.append(
                        {"mode": "outer", "route": route, "round": round_, "workers": workers}
                    )
            for workers in [2, 4, 8]:
                for interval in [0.001, 0.0001]:
                    jobs.append(
                        {
                            "mode": "outer",
                            "route": "native",
                            "round": round_,
                            "workers": workers,
                            "switch_interval": interval,
                        }
                    )
            for route in ["direct", "rust-core"]:
                jobs.append(
                    {"mode": "outer", "route": route, "round": round_, "workers": 4, "shared": True}
                )
    # Keep compilation and all other timed workloads outside this sequential run.
    random.Random(20261021).shuffle(jobs)
    results = []
    for index, job in enumerate(jobs):
        stem = f"{index:03}-{job['mode']}-{job['route']}"
        path = output / f"{stem}.json"
        config = {
            **job,
            "store": str(
                (args.stores / ("arrow" if job["route"].startswith("arrow") else "fastk")).resolve()
            ),
            "manifest": str((args.input / "manifest.json").resolve()),
            "bulk": json.loads(args.bulk.read_text()),
            "short_queries": str(args.short.resolve()),
            "output": str(path),
            "repetitions": args.repetitions,
            "switch_interval": job.get("switch_interval", 0.005),
        }
        config_path = output / f"{stem}-config.json"
        config_path.write_text(json.dumps(config, indent=2) + "\n")
        result = run("attribution.worker", config_path, path, 300)
        if not (result.get("all_results_verified") or result.get("negative_control")):
            raise AssertionError(stem)
        results.append({**job, "file": path.name})
        print("verified", index + 1, "/", len(jobs), job, flush=True)
    import _fastk_attribution
    import fastk_native._native

    environment = {
        "python": sys.version,
        "platform": platform.platform(),
        "native_module_sha256": hashlib.sha256(
            Path(fastk_native._native.__file__).read_bytes()
        ).hexdigest(),
        "probe_module_sha256": hashlib.sha256(
            Path(_fastk_attribution.__file__).read_bytes()
        ).hexdigest(),
        "dependencies": {
            name: importlib.metadata.version(name)
            for name in ["fastk-native", "numpy", "pyarrow", "psutil"]
        },
        "cpu_count": os.cpu_count(),
        "affinity": sorted(os.sched_getaffinity(0)) if hasattr(os, "sched_getaffinity") else None,
        "cgroup": {
            str(p): p.read_text()
            for p in [Path("/sys/fs/cgroup/cpu.max"), Path("/sys/fs/cgroup/cpu.stat")]
            if p.exists()
        },
        "bulk_sha256": hashlib.sha256(args.bulk.read_bytes()).hexdigest(),
        "short_sha256": hashlib.sha256(args.short.read_bytes()).hexdigest(),
        "source_sha256": {
            str(p.relative_to(Path(__file__).parent)): hashlib.sha256(p.read_bytes()).hexdigest()
            for p in sorted(Path(__file__).parent.rglob("*"))
            if p.is_file()
            and not {"__pycache__", "target"}.intersection(
                p.relative_to(Path(__file__).parent).parts
            )
        },
    }
    (output / "summary.json").write_text(
        json.dumps(
            {
                "all_result_jobs_verified": True,
                "negative_controls_are_not_read_validations": True,
                "environment": environment,
                "results": results,
            },
            indent=2,
        )
        + "\n"
    )


if __name__ == "__main__":
    main()
