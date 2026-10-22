"""Repeat request-scoped reads against explicit native wheels and prepared stores."""

import argparse
import hashlib
import importlib.metadata
import json
import os
import platform
import random
import shutil
import statistics
import sys
import zipfile
from pathlib import Path

import numpy as np
from fastk_bench.process import run


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("before", "after", "output"):
        parser.add_argument("--" + name, type=Path, required=name != "after")
    parser.add_argument("--rounds", type=int, default=3)
    parser.add_argument(
        "--backends", nargs="+", default=["fastk", "arrow", "sqlite", "duckdb", "parquet"]
    )
    parser.add_argument(
        "--workloads",
        nargs="+",
        default=["hot-local", "hot-replay", "cycle-short", "cycle-bulk", "catalog-short"],
    )
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[2]
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    for source, name in [
        ("target/v02-final-linux-matrix/short.json", "short.json"),
        ("target/v02-final-linux-matrix/bulk.json", "bulk.json"),
        ("target/external-standard-final/input/manifest.json", "input-manifest.json"),
    ]:
        shutil.copy2(root / source, output / name)
    assert len(json.loads((output / "short.json").read_text())) == 1430
    modules = {}
    for version in ("before", "after"):
        wheel = getattr(args, version)
        if wheel is None:
            continue
        dest = output / (version + "-module")
        with zipfile.ZipFile(wheel) as archive:
            assert all(
                (dest / name).resolve().is_relative_to(dest.resolve())
                for name in archive.namelist()
            )
            archive.extractall(dest)
        native = [p for p in (dest / "fastk_native").iterdir() if p.suffix in (".pyd", ".so")]
        assert len(native) == 1
        modules[version] = dict(root=str(dest), sha256=sha(native[0]), wheel_sha256=sha(wheel))
    jobs = []
    rng = random.Random(20261023)
    for round_index in range(args.rounds):
        batch = []
        for name in args.backends:
            for workload in args.workloads:
                if workload.startswith("hot") and name not in ("fastk", "arrow"):
                    continue
                if workload == "catalog-short" and name not in ("arrow", "parquet"):
                    continue
                for version in modules if name == "fastk" else ["peer"]:
                    batch.append(
                        dict(
                            backend=name,
                            workload=workload,
                            version=version,
                            process_round=round_index,
                        )
                    )
        rng.shuffle(batch)
        jobs.extend(batch)
    results = []
    for index, job in enumerate(jobs):
        stem = f"{index:02}-{job['backend']}-{job['version']}-{job['workload']}-r{job['process_round']}"
        result_path = output / (stem + ".json")
        module = modules.get(job["version"])
        os.environ["PYTHONPATH"] = os.pathsep.join(
            ([module["root"]] if module else []) + [str(root / "external-tests")]
        )
        workload = job["workload"]
        config = dict(
            backend=job["backend"],
            mode=workload if workload.startswith("hot") else "cycle",
            manifest=str(output / "input-manifest.json"),
            store=str(root / "target/external-standard-evidence" / job["backend"]),
            queries=str(output / ("bulk.json" if workload == "cycle-bulk" else "short.json")),
            repetitions=5 if workload == "cycle-bulk" else 1,
            output=str(result_path),
            native_sha256=module["sha256"] if module else None,
            arrow_capacity=256 if workload.startswith("hot") else 64,
            reload_catalog=workload == "catalog-short",
        )
        config_path = output / (stem + "-config.json")
        config_path.write_text(json.dumps(config) + "\n")
        result = run("read_lifecycle.worker", config_path, result_path, 600)
        assert result["all_results_verified"]
        job["output"] = result_path.name
        results.append((job, result))
        print("verified", stem, flush=True)
    rows = []
    for key in sorted(
        {
            (j["backend"], j["version"], j["workload"], s["case"])
            for j, r in results
            for s in r["samples"]
        }
    ):
        name, version, workload, case = key
        matched = [r for j, r in results if (j["backend"], j["version"], j["workload"]) == key[:3]]
        fields = (
            ["read_ns"]
            if workload.startswith("hot")
            else ["open_ns", "read_ns", "close_ns", "total_ns"]
        )
        stats = {}
        for field in fields:
            values = [[s[field] for s in r["samples"] if s["case"] == case] for r in matched]
            stats[field] = dict(
                process_medians=[statistics.median(v) for v in values],
                median_ns=statistics.median(statistics.median(v) for v in values),
                p95_ns=statistics.median(float(np.percentile(v, 95)) for v in values),
                p99_ns=statistics.median(float(np.percentile(v, 99)) for v in values),
            )
        rows.append(
            dict(
                backend=name,
                version=version,
                workload=workload,
                case=case,
                phases=stats,
                peak_private=[r["memory"]["peak_private_bytes"] for r in matched],
                peak_rss=[r["memory"]["peak_rss_bytes"] for r in matched],
            )
        )
    summary = dict(
        jobs=jobs,
        modules=modules,
        process_rounds=args.rounds,
        results=rows,
        verified_timed_outputs=sum(len(r["samples"]) for _, r in results),
        all_results_verified=True,
        environment=dict(
            python=sys.version,
            platform=platform.platform(),
            dependencies={
                n: importlib.metadata.version(n) for n in ["numpy", "duckdb", "pyarrow", "psutil"]
            },
        ),
        source_sha256={
            p.relative_to(root).as_posix(): sha(p)
            for p in [
                *sorted((root / "external-tests/read_lifecycle").glob("*.py")),
                *sorted((root / "external-tests/fastk_bench").glob("*.py")),
            ]
        },
    )
    (output / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")


if __name__ == "__main__":
    main()
