"""Paired before/after processes; production Rust reads never enter Python."""

import argparse
import hashlib
import json
import platform
import random
import statistics
import subprocess
from pathlib import Path


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main():
    p = argparse.ArgumentParser(description=__doc__)
    for name in ("before", "after", "store", "short", "bulk", "output"):
        p.add_argument("--" + name, type=Path, required=True)
    p.add_argument("--rounds", type=int, default=3)
    p.add_argument("--summarize-only", action="store_true")
    args = p.parse_args()
    if not args.summarize_only:
        args.output.mkdir(exist_ok=False)
    jobs = [
        (mode, workers, sharing, r)
        for mode in ("short", "bulk")
        for workers in (1, 2, 4, 8)
        for sharing in (("shared", "independent") if mode == "short" else ("shared",))
        for r in range(args.rounds)
    ]
    random.Random(20261021).shuffle(jobs)
    files = []
    for i, (mode, workers, sharing, r) in enumerate(jobs):
        versions = ("before", "after") if i % 2 == 0 else ("after", "before")
        for version in versions:
            name = f"{i:03}-{version}-{mode}-{sharing}-{workers}-{r}.json"
            command = [
                str(getattr(args, version).resolve()),
                mode,
                str(args.store.resolve()),
                str(getattr(args, mode).resolve()),
                str(workers),
                sharing,
            ]
            if not args.summarize_only:
                with (
                    (args.output / name).open("wb") as out,
                    (args.output / (name + ".log")).open("wb") as err,
                ):
                    subprocess.run(command, stdout=out, stderr=err, check=True, timeout=120)
            d = json.loads((args.output / name).read_text())
            assert d["all_results_verified"]
            files.append(
                dict(
                    version=version, mode=mode, workers=workers, sharing=sharing, round=r, file=name
                )
            )
        print("verified pair", i + 1, "/", len(jobs), mode, workers, sharing, flush=True)
    grouped = {}
    for item in files:
        d = json.loads((args.output / item["file"]).read_text())
        key = f"{item['mode']}-{item['sharing']}-{item['workers']}-{item['version']}"
        group = grouped.setdefault(key, {})
        if item["mode"] == "short":
            for label, start in (("first_qps", 0), ("warm_qps", 5)):
                group.setdefault(label, []).append(1430e9 / sum(d["walls_ns"][start : start + 5]))
            for label, select in (("first_ns", lambda r: r < 5), ("warm_ns", lambda r: r >= 5)):
                group.setdefault(label, []).extend(ns for r, _, ns in d["samples"] if select(r))
            group.setdefault("fd_counts", []).extend(x for x in d["fds"] if x is not None)
        else:
            for sample in d["samples"]:
                group.setdefault(sample["case"], []).extend(sample["times_ns"])
    summary = {}
    for key, group in grouped.items():
        summary[key] = {}
        for metric, values in group.items():
            if not values:
                continue
            values.sort()
            record = {"samples": len(values), "median": statistics.median(values)}
            if len(values) >= 100:
                record.update(
                    p95=values[int((len(values) - 1) * 0.95)],
                    p99=values[int((len(values) - 1) * 0.99)],
                )
            if metric == "fd_counts":
                record["max"] = max(values)
            summary[key][metric] = record
    (args.output / "summary.json").write_text(
        json.dumps(
            {
                "environment": platform.platform(),
                "rounds": args.rounds,
                "hashes": {
                    name: sha(getattr(args, name)) for name in ("before", "after", "short", "bulk")
                },
                "store": str(args.store.resolve()),
                "files": files,
                "results": summary,
                "all_results_verified": True,
                "os_cold_cache": False,
                "memory_sampling_during_timing": False,
            },
            indent=2,
        )
        + "\n"
    )


if __name__ == "__main__":
    main()
