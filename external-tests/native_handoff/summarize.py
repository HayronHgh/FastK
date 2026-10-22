"""Retain process-level samples when summarizing a paired wheel comparison."""

import argparse
import json
import random
import statistics
from collections import defaultdict
from pathlib import Path


def quantile(values, fraction):
    values = sorted(values)
    index = (len(values) - 1) * fraction
    low = int(index)
    high = min(low + 1, len(values) - 1)
    return values[low] + (values[high] - values[low]) * (index - low)


def summarize(folder, supplements=()):
    groups = defaultdict(dict)
    memory = defaultdict(dict)
    entries = []
    for directory in (folder, *supplements):
        manifest = json.loads((directory / "manifest.json").read_text())
        assert manifest["passed"]
        entries += [(directory, manifest, entry) for entry in manifest["results"]]
    for directory, manifest, entry in entries:
        job = entry["job"]
        result = json.loads((directory / entry["output"]).read_text())
        assert result["all_results_verified"]
        assert result["native_sha256"] == manifest["modules"][job["version"]]["sha256"]
        cases = defaultdict(list)
        for sample in result["samples"]:
            cases[sample["case"]].append(sample["ns"])
        for case, values in cases.items():
            groups[(case, job["budget"])].setdefault(job["version"], []).append(
                dict(
                    round=job["round"],
                    cohort=directory.name,
                    median_ns=statistics.median(values),
                    p95_ns=quantile(values, 0.95),
                    p99_ns=quantile(values, 0.99),
                    n=len(values),
                )
            )
        memory[(job["case"], job["budget"])].setdefault(job["version"], []).append(
            dict(round=job["round"], cohort=directory.name, **result["memory"])
        )
    timings = []
    for (case, budget), versions in sorted(groups.items()):
        medians = {
            version: statistics.median(p["median_ns"] for p in processes)
            for version, processes in versions.items()
        }
        paired = defaultdict(dict)
        for version, processes in versions.items():
            for process in processes:
                paired[(process["cohort"], process["round"])][version] = process["median_ns"]
        ratios = [p["after"] / p["before"] for p in paired.values()]
        rng = random.Random(20261021)
        bootstrap = [statistics.median(rng.choices(ratios, k=len(ratios))) for _ in range(5000)]
        timings.append(
            dict(
                case=case,
                budget=budget,
                median_ns=medians,
                ratio_after_before=medians["after"] / medians["before"],
                paired_median_ratio=statistics.median(ratios),
                paired_bootstrap_95=[quantile(bootstrap, 0.025), quantile(bootstrap, 0.975)],
                process_pairs=len(ratios),
                processes=versions,
            )
        )
    memories = []
    for (case, budget), versions in sorted(memory.items()):
        memories.append(
            dict(
                workload=case,
                budget=budget,
                median_peak={
                    version: {
                        key: statistics.median(p[key] for p in processes)
                        for key in ("peak_rss_bytes", "peak_private_bytes")
                    }
                    for version, processes in versions.items()
                },
                processes=versions,
            )
        )
    return dict(
        timings=timings,
        memory=memories,
        method="median of process medians; all process pairs retained, including supplements",
        memory_scope="whole workload process incl. warmup/validation; 5ms sampled lower bounds",
        passed=True,
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directory", type=Path)
    parser.add_argument("output", type=Path)
    parser.add_argument("--supplement", type=Path, action="append", default=[])
    args = parser.parse_args()
    args.output.write_text(json.dumps(summarize(args.directory, args.supplement), indent=2) + "\n")
