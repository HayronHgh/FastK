"""Summarize independent process medians; never mix a query with a 1430-query pass."""

import argparse
import json
import statistics
from collections import defaultdict
from pathlib import Path


def load(path):
    return json.loads(path.read_text(encoding="utf-8"))


def phases(directory, jobs, capacity=False):
    groups = defaultdict(list)
    for job in jobs:
        path = directory / (job["name"] + ".json")
        result = load(path)
        assert result["passed"] and result["cycles"] == job["cycles"]
        events = {
            event["event"]: float(event["counter-value"])
            for line in path.with_suffix(".stat").read_text().splitlines()
            if line.startswith("{")
            for event in [json.loads(line)]
        }
        key = (job["mode"], job["threads"], job["groups"])
        if capacity:
            key = (job["capacity"], *key)
        groups[key].append(
            dict(
                ms=result["phase_ns"] / result["cycles"] / 1e6,
                cpu_ms=events["task-clock"] / result["cycles"],
                context_switches=events["context-switches"] / result["cycles"],
                cache_misses=events["cache-misses"] / result["cycles"],
                page_faults=events["page-faults"] / result["cycles"],
            )
        )
    return [
        dict(
            key=key,
            processes=len(values),
            **{name: statistics.median(v[name] for v in values) for name in values[0]},
        )
        for key, values in sorted(groups.items())
    ]


def summarize(profile, controls, serial):
    bandwidth = defaultdict(list)
    for path in [*controls.glob("bandwidth-*.json"), *serial.glob("*.json")]:
        result = load(path)
        assert result["passed"]
        key = (result["bytes"], result["threads"], result["groups"])
        bandwidth[key].append(statistics.median(result["times_ns"]))
    return dict(
        units="phase ms/cycle; outer/private cycle = 1430 queries; others = one call",
        aggregation="median of process measurements, not pooled samples",
        primary=phases(profile, load(profile / "manifest.json")["jobs"]),
        capacity=phases(controls, load(controls / "manifest.json")["jobs"], True),
        bandwidth=[
            dict(
                bytes=key[0],
                threads=key[1],
                groups=key[2],
                processes=len(values),
                ms=statistics.median(values) / 1e6,
                effective_gb_s=key[0] / statistics.median(values),
            )
            for key, values in sorted(bandwidth.items())
        ],
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("profile", "controls", "serial", "output"):
        parser.add_argument("--" + name, type=Path, required=True)
    args = parser.parse_args()
    args.output.write_text(
        json.dumps(summarize(args.profile, args.controls, args.serial), indent=2) + "\n",
        encoding="utf-8",
    )
