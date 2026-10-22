"""Prespecified nine extra paired processes after a noisy serial gate warning."""

import argparse
import json
import random
import statistics
import subprocess
from pathlib import Path


def main():
    p = argparse.ArgumentParser(description=__doc__)
    for name in ("primary", "before", "after", "store", "queries", "output"):
        p.add_argument("--" + name, type=Path, required=True)
    args = p.parse_args()
    args.output.mkdir(exist_ok=False)
    primary = json.loads((args.primary / "summary.json").read_text())
    pairs = {}
    sources = []
    for item in primary["files"]:
        if item["mode"] == "bulk" and item["workers"] == 1:
            pairs.setdefault(item["round"], {})[item["version"]] = json.loads(
                (args.primary / item["file"]).read_text()
            )
            sources.append(str(args.primary / item["file"]))
    assert len(pairs) == 3
    for round_ in range(9):
        pair = pairs.setdefault(round_ + 3, {})
        order = ("before", "after") if round_ % 2 == 0 else ("after", "before")
        for version in order:
            path = args.output / f"{round_:02}-{version}.json"
            command = [
                str(getattr(args, version).resolve()),
                "bulk",
                str(args.store.resolve()),
                str(args.queries.resolve()),
                "1",
                "shared",
            ]
            with path.open("wb") as stream:
                subprocess.run(command, stdout=stream, check=True, timeout=120)
            pair[version] = json.loads(path.read_text())
            sources.append(str(path))
        print("serial pair", round_ + 1, "/ 9", flush=True)
    results = {}
    cases = [sample["case"] for sample in pairs[0]["before"]["samples"]]
    for case in cases:
        samples = {version: [] for version in ("before", "after")}
        ratios = []
        for pair in pairs.values():
            values = {}
            for version, result in pair.items():
                assert result["all_results_verified"]
                values[version] = next(
                    s["times_ns"] for s in result["samples"] if s["case"] == case
                )
                samples[version].extend(values[version])
            ratios.append(statistics.median(values["after"]) / statistics.median(values["before"]))
        rng = random.Random(20261021)
        bootstrap = sorted(
            statistics.median(rng.choices(ratios, k=len(ratios))) for _ in range(10000)
        )
        before, after = (statistics.median(samples[v]) for v in ("before", "after"))
        results[case] = dict(
            before_ns=before,
            after_ns=after,
            ratio=after / before,
            samples_per_version=len(samples["before"]),
            paired_process_ratios=ratios,
            paired_ratio_median=statistics.median(ratios),
            paired_ratio_bootstrap_95=[bootstrap[249], bootstrap[9749]],
            pooled_median_within_5_percent=after / before <= 1.05,
        )
    (args.output / "summary.json").write_text(
        json.dumps(
            dict(
                primary_rounds=3,
                supplemental_rounds=9,
                combined_rounds=12,
                primary_hashes=primary["hashes"],
                sources=sources,
                results=results,
                all_results_verified=True,
                gate_passed=all(row["pooled_median_within_5_percent"] for row in results.values()),
                ci_unit="paired process median ratio; not independent individual queries",
            ),
            indent=2,
        )
        + "\n"
    )


if __name__ == "__main__":
    main()
