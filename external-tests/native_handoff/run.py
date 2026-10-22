"""Paired native wheels, same core/query contract, actual worker memory sampling."""

import argparse
import hashlib
import json
import os
import random
import sys
import zipfile
from pathlib import Path

from fastk_bench.process import run


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("before", "after", "store", "manifest", "bulk", "short", "output"):
        parser.add_argument("--" + name, type=Path, required=True)
    parser.add_argument("--rounds", type=int, default=3)
    parser.add_argument("--serial-only", action="store_true")
    args = parser.parse_args()
    if args.rounds < 1:
        parser.error("--rounds must be positive")
    args.output = args.output.resolve()
    args.output.mkdir(parents=True, exist_ok=False)
    modules = {}
    for version in ("before", "after"):
        wheel = getattr(args, version)
        dest = (args.output / (version + "-module")).resolve()
        with zipfile.ZipFile(wheel) as archive:
            for name in archive.namelist():
                assert (dest / name).resolve().is_relative_to(dest), name
            archive.extractall(dest)
        candidates = [p for p in (dest / "fastk_native").iterdir() if p.suffix in (".pyd", ".so")]
        assert len(candidates) == 1
        modules[version] = dict(
            root=str(dest),
            sha256=hashlib.sha256(candidates[0].read_bytes()).hexdigest(),
            wheel_sha256=hashlib.sha256(wheel.read_bytes()).hexdigest(),
        )
    assert len(json.loads(args.short.read_text())) == 1430
    # Keep before/after paired by case and process round; randomize pair and order.
    settings = [("short", 1), ("bulk", 1)]
    if not args.serial_only:
        settings += [("bulk", 4), ("bulk", 8)]
    pairs = [
        [dict(round=r, version=v, case=case, budget=n) for v in ("before", "after")]
        for r in range(args.rounds)
        for case, n in settings
    ]
    rng = random.Random(20261021)
    rng.shuffle(pairs)
    for pair in pairs:
        rng.shuffle(pair)
    jobs = [job for pair in pairs for job in pair]
    results = []
    for index, job in enumerate(jobs):
        module = modules[job["version"]]
        os.environ["PYTHONPATH"] = os.pathsep.join(
            [module["root"], str(Path(__file__).resolve().parents[1])]
        )
        stem = f"{index:02}-{job['version']}-{job['case']}-t{job['budget']}-r{job['round']}"
        output = (args.output / (stem + ".json")).resolve()
        config = dict(
            backend="fastk",
            mode="internal",
            budget=job["budget"],
            manifest=str(args.manifest.resolve()),
            store=str(args.store.resolve()),
            queries=str((args.bulk if job["case"] == "bulk" else args.short).resolve()),
            output=str(output),
            rounds=30 if job["case"] == "bulk" else 1,
            expected_native_sha256=module["sha256"],
        )
        path = args.output / (stem + "-config.json")
        path.write_text(json.dumps(config) + "\n")
        result = run("native_handoff.worker", path, output, 600)
        assert result["all_results_verified"]
        results.append(dict(job=job, output=output.name))
        print("verified", stem, flush=True)
    (args.output / "manifest.json").write_text(
        json.dumps(
            dict(modules=modules, results=results, python=sys.version, passed=True), indent=2
        )
        + "\n"
    )


if __name__ == "__main__":
    main()
