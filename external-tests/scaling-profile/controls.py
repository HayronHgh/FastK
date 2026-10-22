"""Capacity and memcpy-size controls; larger cache exists only in a frozen diagnostic copy."""

import argparse
import json
import random
import subprocess
from pathlib import Path
from types import SimpleNamespace

from run import invoke


def main():
    p = argparse.ArgumentParser(description=__doc__)
    for name in ("binary", "larger", "bandwidth", "counts", "store", "short", "bulk", "output"):
        p.add_argument("--" + name, type=Path, required=True)
    args = p.parse_args()
    args.output.mkdir(exist_ok=False)
    jobs = [
        dict(capacity=cap, mode=mode, threads=n, groups=1, cycles=100, round=r)
        for r in range(3)
        for cap in (64, 512)
        for mode, n in (("outer", 1), ("outer", 4), ("outer", 8), ("private", 4), ("private", 8))
    ]
    random.Random(20261021).shuffle(jobs)
    for i, job in enumerate(jobs):
        job["name"] = (
            f"capacity-{i:02}-{job['capacity']}-{job['mode']}-t{job['threads']}-r{job['round']}"
        )
        selected = SimpleNamespace(**vars(args))
        selected.binary = args.binary if job["capacity"] == 64 else args.larger
        invoke(selected, args.output, job)
        print("capacity", i + 1, "/", len(jobs), flush=True)
    sizes = [
        dict(bytes=size * 1024**2, threads=n, groups=g, round=r)
        for r in range(3)
        for size in (1, 4, 20, 80)
        for n, g in ((4, 4), (8, 4), (8, 8))
    ]
    random.Random(20261021).shuffle(sizes)
    for i, job in enumerate(sizes):
        path = args.output / f"bandwidth-{i:02}.json"
        with path.open("wb") as stream:
            subprocess.run(
                [
                    str(args.bandwidth.resolve()),
                    str(job["bytes"]),
                    str(job["threads"]),
                    str(job["groups"]),
                ],
                stdout=stream,
                check=True,
                timeout=120,
            )
        assert json.loads(path.read_text())["passed"]
    for sharing in ("shared", "private"):
        for n in (1, 4, 8):
            path = args.output / f"counts-512-{sharing}-{n}.json"
            with path.open("wb") as stream:
                subprocess.run(
                    [
                        str(args.counts.resolve()),
                        str(args.store.resolve()),
                        str(args.short.resolve()),
                        str(n),
                        sharing,
                        "100",
                    ],
                    stdout=stream,
                    check=True,
                    timeout=120,
                )
            assert json.loads(path.read_text())["passed"]
    (args.output / "manifest.json").write_text(
        json.dumps(dict(jobs=jobs, sizes=sizes, diagnostic_only=True, passed=True), indent=2) + "\n"
    )


if __name__ == "__main__":
    main()
