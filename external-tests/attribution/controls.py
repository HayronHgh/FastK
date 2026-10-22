"""Warm-handle and monitor controls, separate from the primary attribution run."""

import argparse
import hashlib
import json
import random
import subprocess
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import psutil
from fastk_bench.process import run


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--stores", type=Path, required=True)
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--short", type=Path, required=True)
    parser.add_argument("--bulk", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    jobs = []
    for round_ in range(3):
        for workers in [1, 2, 4, 8]:
            jobs.append({"route": "core-control", "workers": workers, "round": round_})
            for route in ["native", "probe"]:
                jobs.append({"route": route, "workers": workers, "round": round_})
        for workers in [1, 8]:
            jobs.append(
                {"route": "core-control", "workers": workers, "round": round_, "monitor": True}
            )
    random.Random(20261021).shuffle(jobs)
    results = []
    for index, job in enumerate(jobs):
        stem = f"{index:03}-{job['route']}"
        path = output / (stem + ".json")
        if job["route"] == "core-control":
            command = [
                str(args.binary.resolve()),
                "parallel",
                str((args.stores / "fastk").resolve()),
                str(args.short.resolve()),
                str(job["workers"]),
                "first",
            ]
            with path.open("wb") as stream:
                child = subprocess.Popen(command, stdout=stream)
                monitor = psutil.Process(child.pid)
                samples = peak = 0
                while child.poll() is None:
                    if job.get("monitor"):
                        try:
                            peak = max(peak, monitor.memory_full_info().uss)
                            samples += 1
                        except psutil.NoSuchProcess:
                            pass
                    time.sleep(0.005)
                if child.wait() != 0:
                    raise RuntimeError(command)
            result = json.loads(path.read_text())
            result["monitor"] = {"pid": child.pid, "uss_samples": samples, "peak_uss": peak}
            path.write_text(json.dumps(result, indent=2) + "\n")
        else:
            config = {
                **job,
                "mode": "outer",
                "store": str((args.stores / "fastk").resolve()),
                "manifest": str((args.input / "manifest.json").resolve()),
                "short_queries": str(args.short.resolve()),
                "output": str(path),
                "switch_interval": 0.005,
            }
            config_path = output / (stem + "-config.json")
            config_path.write_text(json.dumps(config, indent=2) + "\n")
            result = run("attribution.warm_worker", config_path, path, 300)
        assert result["all_results_verified"], stem
        results.append({**job, "file": path.name})
        print("control verified", index + 1, "/", len(jobs), job, flush=True)
    for mode, queries in [("trace", args.short), ("layout", args.bulk)]:
        with (output / (mode + ".json")).open("wb") as stream:
            subprocess.run(
                [
                    str(args.binary.resolve()),
                    mode,
                    str((args.stores / "fastk").resolve()),
                    str(queries.resolve()),
                ],
                stdout=stream,
                check=True,
            )
    (output / "summary.json").write_text(
        json.dumps(
            {
                "all_results_verified": True,
                "binary_sha256": hashlib.sha256(args.binary.read_bytes()).hexdigest(),
                "results": results,
            },
            indent=2,
        )
        + "\n"
    )


if __name__ == "__main__":
    main()
