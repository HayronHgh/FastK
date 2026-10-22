"""Paired descriptor-table initialization controls; never a production workaround."""

import argparse
import hashlib
import json
import random
import subprocess
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from fastk_bench.process import run


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--store", type=Path, required=True)
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--short", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    jobs = [
        {"round": r, "route": route, "workers": n, "prepared": prepared}
        for r in range(3)
        for n in [1, 4, 8]
        for route in ["rust", "native", "probe"]
        for prepared in [False, True]
    ]
    random.Random(20261021).shuffle(jobs)
    results = []
    for index, job in enumerate(jobs):
        path = output / f"{index:03}-{job['route']}.json"
        if job["route"] == "rust":
            command = [
                str(args.binary.resolve()),
                "parallel",
                str(args.store.resolve()),
                str(args.short.resolve()),
                str(job["workers"]),
                "first",
            ]
            if job["prepared"]:
                command.append("--prepare-fds")
            with path.open("wb") as stream:
                subprocess.run(command, stdout=stream, check=True)
            result = json.loads(path.read_text())
        else:
            config = {
                **job,
                "mode": "outer",
                "store": str(args.store.resolve()),
                "manifest": str((args.input / "manifest.json").resolve()),
                "short_queries": str(args.short.resolve()),
                "output": str(path),
                "switch_interval": 0.005,
            }
            config_path = path.with_name(path.stem + "-config.json")
            config_path.write_text(json.dumps(config, indent=2) + "\n")
            result = run(
                "attribution.fd_worker" if job["prepared"] else "attribution.worker",
                config_path,
                path,
                300,
            )
        assert result["all_results_verified"]
        results.append({**job, "file": path.name})
        print("FD control verified", index + 1, "/", len(jobs), job, flush=True)
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
