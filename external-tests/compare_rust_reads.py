"""Compile identical public-API probes against two explicit source trees."""

import argparse
import hashlib
import json
import os
import random
import subprocess
from collections import defaultdict
from pathlib import Path

from run_benchmarks import summarize


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--before-source", type=Path, required=True)
    parser.add_argument("--after-source", type=Path, required=True)
    parser.add_argument("--store", type=Path, required=True)
    parser.add_argument("--queries", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--rounds", type=int, default=3)
    args = parser.parse_args()
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    template = Path(__file__).parent / "rust-read-bench"
    environment = os.environ.copy()
    environment["CARGO_INCREMENTAL"] = "0"
    environment.setdefault("CARGO_TARGET_DIR", str(output / "build"))
    rustc = subprocess.check_output(["rustc", "-vV"], text=True)
    if "windows" in rustc:
        environment["RUSTFLAGS"] = (
            environment.get("RUSTFLAGS", "") + " -C target-feature=+crt-static"
        ).strip()
    binaries = {}
    for name, source in [("before", args.before_source), ("after", args.after_source)]:
        crate = output / name
        (crate / "src").mkdir(parents=True)
        manifest = (
            (template / "Cargo.toml")
            .read_text()
            .replace('path = "../.."', f'path = "{source.resolve().as_posix()}"')
        )
        (crate / "Cargo.toml").write_text(manifest)
        (crate / "src/main.rs").write_bytes((template / "src/main.rs").read_bytes())
        with (output / f"{name}-build.log").open("w") as log:
            subprocess.run(
                [
                    "cargo",
                    "build",
                    "--release",
                    "--offline",
                    "--manifest-path",
                    str(crate / "Cargo.toml"),
                ],
                env=environment,
                stdout=log,
                stderr=subprocess.STDOUT,
                check=True,
            )
        built = (
            Path(environment["CARGO_TARGET_DIR"])
            / "release"
            / ("fastk-read-bench.exe" if os.name == "nt" else "fastk-read-bench")
        )
        target = output / (name + "-probe" + built.suffix)
        target.write_bytes(built.read_bytes())
        target.chmod(0o755)
        binaries[name] = target
    orders = []
    grouped = {name: defaultdict(list) for name in binaries}
    for index in range(args.rounds):
        order = list(binaries)
        random.Random(20261021 + index).shuffle(order)
        orders.append(order)
        for name in order:
            raw = subprocess.check_output(
                [str(binaries[name]), str(args.store.resolve()), str(args.queries.resolve())]
            )
            (output / f"{name}-{index}.json").write_bytes(raw)
            result = json.loads(raw)
            assert result["all_results_verified"]
            for sample in result["samples"]:
                grouped[name][sample["case"]].extend(sample["times_ns"])
    stats = {
        name: {
            case: summarize(values, case in {"short", "latest"}) for case, values in cases.items()
        }
        for name, cases in grouped.items()
    }
    ratios = {
        case: stats["after"][case]["median_ns"] / values["median_ns"]
        for case, values in stats["before"].items()
    }
    summary = {
        **stats,
        "ratio_after_before": ratios,
        "gate": {"maximum_regression": 1.05, "passed": all(r <= 1.05 for r in ratios.values())},
        "all_results_verified": True,
        "rustc": rustc,
        "round_orders": orders,
        "binary_sha256": {
            name: hashlib.sha256(path.read_bytes()).hexdigest() for name, path in binaries.items()
        },
        "probe_source_sha256": hashlib.sha256((template / "src/main.rs").read_bytes()).hexdigest(),
        "limits": [
            "metrics off, one prewarmed handle; OS cache uncontrolled",
            f"{args.rounds} paired process rounds; 32 separately recorded warmup reads per case; 32 measured repeats per short/latest, ten per bulk",
            "query execution only; validation and result destruction outside timer",
        ],
    }
    (output / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
    print(json.dumps(summary["gate"]), flush=True)
    if not summary["gate"]["passed"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
