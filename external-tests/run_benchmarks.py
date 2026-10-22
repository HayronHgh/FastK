"""Reproducible black-box comparisons. Existing output directories are never reset."""

from __future__ import annotations

import argparse
import hashlib
import importlib.metadata
import json
import math
import os
import platform
import random
import shutil
import sys
import time
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

import psutil
from fastk_bench.backends import NAMES, connect
from fastk_bench.data import (
    SEED,
    hash_rows,
    iter_parts,
    maintenance_inputs,
    prepare,
    queries,
    query_rows,
    verify_inputs,
)
from fastk_bench.process import run as run_process

ROOT = Path(__file__).resolve().parent


def summarize(values, tail=True):
    values = sorted(values)

    def percentile(p):
        return values[max(0, math.ceil(p * len(values)) - 1)]

    result = {
        "samples": len(values),
        "min_ns": values[0],
        "median_ns": percentile(0.5),
        "max_ns": values[-1],
    }
    if tail and len(values) >= 100:
        result.update(p95_ns=percentile(0.95), p99_ns=percentile(0.99))
    return result


def footprint(root):
    result = defaultdict(int)
    for path in Path(root).rglob("*"):
        if not path.is_file():
            continue
        kind = {
            ".zmap": "zone_map",
            ".vix": "value_index",
            ".chunk": "chunk_with_sparse_index_and_header",
            ".meta": "manifest",
            ".sqlite": "sqlite_database",
            ".duckdb": "duckdb_database",
            ".parquet": "parquet",
            ".arrow": "arrow",
        }.get(path.suffix, "other_or_journal")
        result[kind] += path.stat().st_size
    return {"total_bytes": sum(result.values()), "components": dict(result)}


def run_worker(config_path, output_path, timeout):
    return run_process("fastk_bench.worker", config_path, output_path, timeout)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument(
        "--bridge",
        type=Path,
        help="optional legacy CLI provenance; main benchmark uses native bindings",
    )
    parser.add_argument("--threads", type=int, default=1)
    parser.add_argument(
        "--queries-from", type=Path, help="replay previously frozen queries instead of regenerating"
    )
    parser.add_argument("--profile", choices=["smoke", "standard", "real"], default="smoke")
    parser.add_argument(
        "--prepared-input",
        type=Path,
        help="reuse a previously checksummed immutable input directory",
    )
    parser.add_argument("--rounds", type=int, default=5)
    parser.add_argument(
        "--reuse-stores-from",
        type=Path,
        help="copy dedicated prepared stores from an interrupted pre-maintenance run; initial build time is not measured",
    )
    parser.add_argument("--short-ops", type=int, default=2000)
    parser.add_argument("--full-scans", type=int, default=5)
    parser.add_argument("--worker-timeout", type=float, default=3600)
    args = parser.parse_args()
    if min(args.rounds, args.short_ops, args.full_scans) <= 0:
        parser.error("sample counts must be positive")
    root = args.root.resolve()
    root.mkdir(parents=True, exist_ok=False)
    binary = args.bridge.resolve() if args.bridge else None
    if binary is not None and not binary.is_file():
        parser.error("bridge must be a built release executable")
    source = args.prepared_input.resolve() if args.prepared_input else root / "input"
    manifest = (
        json.loads((source / "manifest.json").read_text())
        if args.prepared_input
        else prepare(source, args.profile)
    )
    if manifest["profile"] != args.profile:
        parser.error("prepared input profile does not match --profile")
    verify_inputs(source, manifest)
    reuse = args.reuse_stores_from.resolve() if args.reuse_stores_from else None
    if reuse:
        prior_manifest = reuse / "input-manifest.json"
        if not prior_manifest.exists() or json.loads(prior_manifest.read_text()) != manifest:
            parser.error("prepared stores must match the exact input manifest")
        if (reuse / "append-input").exists() or any(reuse.glob("*-v2")):
            parser.error("cannot reuse stores after maintenance has begun")
        if not (reuse / "environment.json").is_file() or any(
            not (reuse / name).is_dir() for name in NAMES
        ):
            parser.error("reuse requires a complete set of dedicated benchmark stores")
    (root / "input-manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    harness_hash = hashlib.sha256()
    for path in sorted([Path(__file__), *(ROOT / "fastk_bench").glob("*.py")]):
        harness_hash.update(path.relative_to(ROOT).as_posix().encode())
        harness_hash.update(path.read_bytes())
    environment = {
        "measured_at_utc": datetime.now(timezone.utc).isoformat(),
        "os": platform.platform(),
        "architecture": platform.machine(),
        "python": sys.version,
        "cpu_logical_count": os.cpu_count(),
        "physical_memory_bytes": psutil.virtual_memory().total,
        "bridge_sha256": hashlib.sha256(binary.read_bytes()).hexdigest() if binary else None,
        "harness_sha256": harness_hash.hexdigest(),
        "reused_prepared_stores": str(reuse) if reuse else None,
        "dependencies": {
            name: importlib.metadata.version(name)
            for name in ["duckdb", "pyarrow", "psutil", "fastk-native", "numpy"]
        },
        "native_module_sha256": hashlib.sha256(
            Path(__import__("fastk_native._native", fromlist=["__file__"]).__file__).read_bytes()
        ).hexdigest(),
        "seed": SEED,
        "profile": args.profile,
        "input_bar_rows": sum(part["rows"] for part in manifest["parts"] if part["kind"] == "bars"),
        "input_scalar_rows": sum(
            part["rows"] for part in manifest["parts"] if part["kind"] != "bars"
        ),
        "logical_fixed_record_payload_bytes": sum(
            part["rows"] * (48 if part["kind"] == "bars" else 16) for part in manifest["parts"]
        ),
        "file_space_definition": "sum of file lengths; filesystem allocation and directory metadata excluded",
        "rounds": args.rounds,
        "short_ops_per_round": args.short_ops,
        "full_scans_per_round": args.full_scans,
        "durability": "FastK: per-month synced files+manifest, Windows directory sync best-effort; SQLite: WAL synchronous FULL month commits, initial-build checkpoint; DuckDB: month transactions, initial-build checkpoint; Parquet/Arrow: fsynced files and POSIX directory sync. Crash guarantees differ.",
        "threads": f"per-query upper budget={args.threads}; native FastK/DuckDB/Arrow pools bounded; SQLite serial",
        "metrics": "query includes execution + complete packed NumPy delivery; validation outside timer; all backends in-process",
    }
    (root / "environment.json").write_text(json.dumps(environment, indent=2) + "\n")
    setup_order = NAMES.copy()
    random.Random(SEED).shuffle(setup_order)
    setup = {}
    for name in setup_order:
        if reuse:
            started = time.perf_counter_ns()
            shutil.copytree(reuse / name, root / name)
            setup[name] = {
                "build_ns": None,
                "build_samples": 0,
                "prepared_copy_ns": time.perf_counter_ns() - started,
                "storage": footprint(root / name),
            }
            print("copied prepared store", name, flush=True)
            continue
        started = time.perf_counter_ns()
        backend = connect(name, root / name, manifest, binary, write=True)
        backend.setup(source)
        backend.close()
        setup[name] = {
            "build_ns": time.perf_counter_ns() - started,
            "storage": footprint(root / name),
            "build_samples": 1,
        }
        print("prepared", name, flush=True)
    results = []
    orders = []
    for round in range(args.rounds):
        requests = (
            json.loads((args.queries_from / f"queries-{round}.json").read_text())
            if args.queries_from
            else queries(source, manifest, args.short_ops, args.full_scans, SEED + round)
        )
        request_file = root / f"queries-{round}.json"
        request_file.write_text(json.dumps(requests, indent=2) + "\n")
        order = NAMES.copy()
        random.Random(SEED + round).shuffle(order)
        orders.append(order)
        for name in order:
            output = root / f"{name}-{round}.json"
            config = {
                "backend": name,
                "round": round,
                "manifest": str(source / "manifest.json"),
                "queries": str(request_file),
                "store": str(root / name),
                "binary": str(binary) if binary else None,
                "threads": args.threads,
                "output": str(output),
            }
            config_file = root / f"{name}-{round}-config.json"
            config_file.write_text(json.dumps(config) + "\n")
            result = run_worker(config_file, output, args.worker_timeout)
            results.append(result)
            print("measured", name, "round", round, flush=True)
    research = {(r["research"]["rows"], r["research"]["checksum"]) for r in results}
    if len(research) != 1:
        raise AssertionError("research outputs differ across backends")
    started = time.perf_counter_ns()
    revised, revision_manifest, extension, extension_manifest = maintenance_inputs(
        source, manifest, root
    )
    recalculate_ns = time.perf_counter_ns() - started
    maintenance = {}
    for name in NAMES:
        backend = connect(name, root / name, manifest, binary, write=True)
        started = time.perf_counter_ns()
        for part, table in iter_parts(extension, extension_manifest):
            backend.append(part, table)
        backend.close()
        append_ns = time.perf_counter_ns() - started
        combined = {
            **manifest,
            "parts": manifest["parts"] + extension_manifest["parts"],
        }
        backend = connect(name, root / name, combined, binary)
        for part in extension_manifest["parts"]:
            q = {
                "symbol": part["symbol"],
                "start_ts": part["start_ts"],
                "end_ts": part["end_ts"],
                "kind": "range",
            }
            if part["kind"] != "bars":
                q["feature"] = part["kind"]
            if hash_rows(backend.query(q)) != hash_rows(
                query_rows(extension, extension_manifest, q)
            ):
                raise AssertionError("extension mismatch: " + name)
        backend.close()
        started = time.perf_counter_ns()
        replacement = connect(name, root / (name + "-v2"), revision_manifest, binary, write=True)
        replacement.setup(revised)
        replacement.close()
        revision_ns = time.perf_counter_ns() - started
        replacement = connect(name, root / (name + "-v2"), revision_manifest, binary)
        for part in revision_manifest["parts"]:
            q = {
                "symbol": part["symbol"],
                "start_ts": part["end_ts"],
                "end_ts": part["end_ts"],
                "kind": "point",
            }
            if part["kind"] != "bars":
                q["feature"] = part["kind"]
            if hash_rows(replacement.query(q)) != hash_rows(
                query_rows(revised, revision_manifest, q)
            ):
                raise AssertionError("revision mismatch: " + name)
        replacement.close()
        maintenance[name] = {
            "append_ns": append_ns,
            "new_version_full_rebuild_ns": revision_ns,
            "samples": 1,
            "after_append_storage": footprint(root / name),
            "new_version_storage": footprint(root / (name + "-v2")),
        }
        print("maintenance verified", name, flush=True)
    verify_inputs(source, manifest)
    grouped = {}
    for name in NAMES:
        runs = [r for r in results if r["backend"] == name]
        by_kind = defaultdict(list)
        for run in runs:
            for sample in run["samples"]:
                by_kind[sample["kind"]].append(sample["ns"])
        grouped[name] = {
            **setup[name],
            "maintenance": maintenance[name],
            "latencies": {
                kind: summarize(values, kind != "scan") for kind, values in by_kind.items()
            },
            "open": summarize([r["open_ns"] for r in runs], False),
            "research": summarize([r["research"]["ns"] for r in runs], False),
            "peak_worker_rss_bytes": max(r["memory"]["peak_rss_bytes"] for r in runs),
            "peak_worker_private_bytes": max(r["memory"]["peak_private_bytes"] for r in runs),
        }
    summary = {
        "environment": environment,
        "setup_order": setup_order,
        "round_orders": orders,
        "all_results_verified": True,
        "maintenance_input_recalculation_ns": recalculate_ns,
        "backends": grouped,
        "limitations": [
            "OS cold cache was not established; no cold-cache claim",
            "All main adapters are in-process and return packed NumPy int64; legacy JSON measurements are separate",
            "build, extension and version rebuild each have one sample; descriptive, not distributions",
            "5ms sampled memory can miss short peaks; process peaks are not added together",
            "research kernel is a deterministic integration workload, not a complete trading backtest",
            "historical corrections are measured as full new-version rebuilds for every backend; no sparse-update claim",
            "when prepared stores are reused, initial build times are omitted; fresh full-version rebuild costs are still measured",
        ],
    }
    (root / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
    lines = [
        "# External comparison",
        "",
        f"Profile: {args.profile}. All returned fields and research checksums verified.",
        "",
        "OS cache was uncontrolled. These are Python integration results, not Rust-only engine timings.",
        "",
        "| Backend | Build (s, one sample) | Bytes | Range median (ms) | Research median (s) | Peak Python worker RSS (MiB) |",
        "|---|---:|---:|---:|---:|---:|",
    ]
    for name, row in grouped.items():
        build = (
            f"{row['build_ns'] / 1e9:.4f}"
            if row["build_ns"] is not None
            else "not measured (reused)"
        )
        lines.append(
            f"| {name} | {build} | {row['storage']['total_bytes']} | {row['latencies'].get('range', {}).get('median_ns', 0) / 1e6:.4f} | {row['research']['median_ns'] / 1e9:.4f} | {row['peak_worker_rss_bytes'] / 2**20:.2f} |"
        )
    lines += [
        "",
        "| Backend | Later-month append (s) | Fresh full-version rebuild (s) |",
        "|---|---:|---:|",
    ]
    for name, row in grouped.items():
        lines.append(
            f"| {name} | {row['maintenance']['append_ns'] / 1e9:.4f} | {row['maintenance']['new_version_full_rebuild_ns'] / 1e9:.4f} |"
        )
    lines += ["", "Limitations:", *["- " + item for item in summary["limitations"]]]
    (root / "REPORT.md").write_text("\n".join(lines) + "\n")
    print(root / "REPORT.md")


if __name__ == "__main__":
    main()
