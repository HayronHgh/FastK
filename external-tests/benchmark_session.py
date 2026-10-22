"""Replay the frozen range workload through one-shot and scoped Python clients."""

import argparse
import gzip
import hashlib
import json
import math
import platform
import random
import subprocess
import time
from collections import defaultdict
from pathlib import Path
from unittest.mock import patch

from fastk_bench.data import FIELDS, hash_rows
from fastk_client import Client
from fastk_client import client as client_module


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def stats(values):
    values = sorted(values)
    result = {
        "samples": len(values),
        "mean_ns": sum(values) / len(values),
        "median_ns": values[math.ceil(len(values) * 0.5) - 1],
        "min_ns": values[0],
        "max_ns": values[-1],
    }
    if len(values) >= 100:
        result.update(
            p95_ns=values[math.ceil(len(values) * 0.95) - 1],
            p99_ns=values[math.ceil(len(values) * 0.99) - 1],
        )
    return result


def validate(response, rows, query):
    assert hash_rows(rows) == query["expected"]
    expected = {
        "symbol": query["symbol"],
        "timeframe": "1m",
        "timeframe_ms": 60_000,
        "start_ts": query["start_ts"],
        "end_ts": query["end_ts"],
        "price_scale": 100_000_000,
        "volume_scale": 100_000_000,
    }
    assert set(response) == set(expected) | {"records"}
    assert {key: response[key] for key in expected} == expected
    assert all(type(response[key]) is int for key in expected if key not in {"symbol", "timeframe"})
    assert all(set(record) == set(FIELDS) for record in response["records"])
    assert all(type(value) is int for row in rows for value in row)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline", type=Path, required=True)
    parser.add_argument("--bridge", type=Path, required=True)
    parser.add_argument("--store", type=Path, required=True)
    parser.add_argument("--prior-results", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=False)
    baseline, bridge, store = args.baseline.resolve(), args.bridge.resolve(), args.store.resolve()
    query_files = [args.prior_results / f"queries-{i}.json" for i in range(5)]
    query_files = [path if path.exists() else path.with_suffix(".json.gz") for path in query_files]
    batches = [
        [
            q
            for q in json.loads(
                gzip.decompress(path.read_bytes()) if path.suffix == ".gz" else path.read_bytes()
            )
            if q["kind"] == "range"
        ]
        for path in query_files
    ]
    assert [len(batch) for batch in batches] == [286] * 5
    routes = {"baseline_one_shot": baseline, "new_one_shot": bridge, "session": bridge}
    original_popen = subprocess.Popen
    rounds, samples, byte_checks = [], [], []
    rng = random.Random(20261020)
    for round_index, queries in enumerate(batches):
        # Check that refactoring the CLI has not changed its success bytes.
        q = queries[0]
        command = [
            "read-kline-range",
            "--root",
            str(store),
            "--symbol",
            q["symbol"],
            "--timeframe",
            "1m",
            "--start-ts",
            str(q["start_ts"]),
            "--end-ts",
            str(q["end_ts"]),
        ]
        payloads = [
            subprocess.run(
                [str(binary), *command], capture_output=True, check=True, timeout=30
            ).stdout
            for binary in [baseline, bridge]
        ]
        assert payloads[0] == payloads[1]
        byte_checks.append(
            {
                "round": round_index,
                "bytes": len(payloads[0]),
                "sha256": hashlib.sha256(payloads[0]).hexdigest(),
            }
        )
        order = list(routes)
        rng.shuffle(order)
        for route in order:
            children = []

            def launch(*popen_args, **kwargs):
                child = original_popen(*popen_args, **kwargs)
                children.append(child)
                return child

            with patch.object(subprocess, "Popen", launch):
                started = time.perf_counter_ns()
                client = Client(store, binary=routes[route], timeout=30)
                constructed = time.perf_counter_ns()
                if route == "session":
                    client.__enter__()
                entered = time.perf_counter_ns()
                try:
                    for index, q in enumerate(queries):
                        before = time.perf_counter_ns()
                        response = client.read_kline_range(
                            q["symbol"], "1m", q["start_ts"], q["end_ts"]
                        )
                        rows = [
                            tuple(record[field] for field in FIELDS)
                            for record in response["records"]
                        ]
                        after = time.perf_counter_ns()
                        if index == 0:
                            first_result_ns = after - started
                        validate(response, rows, q)  # Outside the timed operation.
                        samples.append(
                            {
                                "route": route,
                                "round": round_index,
                                "index": index,
                                "total_ns": after - before,
                                "query": q,
                            }
                        )
                finally:
                    client.close()
                expected_children = 2 if route == "session" else 1 + len(queries)
                assert len(children) == expected_children
                assert all(child.poll() is not None for child in children)
                rounds.append(
                    {
                        "route": route,
                        "round": round_index,
                        "route_order": order,
                        "constructor_ns": constructed - started,
                        "context_entry_ns": entered - constructed,
                        "first_result_from_constructor_ns": first_result_ns,
                        "processes_started": len(children),
                        "queries": len(queries),
                        "worker_pid": children[-1].pid if route == "session" else None,
                        "all_children_reaped": True,
                    }
                )
            print(
                f"round={round_index} route={route} verified={len(queries)} processes={len(children)}",
                flush=True,
            )
    grouped = defaultdict(list)
    for sample in samples:
        grouped[sample["route"]].append(sample["total_ns"])
    result = {
        "all_results_verified": True,
        "one_shot_bytes_identical": byte_checks,
        "queries_per_route": sum(map(len, batches)),
        "groups": {route: stats(values) for route, values in grouped.items()},
        "startup": {
            route: {
                key: stats([r[key] for r in rounds if r["route"] == route])
                for key in [
                    "constructor_ns",
                    "context_entry_ns",
                    "first_result_from_constructor_ns",
                ]
            }
            for route in routes
        },
        "processes_started": {
            route: sum(r["processes_started"] for r in rounds if r["route"] == route)
            for route in routes
        },
        "rounds": rounds,
    }
    environment = {
        "os": platform.platform(),
        "python": platform.python_version(),
        "baseline_sha256": sha(baseline),
        "bridge_sha256": sha(bridge),
        "client_sources": {
            path.name: sha(path) for path in Path(client_module.__file__).parent.glob("*.py")
        },
        "query_files": {path.name: sha(path) for path in query_files},
        "store": str(store),
        "seed": 20261020,
        "cache": "OS cache uncontrolled; same pre-maintenance store; randomized route order each round",
        "timed_scope": "Client read plus original ordered tuple adapter; validation/hash excluded",
        "startup_scope": "constructor version subprocess and context handshake reported separately; first-result includes both",
        "limits": "Short ranges only; no full-research or bulk JSON improvement claimed",
    }
    for name, value in [("summary", result), ("environment", environment), ("samples", samples)]:
        (args.output / f"{name}.json").write_text(json.dumps(value, indent=2) + "\n")
    print(json.dumps(result["groups"], indent=2))


if __name__ == "__main__":
    main()
