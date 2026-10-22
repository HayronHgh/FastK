"""Paired scoped-client replay with fresh-process, external RSS/commit sampling."""

import argparse
import gzip
import hashlib
import json
import os
import platform
import random
import subprocess
import sys
import time
from pathlib import Path

import psutil
from benchmark_session import stats, validate
from fastk_bench.data import FIELDS, hash_rows
from fastk_client import Client
from fastk_client import client as client_module


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def memory_stats(values):
    return {
        key.removesuffix("_ns") + "_bytes" if key.endswith("_ns") else key: value
        for key, value in stats(values).items()
    }


def query(client, q, short):
    before = time.perf_counter_ns()
    if "feature" in q:
        response = client.read_scalar_range(
            q["symbol"], "1m", "feature", q["feature"], q["start_ts"], q["end_ts"]
        )
        fields = ["ts", "value"]
    else:
        response = client.read_kline_range(q["symbol"], "1m", q["start_ts"], q["end_ts"])
        fields = FIELDS
    returned = time.perf_counter_ns()
    if short:
        # Match the frozen short-replay adapter, retaining the returned dicts.
        rows = [tuple(record[field] for field in fields) for record in response["records"]]
    else:
        records = response.pop("records")
        rows = [tuple(record[field] for field in fields) for record in records]
        del records
    converted = time.perf_counter_ns()
    if short:
        validate(response, rows, q)
    else:
        assert hash_rows(rows) == q["expected"]
        assert response["symbol"] == q["symbol"] and response["timeframe_ms"] == 60000
        assert response["timeframe"] == "1m"
        assert (response["start_ts"], response["end_ts"]) == (q["start_ts"], q["end_ts"])
        if "feature" in q:
            assert response["category"] == "feature" and response["name"] == q["feature"]
        else:
            assert response["price_scale"] == response["volume_scale"] == 100_000_000
        assert all(type(value) is int for row in rows for value in row)
    return {
        "client_ns": returned - before,
        "total_ns": converted - before,
        "adapter_ns": converted - returned,
        "query": q,
    }


def child(config_path):
    config = json.loads(config_path.read_text())
    assert Path(client_module.__file__).resolve().parent.parent == Path(config["client_src"])
    with Client(config["store"], binary=config["bridge"], timeout=30) as client:
        worker = client._session.process
        print(json.dumps({"worker_pid": worker.pid, "python_pid": os.getpid()}), flush=True)
        assert sys.stdin.readline().strip() == "go"
        if config["mode"] == "bulk":
            query(client, config["queries"][0], False)  # Untimed validated warm-up.
        results = [query(client, q, config["mode"] == "short") for q in config["queries"]]
        assert worker.poll() is None
        Path(config["result"]).write_text(
            json.dumps(
                {
                    "all_results_verified": True,
                    "samples": results,
                    "client_source_sha256": {
                        p.name: sha(p) for p in Path(client_module.__file__).parent.glob("*.py")
                    },
                },
                indent=2,
            )
            + "\n"
        )
        # Let the independent sampler read OS high-water counters before exit.
        assert sys.stdin.readline().strip() == "stop"
    assert worker.poll() is not None


def run_job(config_path, result_path):
    config = json.loads(config_path.read_text())
    env = os.environ.copy()
    env["PYTHONPATH"] = config["client_src"]
    interval = 0.005
    worker = None
    with result_path.with_suffix(".stderr.log").open("wb") as log:
        process = subprocess.Popen(
            [sys.executable, str(Path(__file__).resolve()), "--child", str(config_path)],
            env=env,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=log,
            text=True,
            encoding="utf-8",
        )
        try:
            ready = process.stdout.readline()
            if not ready:
                raise RuntimeError("benchmark child failed before ready; see " + str(log.name))
            ready = json.loads(ready)
            worker = psutil.Process(ready["worker_pid"])
            parent = psutil.Process(ready["python_pid"])
            # Windows venv launchers may forward execution to a different PID.
            assert parent.pid == process.pid or process.pid in {p.pid for p in parent.parents()}
            launcher = psutil.Process(process.pid) if parent.pid != process.pid else None
            peak_rss = peak_private = peak_parent = peak_worker = count = 0
            peak_parent_os = peak_worker_os = 0

            def sample():
                nonlocal \
                    peak_rss, \
                    peak_private, \
                    peak_parent, \
                    peak_worker, \
                    count, \
                    peak_parent_os, \
                    peak_worker_os
                p, w = parent.memory_info(), worker.memory_info()
                extra = launcher.memory_info() if launcher is not None else None
                tree_rss = p.rss + w.rss + (extra.rss if extra else 0)
                tree_private = p.private + w.private + (extra.private if extra else 0)
                peak_rss = max(peak_rss, tree_rss)
                peak_private = max(peak_private, tree_private)
                peak_parent, peak_worker = max(peak_parent, p.rss), max(peak_worker, w.rss)
                peak_parent_os = max(peak_parent_os, p.peak_wset)
                peak_worker_os = max(peak_worker_os, w.peak_wset)
                count += 1
                return {"rss": tree_rss, "private": tree_private}

            initial = sample()
            process.stdin.write("go\n")
            process.stdin.flush()
            deadline = time.monotonic() + 180
            while not result_path.exists():
                if process.poll() is not None:
                    raise RuntimeError("benchmark child failed; see " + str(log.name))
                if time.monotonic() > deadline:
                    raise TimeoutError("benchmark job exceeded 180s")
                sample()
                time.sleep(interval)
            sample()
            process.stdin.write("stop\n")
            process.stdin.flush()
            if process.wait(timeout=10):
                raise RuntimeError("benchmark cleanup failed; see " + str(log.name))
            result = json.loads(result_path.read_text())
            assert result["all_results_verified"] and not worker.is_running()
            result["memory"] = {
                "initial_tree_bytes": initial,
                "sampled_tree_peak_rss_bytes": peak_rss,
                "sampled_tree_peak_private_bytes": peak_private,
                "sampled_python_peak_rss_bytes": peak_parent,
                "sampled_worker_peak_rss_bytes": peak_worker,
                "os_python_peak_working_set_bytes": peak_parent_os,
                "os_worker_peak_working_set_bytes": peak_worker_os,
                "samples": count,
                "interval_ms": 5,
                "all_children_reaped": True,
                "python_pid": parent.pid,
                "worker_pid": worker.pid,
                "launcher_pid": launcher.pid if launcher is not None else None,
            }
            result_path.write_text(json.dumps(result, indent=2) + "\n")
            return result
        finally:
            if worker is not None and worker.is_running():
                worker.kill()
                worker.wait(timeout=5)
            if process.poll() is None:
                process.kill()
            process.wait()
            for stream in [process.stdin, process.stdout]:
                stream.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--child", type=Path)
    for name in [
        "baseline-client",
        "baseline-bridge",
        "client",
        "bridge",
        "store",
        "bulk-queries",
        "prior-results",
        "output",
    ]:
        parser.add_argument("--" + name, type=Path)
    parser.add_argument("--bulk-rounds", type=int, default=10)
    args = parser.parse_args()
    if args.child:
        child(args.child)
        return
    for name in [
        "baseline_client",
        "baseline_bridge",
        "client",
        "bridge",
        "store",
        "bulk_queries",
        "prior_results",
        "output",
    ]:
        if getattr(args, name) is None:
            parser.error("missing --" + name.replace("_", "-"))
    if os.name != "nt":
        parser.error("This memory sampler uses Windows peak working-set/private counters")
    if args.bulk_rounds < 1:
        parser.error("bulk rounds must be positive")
    args.output.mkdir(parents=True, exist_ok=False)
    for directory in ["configs", "jobs"]:
        (args.output / directory).mkdir()
    routes = {
        "before": (args.baseline_client.resolve(), args.baseline_bridge.resolve()),
        "after": (args.client.resolve(), args.bridge.resolve()),
    }
    queries = json.loads(args.bulk_queries.read_text())
    query_files = [args.prior_results / f"queries-{i}.json" for i in range(5)]
    batches = [
        [
            q
            for q in json.loads(
                p.read_bytes()
                if p.exists()
                else gzip.decompress(p.with_suffix(".json.gz").read_bytes())
            )
            if q["kind"] == "range"
        ]
        for p in query_files
    ]
    assert [len(batch) for batch in batches] == [286] * 5
    jobs = []
    for round_index in range(args.bulk_rounds):
        for label, q in queries.items():
            jobs.extend(
                {
                    "route": route,
                    "mode": "bulk",
                    "case": label,
                    "round": round_index,
                    "queries": [q],
                }
                for route in routes
            )
    for round_index, batch in enumerate(batches):
        jobs.extend(
            {
                "route": route,
                "mode": "short",
                "case": "range",
                "round": round_index,
                "queries": batch,
            }
            for route in routes
        )
    random.Random(20261023).shuffle(jobs)
    completed = []
    for index, job in enumerate(jobs):
        label = f"{index:03d}-{job['route']}-{job['mode']}-{job['case']}-{job['round']}"
        result_path = (args.output / "jobs" / (label + ".json")).resolve()
        client_source, binary = routes[job["route"]]
        config = dict(
            job,
            client_src=str(client_source),
            bridge=str(binary),
            store=str(args.store.resolve()),
            result=str(result_path),
        )
        config_path = (args.output / "configs" / (label + ".json")).resolve()
        config_path.write_text(json.dumps(config, indent=2) + "\n")
        result = run_job(config_path, result_path)
        completed.append(dict(job, result=result))
        print(f"{index + 1}/{len(jobs)} {label} verified={len(result['samples'])}", flush=True)
    groups = {}
    for route in routes:
        groups[route] = {}
        for case in [*queries, "range"]:
            runs = [j for j in completed if j["route"] == route and j["case"] == case]
            samples = [s for j in runs for s in j["result"]["samples"]]
            groups[route][case] = {
                "latency": {
                    key: stats([s[key] for s in samples])
                    for key in ["client_ns", "total_ns", "adapter_ns"]
                },
                "memory": {
                    key: memory_stats([j["result"]["memory"][key] for j in runs])
                    for key in [
                        "sampled_tree_peak_rss_bytes",
                        "sampled_tree_peak_private_bytes",
                        "os_python_peak_working_set_bytes",
                        "os_worker_peak_working_set_bytes",
                    ]
                },
                "runs": len(runs),
            }
    summary = {
        "all_results_verified": True,
        "all_children_reaped": True,
        "groups": groups,
        "short_queries_per_route": 1430,
        "bulk_queries_per_route": args.bulk_rounds * len(queries),
        "jobs": len(jobs),
        "memory_stats_units": "bytes",
    }
    environment = {
        "os": platform.platform(),
        "python": platform.python_version(),
        "psutil": psutil.__version__,
        "seed": 20261023,
        "routes": {
            name: {
                "bridge_sha256": sha(binary),
                "client_source": str(source),
                "client_hashes": {p.name: sha(p) for p in (source / "fastk_client").glob("*.py")},
            }
            for name, (source, binary) in routes.items()
        },
        "bulk_query_sha256": sha(args.bulk_queries),
        "cache": "OS cache uncontrolled, same pre-maintenance store, shuffled fresh-process jobs; no concurrent compile/install",
        "memory_scope": "external coordinator samples actual child-reported Python PID + Rust PID + venv launcher if present every5ms through warm-up, timed reads, caller mapping, validation and cleanup; excludes coordinator. OS per-process peaks include startup and are never summed as simultaneous peaks. Shared RSS pages may count twice.",
        "latency_scope": "public Client API and original caller tuple conversion separately; constructor/handshake/warm-up/validation excluded. Fresh Python+Rust processes per shape/round/route.",
    }
    for name, value in [("summary", summary), ("environment", environment)]:
        (args.output / f"{name}.json").write_text(json.dumps(value, indent=2) + "\n")
    print(args.output / "summary.json", flush=True)


if __name__ == "__main__":
    main()
