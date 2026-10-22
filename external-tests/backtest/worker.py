"""One isolated process: identical application, only the storage adapter varies."""

import hashlib
import importlib
import json
import os
import time
from pathlib import Path

import numpy as np
from fastk_bench import backends
from fastk_bench.batches import from_table, validate
from fastk_bench.data import FIELDS, read_table
from fastk_bench.process import announce

from .engine import Backtest


def schedule(manifest, mode, symbols, months):
    parts = [p for p in manifest["parts"] if p["kind"] == "bars" and p["symbol"] in symbols]
    chosen = sorted({p["month"] for p in parts})[:months]
    parts = [p for p in parts if p["month"] in chosen]
    groups = [parts] if mode == "bulk" else [[p for p in parts if p["month"] == m] for m in chosen]
    batches = []
    for group in groups:
        queries = []
        for symbol in sorted(symbols):
            selected = [p for p in group if p["symbol"] == symbol]
            if not selected:
                raise ValueError("missing symbol/month")
            queries.append(
                {
                    "symbol": symbol,
                    "kind": "range",
                    "case": "backtest",
                    "start_ts": min(p["start_ts"] for p in selected),
                    "end_ts": max(p["end_ts"] for p in selected),
                }
            )
        batches.append(queries)
    if not batches:
        raise ValueError("empty schedule")
    return batches


def canonical(source, manifest, symbols, months):
    groups = schedule(manifest, "bulk", symbols, months)[0]
    result = {}
    for q in groups:
        parts = sorted(
            (
                p
                for p in manifest["parts"]
                if p["kind"] == "bars"
                and p["symbol"] == q["symbol"]
                and p["start_ts"] <= q["end_ts"]
                and p["end_ts"] >= q["start_ts"]
            ),
            key=lambda p: p["start_ts"],
        )
        frames = [from_table(read_table(Path(source) / p["path"]), FIELDS) for p in parts]
        joined = np.concatenate(frames)
        joined.flags.writeable = False
        validate(joined, FIELDS)
        result[q["symbol"]] = joined
    return result


def select_memory(memory, queries):
    return {
        q["symbol"]: memory[q["symbol"]][
            np.searchsorted(
                memory[q["symbol"]]["ts"], q["start_ts"], side="left"
            ) : np.searchsorted(memory[q["symbol"]]["ts"], q["end_ts"], side="right")
        ]
        for q in queries
    }


def read_batch(name, store, manifest, queries):
    """Always close on success or failure. Never retry a query."""
    start = time.perf_counter_ns()
    backend = backends.connect(name, store, manifest, threads=1)
    opened = time.perf_counter_ns()
    try:
        frames = {q["symbol"]: backend.query(q) for q in queries}
        read = time.perf_counter_ns()
    finally:
        backend.close()
        del backend
    closed = time.perf_counter_ns()
    return frames, {"open_ns": opened - start, "read_ns": read - opened, "close_ns": closed - read}


def bootstrap(config):
    name = config["backend"]
    importlib.import_module(
        {
            "fastk": "fastk_native",
            "sqlite": "sqlite3",
            "duckdb": "duckdb",
            "parquet": "pyarrow.dataset",
            "arrow": "pyarrow.dataset",
            "memory": "numpy",
        }[name]
    )
    if name == "fastk":
        native = importlib.import_module("fastk_native._native")
        actual = hashlib.sha256(Path(native.__file__).read_bytes()).hexdigest()
        if actual != config["native_sha256"]:
            raise AssertionError("native artifact changed")


def preflight(config):
    manifest = json.loads(Path(config["manifest"]).read_text())
    expected = canonical(config["source"], manifest, config["symbols"], config["months"])
    checks = []
    for mode in ("monthly", "bulk"):
        for queries in schedule(manifest, mode, config["symbols"], config["months"]):
            frames, _ = read_batch(config["backend"], config["store"], manifest, queries)
            oracle = select_memory(expected, queries)
            for symbol, bars in frames.items():
                validate(bars, FIELDS)
                digest = hashlib.sha256(bars).hexdigest()
                assert digest == hashlib.sha256(oracle[symbol]).hexdigest()
                assert len(bars) == len(oracle[symbol])
                checks.append({"mode": mode, "symbol": symbol, "rows": len(bars), "sha256": digest})
            del frames, oracle
    return {
        "backend": config["backend"],
        "python_pid": os.getpid(),
        "checks": checks,
        "all_results_verified": True,
    }


def measure(config):
    bootstrap(config)
    if config.get("preflight"):
        return preflight(config)
    memory = None
    if config["backend"] == "memory":
        # Explicit resident-data control; persistence/loading cost is excluded.
        manifest = json.loads(Path(config["manifest"]).read_text())
        memory = canonical(config["source"], manifest, config["symbols"], config["months"])
    start = time.perf_counter_ns()
    manifest = json.loads(Path(config["manifest"]).read_text())
    batches = schedule(manifest, config["mode"], config["symbols"], config["months"])
    engine = Backtest(config["case"], config["symbols"], manifest["scale"])
    ready = time.perf_counter_ns()
    samples = []
    first_data_ns = None
    for index, queries in enumerate(batches):
        begin = time.perf_counter_ns()
        if memory is None:
            frames, phases = read_batch(config["backend"], config["store"], manifest, queries)
        else:
            frames = select_memory(memory, queries)
            phases = {"open_ns": 0, "read_ns": time.perf_counter_ns() - begin, "close_ns": 0}
        loaded = time.perf_counter_ns()
        if first_data_ns is None:
            first_data_ns = loaded - start
        engine.consume(frames)
        computed = time.perf_counter_ns()
        del frames
        released = time.perf_counter_ns()
        samples.append(
            {
                "batch": index,
                **phases,
                "load_ns": loaded - begin,
                "compute_ns": computed - loaded,
                "release_ns": released - computed,
            }
        )
    finishing = time.perf_counter_ns()
    outcome = engine.result()
    del engine
    done = time.perf_counter_ns()
    result = {
        "backend": config["backend"],
        "case": config["case"],
        "mode": config["mode"],
        "python_pid": os.getpid(),
        "total_ns": done - start,
        "first_data_ns": first_data_ns,
        "setup_ns": ready - start,
        "finalize_ns": done - finishing,
        "samples": samples,
        "outcome": outcome,
        "queries": sum(map(len, batches)),
        "opens": 0 if memory is not None else len(batches),
        "resident_control_bytes": sum(a.nbytes for a in memory.values()) if memory else 0,
    }
    if memory is not None:
        # Resident input belongs to the control, not storage-backed application cleanup.
        del memory
    return result


if __name__ == "__main__":
    import sys

    config = json.loads(Path(sys.argv[1]).read_text())
    announce(config)
    result = measure(config)
    Path(config["output"]).write_text(json.dumps(result, indent=2, allow_nan=False) + "\n")
