"""Fresh-process measurements; result validation is outside each timed query."""

import argparse
import json
import os
import time
from pathlib import Path

import numpy as np

from .backends import connect
from .batches import columns_for, exact_sum, validate
from .data import FEATURES, hash_rows
from .process import announce


def measure(config):
    announce(config)
    manifest = json.loads(Path(config["manifest"]).read_text())
    queries = json.loads(Path(config["queries"]).read_text())
    started = time.perf_counter_ns()
    backend = connect(
        config["backend"],
        config["store"],
        manifest,
        config.get("binary"),
        threads=config.get("threads", 1),
    )
    open_ns = time.perf_counter_ns() - started
    samples = []
    # First query is separately labelled, then an untimed prewarm precedes the mix.
    first = queries[0]
    started = time.perf_counter_ns()
    result = backend.query(first)
    first_ns = time.perf_counter_ns() - started
    validate(result, columns_for(first))
    if hash_rows(result) != first["expected"]:
        raise AssertionError("first-touch mismatch")
    del result
    for query in queries:
        started = time.perf_counter_ns()
        result = backend.query(query)
        elapsed = time.perf_counter_ns() - started
        validate(result, columns_for(query))
        observed = hash_rows(result)
        if observed != query["expected"]:
            raise AssertionError(f"result mismatch: {query}")
        samples.append({"kind": query["kind"], "ns": elapsed, "result": observed})
        del result
    # Identical caller computation and data requests on every backend.
    symbols = sorted({part["symbol"] for part in manifest["parts"]})
    workload_start = time.perf_counter_ns()
    checksum = 0
    loaded = 0
    for symbol in symbols:
        parts = [
            part
            for part in manifest["parts"]
            if part["symbol"] == symbol and part["kind"] == "bars"
        ]
        bounds = {
            "symbol": symbol,
            "start_ts": min(p["start_ts"] for p in parts),
            "end_ts": max(p["end_ts"] for p in parts),
            "kind": "scan",
        }
        bars = backend.query(bounds)
        indicators = {name: backend.query(dict(bounds, feature=name)) for name in FEATURES}
        for values in indicators.values():
            if not np.array_equal(values["ts"], bars["ts"]):
                raise AssertionError("feature alignment mismatch")
            checksum += exact_sum(values["value"])
        # Use Python integer arithmetic for potentially overflowing price differences.
        checksum += sum(
            (int(close) - int(open_)) * (1 if close >= average else -1)
            for close, open_, average in zip(
                bars["close"], bars["open"], indicators["ma20"]["value"]
            )
        )
        loaded += len(bars)
    workload_ns = time.perf_counter_ns() - workload_start
    backend.close()
    return {
        "backend": config["backend"],
        "python_pid": os.getpid(),
        "threads": config.get("threads", 1),
        "delivery": "packed-readonly-numpy-int64",
        "round": config["round"],
        "open_ns": open_ns,
        "first_query_ns": first_ns,
        "samples": samples,
        "research": {"ns": workload_ns, "rows": loaded, "checksum": str(checksum)},
        "cache_state": "OS cache uncontrolled; fresh worker per round, first touch separate; all backends reuse in-process handles",
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("config", type=Path)
    args = parser.parse_args()
    config = json.loads(args.config.read_text())
    result = measure(config)
    Path(config["output"]).write_text(json.dumps(result, indent=2) + "\n")
