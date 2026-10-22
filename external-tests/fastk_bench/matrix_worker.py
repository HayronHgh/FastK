"""Read-only internal parallelism and outer query concurrency measurements."""

import hashlib
import json
import os
import random
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from .backends import connect
from .batches import columns_for, validate
from .data import hash_rows
from .process import announce


def checked(result, query):
    validate(result, columns_for(query))
    if hash_rows(result) != query["expected"]:
        raise AssertionError(f"canonical mismatch: {query}")


def measure(config):
    announce(config)
    manifest = json.loads(Path(config["manifest"]).read_text())
    queries = json.loads(Path(config["queries"]).read_text())
    samples = []
    groups = []
    budget = config["budget"]
    reader_ownership = "single"

    def open_backend(threads):
        if config.get("legacy_bridge"):
            from .legacy import LegacyJSON

            return LegacyJSON(config["store"], config["legacy_bridge"])
        return connect(config["backend"], config["store"], manifest, threads=threads)

    if config["mode"] == "internal":
        opened = time.perf_counter_ns()
        backend = open_backend(budget)
        open_ns = time.perf_counter_ns() - opened
        try:
            expected_bytes = []
            for query in queries:
                result = backend.query(query)
                checked(result, query)
                expected_bytes.append(hashlib.sha256(result).digest())
                del result
                for _ in range(31):
                    result = backend.query(query)
                    validate(result, columns_for(query))
                    if hashlib.sha256(result).digest() != expected_bytes[-1]:
                        raise AssertionError("warmup differs from canonical output")
                    del result
            for round_index in range(config["rounds"]):
                order = list(enumerate(queries))
                random.Random(20261021 + round_index).shuffle(order)
                for index, query in order:
                    start = time.perf_counter_ns()
                    result = backend.query(query)
                    elapsed = time.perf_counter_ns() - start
                    validate(result, columns_for(query))
                    if (
                        len(result) != query["expected"]["rows"]
                        or hashlib.sha256(result).digest() != expected_bytes[index]
                    ):
                        raise AssertionError("result differs from canonically verified warmup")
                    samples.append(
                        {"index": index, "case": query["case"], "round": round_index, "ns": elapsed}
                    )
                    del result
        finally:
            backend.close()
    else:
        # FastK shares one native Reader. SQL connections and other adapters remain
        # thread-owned; their connection rules differ from the native reader lease.
        rounds = sorted({query["round"] for query in queries})
        batches = [[query for query in queries if query["round"] == round_] for round_ in rounds]
        chunks = [[batch[i::budget] for batch in batches] for i in range(budget)]
        boundaries = []
        barrier = threading.Barrier(
            budget + 1, action=lambda: boundaries.append(time.perf_counter_ns())
        )

        shared = None
        if config["backend"] == "fastk" and not config.get("legacy_bridge"):
            shared = open_backend(1)
        reader_ownership = "shared" if shared is not None else "per_worker"

        def worker(chunks):
            backend = None
            try:
                backend = shared if shared is not None else open_backend(1)
                checked(backend.query(chunks[0][0]), chunks[0][0])
                local = []
                for chunk in chunks:
                    retained = []
                    result = None
                    barrier.wait()
                    for query in chunk:
                        start = time.perf_counter_ns()
                        result = backend.query(query)
                        elapsed = time.perf_counter_ns() - start
                        local.append({"case": query["case"], "ns": elapsed})
                        retained.append((result, query))
                    barrier.wait()
                    # Full validation is outside the measured group wall time.
                    for result, query in retained:
                        checked(result, query)
                return local
            except BaseException:
                barrier.abort()
                raise
            finally:
                if backend is not None and shared is None:
                    backend.close()

        try:
            with ThreadPoolExecutor(max_workers=budget) as pool:
                futures = [pool.submit(worker, chunk) for chunk in chunks]
                try:
                    for index, batch in enumerate(batches):
                        barrier.wait(timeout=120)
                        barrier.wait(timeout=config["timeout"])
                        elapsed = boundaries[index * 2 + 1] - boundaries[index * 2]
                        groups.append(
                            {"round": rounds[index], "queries": len(batch), "ns": elapsed}
                        )
                    for future in futures:
                        samples.extend(future.result())
                finally:
                    barrier.abort()
        finally:
            if shared is not None:
                shared.close()
    return {
        "backend": config["backend"],
        "mode": config["mode"],
        "budget": budget,
        "python_pid": os.getpid(),
        "samples": samples,
        "groups": groups,
        "open_ns": open_ns if config["mode"] == "internal" else None,
        "all_results_verified": True,
        "reader_ownership": reader_ownership,
    }


if __name__ == "__main__":
    config = json.loads(Path(sys.argv[1]).read_text())
    Path(config["output"]).write_text(json.dumps(measure(config), indent=2) + "\n")
