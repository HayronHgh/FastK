"""Separate hot hits, working-set replay, and open/read/close request cycles."""

import hashlib
import importlib
import inspect
import json
import os
import random
import sys
import textwrap
import time
from pathlib import Path

from fastk_bench import backends
from fastk_bench.batches import columns_for, validate
from fastk_bench.matrix_worker import checked
from fastk_bench.process import announce


def measure(config):
    announce(config)
    name = config["backend"]
    # Imports belong to the resident Python process, not to an open Reader.
    importlib.import_module(
        {
            "fastk": "fastk_native",
            "arrow": "pyarrow.dataset",
            "parquet": "pyarrow.dataset",
            "duckdb": "duckdb",
            "sqlite": "sqlite3",
        }[name]
    )
    native_sha = None
    if name == "fastk":
        native = importlib.import_module("fastk_native._native")
        native_sha = hashlib.sha256(Path(native.__file__).read_bytes()).hexdigest()
        assert native_sha == config["native_sha256"]
    patched = None
    if name == "arrow" and config.get("arrow_capacity", 64) != 64:
        original = textwrap.dedent(inspect.getsource(backends.Files.query_ipc))
        needle = "if len(self.ipc_batches) > 64:"
        assert original.count(needle) == 1
        patched = original.replace(
            needle, f"if len(self.ipc_batches) > {config['arrow_capacity']}:"
        )
        namespace = vars(backends).copy()
        exec(compile(patched, "<isolated-arrow-cache-cap>", "exec"), namespace)
        backends.Files.query_ipc = namespace["query_ipc"]
    catalog = json.loads(Path(config["manifest"]).read_text())
    queries = json.loads(Path(config["queries"]).read_text())
    expected = {}

    def verify(result, index):
        query = queries[index]
        validate(result, columns_for(query))
        digest = hashlib.sha256(result).digest()
        if index not in expected:
            checked(result, query)
            expected[index] = digest
        assert len(result) == query["expected"]["rows"] and digest == expected[index]

    def connect():
        manifest = (
            json.loads(Path(config["manifest"]).read_text())
            if config.get("reload_catalog")
            else catalog
        )
        return backends.connect(name, config["store"], manifest, threads=1)

    mode = config["mode"]
    samples = []
    if mode.startswith("hot"):
        backend = connect()
        try:
            for index, query in enumerate(queries):
                for _ in range(32):
                    result = backend.query(query)
                    verify(result, index)
                    del result
                if mode == "hot-local":
                    start = time.perf_counter_ns()
                    result = backend.query(query)
                    elapsed = time.perf_counter_ns() - start
                    verify(result, index)
                    samples.append(dict(index=index, case=query["case"], read_ns=elapsed))
                    del result
            if mode == "hot-replay":
                order = list(range(len(queries)))
                random.Random(20261021).shuffle(order)
                for index in order:
                    start = time.perf_counter_ns()
                    result = backend.query(queries[index])
                    elapsed = time.perf_counter_ns() - start
                    verify(result, index)
                    samples.append(dict(index=index, case=queries[index]["case"], read_ns=elapsed))
                    del result
        finally:
            backend.close()
    else:
        for round_index in range(config["repetitions"]):
            order = list(range(len(queries)))
            random.Random(20261021 + round_index).shuffle(order)
            for index in order:
                start = time.perf_counter_ns()
                backend = connect()
                opened = time.perf_counter_ns()
                try:
                    result = backend.query(queries[index])
                    read = time.perf_counter_ns()
                finally:
                    backend.close()
                    del backend
                closed = time.perf_counter_ns()
                # Results must remain usable after all request-owned handles close.
                verify(result, index)
                samples.append(
                    dict(
                        index=index,
                        case=queries[index]["case"],
                        round=round_index,
                        open_ns=opened - start,
                        read_ns=read - opened,
                        close_ns=closed - read,
                        total_ns=closed - start,
                    )
                )
                del result
    return dict(
        backend=name,
        mode=mode,
        python_pid=os.getpid(),
        samples=samples,
        native_sha256=native_sha,
        all_results_verified=True,
        arrow_capacity=config.get("arrow_capacity", 64),
        patched_arrow_function_sha256=hashlib.sha256(patched.encode()).hexdigest()
        if patched
        else None,
        catalog_policy="reload_each_open" if config.get("reload_catalog") else "caller_resident",
        limits=[
            "OS cache uncontrolled; a fresh handle is not a cold disk",
            "Libraries imported before timers; Python process remains resident",
            "Cycle close includes adapter.close and destruction; validation and output destruction excluded",
        ],
    )


if __name__ == "__main__":
    config = json.loads(Path(sys.argv[1]).read_text())
    Path(config["output"]).write_text(json.dumps(measure(config), indent=2) + "\n")
