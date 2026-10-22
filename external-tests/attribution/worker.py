"""Phase attribution with unchanged production controls and canonical checks."""

import hashlib
import json
import os
import random
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import numpy as np
from fastk_bench.backends import connect
from fastk_bench.batches import columns_for, dtype, validate
from fastk_bench.data import hash_rows
from fastk_bench.process import announce

PHASES = [
    "reader_lock",
    "core",
    "flatten_allocate_copy_drop",
    "gil_reacquire",
    "dtype_lookup",
    "numpy_ownership_handoff",
    "base_readonly",
    "structured_view",
    "view_readonly",
    "instrumented_body",
    "record_capacity",
    "word_capacity",
    "word_length",
    "handoff_same_pointer",
    "detach_entry",
]


def usage():
    result = {"thread_cpu_ns": time.thread_time_ns()}
    if sys.platform == "linux":
        import resource

        value = resource.getrusage(resource.RUSAGE_THREAD)
        result.update(
            voluntary_switches=value.ru_nvcsw,
            involuntary_switches=value.ru_nivcsw,
            minor_faults=value.ru_minflt,
            major_faults=value.ru_majflt,
        )
    return result


class ArrowProbe:
    """Mirror the production IPC range path; extra clocks are diagnostic overhead."""

    def __init__(self, backend):
        self.backend = backend

    def query(self, q):
        import pyarrow as pa

        total = time.perf_counter_ns()
        step = total
        fields = columns_for(q)
        kind = q.get("feature", "bars")
        parts = [
            p
            for p in self.backend.parts.get((q["symbol"], kind), [])
            if p["end_ts"] >= q["start_ts"] and p["start_ts"] <= q["end_ts"]
        ]
        phases = {"partition_selection": time.perf_counter_ns() - step}
        mapping = search = 0
        pieces = []
        for part in parts:
            step = time.perf_counter_ns()
            path = str(self.backend.path(part))
            if path not in self.backend.ipc_batches:
                source = pa.memory_map(path, "r")
                table = pa.ipc.open_file(source).read_all()
                columns = {
                    name: table[name].chunk(0).to_numpy(zero_copy_only=True)
                    if table[name].num_chunks == 1
                    else table[name].to_numpy()
                    for name in table.column_names
                }
                self.backend.ipc_batches[path] = source, columns
                if len(self.backend.ipc_batches) > 64:
                    self.backend.ipc_batches.popitem(last=False)
            self.backend.ipc_batches.move_to_end(path)
            _, columns = self.backend.ipc_batches[path]
            mapping += time.perf_counter_ns() - step
            step = time.perf_counter_ns()
            start = np.searchsorted(columns["ts"], q["start_ts"], side="left")
            end = np.searchsorted(columns["ts"], q["end_ts"], side="right")
            pieces.append({name: columns[name][start:end] for name in fields})
            search += time.perf_counter_ns() - step
        phases.update(mapping_and_column_views=mapping, timestamp_search_and_slice=search)
        step = time.perf_counter_ns()
        result = np.empty(sum(len(p["ts"]) for p in pieces), dtype=dtype(fields))
        phases["numpy_allocation"] = time.perf_counter_ns() - step
        step = time.perf_counter_ns()
        offset = 0
        for piece in pieces:
            count = len(piece["ts"])
            for name in fields:
                result[name][offset : offset + count] = piece[name]
            offset += count
        phases["column_to_packed_copy"] = time.perf_counter_ns() - step
        step = time.perf_counter_ns()
        result.flags.writeable = False
        phases["readonly"] = time.perf_counter_ns() - step
        phases["instrumented_body"] = time.perf_counter_ns() - total
        return result, phases


def native_call(handle, q):
    if "feature" in q:
        return handle.read_scalar_range(
            q["symbol"], "1m", "feature", q["feature"], q["start_ts"], q["end_ts"]
        )
    return handle.read_kline_range(q["symbol"], "1m", q["start_ts"], q["end_ts"])


def open_route(config, manifest):
    route = config["route"]
    if route == "probe":
        from _fastk_attribution import ProbeReader

        reader = ProbeReader(config["store"], config.get("internal", 1))

        def call(q):
            batch, values = reader.read(q["symbol"], q["start_ts"], q["end_ts"], q.get("feature"))
            return batch, dict(zip(PHASES, values))

        return call, lambda: None
    if route in {"copy-batch", "numpy-copy"}:
        from _fastk_attribution import copy_batch
        from fastk_native import Reader

        reader = Reader(config["store"])
        cached = {q["case"]: native_call(reader, q) for q in config["bulk"]}
        reader.close()

        def call(q):
            batch = cached[q["case"]]
            if route == "numpy-copy":
                start = time.perf_counter_ns()
                result = batch.copy()
                duration = time.perf_counter_ns() - start
                result.flags.writeable = False
                return result, {"numpy_allocate_memcpy": duration}
            result, values = copy_batch(
                batch.view(np.int64), "SCALAR_DTYPE" if "feature" in q else "KLINE_DTYPE"
            )
            return result, dict(zip(PHASES, values))

        return call, lambda: None
    backend = connect(
        "arrow" if route.startswith("arrow") else "fastk",
        config["store"],
        manifest,
        threads=config.get("internal", 1),
    )
    if route == "arrow-probe":
        return ArrowProbe(backend).query, backend.close
    if route == "direct":
        return lambda q: (native_call(backend.handle, q), {}), backend.close
    return lambda q: (backend.query(q), {}), backend.close


def check(batch, q, expected=None):
    validate(batch, columns_for(q))
    if expected is None:
        if hash_rows(batch) != q["expected"]:
            raise AssertionError("canonical values differ")
    elif hashlib.sha256(batch).digest() != expected:
        raise AssertionError("raw batch differs from canonical warmup")


def bulk(config, manifest):
    queries = config["bulk"]
    call, close = open_route(config, manifest)
    expected = {}
    warmups = []
    samples = []
    try:
        for q in queries:
            result, phases = call(q)
            check(result, q)
            expected[q["case"]] = hashlib.sha256(result).digest()
            del result
            for _ in range(32):
                before = usage()
                start = time.perf_counter_ns()
                result, phases = call(q)
                elapsed = time.perf_counter_ns() - start
                after = usage()
                warmups.append(
                    {
                        "case": q["case"],
                        "ns": elapsed,
                        "phases": phases,
                        "usage": {k: after[k] - before[k] for k in before},
                    }
                )
                check(result, q, expected[q["case"]])
                del result
        for round_ in range(config["repetitions"]):
            order = queries.copy()
            random.Random(20261021 + round_).shuffle(order)
            for q in order:
                before = usage()
                start = time.perf_counter_ns()
                result, phases = call(q)
                elapsed = time.perf_counter_ns() - start
                after = usage()
                samples.append(
                    {
                        "case": q["case"],
                        "ns": elapsed,
                        "phases": phases,
                        "usage": {k: after[k] - before[k] for k in before},
                    }
                )
                check(result, q, expected[q["case"]])
                del result
    finally:
        close()
    return {"samples": samples, "warmups": warmups, "all_results_verified": True}


def outer(config, manifest):
    queries = json.loads(Path(config["short_queries"]).read_text())
    if config["route"].startswith("rust-"):
        from _fastk_attribution import core_parallel

        result = json.loads(
            core_parallel(
                config["store"],
                json.dumps(queries),
                config["workers"],
                config.get("shared", False),
                config["route"] == "rust-words",
            )
        )
        result["samples"] = [{"index": i, "ns": n} for i, n in result["samples"]]
        return result
    workers = config["workers"]
    boundaries = []
    barrier = threading.Barrier(
        workers + 1, action=lambda: boundaries.append(time.perf_counter_ns())
    )
    shared = open_route(config, manifest) if config.get("shared") else None

    def worker(index):
        if config["route"] == "gil-only":
            from _fastk_attribution import gil_roundtrip

            def call(q):
                return None, {"gil_reacquire": gil_roundtrip()}

            def close():
                return None
        else:
            call, close = shared or open_route(config, manifest)
        local = []
        usages = []
        try:
            first = queries[index]
            batch, _ = call(first)
            if batch is not None:
                check(batch, first)
            del batch
            for round_ in range(5):
                chunk = [q for q in queries if q["round"] == round_][index::workers]
                retained = []
                batch = None
                before = usage()
                barrier.wait(timeout=120)
                for q in chunk:
                    start = time.perf_counter_ns()
                    batch, phases = call(q)
                    elapsed = time.perf_counter_ns() - start
                    local.append({"case": "short", "ns": elapsed, "phases": phases})
                    retained.append((batch, q))
                barrier.wait(timeout=120)
                after = usage()
                usages.append({name: after[name] - before[name] for name in before})
                for batch, q in retained:
                    if batch is not None:
                        check(batch, q)
            return local, usages
        except BaseException:
            barrier.abort()
            raise
        finally:
            if shared is None:
                close()

    try:
        with ThreadPoolExecutor(max_workers=workers) as pool:
            futures = [pool.submit(worker, i) for i in range(workers)]
            for _ in range(5):
                barrier.wait(timeout=120)
                barrier.wait(timeout=120)
            samples, usages = [], []
            for future in futures:
                s, u = future.result()
                samples.extend(s)
                usages.extend(u)
    finally:
        if shared:
            shared[1]()
    walls = [boundaries[i + 1] - boundaries[i] for i in range(0, 10, 2)]
    return {
        "walls_ns": walls,
        "samples": samples,
        "timed_thread_usage": {key: sum(u[key] for u in usages) for key in usages[0]},
        "all_results_verified": config["route"] != "gil-only",
        "negative_control": config["route"] == "gil-only",
    }


def main():
    config = json.loads(Path(sys.argv[1]).read_text())
    announce(config)
    manifest = json.loads(Path(config["manifest"]).read_text())
    previous = sys.getswitchinterval()
    if config.get("switch_interval") is not None:
        sys.setswitchinterval(config["switch_interval"])
    try:
        result = bulk(config, manifest) if config["mode"] == "bulk" else outer(config, manifest)
        result.update(
            python_pid=os.getpid(),
            switch_interval=sys.getswitchinterval(),
            route=config["route"],
            workers=config.get("workers", 1),
            shared=config.get("shared", False),
        )
        Path(config["output"]).write_text(json.dumps(result, indent=2) + "\n")
    finally:
        sys.setswitchinterval(previous)


if __name__ == "__main__":
    main()
