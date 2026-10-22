"""Observe compact response decoding and caller mapping; never patch shipped sources."""

import argparse
import gc
import importlib
import json
import platform
import random
import sys
import time
from collections import defaultdict
from contextlib import ExitStack
from operator import itemgetter
from pathlib import Path
from types import SimpleNamespace

import measure_session_json as measured
import orjson
from benchmark_session import stats
from fastk_bench.data import FIELDS, hash_rows
from fastk_client import Client, _session


def split_decoder(trace):
    def loads(data):
        start = measured.ticks()
        encoding = json.detect_encoding(data)
        detected = measured.ticks()
        text = data.decode(encoding, "surrogatepass")
        decoded = measured.ticks()
        value = json.loads(text)
        parsed = measured.ticks()
        del text
        end = measured.ticks()
        trace.update(
            decode_start_tick=start,
            decode_end_tick=end,
            wire_bytes=len(data),
            id=value.get("id"),
            decode_parts={
                "encoding_detection": measured.ns(detected - start),
                "utf8_decode": measured.ns(decoded - detected),
                "json_parse_and_objects": measured.ns(parsed - decoded),
                "decoded_text_release": measured.ns(end - parsed),
            },
        )
        if trace.pop("capture_raw", False):
            trace["raw"] = data
        return value

    return SimpleNamespace(loads=loads, dumps=json.dumps)


def assert_tree(actual, expected):
    """Value equality alone would silently accept int/float/bool substitutions."""
    assert type(actual) is type(expected), (type(actual), type(expected))
    if isinstance(expected, dict):
        assert list(actual) == list(expected)
        for key in expected:
            assert_tree(actual[key], expected[key])
    elif isinstance(expected, list):
        assert len(actual) == len(expected)
        for a, e in zip(actual, expected):
            assert_tree(a, e)
    else:
        assert actual == expected


def compatibility():
    common = {
        "id": 2**64 - 1,
        "ok": True,
        "result": {
            "symbol": '中文\n"quoted"\u0000',
            "records": [
                {"ts": -(2**63), "value": 2**63 - 1},
                {"ts": 2**53 + 1, "value": -(2**53 + 1)},
            ],
            "empty": [],
            "optional": None,
        },
    }
    raw = json.dumps(common, ensure_ascii=False, separators=(",", ":")).encode()
    for data in [raw, bytearray(raw), raw.decode()]:
        assert_tree(orjson.loads(data), common)
    assert issubclass(orjson.JSONDecodeError, ValueError)
    cases = {
        "truncated": b'{"value":',
        "invalid_utf8": b'{"value":"\xff"}',
        "nonfinite": b'{"value":NaN}',
        "unpaired_surrogate": b'{"value":"\\ud800"}',
        "beyond_u64": b'{"value":18446744073709551616}',
        "below_i64": b'{"value":-9223372036854775809}',
        "utf16_bom": '{"value":1}'.encode("utf-16"),
    }
    outcomes = {}
    for name, data in cases.items():
        outcomes[name] = {}
        for label, loader in [("stdlib", json.loads), ("orjson", orjson.loads)]:
            try:
                value = loader(data)
                outcomes[name][label] = {
                    "accepted": True,
                    "value_repr": repr(value),
                    "value_type": type(value["value"]).__name__,
                }
            except (ValueError, RecursionError) as error:
                outcomes[name][label] = {"accepted": False, "exception": type(error).__name__}
    return {"engine_edge_values_exact": True, "non_drop_in_differences": outcomes}


class GcPauses:
    """Nested diagnostic only: never disable or retune the process collector."""

    def __init__(self):
        self.started = 0
        self.pauses_ns = 0
        self.collections = [0, 0, 0]

    def callback(self, phase, info):
        if phase == "start":
            self.started = time.perf_counter_ns()
        else:
            self.pauses_ns += time.perf_counter_ns() - self.started
            self.collections[info["generation"]] += 1


def controls(output, queries, rounds):
    samples = []
    rng = random.Random(20261024)
    for case, q in queries.items():
        raw = bytearray((output / f"{case}-frame.json").read_bytes())
        text = raw.decode("utf-8", "surrogatepass")
        reference = json.loads(raw)
        fields = ["ts", "value"] if "feature" in q else FIELDS
        records = reference["result"]["records"]
        getter = itemgetter(*fields)
        expected = list(map(getter, records))
        assert hash_rows(expected) == q["expected"]
        jobs = [
            "stdlib_bytearray",
            "stdlib_text",
            "orjson_bytearray",
            "utf8_decode",
            "tuple_generator_retained",
            "tuple_itemgetter_retained",
            "tuple_generator_consumed",
            "tuple_itemgetter_consumed",
            "release_records_only",
        ]
        for repeat in range(rounds):
            rng.shuffle(jobs)
            for job in jobs:
                # Consumed records own newly parsed ints/dicts. No shared
                # fixture reference keeps those dictionaries artificially alive.
                consumed = job.endswith("consumed") or job == "release_records_only"
                if consumed:
                    container = json.loads(raw)
                    source = container["result"].pop("records")
                    del container
                elif job.startswith("tuple_"):
                    source = records
                monitor = GcPauses()
                gc.callbacks.append(monitor.callback)
                try:
                    start = time.perf_counter_ns()
                    if job == "stdlib_bytearray":
                        value = json.loads(raw)
                    elif job == "stdlib_text":
                        value = json.loads(text)
                    elif job == "orjson_bytearray":
                        value = orjson.loads(raw)
                    elif job == "utf8_decode":
                        value = raw.decode("utf-8", "surrogatepass")
                    elif job == "release_records_only":
                        del source
                    elif "itemgetter" in job:
                        value = list(map(getter, source))
                    else:
                        value = [tuple(row[field] for field in fields) for row in source]
                    mapped = time.perf_counter_ns()
                    if consumed and job != "release_records_only":
                        del source
                    end = time.perf_counter_ns()
                finally:
                    gc.callbacks.remove(monitor.callback)
                sample = {
                    "case": case,
                    "round": repeat,
                    "control": job,
                    "operation_ns": mapped - start,
                    "record_release_ns": end - mapped,
                    "total_ns": end - start,
                    "nested_gc_pause_ns": monitor.pauses_ns,
                    "gc_collections": monitor.collections,
                }
                if job in {"stdlib_bytearray", "stdlib_text", "orjson_bytearray"}:
                    assert_tree(value, reference)
                elif job == "utf8_decode":
                    assert value == text
                elif job != "release_records_only":
                    assert value == expected and hash_rows(value) == q["expected"]
                if job != "release_records_only":
                    del value  # Result destruction is outside these controls.
                if not consumed and job.startswith("tuple_"):
                    del source
                samples.append(sample)
            print(f"controls {case} round={repeat} verified", flush=True)
        del raw, text, reference, records, expected
    return samples


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ["bridge", "probe", "probe-source", "store", "queries", "output"]:
        parser.add_argument("--" + name, type=Path, required=True)
    parser.add_argument("--rounds", type=int, default=10)
    args = parser.parse_args()
    if args.rounds < 1:
        parser.error("rounds must be positive")
    args.output.mkdir(parents=True, exist_ok=False)
    queries = json.loads(args.queries.read_text())
    assert len(queries) == 4
    (args.output / "queries.json").write_text(json.dumps(queries, indent=2) + "\n")
    (args.output / "compatibility.json").write_text(json.dumps(compatibility(), indent=2) + "\n")
    sys.path.insert(0, str(args.probe_source.resolve() / "python"))
    package = importlib.import_module("fastk_cost_client")
    transport = importlib.import_module("fastk_cost_client._session")
    trace = importlib.import_module("fastk_cost_client._measure").TRACE
    traces = {"production": {}, "probe_split": trace}
    modules = {"production": _session, "probe_split": transport}
    originals = {name: module.json for name, module in modules.items()}
    _session.json = measured.decoder(traces["production"])
    transport.json = split_decoder(trace)
    samples, fixtures = [], {}
    rng = random.Random(20261024)
    try:
        with ExitStack() as stack:
            clients = {
                "production": stack.enter_context(
                    Client(args.store, binary=args.bridge, timeout=120)
                ),
                "probe_split": stack.enter_context(
                    package.Client(args.store, binary=args.probe, timeout=120)
                ),
            }
            pids = {name: c._session.process.pid for name, c in clients.items()}
            for case, q in queries.items():
                captured = []
                for name, client in clients.items():
                    _, raw = measured.call(
                        client, q, traces[name], instrumented=name == "probe_split", capture=True
                    )
                    captured.append(raw)
                assert captured[0] == captured[1]
                (args.output / f"{case}-frame.json").write_bytes(captured[0])
                fixtures[case] = {
                    "bytes": len(raw),
                    "sha256": measured.sha(args.output / f"{case}-frame.json"),
                }
                del captured, raw
            for repeat in range(args.rounds):
                jobs = [(route, case) for route in clients for case in queries]
                rng.shuffle(jobs)
                for route, case in jobs:
                    sample, _ = measured.call(
                        clients[route],
                        queries[case],
                        traces[route],
                        instrumented=route == "probe_split",
                    )
                    if route == "probe_split":
                        assert sum(trace["decode_parts"].values()) == sample["stages"].pop(
                            "python_parse_and_objects"
                        )
                        sample["stages"].update(trace["decode_parts"])
                        sample["stages"]["rust_frame_finalize"] = sample["stages"].pop(
                            "rust_envelope_copy"
                        )
                        assert sum(sample["stages"].values()) == sample["total_ns"]
                    sample.update(route=route, case=case, round=repeat)
                    assert clients[route]._session.process.pid == pids[route]
                    assert clients[route]._session.process.poll() is None
                    samples.append(sample)
                    print(f"live {route} {case} round={repeat} verified", flush=True)
        assert all(c._session.process.poll() is not None for c in clients.values())
    finally:
        for name, module in modules.items():
            module.json = originals[name]
    cached = controls(args.output, queries, args.rounds)
    groups = defaultdict(dict)
    for route in clients:
        for case in queries:
            rows = [s for s in samples if s["route"] == route and s["case"] == case]
            group = {
                key: stats([s[key] for s in rows])
                for key in [
                    "client_call_ns",
                    "total_ns",
                    "python_parse_and_objects_ns",
                    "caller_tuple_and_record_release_ns",
                ]
            }
            if route == "probe_split":
                group["stages"] = {
                    key: stats([s["stages"][key] for s in rows]) for key in rows[0]["stages"]
                }
            groups[route][case] = group
    control_groups = defaultdict(dict)
    for case in queries:
        for name in sorted({s["control"] for s in cached}):
            rows = [s for s in cached if s["case"] == case and s["control"] == name]
            control_groups[case][name] = {
                key: stats([s[key] for s in rows])
                for key in ["total_ns", "operation_ns", "record_release_ns", "nested_gc_pause_ns"]
            }
    outputs = {
        "summary": {
            "all_results_verified": True,
            "all_children_reaped": True,
            "timed_responses": len(samples),
            "same_worker_pids": pids,
            "groups": groups,
            "controls": control_groups,
            "fixtures": fixtures,
        },
        "samples": samples,
        "control-samples": cached,
        "environment": {
            "os": platform.platform(),
            "python": platform.python_version(),
            "orjson": orjson.__version__,
            "rounds": args.rounds,
            "seed": 20261024,
            "bridge_sha256": measured.sha(args.bridge),
            "probe_sha256": measured.sha(args.probe),
            "query_sha256": measured.sha(args.queries),
            "production_hashes": json.loads(
                (args.probe_source / "production-hashes.json").read_text()
            ),
            "timing": "Main parse split follows CPython bytes decode then json.loads(text); stages close per sample. Cached controls retain raw/text/reference; no memory claim. GC counters nested, collector never retuned.",
            "limits": "10 descriptive observations per shape. Instrumented vs normal live control. Tokenization and dict/list/int allocation are inseparable in this stdlib measurement. No production edits or adopted native decoder.",
        },
    }
    for name, value in outputs.items():
        (args.output / (name + ".json")).write_text(json.dumps(value, indent=2) + "\n")
    print(args.output / "summary.json", flush=True)


if __name__ == "__main__":
    main()
