"""Measure unchanged scoped JSON reads using an isolated observational probe."""

import argparse
import ctypes
import hashlib
import importlib
import json
import os
import platform
import random
import sys
import time
from collections import defaultdict
from contextlib import ExitStack
from operator import itemgetter
from pathlib import Path
from types import SimpleNamespace

from fastk_bench.data import FIELDS, hash_rows, query_rows
from fastk_client import Client
from fastk_client import _session as production_transport

if os.name != "nt":
    raise SystemExit("This probe uses the Windows QPC clock; run on the measured Windows host.")
KERNEL = ctypes.WinDLL("kernel32", use_last_error=True)
FREQUENCY = ctypes.c_longlong()
assert KERNEL.QueryPerformanceFrequency(ctypes.byref(FREQUENCY))


def ticks():
    value = ctypes.c_longlong()
    assert KERNEL.QueryPerformanceCounter(ctypes.byref(value))
    return value.value


def ns(delta):
    return delta * 1_000_000_000 // FREQUENCY.value


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def stats(values):
    values = sorted(values)
    return {
        "samples": len(values),
        "mean_ns": sum(values) / len(values),
        "median_ns": values[(len(values) - 1) // 2],
        "min_ns": values[0],
        "max_ns": values[-1],
    }


def decoder(trace):
    def loads(data):
        started = ticks()
        value = json.loads(data)
        ended = ticks()
        trace.update(decode_start_tick=started, decode_end_tick=ended, wire_bytes=len(data))
        trace["id"] = value.get("id")
        if trace.pop("capture_raw", False):
            trace["raw"] = data
        return value

    return SimpleNamespace(loads=loads, dumps=json.dumps)


def profile(client, request_id):
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        latest_json = None
        for line in bytes(client._session._cost_stderr).splitlines():
            prefix, _, body = line.partition(b" ")
            try:
                if prefix == b"FASTK_BULK_JSON":
                    latest_json = json.loads(body)
                elif prefix == b"FASTK_BULK_SESSION":
                    report = json.loads(body)
                    if report["id"] == request_id and latest_json is not None:
                        return {"json": latest_json, "session": report}
            except ValueError:
                pass  # A partial final line can still be in the drainer pipe.
        time.sleep(0.001)
    raise AssertionError("instrumented response profile missing")


def call(client, q, trace, *, instrumented, capture=False):
    trace.clear()
    if capture:
        trace["capture_raw"] = True
    started = ticks()
    if "feature" in q:
        response = client.read_scalar_range(
            q["symbol"], "1m", "feature", q["feature"], q["start_ts"], q["end_ts"]
        )
        fields = ["ts", "value"]
    else:
        response = client.read_kline_range(q["symbol"], "1m", q["start_ts"], q["end_ts"])
        fields = FIELDS
    returned = ticks()
    records = response.pop("records")
    rows = [tuple(row[field] for field in fields) for row in records]
    del records
    converted = ticks()
    # All validation, stderr collection and hashes are outside timed sections.
    assert hash_rows(rows) == q["expected"]
    assert response["symbol"] == q["symbol"] and response["timeframe"] == "1m"
    assert response["timeframe_ms"] == 60000
    assert (response["start_ts"], response["end_ts"]) == (q["start_ts"], q["end_ts"])
    if "feature" in q:
        assert response["category"] == "feature" and response["name"] == q["feature"]
    else:
        assert response["price_scale"] == response["volume_scale"] == 100_000_000
    sample = {
        "total_ns": ns(converted - started),
        "client_call_ns": ns(returned - started),
        "python_parse_and_objects_ns": ns(trace["decode_end_tick"] - trace["decode_start_tick"]),
        "caller_tuple_and_record_release_ns": ns(converted - returned),
        "wire_bytes": trace["wire_bytes"],
        "metadata": response,
    }
    if instrumented:
        report = profile(client, trace["id"])
        child, encoding = report["session"], report["json"]
        points = [
            started,
            child["dispatch_start_tick"],
            encoding["start_tick"],
            encoding["end_tick"],
            child["dispatch_end_tick"],
            child["write_start_tick"],
            trace["io_return_tick"],
            trace["decode_start_tick"],
            trace["decode_end_tick"],
            returned,
            converted,
        ]
        assert points == sorted(points), points
        names = [
            "request_dispatch",
            "engine_and_response",
            "rust_json_encode_buffer",
            "rust_post_encode_cleanup_and_log",
            "rust_envelope_copy",
            "wire_and_receive",
            "python_handoff",
            "python_parse_and_objects",
            "python_return_cleanup",
            "caller_tuple_and_record_release",
        ]
        stages = {name: ns(end - start) for name, start, end in zip(names, points, points[1:])}
        assert sum(stages.values()) == sample["total_ns"]
        assert child["frame_bytes"] == trace["wire_bytes"]
        sample.update(
            stages=stages,
            rust_profile=report,
            reads=list(trace["reads"]),
            overlapping_child_pipe_write_ns=ns(child["write_end_tick"] - child["write_start_tick"]),
            receiver_append_ns=sum(r["append_ns"] for r in trace["reads"]),
            receiver_final_copy_ns=sum(r["final_bytes_copy_ns"] for r in trace["reads"]),
            receiver_read_calls=sum(r["calls"] for r in trace["reads"]),
        )
    return sample, trace.get("raw")


def cached_controls(output, queries, repeats):
    samples, sizes = [], {}
    rng = random.Random(20261021)
    for label, query in queries.items():
        pretty = (output / f"{label}-frame.json").read_bytes()
        reference = json.loads(pretty)
        compact = json.dumps(
            reference, separators=(",", ":"), ensure_ascii=False, allow_nan=False
        ).encode()
        assert json.loads(compact) == reference
        fields = ["ts", "value"] if "feature" in query else FIELDS
        records = reference["result"]["records"]
        expected_rows = [tuple(row[field] for field in fields) for row in records]
        assert hash_rows(expected_rows) == query["expected"]
        sizes[label] = {
            "pretty_bytes": len(pretty),
            "compact_bytes": len(compact),
            "rows": len(records),
        }
        fixtures = {
            "pretty_bytes": pretty,
            "compact_bytes": compact,
            "pretty_text": pretty.decode(),
            "compact_text": compact.decode(),
        }
        getter = itemgetter(*fields)
        for repeat in range(repeats):
            jobs = list(fixtures) + [
                "utf8_pretty",
                "utf8_compact",
                "tuples_comprehension",
                "tuples_itemgetter",
            ]
            rng.shuffle(jobs)
            for job in jobs:
                before = time.perf_counter_ns()
                if job in fixtures:
                    value = json.loads(fixtures[job])
                elif job.startswith("utf8_"):
                    value = (pretty if job.endswith("pretty") else compact).decode()
                elif job == "tuples_comprehension":
                    value = [tuple(row[field] for field in fields) for row in records]
                else:
                    value = list(map(getter, records))
                after = time.perf_counter_ns()
                if job in fixtures:
                    assert value == reference
                elif job.startswith("utf8_"):
                    assert value == fixtures[job.removeprefix("utf8_") + "_text"]
                else:
                    assert value == expected_rows
                del value  # Destruction is outside isolated control timers.
                samples.append(
                    {"case": label, "round": repeat, "control": job, "total_ns": after - before}
                )
            print(f"cached case={label} round={repeat} verified", flush=True)
        del reference, records, expected_rows, pretty, compact, fixtures
    # Exact integer values and Unicode survive whitespace-only wire changes.
    edge = {
        "symbol": "中文",
        "records": [{"ts": -(2**63), "value": 2**63 - 1}, {"ts": 2**53 + 1, "value": -(2**53 + 1)}],
        "optional": None,
        "empty": [],
    }
    assert (
        json.loads(json.dumps(edge))
        == json.loads(json.dumps(edge, ensure_ascii=False, separators=(",", ":")))
        == edge
    )
    return samples, sizes


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ["bridge", "probe", "probe-source", "store", "input", "output"]:
        parser.add_argument("--" + name, type=Path, required=True)
    parser.add_argument("--rounds", type=int, default=10)
    parser.add_argument("--control-rounds", type=int, default=5)
    args = parser.parse_args()
    if args.rounds < 1 or args.control_rounds < 1:
        parser.error("round counts must be positive")
    args.output.mkdir(parents=True, exist_ok=False)
    sys.path.insert(0, str(args.probe_source.resolve() / "python"))
    probe_package = importlib.import_module("fastk_cost_client")
    probe_transport = importlib.import_module("fastk_cost_client._session")
    probe_measure = importlib.import_module("fastk_cost_client._measure")
    assert probe_measure.FREQUENCY.value == FREQUENCY.value
    manifest = json.loads((args.input / "manifest.json").read_text())
    small = {"symbol": "S05", "start_ts": 1716414480000, "end_ts": 1716422100000, "kind": "range"}
    medium = dict(small, end_ts=small["start_ts"] + 4095 * 60000)
    bulk = {"symbol": "S00", "start_ts": 1704067260000, "end_ts": 1730419140000, "kind": "range"}
    queries = {
        "small_kline": small,
        "medium_kline": medium,
        "bulk_kline": bulk,
        "bulk_scalar": dict(bulk, feature="ma20"),
    }
    for query in queries.values():
        query["expected"] = hash_rows(query_rows(args.input, manifest, query))
    (args.output / "queries.json").write_text(json.dumps(queries, indent=2) + "\n")
    traces = {"production": {}, "probe": probe_measure.TRACE}
    transports = {"production": production_transport, "probe": probe_transport}
    originals = {name: module.json for name, module in transports.items()}
    for name, module in transports.items():
        module.json = decoder(traces[name])
    samples, fixtures, pids = [], {}, {}
    rng = random.Random(20261020)
    try:
        with ExitStack() as stack:
            clients = {
                "production": stack.enter_context(
                    Client(args.store, binary=args.bridge, timeout=120)
                ),
                "probe": stack.enter_context(
                    probe_package.Client(args.store, binary=args.probe, timeout=120)
                ),
            }
            pids = {name: client._session.process.pid for name, client in clients.items()}
            for label, query in queries.items():
                raw = {}
                for name, client in clients.items():
                    _, raw[name] = call(
                        client, query, traces[name], instrumented=name == "probe", capture=True
                    )
                # Same id and result bytes: probe adds no data to the wire.
                assert raw["production"] == raw["probe"]
                (args.output / f"{label}-frame.json").write_bytes(raw["production"])
                fixtures[label] = {
                    "bytes": len(raw["production"]),
                    "sha256": hashlib.sha256(raw["production"]).hexdigest(),
                }
                del raw
            for round_index in range(args.rounds):
                jobs = [(name, label) for name in clients for label in queries]
                rng.shuffle(jobs)
                for name, label in jobs:
                    sample, _ = call(
                        clients[name], queries[label], traces[name], instrumented=name == "probe"
                    )
                    sample.update(route=name, case=label, round=round_index)
                    assert clients[name]._session.process.pid == pids[name]
                    assert clients[name]._session.process.poll() is None
                    samples.append(sample)
                    print(f"round={round_index} route={name} case={label} verified", flush=True)
        assert all(client._session.process.poll() is not None for client in clients.values())
    finally:
        for name, module in transports.items():
            module.json = originals[name]
    controls, sizes = cached_controls(args.output, queries, args.control_rounds)
    groups = {}
    for route in ["production", "probe"]:
        groups[route] = {}
        for label in queries:
            rows = [s for s in samples if s["route"] == route and s["case"] == label]
            keys = [
                "total_ns",
                "client_call_ns",
                "python_parse_and_objects_ns",
                "caller_tuple_and_record_release_ns",
            ]
            if route == "probe":
                keys += [
                    "overlapping_child_pipe_write_ns",
                    "receiver_append_ns",
                    "receiver_final_copy_ns",
                ]
            group = {key: stats([row[key] for row in rows]) for key in keys}
            if route == "probe":
                counts = sorted(row["receiver_read_calls"] for row in rows)
                group["receiver_read_calls"] = {
                    "samples": len(counts),
                    "mean": sum(counts) / len(counts),
                    "median": counts[(len(counts) - 1) // 2],
                    "min": counts[0],
                    "max": counts[-1],
                }
                group["stages"] = {
                    key: stats([row["stages"][key] for row in rows]) for key in rows[0]["stages"]
                }
            groups[route][label] = group
    control_groups = defaultdict(dict)
    for label in queries:
        for control in sorted({s["control"] for s in controls}):
            control_groups[label][control] = stats(
                [s["total_ns"] for s in controls if s["case"] == label and s["control"] == control]
            )
    result = {
        "all_results_verified": True,
        "same_worker_per_route": pids,
        "all_children_reaped": True,
        "groups": groups,
        "cached_controls": control_groups,
        "fixture_bytes_identical": fixtures,
        "sizes": sizes,
        "timed_responses_verified": len(samples),
        "limits": "Descriptive means/medians only; overlapping writer/read counters are not additive; compact and tuple controls are isolated, not a deployed speedup",
    }
    environment = {
        "os": platform.platform(),
        "python": platform.python_version(),
        "qpc_frequency": FREQUENCY.value,
        "bridge_sha256": sha(args.bridge),
        "probe_sha256": sha(args.probe),
        "rounds": args.rounds,
        "control_rounds": args.control_rounds,
        "seed": 20261020,
        "startup": "Constructor/version and worker handshake excluded; one worker per route for entire experiment",
        "cache": "Uncontrolled OS cache, prepared pre-maintenance store; job order shuffled each round",
        "store": str(args.store.resolve()),
        "input_manifest_sha256": sha(args.input / "manifest.json"),
        "production_sources": json.loads(
            (args.probe_source / "production-hashes.json").read_text()
        ),
        "probe_overhead": "Paired unchanged production observations recorded; extra per-read timers and stderr logs exist only in private clone",
    }
    for name, value in [
        ("summary", result),
        ("environment", environment),
        ("samples", samples),
        ("control-samples", controls),
    ]:
        (args.output / f"{name}.json").write_text(json.dumps(value, indent=2) + "\n")
    print(args.output / "summary.json", flush=True)


if __name__ == "__main__":
    main()
