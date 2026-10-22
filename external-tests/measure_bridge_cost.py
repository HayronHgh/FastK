"""Windows QPC timeline for the original bridge and an isolated diagnostic copy."""

import argparse
import ctypes
import hashlib
import json
import math
import os
import platform
import random
import subprocess
import time
from collections import defaultdict
from pathlib import Path
from types import SimpleNamespace

from fastk_bench.data import FIELDS, hash_rows, query_rows
from fastk_client import Client
from fastk_client import client as client_module

if os.name != "nt":
    raise SystemExit("This diagnostic measures the Windows 24ms result; run it on Windows.")
KERNEL = ctypes.WinDLL("kernel32", use_last_error=True)
FREQUENCY = ctypes.c_longlong()
assert KERNEL.QueryPerformanceFrequency(ctypes.byref(FREQUENCY))


def ticks():
    value = ctypes.c_longlong()
    assert KERNEL.QueryPerformanceCounter(ctypes.byref(value))
    return value.value


def ns(delta):
    return delta * 1_000_000_000 // FREQUENCY.value


def summary(values):
    values = sorted(values)
    result = {
        "samples": len(values),
        "mean_ns": sum(values) / len(values),
        "median_ns": values[(len(values) - 1) // 2],
        "min_ns": values[0],
        "max_ns": values[-1],
    }
    if len(values) >= 100:
        result.update(
            p95_ns=values[math.ceil(len(values) * 0.95) - 1],
            p99_ns=values[math.ceil(len(values) * 0.99) - 1],
        )
    return result


CURRENT = {}
POPEN = subprocess.Popen


class TimedProcess:
    def __init__(self, *args, **kwargs):
        CURRENT["launch_tick"] = ticks()
        self.child = POPEN(*args, **kwargs)
        CURRENT["popen_return_tick"] = ticks()

    @property
    def returncode(self):
        return self.child.returncode

    def communicate(self, *args, **kwargs):
        CURRENT["communicate_start_tick"] = ticks()
        stdout, stderr = self.child.communicate(*args, **kwargs)
        CURRENT["communicate_end_tick"] = ticks()
        CURRENT["stdout"] = stdout
        CURRENT["stderr"] = stderr
        CURRENT["returncode"] = self.child.returncode
        return stdout, stderr

    def kill(self):
        return self.child.kill()


def decoded(data):
    start = ticks()
    value = json.loads(data)
    CURRENT["decode_ns"] = ns(ticks() - start)
    return value


def reports(stderr):
    result = {}
    for line in stderr.decode().splitlines():
        prefix, _, body = line.partition(" ")
        if not prefix.startswith("FASTK_COST_"):
            raise AssertionError("unexpected diagnostic stderr: " + line)
        result[prefix.removeprefix("FASTK_COST_").lower()] = json.loads(body)
    return result


def finish(start, end, *, group, round, case=None):
    trace = dict(CURRENT)
    assert trace["returncode"] == 0
    profile = reports(trace.pop("stderr"))
    stdout = trace.pop("stdout")
    result = {
        "group": group,
        "round": round,
        "total_ns": ns(end - start),
        "decode_ns": trace.pop("decode_ns", 0),
        "adapter_ns": trace.pop("adapter_ns", 0),
        "stdout_bytes": len(stdout),
        "stdout_sha256": hashlib.sha256(stdout).hexdigest(),
        "popen_call_ns": ns(trace["popen_return_tick"] - trace["launch_tick"]),
        "profile": profile,
    }
    if case is not None:
        result["case"] = case
    if "lifecycle" in profile:
        life = profile["lifecycle"]
        assert (
            trace["launch_tick"]
            <= life["main_tick"]
            <= life["end_tick"]
            <= trace["communicate_end_tick"]
        )
        result["stages"] = {
            "parent_before_launch_ns": ns(trace["launch_tick"] - start),
            "launch_to_main_ns": ns(life["main_tick"] - trace["launch_tick"]),
            "child_main_ns": ns(life["end_tick"] - life["main_tick"]),
            "after_main_to_receive_ns": ns(trace["communicate_end_tick"] - life["end_tick"]),
            "python_decode_ns": result["decode_ns"],
            "python_adapter_ns": result["adapter_ns"],
        }
        result["stages"]["parent_other_ns"] = result["total_ns"] - sum(result["stages"].values())
        assert result["stages"]["parent_other_ns"] >= 0
    return result, stdout


def read(client, q, *, group, round):
    CURRENT.clear()
    start = ticks()
    records = client.read_kline_range(q["symbol"], "1m", q["start_ts"], q["end_ts"])["records"]
    t = ticks()
    rows = [tuple(row[field] for field in FIELDS) for row in records]
    del records
    CURRENT["adapter_ns"] = ns(ticks() - t)
    end = ticks()
    result, stdout = finish(start, end, group=group, round=round, case=q)
    assert hash_rows(rows) == q["expected"]
    return result, stdout


def raw(binary, command, *, group, round):
    CURRENT.clear()
    start = ticks()
    process = TimedProcess(
        [str(binary), *command],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        creationflags=subprocess.CREATE_NO_WINDOW,
    )
    stdout, _ = process.communicate(timeout=180)
    if stdout:
        payload = decoded(stdout)
        assert isinstance(payload, dict)
        del payload
    end = ticks()
    return finish(start, end, group=group, round=round)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bridge", type=Path, required=True)
    parser.add_argument("--probe", type=Path, required=True)
    parser.add_argument("--store", type=Path, required=True)
    parser.add_argument("--prior-results", type=Path, required=True)
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument(
        "--range-cases", type=int, default=40, help="per round; 0 replays all prior ranges"
    )
    parser.add_argument("--short-only", action="store_true")
    args = parser.parse_args()
    if args.range_cases < 0:
        parser.error("range-cases must be nonnegative")
    if os.name != "nt":
        parser.error("this diagnostic measures the Windows 24ms result")
    args.output.mkdir(parents=True, exist_ok=False)
    bridge, probe = args.bridge.resolve(), args.probe.resolve()
    original_json, original_subprocess = client_module.json, client_module.subprocess
    client_module.json = SimpleNamespace(loads=decoded, dumps=json.dumps)
    client_module.subprocess = SimpleNamespace(
        Popen=TimedProcess,
        PIPE=subprocess.PIPE,
        CREATE_NO_WINDOW=subprocess.CREATE_NO_WINDOW,
        TimeoutExpired=subprocess.TimeoutExpired,
    )
    clients = {
        "original": Client(args.store, binary=bridge, timeout=180),
        "probe": Client(args.store, binary=probe, timeout=180),
    }
    manifest = json.loads((args.input / "manifest.json").read_text())
    cases = []
    for r in range(5):
        cases.append(
            [
                q
                for q in json.loads((args.prior_results / f"queries-{r}.json").read_text())
                if q["kind"] == "range"
            ][: args.range_cases or None]
        )
    first, fixture = read(clients["original"], cases[0][0], group="fixture", round=-1)
    fixture_path = args.output.resolve() / "small-response.json"
    fixture_path.write_bytes(fixture)
    samples = []
    for round in range(5):
        tasks = [(name, q) for name in clients for q in cases[round]]
        tasks += [
            (control, None)
            for control in ["original-version", "probe-version", "probe-null", "probe-replay"]
            for _ in range(20)
        ]
        random.Random(20261020 + round).shuffle(tasks)
        for name, q in tasks:
            if q is not None:
                result, stdout = read(clients[name], q, group=name + "-range", round=round)
            else:
                binary = bridge if name.startswith("original") else probe
                command = (
                    ["version", "--json"]
                    if name.endswith("version")
                    else ["cost-null"]
                    if name.endswith("null")
                    else ["cost-replay", str(fixture_path)]
                )
                result, stdout = raw(binary, command, group=name, round=round)
                if name.endswith("replay"):
                    assert stdout == fixture
            samples.append(result)
        print("short round complete", round, flush=True)
    # Same public read, with increasing returned row counts. Expected hashes come from Arrow input.
    larger = [
        dict(
            symbol="S05",
            start_ts=cases[0][0]["start_ts"],
            end_ts=cases[0][0]["start_ts"] + 4095 * 60000,
            kind="range",
        ),
        dict(symbol="S00", start_ts=1704067260000, end_ts=1730419140000, kind="range"),
    ]
    if args.short_only:
        larger = []
    cached = [("small", fixture)]
    for label, q in zip(["medium", "bulk"], larger):
        q["expected"] = hash_rows(query_rows(args.input, manifest, q))
        matched = {}
        for round in range(5):
            names = list(clients)
            random.Random(round).shuffle(names)
            for name in names:
                result, stdout = read(clients[name], q, group=name + "-" + label, round=round)
                matched[name] = result["stdout_sha256"]
                samples.append(result)
            assert len(set(matched.values())) == 1
        fixture_file = args.output.resolve() / (label + "-response.json")
        fixture_file.write_bytes(stdout)
        for round in range(5):
            result, replayed = raw(
                probe,
                ["cost-replay", str(fixture_file)],
                group="probe-replay-" + label,
                round=round,
            )
            assert replayed == stdout
            samples.append(result)
        cached.append((label, stdout))
        print("size complete", label, q["expected"]["rows"], flush=True)
    decode_samples = []
    for label, payload in cached:
        for repeat in range(1000 if label == "small" else 20 if label == "medium" else 5):
            start = time.perf_counter_ns()
            decoded_payload = json.loads(payload)
            decoded_end = time.perf_counter_ns()
            records = decoded_payload.pop("records")
            rows = [tuple(row[f] for f in FIELDS) for row in records]
            del records, decoded_payload
            converted = time.perf_counter_ns()
            decode_samples.append(
                {
                    "size": label,
                    "rows": len(rows),
                    "decode_ns": decoded_end - start,
                    "adapter_ns": converted - decoded_end,
                }
            )
            del rows
    loop_requests = args.output.resolve() / "loop-requests.json"
    requests = [q for batch in cases for q in batch]
    loop_requests.write_text(json.dumps(requests))
    process = subprocess.run(
        [
            str(probe),
            "cost-loop",
            "--root",
            str(args.store.resolve()),
            "--requests",
            str(loop_requests),
            "--repeat",
            "5",
        ],
        capture_output=True,
        check=True,
        timeout=180,
        creationflags=subprocess.CREATE_NO_WINDOW,
    )
    loop = json.loads(process.stdout)
    for i, sample in enumerate(loop["samples"]):
        assert {k: sample[k] for k in ["rows", "sha256"]} == requests[i % len(requests)]["expected"]
    client_module.json, client_module.subprocess = original_json, original_subprocess
    grouped = defaultdict(list)
    for sample in samples:
        grouped[sample["group"]].append(sample)
    paired = {}
    for name in ["original-range", "probe-range"]:
        paired[name] = {
            (s["round"], s["case"]["symbol"], s["case"]["start_ts"], s["case"]["end_ts"]): s[
                "stdout_sha256"
            ]
            for s in grouped[name]
        }
    assert paired["original-range"] == paired["probe-range"]
    result = {
        "all_results_verified": True,
        "environment": {
            "os": platform.platform(),
            "python": platform.python_version(),
            "qpc_frequency": FREQUENCY.value,
            "bridge_sha256": hashlib.sha256(bridge.read_bytes()).hexdigest(),
            "probe_sha256": hashlib.sha256(probe.read_bytes()).hexdigest(),
            "os_cache": "uncontrolled; same prepared store; fresh child per call",
            "rounds": 5,
            "short_range_cases_per_round": [len(batch) for batch in cases],
            "client_source_sha256": hashlib.sha256(
                Path(client_module.__file__).read_bytes()
            ).hexdigest(),
        },
        "groups": {
            name: {
                "total": summary([s["total_ns"] for s in rows]),
                "decode": summary([s["decode_ns"] for s in rows]),
                "adapter": summary([s["adapter_ns"] for s in rows]),
                "popen_call": summary([s["popen_call_ns"] for s in rows]),
            }
            for name, rows in grouped.items()
        },
        "probe_range_stages": {
            key: summary([s["stages"][key] for s in grouped["probe-range"]])
            for key in grouped["probe-range"][0]["stages"]
        },
        "warm_engine": {
            "open_ns": loop["open_ns"],
            "first_read_ns": loop["samples"][0]["read_ns"],
            "all_reads": summary([s["read_ns"] for s in loop["samples"]]),
            "after_first": summary([s["read_ns"] for s in loop["samples"]][1:]),
        },
    }
    for name in grouped:
        profile = defaultdict(list)
        for s in grouped[name]:
            for stage, values in s["profile"].items():
                if stage != "lifecycle":
                    for key, value in values.items():
                        if key.endswith("_ns"):
                            profile[stage + "." + key].append(value)
        result["groups"][name]["child"] = {k: summary(v) for k, v in profile.items()}
    result["cached_json"] = {
        label: {
            key: summary([s[key] for s in decode_samples if s["size"] == label])
            for key in ["decode_ns", "adapter_ns"]
        }
        for label, _ in cached
    }
    (args.output / "summary.json").write_text(json.dumps(result, indent=2) + "\n")
    for name, data in [
        ("samples", samples),
        ("cached-json-samples", decode_samples),
        ("warm-engine", loop),
    ]:
        (args.output / (name + ".json")).write_text(json.dumps(data, indent=2) + "\n")
    print(args.output / "summary.json")


if __name__ == "__main__":
    main()
