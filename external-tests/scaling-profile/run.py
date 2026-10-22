"""Linux perf diagnostics; neither modifies nor instruments the product crate."""

import argparse
import hashlib
import json
import os
import random
import shutil
import subprocess
import tempfile
from pathlib import Path

EVENTS = "task-clock,cycles,instructions,cache-references,cache-misses,context-switches,cpu-migrations,page-faults"


def invoke(args, output, config, *, record=False, counters=True):
    stem = config["name"]
    with tempfile.TemporaryDirectory(prefix="fastk-perf-") as fifo_dir:
        control, ack = (Path(fifo_dir) / n for n in ("control", "ack"))
        os.mkfifo(control)
        os.mkfifo(ack)
        query = args.short if config["mode"] in ("outer", "private") else args.bulk
        command = [
            str(args.binary.resolve()),
            config["mode"],
            str(args.store.resolve()),
            str(query.resolve()),
            str(config["threads"]),
            str(config["groups"]),
            str(config["cycles"]),
        ]
        env = dict(os.environ)
        if counters:
            env.update(PROFILE_CONTROL=str(control), PROFILE_ACK=str(ack))
            if record:
                command = [
                    "perf",
                    "record",
                    "-e",
                    "cycles",
                    "-F",
                    "199",
                    "-g",
                    "--call-graph",
                    "fp",
                    "--control=fifo:" + str(control) + "," + str(ack),
                    "--delay=-1",
                    "-o",
                    str(Path(fifo_dir) / "perf.data"),
                    "--",
                    *command,
                ]
            else:
                command = [
                    "perf",
                    "stat",
                    "-j",
                    "-e",
                    EVENTS,
                    "--control=fifo:" + str(control) + "," + str(ack),
                    "--delay=-1",
                    "-o",
                    str(output / (stem + ".stat")),
                    "--",
                    *command,
                ]
        with (
            (output / (stem + ".json")).open("wb") as stream,
            (output / (stem + ".log")).open("wb") as log,
        ):
            subprocess.run(command, env=env, stdout=stream, stderr=log, check=True, timeout=180)
        result = json.loads((output / (stem + ".json")).read_text())
        assert result["passed"]
        if counters and not record:
            events = [
                json.loads(line)
                for line in (output / (stem + ".stat")).read_text().splitlines()
                if line.startswith("{")
            ]
            assert len(events) == len(EVENTS.split(","))
            assert all(event["pcnt-running"] >= 99 for event in events), events
        if record:
            shutil.copy2(Path(fifo_dir) / "perf.data", output / (stem + ".data"))
            for label, flags in [
                ("self", ["--no-children", "--call-graph", "none"]),
                ("callers", ["--call-graph", "graph,0.5,caller"]),
            ]:
                with (output / (stem + "-" + label + ".txt")).open("wb") as stream:
                    subprocess.run(
                        [
                            "perf",
                            "report",
                            "--stdio",
                            "-i",
                            str(output / (stem + ".data")),
                            "--percent-limit",
                            "0.5",
                            *flags,
                        ],
                        stdout=stream,
                        stderr=subprocess.DEVNULL,
                        check=True,
                        timeout=60,
                    )


def main():
    p = argparse.ArgumentParser(description=__doc__)
    for name in ("binary", "store", "short", "bulk", "output"):
        p.add_argument("--" + name, type=Path, required=True)
    p.add_argument("--records-only", action="store_true")
    args = p.parse_args()
    args.output.mkdir(exist_ok=args.records_only)
    settings = []
    for mode in ("copy", "pread", "schedule"):
        for threads, groups in ((4, 4), (5, 5), (8, 4), (8, 5), (8, 8)):
            settings.append(
                dict(
                    mode=mode,
                    threads=threads,
                    groups=groups,
                    cycles=10000 if mode == "schedule" else 400,
                )
            )
    settings += [
        dict(mode="engine", threads=n, groups=min(n, 5), cycles=400) for n in (1, 2, 4, 5, 8)
    ]
    settings += [
        dict(mode=mode, threads=n, groups=1, cycles=100)
        for mode in ("outer", "private")
        for n in (1, 4, 8)
    ]
    jobs = [dict(s, round=r) for r in range(3) for s in settings]
    random.Random(20261021).shuffle(jobs)
    for index, job in enumerate(jobs):
        job["name"] = f"{index:03}-{job['mode']}-t{job['threads']}-g{job['groups']}-r{job['round']}"
        if not args.records_only:
            invoke(args, args.output, job)
        else:
            prior = json.loads((args.output / (job["name"] + ".json")).read_text())
            assert (
                prior["passed"]
                and prior["mode"] == job["mode"]
                and prior["threads"] == job["threads"]
            )
        print("counters", index + 1, "/", len(jobs), job["name"], flush=True)
    # Identical executable without perf: instrumentation-impact control, not a product ranking.
    for index, config in enumerate(settings):
        control = dict(
            config,
            name=f"direct-{index:02}-{config['mode']}-t{config['threads']}-g{config['groups']}",
        )
        if not args.records_only:
            invoke(args, args.output, control, counters=False)
        else:
            assert json.loads((args.output / (control["name"] + ".json")).read_text())["passed"]
    records = []
    for mode, threads in (
        ("engine", 4),
        ("engine", 5),
        ("engine", 8),
        ("outer", 4),
        ("outer", 8),
        ("private", 4),
        ("private", 8),
    ):
        config = dict(
            mode=mode,
            threads=threads,
            groups=min(threads, 5),
            cycles=2000 if mode == "engine" else 500,
            name=f"record-native-{mode}-t{threads}",
        )
        invoke(args, args.output, config, record=True)
        records.append(config)
        print("profile", config["name"], flush=True)
    (args.output / "manifest.json").write_text(
        json.dumps(
            dict(
                binary_sha256=hashlib.sha256(args.binary.read_bytes()).hexdigest(),
                jobs=jobs,
                records=records,
                settings=settings,
                counters_exclude_setup_validation=True,
                frames_for_diagnostics_only=True,
                passed=True,
            ),
            indent=2,
        )
        + "\n"
    )


if __name__ == "__main__":
    main()
