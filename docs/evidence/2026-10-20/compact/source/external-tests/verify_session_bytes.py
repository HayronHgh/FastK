"""Verify session values/encoding, CLI bytes and business-error continuity."""

import argparse
import hashlib
import json
import subprocess
from contextlib import ExitStack
from pathlib import Path
from types import SimpleNamespace

from fastk_client import Client, FastKError, _session


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ["before", "after", "store", "queries", "output"]:
        parser.add_argument("--" + name, type=Path, required=True)
    parser.add_argument("--compact-session", action="store_true")
    args = parser.parse_args()
    if args.output.exists():
        parser.error("output already exists")
    captured = []
    original = _session.json

    def decode(data):
        captured.append(data)
        return json.loads(data)

    _session.json = SimpleNamespace(loads=decode, dumps=json.dumps)
    checks = []
    try:
        with ExitStack() as stack:
            clients = [
                stack.enter_context(Client(args.store, binary=binary, timeout=120))
                for binary in [args.before, args.after]
            ]
            pids = [c._session.process.pid for c in clients]
            cases = json.loads(args.queries.read_text())
            for label, query in [
                *cases.items(),
                ("application_error", None),
                ("after_error", next(iter(cases.values()))),
            ]:
                captured.clear()
                for client in clients:
                    if query is None:
                        try:
                            client._run("invalid-command-for-parity")
                        except FastKError as error:
                            assert error.code not in {"TIMEOUT", "PROTOCOL", "SESSION_BROKEN"}
                        else:
                            raise AssertionError("expected application error")
                    elif "feature" in query:
                        client.read_scalar_range(
                            query["symbol"],
                            "1m",
                            "feature",
                            query["feature"],
                            query["start_ts"],
                            query["end_ts"],
                        )
                    else:
                        client.read_kline_range(
                            query["symbol"], "1m", query["start_ts"], query["end_ts"]
                        )
                assert len(captured) == 2, label
                before, after = map(json.loads, captured)
                assert before == after, label
                if args.compact_session:
                    # Re-encode the old ordered object: integers, key order,
                    # escaped strings and null/empty shapes must all survive.
                    expected = json.dumps(
                        before, ensure_ascii=False, separators=(",", ":")
                    ).encode()
                    assert captured[1] == expected, label
                else:
                    assert captured[0] == captured[1], label
                if query is None:
                    assert captured[0] == captured[1], "error bytes changed"
                assert [c._session.process.pid for c in clients] == pids
                checks.append(
                    {
                        "case": label,
                        "before_bytes": len(captured[0]),
                        "after_bytes": len(captured[1]),
                        "before_sha256": hashlib.sha256(captured[0]).hexdigest(),
                        "after_sha256": hashlib.sha256(captured[1]).hexdigest(),
                        "bytes_identical": captured[0] == captured[1],
                    }
                )
                captured.clear()
        assert all(c._session.process.poll() is not None for c in clients)
    finally:
        _session.json = original
    one_shot = []
    commands = [("version", ["version", "--json"])]
    for label in ["small_kline", "medium_kline"]:
        q = cases[label]
        commands.append(
            (
                label,
                [
                    "read-kline-range",
                    "--root",
                    str(args.store.resolve()),
                    "--symbol",
                    q["symbol"],
                    "--timeframe",
                    "1m",
                    "--start-ts",
                    str(q["start_ts"]),
                    "--end-ts",
                    str(q["end_ts"]),
                ],
            )
        )
    for label, command in commands:
        payloads = [
            subprocess.run(
                [str(binary.resolve()), *command], capture_output=True, check=True, timeout=30
            ).stdout
            for binary in [args.before, args.after]
        ]
        assert payloads[0] == payloads[1]
        one_shot.append(
            {
                "case": label,
                "bytes": len(payloads[0]),
                "sha256": hashlib.sha256(payloads[0]).hexdigest(),
            }
        )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(
            {
                "all_session_values_identical": True,
                "compact_session": args.compact_session,
                "all_one_shot_bytes_identical": True,
                "all_children_reaped": True,
                "checks": checks,
                "one_shot": one_shot,
            },
            indent=2,
        )
        + "\n"
    )
    print(args.output)


if __name__ == "__main__":
    main()
