"""Run from an empty directory using an installed wheel, without Rust or repo imports."""

import json
import shutil
import subprocess
import sys
from pathlib import Path

import fastk_client
from fastk_client import Client, FastKError


def main():
    assert shutil.which("cargo") is None, "smoke environment must not expose Cargo"
    root = Path.cwd() / "研究 store with spaces"
    client = Client(root)
    value = 2**53 + 1
    start = 1706745600000
    client.write_scalar_range(
        "BTCUSDT",
        "1m",
        "feature",
        "test",
        [{"ts": start, "value": value}],
        timeframe_ms=60000,
    )
    client.write_scalar_range(
        "BTCUSDT",
        "1m",
        "feature",
        "test",
        [{"ts": start + 60000, "value": -value}],
        timeframe_ms=60000,
    )
    reopened = Client(root)
    rows = reopened.read_scalar_range("BTCUSDT", "1m", "feature", "test", start, start + 60000)[
        "records"
    ]
    assert rows == [
        {"ts": start, "value": value},
        {"ts": start + 60000, "value": -value},
    ], rows
    matches = client.query_scalar_predicate(
        "BTCUSDT",
        "1m",
        "feature",
        "test",
        start,
        start + 60000,
        predicate="eq",
        value=value,
    )
    assert len(matches["matches"]) == 1
    try:
        client.write_scalar_range(
            "BTCUSDT",
            "1m",
            "feature",
            "test",
            [{"ts": start, "value": 0}],
            timeframe_ms=60000,
        )
    except FastKError as error:
        assert error.code == "INVALID_INPUT"
    else:
        raise AssertionError("overlapping writes must fail")
    if len(sys.argv) > 1:
        subprocess.run(
            [sys.argv[1], "validate", "--root", str(root)],
            check=True,
            capture_output=True,
        )
    with Client(root) as session:
        assert (
            session.read_scalar_range("BTCUSDT", "1m", "feature", "test", start, start + 60000)[
                "records"
            ]
            == rows
        )
        assert session.scalar_inventory("BTCUSDT", "1m", "feature", "test")["record_count"] == 2
    sealed = client.seal_snapshot()
    assert client.verify_snapshot(sealed["digest"])["digest"] == sealed["digest"]
    try:
        client.write_scalar_range(
            "BTCUSDT",
            "1m",
            "feature",
            "test",
            [{"ts": start + 120000, "value": 0}],
            timeframe_ms=60000,
        )
    except FastKError as error:
        assert error.code == "INVALID_INPUT"
    else:
        raise AssertionError("sealed writes must fail")
    print(
        json.dumps(
            {
                "installed_client": fastk_client.__file__,
                "contract_version": client.version["contract_version"],
                "rows": len(rows),
                "integer_roundtrip": True,
                "snapshot_verified": True,
                "session_reuse": True,
            }
        )
    )


if __name__ == "__main__":
    main()
