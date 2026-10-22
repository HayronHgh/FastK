"""Offline native import/use in a clean environment outside the source checkout."""

import argparse
import os
import subprocess
import sys
import tempfile
from pathlib import Path

PROBE = """
import json, sys
import numpy as np
from fastk_native import KLINE_DTYPE, Reader, Writer, __version__
rows = np.array([(1706745600000, 2**53+1, 2**53+2, -2**63, 2**63-1, 7)], dtype=KLINE_DTYPE)
with Writer("store") as writer:
    writer.write_kline_range("A", "1m", rows, timeframe_ms=60000, price_scale=100, volume_scale=1)
with Reader("store") as reader:
    actual = reader.read_kline_latest("A", "1m", 1)
np.testing.assert_array_equal(actual, rows)
assert not actual.flags.writeable
print(json.dumps({"version": __version__, "python": sys.version, "dtype": str(actual.dtype), "compiler_or_bridge_required": False, "passed": True}))
"""


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--wheel", type=Path, required=True)
    parser.add_argument("--wheelhouse", type=Path, required=True)
    args = parser.parse_args()
    with tempfile.TemporaryDirectory(prefix="fastk-native-offline-") as directory:
        work = Path(directory)
        subprocess.run([sys.executable, "-m", "venv", str(work / "env")], check=True)
        python = work / "env" / ("Scripts/python.exe" if os.name == "nt" else "bin/python")
        environment = os.environ.copy()
        for name in ["PYTHONPATH", "PYTHONHOME", "FASTK_BRIDGE_BIN", "PYO3_PYTHON"]:
            environment.pop(name, None)
        subprocess.run(
            [
                str(python),
                "-m",
                "pip",
                "install",
                "--no-index",
                "--find-links",
                str(args.wheelhouse.resolve()),
                str(args.wheel.resolve()),
            ],
            cwd=work,
            env=environment,
            check=True,
        )
        environment["PATH"] = str(work / "empty-path")
        subprocess.run([str(python), "-I", "-c", PROBE], cwd=work, env=environment, check=True)


if __name__ == "__main__":
    main()
