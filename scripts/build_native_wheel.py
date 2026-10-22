"""Build a CPython-specific native wheel using the checked-in native profile."""

import argparse
import subprocess
import sys
from pathlib import Path


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--interpreter", default=sys.executable)
    parser.add_argument("--output", type=Path, default=Path("dist/native"))
    parser.add_argument("--offline", action="store_true")
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[1]
    command = [
        sys.executable,
        "-m",
        "maturin",
        "build",
        "--manifest-path",
        str(root / "clients/python-native/Cargo.toml"),
        "--profile",
        "native",
        "--locked",
        "--interpreter",
        args.interpreter,
        "--out",
        str(args.output.resolve()),
    ]
    if args.offline:
        command.append("--offline")
    subprocess.run(command, cwd=root, check=True)


if __name__ == "__main__":
    main()
