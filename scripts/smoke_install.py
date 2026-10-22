"""Install a wheel offline into a temporary venv and exercise it without the checkout."""

import argparse
import os
import shutil
import subprocess
import tempfile
import venv
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--wheel", type=Path, required=True)
    parser.add_argument("--admin", type=Path)
    args = parser.parse_args()
    wheel = args.wheel.resolve()
    with tempfile.TemporaryDirectory(prefix="fastk-installed-") as work:
        work = Path(work)
        venv.EnvBuilder(with_pip=True).create(work / "venv")
        executable = work / "venv" / ("Scripts/python.exe" if os.name == "nt" else "bin/python")
        env = os.environ.copy()
        env.pop("PYTHONPATH", None)
        env.pop("PYTHONHOME", None)
        env["PATH"] = str(executable.parent) + (
            os.pathsep + str(Path(os.environ["SystemRoot"]) / "System32")
            if os.name == "nt"
            else ":/usr/bin:/bin"
        )
        subprocess.run(
            [
                str(executable),
                "-m",
                "pip",
                "install",
                "--no-index",
                "--no-deps",
                str(wheel),
            ],
            cwd=work,
            env=env,
            check=True,
        )
        shutil.copy2(ROOT / "external-tests/smoke_installed.py", work / "smoke.py")
        command = [str(executable), str(work / "smoke.py")]
        if args.admin:
            command.append(str(args.admin.resolve()))
        subprocess.run(command, cwd=work, env=env, check=True)


if __name__ == "__main__":
    main()
