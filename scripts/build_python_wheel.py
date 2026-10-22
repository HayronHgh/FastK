"""Build a platform wheel without modifying the client source tree.

Uses only Python's standard library. The wheel RECORD hashes every entry;
native binaries are bundled in a platform-tagged wheel, never a universal wheel.
"""

from __future__ import annotations

import argparse
import base64
import csv
import hashlib
import io
import os
import tomllib
import zipfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def build(bridge: Path, output: Path, platform: str) -> Path:
    if platform not in {"win_amd64", "manylinux_2_17_x86_64", "manylinux_2_28_x86_64"}:
        raise ValueError("unsupported platform tag")
    executable = "fastk_bridge.exe" if platform == "win_amd64" else "fastk_bridge"
    if bridge.name != executable or not bridge.is_file():
        raise ValueError(f"expected built {executable}")
    version = tomllib.loads((ROOT / "clients/python/pyproject.toml").read_text())["project"][
        "version"
    ]
    info = f"fastk_client-{version}.dist-info"
    files = {
        str(path.relative_to(ROOT / "clients/python/src")).replace(os.sep, "/"): path.read_bytes()
        for path in (ROOT / "clients/python/src/fastk_client").rglob("*")
        if path.is_file() and "__pycache__" not in path.parts and "_bin" not in path.parts
    }
    files[f"fastk_client/_bin/{executable}"] = bridge.read_bytes()
    files[f"{info}/METADATA"] = (
        f"Metadata-Version: 2.4\nName: fastk-client\nVersion: {version}\nSummary: FastK subprocess storage client\nRequires-Python: >=3.11\nLicense-Expression: MIT\nLicense-File: LICENSE\n\n"
        + (ROOT / "clients/python/README.md").read_text()
    ).encode()
    files[f"{info}/WHEEL"] = (
        f"Wheel-Version: 1.0\nGenerator: fastk\nRoot-Is-Purelib: false\nTag: py3-none-{platform}\n".encode()
    )
    files[f"{info}/licenses/LICENSE"] = (ROOT / "LICENSE").read_bytes()
    record = io.StringIO(newline="")
    writer = csv.writer(record, lineterminator="\n")
    for path, data in sorted(files.items()):
        digest = base64.urlsafe_b64encode(hashlib.sha256(data).digest()).rstrip(b"=").decode()
        writer.writerow([path, "sha256=" + digest, len(data)])
    writer.writerow([f"{info}/RECORD", "", ""])
    files[f"{info}/RECORD"] = record.getvalue().encode()
    output.mkdir(parents=True, exist_ok=True)
    wheel = output / f"fastk_client-{version}-py3-none-{platform}.whl"
    with zipfile.ZipFile(wheel, "w", zipfile.ZIP_DEFLATED) as archive:
        for path, data in sorted(files.items()):
            entry = zipfile.ZipInfo(path, date_time=(2024, 1, 1, 0, 0, 0))
            entry.create_system = 3
            entry.external_attr = (0o100755 if path.endswith(executable) else 0o100644) << 16
            archive.writestr(entry, data, compress_type=zipfile.ZIP_DEFLATED)
    return wheel


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bridge", type=Path, required=True)
    parser.add_argument("--output", type=Path, default=ROOT / "dist/wheels")
    parser.add_argument("--platform", required=True)
    args = parser.parse_args()
    print(build(args.bridge, args.output, args.platform))
