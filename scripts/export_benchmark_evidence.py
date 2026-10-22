"""Export compact, checksummed benchmark evidence without copying datasets/stores."""

from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import shutil
from pathlib import Path


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--results", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    results = args.results.resolve()
    summary = json.loads((results / "summary.json").read_text())
    if summary.get("all_results_verified") is not True:
        parser.error("only completed, verified runs may be exported")
    output = args.output
    output.mkdir(parents=True, exist_ok=False)
    for name in ["summary.json", "environment.json", "REPORT.md"]:
        shutil.copy2(results / name, output / name)
    input_manifest = results / "input-manifest.json"
    if not input_manifest.exists():
        input_manifest = results / "input/manifest.json"
    shutil.copy2(input_manifest, output / "input-manifest.json")
    for name in ["append-input", "revised-input"]:
        maintenance_manifest = results / name / "manifest.json"
        if maintenance_manifest.is_file():
            shutil.copy2(maintenance_manifest, output / (name + "-manifest.json"))
    raw = output / "raw"
    raw.mkdir()
    for path in sorted(results.glob("*.json")):
        if path.name in {
            "summary.json",
            "environment.json",
            "input-manifest.json",
        } or path.name.endswith("-config.json"):
            continue
        (raw / (path.name + ".gz")).write_bytes(gzip.compress(path.read_bytes(), mtime=0))
    checksums = []
    for path in sorted(output.rglob("*")):
        if path.is_file():
            checksums.append(
                f"{hashlib.sha256(path.read_bytes()).hexdigest()}  {path.relative_to(output).as_posix()}"
            )
    (output / "SHA256SUMS").write_text("\n".join(checksums) + "\n", encoding="ascii")
    print(output)


if __name__ == "__main__":
    main()
