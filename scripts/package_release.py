"""Build verified release artifacts, or explicitly labelled local development artifacts."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import subprocess
import tomllib
import zipfile
from datetime import datetime, timezone
from pathlib import Path

from build_python_wheel import build as build_wheel

ROOT = Path(__file__).resolve().parents[1]


def capture(*args: str) -> str:
    return subprocess.check_output(
        args, cwd=ROOT, text=True, encoding="utf-8", stderr=subprocess.PIPE
    ).strip()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--target")
    parser.add_argument(
        "--development",
        action="store_true",
        help="allow dirty/no checkout; not an official release",
    )
    args = parser.parse_args()
    if not args.development:
        try:
            dirty = capture("git", "status", "--porcelain", "--untracked-files=normal")
        except (OSError, subprocess.CalledProcessError):
            parser.error(
                "official releases require a Git checkout; use --development for local artifacts"
            )
        if dirty:
            parser.error(
                "official releases require a clean commit; use --development for local artifacts"
            )
    version = tomllib.loads((ROOT / "Cargo.toml").read_text())["package"]["version"]
    python_version = tomllib.loads((ROOT / "clients/python/pyproject.toml").read_text())["project"][
        "version"
    ]
    if python_version != version.replace("-rc.", "rc"):
        parser.error("Rust and Python versions disagree")
    rustc = capture("rustc", "-vV")
    host = next(line.split(": ", 1)[1] for line in rustc.splitlines() if line.startswith("host:"))
    target = args.target or host
    tags = {
        "x86_64-pc-windows-msvc": "win_amd64",
        "x86_64-pc-windows-gnu": "win_amd64",
        "x86_64-unknown-linux-musl": "manylinux_2_17_x86_64",
        "x86_64-unknown-linux-gnu": "manylinux_2_28_x86_64",
    }
    if target not in tags:
        parser.error("supported release targets: " + ", ".join(tags))
    if target.endswith("linux-gnu") and not args.development:
        parser.error(
            "official Linux artifacts use the static musl target; GNU builds are development-only"
        )
    env = os.environ.copy()
    if target in {"x86_64-pc-windows-msvc", "x86_64-pc-windows-gnu"}:
        env["RUSTFLAGS"] = (env.get("RUSTFLAGS", "") + " -C target-feature=+crt-static").strip()
    build = ["cargo", "build", "--locked", "--release", "--examples"]
    if args.target:
        build.extend(["--target", target])
    subprocess.run(build, cwd=ROOT, env=env, check=True)
    package = ["cargo", "package", "--locked"]
    if args.development:
        package.extend(["--allow-dirty", "--no-verify"])
    subprocess.run(package, cwd=ROOT, env=env, check=True)
    target_root = Path(os.environ.get("CARGO_TARGET_DIR", str(ROOT / "target")))
    if not target_root.is_absolute():
        target_root = ROOT / target_root
    release = target_root
    if args.target:
        release /= target
    release /= "release/examples"
    suffix = "-development" if args.development else ""
    output = ROOT / "dist" / f"fastk-{version}-{target}{suffix}"
    # No recursive delete: refuse stale output instead of risking user artifacts.
    if output.exists():
        parser.error(f"output already exists: {output}; select/archive it before rebuilding")
    output.mkdir(parents=True)
    extension = ".exe" if "windows" in target else ""
    for directory in ["bin", "crate", "docs", "schemas", "python", "validation"]:
        (output / directory).mkdir()
    for binary in ["fastk_bridge", "fastk_admin"]:
        shutil.copy2(release / (binary + extension), output / "bin" / (binary + extension))
    shutil.copy2(target_root / "package" / f"fastk-{version}.crate", output / "crate")
    shutil.copy2(ROOT / "LICENSE", output / "LICENSE")
    shutil.copy2(ROOT / "README.md", output / "docs/README.md")
    for document in sorted((ROOT / "docs").rglob("*")):
        if document.is_file():
            document_target = output / "docs" / document.relative_to(ROOT / "docs")
            document_target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(document, document_target)
    for schema in sorted((ROOT / "schemas").glob("*.json")):
        shutil.copy2(schema, output / "schemas" / schema.name)
    for tool in sorted((ROOT / "external-tests").rglob("*")):
        if (
            tool.is_file()
            and tool.suffix in {".py", ".lock", ".md", ".rs", ".toml"}
            and not {"__pycache__", "target", "build"}.intersection(
                tool.relative_to(ROOT / "external-tests").parts
            )
        ):
            validation_target = output / "validation" / tool.relative_to(ROOT / "external-tests")
            validation_target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(tool, validation_target)
    shutil.copy2(
        ROOT / "scripts/export_benchmark_evidence.py",
        output / "validation/export_benchmark_evidence.py",
    )
    build_wheel(output / "bin" / ("fastk_bridge" + extension), output / "python", tags[target])
    kinds = {
        "bin": "binary",
        "crate": "crate",
        "docs": "document",
        "schemas": "schema",
        "python": "wheel",
        "validation": "validation",
    }
    artifacts = []
    for path in sorted(output.rglob("*")):
        if path.is_file():
            relative = path.relative_to(output)
            artifacts.append(
                {
                    "path": relative.as_posix(),
                    "kind": "license"
                    if relative.as_posix() == "LICENSE"
                    else kinds[relative.parts[0]],
                    "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
                }
            )
    try:
        commit = capture("git", "rev-parse", "HEAD")
    except (OSError, subprocess.CalledProcessError):
        commit = "unknown"
    manifest = {
        "name": "fastk",
        "version": version,
        "target": target,
        "git_commit": commit,
        "rustc": rustc,
        "cargo": capture("cargo", "--version"),
        "build_time_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "development": args.development,
        "contract_version": 1,
        "artifacts": artifacts,
        "stable_surfaces": [
            "FastKStore",
            "KlineRecord",
            "ScalarRecord",
            "DatasetRef",
            "fastk_bridge JSON contract v1",
            "fastk_client.Client",
        ],
        "experimental_surfaces": [
            "snapshot APIs (RC)",
            "high-frequency records",
            "internal index types",
            "benchmark helpers",
        ],
    }
    (output / "release_manifest.json").write_text(
        json.dumps(manifest, indent=2) + "\n", encoding="utf-8"
    )
    sums = [
        f"{hashlib.sha256(path.read_bytes()).hexdigest()}  {path.relative_to(output).as_posix()}"
        for path in sorted(output.rglob("*"))
        if path.is_file()
    ]
    (output / "SHA256SUMS").write_text("\n".join(sums) + "\n", encoding="ascii")
    with zipfile.ZipFile(Path(str(output) + ".zip"), "w", zipfile.ZIP_DEFLATED) as archive:
        for path in sorted(output.rglob("*")):
            if path.is_file():
                archive.write(path, path.relative_to(output.parent))
    print(output)


if __name__ == "__main__":
    main()
