"""Check the selected binary before running the unchanged public read matrix."""

import hashlib
import json
import sys
from pathlib import Path

import fastk_native._native as native
from fastk_bench.matrix_worker import measure


def main():
    config = json.loads(Path(sys.argv[1]).read_text())
    module = Path(native.__file__)
    digest = hashlib.sha256(module.read_bytes()).hexdigest()
    if digest != config["expected_native_sha256"]:
        raise AssertionError(f"wrong native binary: {module}")
    result = measure(config)
    result.update(native_module=str(module), native_sha256=digest)
    Path(config["output"]).write_text(json.dumps(result, indent=2) + "\n")


if __name__ == "__main__":
    main()
