"""Linux-only causal control. No dummy descriptor survives into timed reads."""

import fcntl
import json
import os
import sys
import time
from pathlib import Path

from attribution import worker


def fdsize():
    return next(
        line
        for line in Path("/proc/self/status").read_text().splitlines()
        if line.startswith("FDSize:")
    )


def main():
    before = fdsize()
    start = time.perf_counter_ns()
    source = os.open("/dev/null", os.O_RDONLY)
    try:
        duplicate = fcntl.fcntl(source, fcntl.F_DUPFD_CLOEXEC, 2048)
        os.close(duplicate)
    finally:
        os.close(source)
    elapsed = time.perf_counter_ns() - start
    after = fdsize()
    worker.main()
    config = json.loads(Path(sys.argv[1]).read_text())
    output = Path(config["output"])
    result = json.loads(output.read_text())
    result["fd_preparation"] = {"before": before, "after": after, "ns": elapsed}
    output.write_text(json.dumps(result, indent=2) + "\n")


if __name__ == "__main__":
    main()
