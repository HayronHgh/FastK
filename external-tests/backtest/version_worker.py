"""Historical public CLI delivery into the unchanged backtest measurement loop."""

import hashlib
import json
import subprocess
import sys
import time
from operator import itemgetter
from pathlib import Path

import numpy as np
from fastk_bench.batches import dtype
from fastk_bench.data import FIELDS
from fastk_bench.process import announce

from . import worker


def measure(config):
    if config["version"] == "public-814c566":
        binary = Path(config["binary"])
        assert hashlib.sha256(binary.read_bytes()).hexdigest() == config["binary_sha256"]

        def read_batch(name, store, manifest, queries):
            start = time.perf_counter_ns()
            frames = {}
            for q in queries:
                command = [
                    str(binary),
                    "read-kline-range",
                    "--root",
                    store,
                    "--symbol",
                    q["symbol"],
                    "--timeframe",
                    "1m",
                    "--start-ts",
                    str(q["start_ts"]),
                    "--end-ts",
                    str(q["end_ts"]),
                ]
                # The original release has no resident session/native module.
                # Each public CLI command opens and closes its own store.
                result = subprocess.run(command, capture_output=True, check=True, timeout=120)
                records = json.loads(result.stdout)["records"]
                bars = np.fromiter(
                    map(itemgetter(*FIELDS), records), dtype=dtype(FIELDS), count=len(records)
                )
                bars.flags.writeable = False
                frames[q["symbol"]] = bars
                del result, records
            return frames, {"open_ns": 0, "read_ns": time.perf_counter_ns() - start, "close_ns": 0}

        worker.read_batch = read_batch
        worker.bootstrap = lambda config: None
    result = worker.measure(config)
    result["version"] = config["version"]
    return result


if __name__ == "__main__":
    config = json.loads(Path(sys.argv[1]).read_text())
    announce(config)
    Path(config["output"]).write_text(json.dumps(measure(config), indent=2, allow_nan=False) + "\n")
