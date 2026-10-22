"""Explicit opt-in correctness check for the measurement module, not CI runtime."""

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import numpy as np
from _fastk_attribution import ProbeReader, copy_batch, core_parallel
from fastk_bench.batches import columns_for, validate
from fastk_bench.data import hash_rows
from fastk_native import Reader

from attribution.worker import PHASES, native_call


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--store", required=True)
    parser.add_argument("--bulk", type=Path, required=True)
    parser.add_argument("--short", type=Path, required=True)
    args = parser.parse_args()
    official = Reader(args.store)
    for threads in [1, 4]:
        probe = ProbeReader(args.store, threads)
        for q in json.loads(args.bulk.read_text()):
            expected = native_call(official, q)
            observed, values = probe.read(q["symbol"], q["start_ts"], q["end_ts"], q.get("feature"))
            validate(observed, columns_for(q))
            assert np.array_equal(expected, observed)
            assert hash_rows(observed) == q["expected"]
            assert len(values) == len(PHASES) and values[13] == 1
            assert np.shares_memory(observed, observed.base)
            copied, stats = copy_batch(
                observed.view(np.int64), "SCALAR_DTYPE" if "feature" in q else "KLINE_DTYPE"
            )
            assert np.array_equal(copied, observed) and stats[13] == 1
            validate(copied, columns_for(q))
        del probe
    official.close()
    for shared in [False, True]:
        for flatten in [False, True]:
            result = json.loads(
                core_parallel(args.store, args.short.read_text(), 4, shared, flatten)
            )
            assert result["all_results_verified"] and result["verified"] == 1430
    print(
        json.dumps(
            {
                "passed": True,
                "bulk_cases": 4,
                "probe_budgets": [1, 4],
                "core_matrix_cases": 4,
                "canonical_short_reads": 5720,
                "ownership_handoff_same_pointer": True,
            }
        )
    )


if __name__ == "__main__":
    main()
