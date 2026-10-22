"""Forward compatibility of v0.2 writes with the preserved v0.1 bridge."""

import os
import tempfile
import unittest

import numpy as np
from fastk_native import KLINE_DTYPE, SCALAR_DTYPE, Writer


@unittest.skipUnless(
    os.environ.get("FASTK_BASELINE_BRIDGE_BIN"), "requires preserved pre-v0.2 bridge"
)
class DiskCompatibilityTests(unittest.TestCase):
    def test_old_bridge_reads_new_native_int64_records_and_sidecars(self):
        from fastk_client import Client

        times = [1706745540000, 1706745600000]
        bars = np.array(
            [(ts, 2**53 + 1, 2**63 - 1, -(2**63), -1, 7) for ts in times], dtype=KLINE_DTYPE
        )
        values = np.array([(times[0], -(2**63)), (times[1], 2**63 - 1)], dtype=SCALAR_DTYPE)
        with tempfile.TemporaryDirectory() as root:
            with Writer(root) as writer:
                writer.write_kline_range(
                    "A",
                    "1m",
                    bars,
                    timeframe_ms=60000,
                    price_scale=100000000,
                    volume_scale=100000000,
                )
                writer.write_scalar_range("A", "1m", "feature", "x", values, timeframe_ms=60000)
            client = Client(root, binary=os.environ["FASTK_BASELINE_BRIDGE_BIN"])
            actual = client.read_kline_range("A", "1m", times[0], times[-1])
            self.assertEqual(
                [tuple(row[name] for name in KLINE_DTYPE.names) for row in actual["records"]],
                bars.tolist(),
            )
            actual = client.read_scalar_range("A", "1m", "feature", "x", times[0], times[-1])
            self.assertEqual(
                [(row["ts"], row["value"]) for row in actual["records"]], values.tolist()
            )
            actual = client.query_scalar_predicate(
                "A", "1m", "feature", "x", times[0], times[-1], predicate="eq", value=2**63 - 1
            )
            self.assertEqual(
                [(row["ts"], row["value"]) for row in actual["matches"]], [values.tolist()[-1]]
            )


if __name__ == "__main__":
    unittest.main()
