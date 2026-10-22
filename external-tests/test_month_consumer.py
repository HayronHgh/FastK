"""Caller-owned position state survives independently prefetched month batches."""

import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import numpy as np
from fastk_native import KLINE_DTYPE, Reader, Writer


class MonthConsumerTests(unittest.TestCase):
    def test_prefetch_parallel_execution_ordered_with_carried_position(self):
        bounds = [
            (1706659200000, 1706745599999),
            (1706745600000, 1709251199999),
            (1709251200000, 1709337599999),
        ]
        rows = np.array(
            [
                (bounds[0][0], 10, 10, 10, 10, 1),
                (bounds[0][1] - 59999, 12, 12, 12, 12, 1),
                (bounds[1][0], 15, 15, 15, 15, 1),
                (bounds[1][1] - 59999, 18, 18, 18, 18, 1),
                (bounds[2][0], 20, 20, 20, 20, 1),
            ],
            dtype=KLINE_DTYPE,
        )

        def consume(batches):
            position = None
            cash = 0
            events = []
            for batch in batches:
                for row in batch:
                    price = int(row["close"])
                    if position is None and price == 12:
                        position = price
                        events.append((int(row["ts"]), "open"))
                    elif position is not None and price >= 18:
                        cash += price - position
                        position = None
                        events.append((int(row["ts"]), "close"))
            return cash, position, events

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "store"
            with Writer(root) as writer:
                writer.write_kline_range(
                    "A", "1m", rows, timeframe_ms=60000, price_scale=1, volume_scale=1
                )
            with Reader(root) as reader, ThreadPoolExecutor(max_workers=3) as pool:
                futures = [
                    pool.submit(reader.read_kline_range, "A", "1m", start, end)
                    for start, end in reversed(bounds)
                ]
                batches = list(reversed([future.result() for future in futures]))
                full = reader.read_kline_range("A", "1m", bounds[0][0], bounds[-1][1])
                self.assertEqual(consume(batches), consume([full]))
                self.assertEqual(consume(batches)[:2], (6, None))
                self.assertNotEqual(sum(consume([batch])[0] for batch in batches), 6)


if __name__ == "__main__":
    unittest.main()
