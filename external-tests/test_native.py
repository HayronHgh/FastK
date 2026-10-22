"""Installed native module acceptance through its public API only."""

import gc
import tempfile
import threading
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import numpy as np
from fastk_native import KLINE_DTYPE, SCALAR_DTYPE, FastKError, Reader, Writer

JAN = 1_706_745_540_000  # 2024-01-31 23:59 UTC


def bars(times):
    result = np.zeros(len(times), dtype=KLINE_DTYPE)
    result["ts"] = times
    for offset, name in enumerate(KLINE_DTYPE.names[1:]):
        result[name] = np.arange(len(times), dtype=np.int64) + 2**53 + offset
    return result


class NativeTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name) / "store"
        self.rows = bars([JAN, JAN + 60_000, JAN + 180_000, JAN + 240_000])
        self.features = np.array(
            [(int(ts), value) for ts, value in zip(self.rows["ts"], [-(2**63), -1, 0, 2**63 - 1])],
            dtype=SCALAR_DTYPE,
        )
        with Writer(self.root) as writer:
            writer.write_kline_range(
                "A",
                "1m",
                self.rows,
                timeframe_ms=60_000,
                price_scale=100_000_000,
                volume_scale=100_000_000,
            )
            writer.write_scalar_range("A", "1m", "feature", "x", self.features, timeframe_ms=60_000)

    def tearDown(self):
        self.tmp.cleanup()

    def test_reader_requires_initialization_and_cannot_attach_without_a_lease(self):
        for index, create_empty in enumerate([False, True]):
            root = Path(self.tmp.name) / f"uninitialized-{index}"
            if create_empty:
                root.mkdir()
            with self.assertRaises(FastKError) as error:
                Reader(root)
            self.assertIn("initialize", str(error.exception))
            self.assertEqual(root.exists(), create_empty)
            with Writer(root) as writer:
                writer.write_kline_range(
                    "A", "1m", self.rows, timeframe_ms=60000, price_scale=1, volume_scale=1
                )
            with Reader(root) as reader:
                np.testing.assert_array_equal(reader.read_kline_latest("A", "1m", 10), self.rows)
                with Writer(root) as writer, self.assertRaises(FastKError):
                    writer.write_kline_range(
                        "A",
                        "1m",
                        bars([JAN + 500000]),
                        timeframe_ms=60000,
                        price_scale=1,
                        volume_scale=1,
                    )

    def test_layout_range_metadata_and_empty_results(self):
        self.assertEqual(KLINE_DTYPE.itemsize, 48)
        self.assertEqual(SCALAR_DTYPE.itemsize, 16)
        self.assertEqual(
            [KLINE_DTYPE.fields[n][1] for n in KLINE_DTYPE.names], list(range(0, 48, 8))
        )
        with Reader(self.root) as reader:
            actual = reader.read_kline_range("A", "1m", JAN, JAN + 240_000)
            np.testing.assert_array_equal(actual, self.rows)
            self.assertFalse(actual.flags.writeable)
            self.assertEqual(reader.kline_info("A", "1m")["price_scale"], 100_000_000)
            self.assertEqual(reader.scalar_info("A", "1m", "feature", "x")["schema_id"], 2)
            for start, end in [(JAN + 1, JAN + 59_999), (JAN + 1, JAN), (0, 1)]:
                empty = reader.read_kline_range("A", "1m", start, end)
                self.assertEqual(empty.shape, (0,))
                self.assertEqual(empty.dtype, KLINE_DTYPE)
            np.testing.assert_array_equal(reader.read_kline_latest("A", "1m", 2), self.rows[-2:])
            self.assertEqual(reader.read_kline_latest("A", "1m", 0).shape, (0,))

    def test_arrays_own_their_lifetime_without_retaining_store_lock(self):
        reader = Reader(self.root)
        actual = reader.read_kline_range("A", "1m", JAN, JAN + 240_000)
        reader.close()
        reader.close()
        del reader
        gc.collect()
        with Writer(self.root) as writer:
            writer.write_kline_range(
                "A",
                "1m",
                bars([JAN + 300_000]),
                timeframe_ms=60_000,
                price_scale=100_000_000,
                volume_scale=100_000_000,
            )
        np.testing.assert_array_equal(actual, self.rows)
        with self.assertRaises(ValueError):
            actual["close"][0] = 0

    def test_all_scalar_predicates_preserve_int64_values(self):
        cases = [
            ({"predicate": "eq", "value": -(2**63)}, [True, False, False, False]),
            ({"predicate": "ne", "value": 0}, [True, True, False, True]),
            ({"predicate": "gt", "value": 0}, [False, False, False, True]),
            ({"predicate": "gte", "value": 0}, [False, False, True, True]),
            ({"predicate": "lt", "value": 0}, [True, True, False, False]),
            ({"predicate": "lte", "value": 0}, [True, True, True, False]),
            ({"predicate": "between", "min": -1, "max": 0}, [False, True, True, False]),
            ({"predicate": "in", "values": [-1, 2**63 - 1]}, [False, True, False, True]),
            ({"predicate": "not-in", "values": [-1, 0]}, [True, False, False, True]),
        ]
        with Reader(self.root) as reader:
            np.testing.assert_array_equal(
                reader.read_scalar_range("A", "1m", "feature", "x", JAN, JAN + 240_000),
                self.features,
            )
            for parameters, selected in cases:
                actual = reader.query_scalar_predicate(
                    "A", "1m", "feature", "x", JAN, JAN + 240_000, **parameters
                )
                np.testing.assert_array_equal(actual, self.features[selected])

    def test_invalid_input_does_not_dirty_store_or_replay_failed_write(self):
        with Writer(self.root) as writer:
            with self.assertRaises(FastKError) as error:
                writer.write_kline_range(
                    "A",
                    "1m",
                    self.rows[::-1],
                    timeframe_ms=60_000,
                    price_scale=100_000_000,
                    volume_scale=100_000_000,
                )
            self.assertEqual(error.exception.code, "INVALID_INPUT")
            with self.assertRaises(FastKError):
                writer.write_kline_range(
                    "A",
                    "1m",
                    self.rows,
                    timeframe_ms=60_000,
                    price_scale=100_000_000,
                    volume_scale=100_000_000,
                )
        with Reader(self.root) as reader:
            np.testing.assert_array_equal(
                reader.read_kline_range("A", "1m", JAN, JAN + 240_000), self.rows
            )
        self.assertFalse((self.root / ".fastk.recovery.pending").exists())

    def test_dtype_shape_and_integer_overflow_rejected(self):
        with Writer(self.root) as writer:
            for invalid in [
                np.zeros(4, dtype="f8"),
                np.zeros((4, 6), dtype="i8"),
                np.zeros(4, dtype=[(n, "<u8") for n in KLINE_DTYPE.names]),
            ]:
                with self.assertRaises(ValueError):
                    writer.write_kline_range(
                        "A", "1m", invalid, timeframe_ms=60_000, price_scale=1, volume_scale=1
                    )
        with Reader(self.root) as reader:
            with self.assertRaises(OverflowError):
                reader.read_kline_range("A", "1m", 2**63, 2**63)
            with self.assertRaises(TypeError):
                reader.read_kline_range("A", "1m", 0.5, 1)
            with self.assertRaises(FastKError):
                reader.read_kline_range("missing", "1m", 0, 1)

    def test_noncontiguous_input_and_independent_readers(self):
        other = Path(self.tmp.name) / "other"
        with Writer(other) as writer:
            writer.write_kline_range(
                "A", "1m", self.rows[::2], timeframe_ms=60_000, price_scale=1, volume_scale=1
            )
        with Reader(other) as first, Reader(other) as second:
            np.testing.assert_array_equal(first.read_kline_latest("A", "1m", 10), self.rows[::2])
            np.testing.assert_array_equal(second.read_kline_latest("A", "1m", 10), self.rows[::2])
            with Writer(other) as writer, self.assertRaises(FastKError):
                writer.write_kline_range(
                    "A",
                    "1m",
                    bars([JAN + 500_000]),
                    timeframe_ms=60_000,
                    price_scale=1,
                    volume_scale=1,
                )

    def test_shared_reader_parallel_chunks_and_close_race(self):
        root = Path(self.tmp.name) / "large"
        large = bars(JAN - 120_000_000 + np.arange(120_000, dtype=np.int64) * 1_000)
        with Writer(root) as writer:
            writer.write_kline_range(
                "A", "1s", large, timeframe_ms=1_000, price_scale=1, volume_scale=1
            )
        with Reader(root, threads=1) as serial, Reader(root, threads=4) as parallel:
            np.testing.assert_array_equal(parallel.read_kline_latest("A", "1s", len(large)), large)
            np.testing.assert_array_equal(
                parallel.read_kline_range("A", "1s", int(large["ts"][0]), int(large["ts"][-1])),
                serial.read_kline_range("A", "1s", int(large["ts"][0]), int(large["ts"][-1])),
            )
            barrier = threading.Barrier(8)

            def read(worker):
                offset = 1_000 + worker * 1_000
                expected = large[offset : offset + 127]
                barrier.wait()
                for _ in range(128):
                    actual = parallel.read_kline_range(
                        "A", "1s", int(expected["ts"][0]), int(expected["ts"][-1])
                    )
                    np.testing.assert_array_equal(actual, expected)

            with ThreadPoolExecutor(max_workers=8) as pool:
                list(pool.map(read, range(8)))
        reader = Reader(root, threads=4)
        with ThreadPoolExecutor(max_workers=2) as pool:
            future = pool.submit(reader.read_kline_latest, "A", "1s", len(large))
            reader.close()
            try:
                result = future.result()
                np.testing.assert_array_equal(result, large)
            except FastKError as error:
                self.assertIn("closed", str(error))
        with self.assertRaises(FastKError):
            reader.read_kline_latest("A", "1s", 1)

    def test_parallel_scalar_and_truncated_payload_fail_without_partial_result(self):
        root = Path(self.tmp.name) / "large-scalar"
        values = np.zeros(300_000, dtype=SCALAR_DTYPE)
        values["ts"] = JAN - 400_000_000 + np.arange(len(values), dtype=np.int64) * 1000
        values["value"] = np.arange(len(values), dtype=np.int64) - 150_000
        with Writer(root) as writer:
            writer.write_scalar_range("A", "1s", "feature", "x", values, timeframe_ms=1000)
        with Reader(root, threads=4) as reader:
            np.testing.assert_array_equal(
                reader.read_scalar_range(
                    "A", "1s", "feature", "x", int(values["ts"][0]), int(values["ts"][-1])
                ),
                values,
            )
            # Simulate out-of-band corruption after the header/index are cached.
            # This forces failed payload tasks, rather than failing while loading an index.
            chunk = next(root.rglob("*.chunk"))
            with chunk.open("rb+") as file:
                file.truncate(128 + 5000 * 16)
            with self.assertRaises(FastKError):
                reader.read_scalar_range(
                    "A", "1s", "feature", "x", int(values["ts"][0]), int(values["ts"][-1])
                )
            np.testing.assert_array_equal(
                reader.read_scalar_range(
                    "A", "1s", "feature", "x", int(values["ts"][0]), int(values["ts"][99])
                ),
                values[:100],
            )


if __name__ == "__main__":
    unittest.main()
