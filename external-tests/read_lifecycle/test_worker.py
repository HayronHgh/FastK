"""Check measurement ownership and abort behavior without engine test hooks."""

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import numpy as np
from fastk_bench.batches import dtype
from fastk_bench.data import FIELDS, hash_rows

from read_lifecycle.worker import measure


class OwnershipTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        root = Path(self.tmp.name)
        self.events = []
        self.batches = {}
        queries = []
        for index in range(2):
            batch = np.array([(index + 1, 1, 2, 0, 1, 3)], dtype=dtype(FIELDS))
            batch.flags.writeable = False
            self.batches[index + 1] = batch
            queries.append(
                dict(
                    kind="range",
                    symbol="A",
                    start_ts=index + 1,
                    end_ts=index + 1,
                    case="short",
                    expected=hash_rows(batch),
                )
            )
        (root / "manifest.json").write_text("{}")
        (root / "queries.json").write_text(json.dumps(queries))
        self.config = dict(
            backend="sqlite",
            mode="cycle",
            repetitions=1,
            store=str(root),
            manifest=str(root / "manifest.json"),
            queries=str(root / "queries.json"),
            pid_file=str(root / "worker.pid"),
        )

    def connection(self, fail=False):
        events, batches = self.events, self.batches

        class Backend:
            def __init__(self):
                self.closed = False
                events.append("open")

            def query(self, query):
                assert not self.closed
                events.append("query")
                if fail:
                    raise RuntimeError("injected read failure")
                return batches[query["start_ts"]]

            def close(self):
                assert not self.closed
                self.closed = True
                events.append("close")

        return Backend()

    def test_each_cycle_opens_and_closes_before_validation(self):
        with patch(
            "read_lifecycle.worker.backends.connect", side_effect=lambda *a, **k: self.connection()
        ):
            result = measure(self.config)
        self.assertEqual(self.events, ["open", "query", "close"] * 2)
        self.assertEqual(len(result["samples"]), 2)
        for sample in result["samples"]:
            self.assertEqual(
                sample["total_ns"], sum(sample[k] for k in ("open_ns", "read_ns", "close_ns"))
            )

    def test_read_failure_closes_once_and_never_replays(self):
        with patch(
            "read_lifecycle.worker.backends.connect",
            side_effect=lambda *a, **k: self.connection(fail=True),
        ):
            with self.assertRaisesRegex(RuntimeError, "injected read failure"):
                measure(self.config)
        self.assertEqual(self.events, ["open", "query", "close"])

    def test_hot_mode_keeps_only_one_request_handle(self):
        self.config["mode"] = "hot-local"
        with patch(
            "read_lifecycle.worker.backends.connect", side_effect=lambda *a, **k: self.connection()
        ):
            result = measure(self.config)
        self.assertEqual(self.events.count("open"), 1)
        self.assertEqual(self.events.count("close"), 1)
        self.assertEqual(len(result["samples"]), 2)
