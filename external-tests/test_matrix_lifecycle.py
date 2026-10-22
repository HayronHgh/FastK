"""Consumer ownership/failure tests; production code needs no instrumentation."""

import json
import tempfile
import threading
import unittest
from pathlib import Path
from unittest.mock import patch

import numpy as np
from fastk_bench import matrix_worker
from fastk_bench.batches import dtype
from fastk_bench.data import FIELDS, hash_rows


class MatrixLifecycleTests(unittest.TestCase):
    def run_case(self, backend, *, fail_open=False, fail_query=False):
        batch = np.zeros(2, dtype=dtype(FIELDS))
        batch["ts"] = [1, 2]
        batch.flags.writeable = False
        expected = hash_rows(batch)
        handles = []
        lock = threading.Lock()

        class Handle:
            def __init__(self):
                self.owner = threading.get_ident()
                self.close_threads = []
                self.calls = []
                self.closed = False

            def query(self, query):
                with lock:
                    if self.closed:
                        raise AssertionError("reader closed while workers are active")
                    self.calls.append(query["id"])
                    if fail_query and query["id"] == 0:
                        raise RuntimeError("injected read failure")
                return batch

            def close(self):
                with lock:
                    self.close_threads.append(threading.get_ident())
                    self.closed = True

        def connect(*args, **kwargs):
            if fail_open:
                raise RuntimeError("injected open failure")
            handle = Handle()
            with lock:
                handles.append(handle)
            return handle

        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            (root / "manifest.json").write_text("{}")
            queries = [
                dict(id=i, round=i // 8, case="short", kind="range", expected=expected)
                for i in range(16)
            ]
            (root / "queries.json").write_text(json.dumps(queries))
            config = dict(
                backend=backend,
                mode="outer",
                budget=4,
                timeout=5,
                store=str(root),
                manifest=str(root / "manifest.json"),
                queries=str(root / "queries.json"),
                pid_file=str(root / "pid"),
            )
            with patch.object(matrix_worker, "connect", side_effect=connect):
                if fail_query or fail_open:
                    with self.assertRaises((RuntimeError, threading.BrokenBarrierError)):
                        matrix_worker.measure(config)
                    result = None
                else:
                    result = matrix_worker.measure(config)
        return result, handles

    def test_fastk_reader_is_shared_and_closed_once_after_workers(self):
        result, handles = self.run_case("fastk")
        self.assertEqual(len(handles), 1)
        self.assertEqual(handles[0].close_threads, [handles[0].owner])
        self.assertEqual(len(handles[0].calls), 20)  # Four warmups + 16 timed reads.
        self.assertEqual(len(result["samples"]), 16)
        self.assertTrue(result["all_results_verified"])
        self.assertEqual(result["reader_ownership"], "shared")

    def test_sql_connection_is_opened_and_closed_on_its_worker(self):
        result, handles = self.run_case("sqlite")
        self.assertEqual(len(handles), 4)
        self.assertTrue(all(h.close_threads == [h.owner] for h in handles))
        self.assertEqual(result["reader_ownership"], "per_worker")

    def test_shared_read_failure_aborts_workers_closes_once_and_is_not_replayed(self):
        _, handles = self.run_case("fastk", fail_query=True)
        self.assertEqual(len(handles), 1)
        self.assertEqual(handles[0].calls.count(0), 1)
        self.assertEqual(len(handles[0].close_threads), 1)

    def test_worker_open_failure_aborts_the_barrier(self):
        _, handles = self.run_case("sqlite", fail_open=True)
        self.assertEqual(handles, [])


if __name__ == "__main__":
    unittest.main()
