"""Session faults use real child processes; storage cases use the actual bridge."""

import io
import json
import os
import struct
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from unittest.mock import patch

from fastk_client import Client, FastKError, FastKProtocolError, FastKTimeout

sys.path.insert(0, str(Path(__file__).parent / "fixtures"))
from session_lock import hold_lock  # noqa: E402

START = 1706745600000


class FrameReads(unittest.TestCase):
    def test_fragmented_readinto_and_exact_integer_unicode_values(self):
        from fastk_client import _session

        class Fragments(io.BytesIO):
            def readinto(self, target):
                return super().readinto(target[:3])

        value = {"symbol": "中文", "records": [{"ts": -(2**63), "value": 2**63 - 1}]}
        body = json.dumps(value, ensure_ascii=False).encode()
        received = _session._read_frame(Fragments(struct.pack("<Q", len(body)) + body))
        self.assertIsInstance(received, bytearray)
        self.assertEqual(json.loads(received), value)

    def test_bad_sizes_rejected_before_body_allocation_and_truncation(self):
        from fastk_client import _session

        for size in [0, _session.MAX_FRAME_BYTES + 1]:
            allocated = []

            def allocate(amount):
                allocated.append(amount)
                return bytearray(amount)

            with patch.object(_session, "bytearray", side_effect=allocate, create=True):
                with self.assertRaises(_session.SessionProtocolError):
                    _session._read_frame(io.BytesIO(struct.pack("<Q", size)))
            self.assertEqual(allocated, [8])
        for data in [b"\x01", struct.pack("<Q", 10) + b"{}"]:
            with self.assertRaises(_session.SessionProtocolError):
                _session._read_frame(io.BytesIO(data))


def wait_for(predicate):
    deadline = time.monotonic() + 5
    while not predicate():
        if time.monotonic() > deadline:
            raise AssertionError("child did not reach expected state")
        time.sleep(0.005)


class SessionFaults(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.binary = self.root / "fake bridge.exe"
        self.binary.touch()
        self.log = self.root / "calls.jsonl"
        self.children = []
        self.mode = "normal"
        self.popen = subprocess.Popen
        self.launcher = patch("subprocess.Popen", side_effect=self.launch)
        self.launcher.start()
        self.addCleanup(self.launcher.stop)
        self.addCleanup(self.assert_reaped)

    def launch(self, args, **kwargs):
        if str(args[0]) == str(self.binary):
            args = [
                sys.executable,
                "-u",
                str(Path(__file__).parent / "fixtures/session_worker.py"),
                self.mode,
                str(self.log),
                *args[1:],
            ]
        child = self.popen(args, **kwargs)
        self.children.append(child)
        return child

    def client(self, timeout=2):
        client = Client(self.root / "data", binary=self.binary, timeout=timeout)
        self.addCleanup(client.close)
        return client

    def calls(self):
        return (
            [json.loads(line) for line in self.log.read_text().splitlines()]
            if self.log.exists()
            else []
        )

    def assert_reaped(self):
        for child in self.children:
            if child.poll() is None:
                child.kill()
                child.wait()
                self.fail("session left a live child")
        self.assertFalse(any(t.name.startswith("fastk-session-") for t in threading.enumerate()))

    def test_scope_reuses_one_child_and_close_is_idempotent(self):
        c = self.client()
        with c:
            self.assertEqual(c.init(), {"done": True})
            pid = c._session.process.pid
            self.assertEqual(c.init(), {"done": True})
            self.assertEqual(pid, c._session.process.pid)
        c.close()
        self.assertEqual(len(self.children), 2)  # constructor version + worker
        self.assertEqual([r["id"] for r in self.calls()], [1, 2])
        with self.assertRaises(FastKError) as error:
            c.init()
        self.assertEqual(error.exception.code, "CLIENT_CLOSED")

    def test_timeout_reaps_and_no_failed_write_is_replayed(self):
        self.mode = "hang"
        c = self.client()
        with c:
            c.timeout = 0.2
            with self.assertRaises(FastKTimeout):
                c.write_scalar_range("BTC", "1m", "feature", "x", [{"ts": 1, "value": 9}])
            self.assertIsNotNone(c._session.process.poll())
            with self.assertRaises(FastKError) as error:
                c.init()
            self.assertEqual(error.exception.code, "SESSION_BROKEN")
        self.assertEqual(len(self.children), 2)
        self.assertEqual(len(self.calls()), 1)

    def test_write_blocked_in_pipe_is_interruptible(self):
        self.mode = "no-read"
        c = self.client()
        with c:
            c.timeout = 0.2
            with self.assertRaises(FastKTimeout):
                c.write_scalar_range(
                    "BTC", "1m", "feature", "x", [{"ts": i, "value": i} for i in range(20000)]
                )
            self.assertIsNotNone(c._session.process.poll())
        self.assertEqual(self.calls(), [])

    def test_partial_body_timeout_and_close_reap_without_replay(self):
        self.mode = "partial-body"
        c = self.client()
        with c:
            c.timeout = 0.2
            with self.assertRaises(FastKTimeout):
                c.init()
            with self.assertRaises(FastKError) as error:
                c.init()
            self.assertEqual(error.exception.code, "SESSION_BROKEN")
        self.assertEqual(len(self.calls()), 1)
        with self.client() as other, ThreadPoolExecutor(max_workers=1) as pool:
            pending = pool.submit(other.init)
            wait_for(lambda: len(self.calls()) == 2)
            other.close()
            with self.assertRaises(FastKError):
                pending.result(timeout=2)
        self.assertEqual(len(self.calls()), 2)

    def test_body_allocation_failure_poisons_and_reaps(self):
        from fastk_client import _session

        def allocate(amount):
            if amount > 8:
                raise MemoryError("fixture allocation failure")
            return bytearray(amount)

        with self.client() as c:
            with patch.object(_session, "bytearray", side_effect=allocate, create=True):
                with self.assertRaises(FastKProtocolError):
                    c.init()
            with self.assertRaises(FastKError) as error:
                c.init()
            self.assertEqual(error.exception.code, "SESSION_BROKEN")
        self.assertEqual(len(self.calls()), 1)

    def test_fragmented_worker_frames(self):
        self.mode = "fragmented"
        with self.client() as c:
            self.assertEqual(c.init(), {"done": True})

    def test_startup_failures_reap_without_fallback(self):
        for mode in ["startup-hang", "bad-handshake"]:
            with self.subTest(mode=mode):
                self.mode = mode
                c = self.client()
                c.timeout = 0.2
                with self.assertRaises((FastKTimeout, FastKProtocolError)):
                    c.__enter__()
                with self.assertRaises(FastKError) as error:
                    c.init()
                self.assertEqual(error.exception.code, "CLIENT_CLOSED")
                self.assert_reaped()

    def test_malformed_frames_and_crashes_poison_session(self):
        for mode in ["exit", "truncated", "oversized", "wrong-id", "bad-json", "bad-error"]:
            with self.subTest(mode=mode):
                self.mode = mode
                c = self.client()
                before = len(self.calls())
                count = len(self.children)
                with c:
                    with self.assertRaises(FastKProtocolError):
                        c.init()
                    with self.assertRaises(FastKError) as error:
                        c.init()
                    self.assertEqual(error.exception.code, "SESSION_BROKEN")
                self.assertEqual(len(self.calls()), before + 1)
                self.assertEqual(len(self.children), count + 1)

    def test_stderr_is_continuously_drained_and_bounded(self):
        self.mode = "stderr"
        with self.client() as c:
            self.assertEqual(c.init(), {"done": True})
            self.assertLessEqual(sum(map(len, c._session._stderr)), 65536)

    def test_queue_timeout_never_interrupts_or_sends_another_request(self):
        self.mode = "delay"
        with self.client() as c, ThreadPoolExecutor(max_workers=1) as pool:
            first = pool.submit(c.init)
            wait_for(lambda: len(self.calls()) == 1)
            c.timeout = 0.03
            with self.assertRaises(FastKTimeout) as error:
                c.init()
            self.assertIn("before dispatch", str(error.exception))
            self.assertIsNone(c._session.process.poll())
            self.assertEqual(first.result(timeout=2), {"done": True})
            c.timeout = 2
            self.assertEqual(c.init(), {"done": True})
        self.assertEqual(len(self.calls()), 2)

    def test_close_interrupts_inflight_and_reaps(self):
        self.mode = "hang"
        c = self.client()
        with c, ThreadPoolExecutor(max_workers=1) as pool:
            first = pool.submit(c.init)
            wait_for(lambda: len(self.calls()) == 1)
            c.close()
            with self.assertRaises(FastKError):
                first.result(timeout=2)
        self.assertEqual(len(self.calls()), 1)

    def test_close_during_frame_construction_never_enqueues_or_waits(self):
        from fastk_client import _session

        prepared, release = threading.Event(), threading.Event()
        encode = json.dumps

        def pause(value, *args, **kwargs):
            if isinstance(value, dict) and "id" in value and "args" in value:
                prepared.set()
                if not release.wait(5):
                    raise AssertionError("frame construction not released")
            return encode(value, *args, **kwargs)

        with Client(self.root, binary=self.binary, timeout=10) as client:
            with ThreadPoolExecutor(max_workers=1) as pool:
                with patch.object(_session.json, "dumps", side_effect=pause):
                    future = pool.submit(client.init)
                    try:
                        self.assertTrue(prepared.wait(5))
                        client.close()
                    finally:
                        release.set()
                    with self.assertRaises(FastKError) as error:
                        future.result(timeout=2)
                    self.assertEqual(error.exception.code, "SESSION_BROKEN")
            self.assertFalse(self.log.exists())

    def test_nested_context_and_invalid_input_do_not_disrupt_worker(self):
        with self.client() as c:
            with self.assertRaises(FastKError):
                c.__enter__()
            with self.assertRaises(ValueError):
                c.write_scalar_range(
                    "BTC", "1m", "feature", "x", [{"ts": 1, "value": float("nan")}]
                )
            self.assertEqual(self.calls(), [])
            self.assertEqual(c.init(), {"done": True})

    def test_external_kill_does_not_restart_worker(self):
        with self.client() as c:
            c._session.process.kill()
            c._session.process.wait()
            with self.assertRaises(FastKError) as error:
                c.init()
            self.assertEqual(error.exception.code, "SESSION_BROKEN")
        self.assertEqual(len(self.children), 2)
        self.assertEqual(self.calls(), [])

    def test_thread_start_failure_reaps_child(self):
        c = self.client()
        with patch("threading.Thread.start", side_effect=RuntimeError("cannot start IO")):
            with self.assertRaises(FastKProtocolError):
                c.__enter__()
        self.assert_reaped()

    def test_user_exception_and_interrupted_request_cleanup(self):
        c = self.client()
        with self.assertRaisesRegex(ValueError, "user failure"):
            with c:
                raise ValueError("user failure")
        self.assertIsNotNone(c._session.process.poll())
        with self.client() as other:
            with patch("fastk_client._session.json.loads", side_effect=KeyboardInterrupt):
                with self.assertRaises(KeyboardInterrupt):
                    other.init()
            self.assertIsNotNone(other._session.process.poll())
            with self.assertRaises(FastKError) as error:
                other.init()
            self.assertEqual(error.exception.code, "SESSION_BROKEN")

    @unittest.skipUnless(os.environ.get("FASTK_BRIDGE_BIN"), "requires a real bridge")
    def test_timeout_releases_actual_os_write_lock(self):
        self.mode = "lock-hang"
        actual = Client(self.root / "data", binary=os.environ["FASTK_BRIDGE_BIN"])
        actual.init()
        with self.client() as c, ThreadPoolExecutor(max_workers=1) as pool:
            c.timeout = 0.75
            pending = pool.submit(c.init)
            wait_for(lambda: Path(str(self.log) + ".locked").exists())
            args = ("BTC", "1m", "feature", "x", [{"ts": START, "value": 10}])
            with self.assertRaises(FastKError) as error:
                actual.write_scalar_range(*args, timeframe_ms=60000)
            self.assertIn("busy", str(error.exception))
            with self.assertRaises(FastKTimeout):
                pending.result(timeout=3)
            actual.write_scalar_range(*args, timeframe_ms=60000)
            self.assertEqual(len(self.calls()), 1)


@unittest.skipUnless(os.environ.get("FASTK_BRIDGE_BIN"), "requires a built bridge")
class SessionStorage(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name) / "中文 store"
        self.binary = os.environ["FASTK_BRIDGE_BIN"]
        self.one = Client(self.root, binary=self.binary)
        self.one.init()

    def scalar(self, client, ts=START, value=2**53 + 1):
        return client.write_scalar_range(
            "BTC", "1m", "feature", "x", [{"ts": ts, "value": value}], timeframe_ms=60000
        )

    def test_metadata_refresh_between_requests_and_no_idle_read_lock(self):
        self.scalar(self.one)
        with Client(self.root, binary=self.binary) as c:
            first = c.read_scalar_range("BTC", "1m", "feature", "x", START, START + 120000)
            pid = c._session.process.pid
            self.scalar(self.one, START + 60000, -(2**53 + 1))
            after = c.read_scalar_range("BTC", "1m", "feature", "x", START, START + 120000)
            self.assertEqual(len(first["records"]), 1)
            self.assertEqual(len(after["records"]), 2)
            self.assertEqual(
                after,
                self.one.read_scalar_range("BTC", "1m", "feature", "x", START, START + 120000),
            )
            self.assertEqual(pid, c._session.process.pid)

    def test_real_shared_and_exclusive_ownership(self):
        self.scalar(self.one)
        with Client(self.root, binary=self.binary) as c:
            with hold_lock(self.root / ".fastk.write.lock"):
                with self.assertRaises(FastKError) as error:
                    c.scalar_inventory("BTC", "1m", "feature", "x")
                self.assertIn("busy", str(error.exception))
            with hold_lock(self.root / ".fastk.write.lock", shared=True):
                self.assertEqual(
                    len(c.read_scalar_range("BTC", "1m", "feature", "x", START, START)["records"]),
                    1,
                )
                with self.assertRaises(FastKError):
                    self.scalar(c, START + 60000)
            self.scalar(c, START + 60000)

    def test_partial_month_error_and_explicit_recovery_without_replay(self):
        block = self.root / "series/BTC/feature/1m@@x/chunks/2024-03.chunk"
        block.mkdir(parents=True)
        with Client(self.root, binary=self.binary) as c:
            with self.assertRaises(FastKError) as error:
                c.write_scalar_range(
                    "BTC",
                    "1m",
                    "feature",
                    "x",
                    [{"ts": START, "value": 10}, {"ts": 1709251200000, "value": 20}],
                    timeframe_ms=60000,
                )
            self.assertIn('completed_months=["2024-02"]', str(error.exception))
            with self.assertRaises(FastKError):
                c.read_scalar_range("BTC", "1m", "feature", "x", START, START)
            c.init()
            self.assertEqual(
                c.read_scalar_range("BTC", "1m", "feature", "x", START, 1709251200000)["records"],
                [{"ts": START, "value": 10}],
            )

    def test_kline_and_indicator_shapes_and_sealed_writes(self):
        rows = [
            {"ts": START + i * 60000, "open": 9, "high": 12, "low": 8, "close": 10, "volume": 2}
            for i in range(3)
        ]
        with Client(self.root, binary=self.binary) as c:
            c.write_kline_range(
                "BTC", "1m", rows, timeframe_ms=60000, price_scale=100000000, volume_scale=1000000
            )
            self.assertEqual(
                c.read_kline_range("BTC", "1m", START, START + 120000),
                self.one.read_kline_range("BTC", "1m", START, START + 120000),
            )
            self.assertEqual(
                c.read_kline_latest("BTC", "1m", 2), self.one.read_kline_latest("BTC", "1m", 2)
            )
            self.assertEqual(c.kline_inventory("BTC", "1m"), self.one.kline_inventory("BTC", "1m"))
            c.write_indicator_range("BTC", "1m", "ma", [{"ts": START, "value": 2**53 + 1}])
            self.assertEqual(
                c.read_indicator_range("BTC", "1m", "ma", START, START),
                self.one.read_indicator_range("BTC", "1m", "ma", START, START),
            )
            self.assertEqual(
                c.indicator_inventory("BTC", "1m", "ma"),
                self.one.indicator_inventory("BTC", "1m", "ma"),
            )
            self.scalar(c)
            query = ("BTC", "1m", "feature", "x", START, START)
            self.assertEqual(
                c.query_scalar_predicate(*query, predicate="eq", value=2**53 + 1),
                self.one.query_scalar_predicate(*query, predicate="eq", value=2**53 + 1),
            )
            sealed = c.seal_snapshot()
            with self.assertRaises(FastKError):
                self.scalar(c, START + 60000)
            self.assertEqual(
                c.verify_snapshot(sealed["digest"]), self.one.verify_snapshot(sealed["digest"])
            )


if __name__ == "__main__":
    unittest.main()
