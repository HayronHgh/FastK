"""Transport tests use a fake process; integration uses only the published client/CLI."""

import json
import os
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

from fastk_client import Client, FastKError, FastKProtocolError, FastKTimeout

VERSION = json.dumps({"name": "fastk", "contract_version": 1, "version": "test"}).encode()


class TransportTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.binary = Path(self.temp.name) / "fake bridge.exe"
        self.binary.touch()

    def process(self, stdout=VERSION, stderr=b"", returncode=0):
        process = Mock()
        process.communicate.return_value = (stdout, stderr)
        process.returncode = returncode
        return process

    def test_missing_binary_has_explicit_error(self):
        with self.assertRaises(FastKError) as result:
            Client(self.temp.name, binary=self.binary.with_name("missing"))
        self.assertEqual(result.exception.code, "EXECUTABLE_NOT_FOUND")

    def test_unknown_contract_is_rejected(self):
        process = self.process(b'{"name":"fastk","contract_version":2}')
        with (
            patch("subprocess.Popen", return_value=process),
            self.assertRaises(FastKProtocolError),
        ):
            Client(self.temp.name, binary=self.binary)

    def test_invalid_response_is_not_accepted(self):
        for output in [b"log\n{}", b"[]", b"\xff"]:
            with (
                self.subTest(output=output),
                patch("subprocess.Popen", return_value=self.process(output)),
                self.assertRaises(FastKProtocolError),
            ):
                Client(self.temp.name, binary=self.binary)

    def test_timeout_kills_and_reaps_once_without_retry(self):
        normal = self.process()
        hanging = self.process()
        hanging.communicate.side_effect = [
            subprocess.TimeoutExpired("fake", 1),
            (b"", b""),
        ]
        with patch("subprocess.Popen", side_effect=[normal, hanging]) as popen:
            client = Client(self.temp.name, binary=self.binary)
            with self.assertRaises(FastKTimeout):
                client.init()
            hanging.kill.assert_called_once()
            self.assertEqual(hanging.communicate.call_count, 2)
            self.assertEqual(popen.call_count, 2)

    def test_machine_error_and_integer_payload_without_shell(self):
        failed = self.process(b"", b'{"error":{"code":"INVALID_INPUT","message":"overlap"}}', 1)
        with patch("subprocess.Popen", side_effect=[self.process(), failed]) as popen:
            client = Client(Path(self.temp.name) / "中文 path", binary=self.binary)
            with self.assertRaises(FastKError) as result:
                client.write_scalar_range(
                    "BTC",
                    "1m",
                    "feature",
                    "x",
                    [{"ts": 1, "value": 2**53 + 1}],
                    timeframe_ms=60000,
                )
            self.assertEqual(result.exception.code, "INVALID_INPUT")
            self.assertNotIn("shell", popen.call_args.kwargs)
            self.assertIn("--error-format=json", popen.call_args.args[0])
            payload = failed.communicate.call_args.args[0]
            self.assertEqual(json.loads(payload)["records"][0]["value"], 2**53 + 1)

    def test_invalid_payload_does_not_spawn_a_child(self):
        with patch("subprocess.Popen", return_value=self.process()) as popen:
            client = Client(self.temp.name, binary=self.binary)
            with self.assertRaises(ValueError):
                client.write_scalar_range(
                    "BTC", "1m", "feature", "x", [{"ts": 1, "value": float("nan")}]
                )
            self.assertEqual(popen.call_count, 1)


@unittest.skipUnless(os.environ.get("FASTK_BRIDGE_BIN"), "requires a built release bridge")
class IntegrationTests(unittest.TestCase):
    def test_cross_month_and_large_integer_roundtrip(self):
        with tempfile.TemporaryDirectory() as root:
            client = Client(Path(root) / "中文 store", binary=os.environ["FASTK_BRIDGE_BIN"])
            rows = [
                {"ts": 1709251140000, "value": 2**53 + 1},
                {"ts": 1709251200000, "value": -(2**53 + 1)},
            ]
            client.write_scalar_range("BTC", "1m", "feature", "x", rows, timeframe_ms=60000)
            self.assertEqual(
                client.read_scalar_range("BTC", "1m", "feature", "x", rows[0]["ts"], rows[1]["ts"])[
                    "records"
                ],
                rows,
            )
            with self.assertRaises(FastKError):
                client.write_scalar_range("BTC", "1m", "feature", "x", rows, timeframe_ms=1000)


if __name__ == "__main__":
    unittest.main()
