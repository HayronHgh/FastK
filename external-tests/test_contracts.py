"""Validate real installed bridge payloads against the checked-in JSON contracts."""

import hashlib
import json
import os
import subprocess
import tempfile
import unittest
from pathlib import Path

import jsonschema
from fastk_client import Client

ROOT = Path(__file__).resolve().parents[1]
SCHEMA = json.loads((ROOT / "schemas/bridge_commands.json").read_text())


def validate(name, payload):
    jsonschema.Draft202012Validator({"$defs": SCHEMA["$defs"], "$ref": "#/$defs/" + name}).validate(
        payload
    )


@unittest.skipUnless(os.environ.get("FASTK_BRIDGE_BIN"), "requires a built release bridge")
class Contracts(unittest.TestCase):
    def test_real_version_write_read_latest_and_error_schemas(self):
        with tempfile.TemporaryDirectory() as root:
            client = Client(root, binary=os.environ["FASTK_BRIDGE_BIN"])
            validate("version_response", client.version)
            validate("init_response", client.init())
            record = {
                "ts": 1706745600000,
                "open": 100,
                "high": 110,
                "low": 90,
                "close": 101,
                "volume": 200,
            }
            response = client.write_kline_range(
                "BTCUSDT",
                "1m",
                [record],
                timeframe_ms=60000,
                price_scale=1,
                volume_scale=1,
            )
            validate("write_kline_response", response)
            validate(
                "kline_range_response",
                client.read_kline_range("BTCUSDT", "1m", record["ts"], record["ts"]),
            )
            validate("latest_response", client.read_kline_latest("BTCUSDT", "1m"))
            process = subprocess.run(
                [
                    os.environ["FASTK_BRIDGE_BIN"],
                    "read-kline-range",
                    "--error-format=json",
                ],
                capture_output=True,
                check=False,
            )
            self.assertEqual(process.returncode, 1)
            self.assertEqual(process.stdout, b"")
            validate("error_response", json.loads(process.stderr))

    @unittest.skipUnless(os.environ.get("FASTK_RELEASE_PACKAGE"), "requires packaged artifacts")
    def test_release_manifest_schema_and_all_artifact_checksums(self):
        package = Path(os.environ["FASTK_RELEASE_PACKAGE"])
        manifest = json.loads((package / "release_manifest.json").read_text())
        schema = json.loads((ROOT / "schemas/release_manifest.schema.json").read_text())
        jsonschema.Draft202012Validator(schema).validate(manifest)
        self.assertTrue(manifest["artifacts"])
        for artifact in manifest["artifacts"]:
            actual = hashlib.sha256((package / artifact["path"]).read_bytes()).hexdigest()
            self.assertEqual(actual, artifact["sha256"], artifact["path"])
        for line in (package / "SHA256SUMS").read_text().splitlines():
            expected, path = line.split("  ", 1)
            self.assertEqual(hashlib.sha256((package / path).read_bytes()).hexdigest(), expected)


if __name__ == "__main__":
    unittest.main()
