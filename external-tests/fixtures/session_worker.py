"""Adversarial real child, selected only by test launch interception."""

import json
import os
import struct
import sys
import time
from pathlib import Path

from session_lock import hold_lock

mode, log = sys.argv[1:3]
if sys.argv[3] == "version":
    print(json.dumps({"name": "fastk", "version": "fixture", "contract_version": 1}))
    sys.exit(0)


def send(value):
    body = json.dumps(value).encode()
    frame = struct.pack("<Q", len(body)) + body
    if mode == "fragmented":
        for offset in range(0, len(frame), 3):
            sys.stdout.buffer.write(frame[offset : offset + 3])
            sys.stdout.buffer.flush()
            time.sleep(0.001)
    else:
        sys.stdout.buffer.write(frame)
        sys.stdout.buffer.flush()


if mode == "startup-hang":
    time.sleep(60)
if mode == "bad-handshake":
    send({"session_protocol": True, "contract_version": 1, "max_frame_bytes": 1024**3})
else:
    send({"session_protocol": 1, "contract_version": 1, "max_frame_bytes": 1024**3})
if mode == "no-read":
    time.sleep(60)
while prefix := sys.stdin.buffer.read(8):
    size = struct.unpack("<Q", prefix)[0]
    request = json.loads(sys.stdin.buffer.read(size))
    with open(log, "a", encoding="utf-8") as calls:
        calls.write(json.dumps(request) + "\n")
    if mode == "lock-hang":
        args = request["args"]
        with hold_lock(Path(args[args.index("--root") + 1]) / ".fastk.write.lock"):
            Path(log + ".locked").touch()
            time.sleep(60)
    if mode == "hang":
        time.sleep(60)
    if mode == "partial-body":
        sys.stdout.buffer.write(struct.pack("<Q", 8192) + b'{"id":')
        sys.stdout.buffer.flush()
        time.sleep(60)
    if mode == "delay":
        time.sleep(0.3)
    if mode == "stderr":
        for _ in range(512):
            os.write(2, b"x" * 8192)
    if mode == "exit":
        os._exit(19)
    if mode == "truncated":
        sys.stdout.buffer.write(struct.pack("<Q", 100) + b"{")
        sys.stdout.buffer.flush()
        os._exit(20)
    if mode == "oversized":
        sys.stdout.buffer.write(struct.pack("<Q", 1024**3 + 1))
        sys.stdout.buffer.flush()
        time.sleep(60)
    if mode == "bad-error":
        send({"id": request["id"], "ok": False, "error": {"code": 1, "message": "bad"}})
    elif mode == "bad-json":
        send([])
    else:
        send(
            {
                "id": request["id"] + (1 if mode == "wrong-id" else 0),
                "ok": True,
                "result": {"done": True},
            }
        )
