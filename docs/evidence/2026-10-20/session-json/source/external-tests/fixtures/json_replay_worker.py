"""Measurement-only cached responder; no engine or production worker changes."""

import json
import struct
import sys
from pathlib import Path


def exact(stream, size):
    data = bytearray()
    while len(data) < size:
        chunk = stream.read(size - len(data))
        if not chunk:
            raise EOFError("truncated request")
        data.extend(chunk)
    return data


def main():
    if "--session-stdio" not in sys.argv:
        print(json.dumps({"name": "fastk", "version": "measurement-only", "contract_version": 1}))
        return
    # Strip only the envelope prefix. Stream the unchanged result bytes without
    # another full-body allocation on each request.
    frame = Path(sys.argv[1]).read_bytes()
    _, result = frame.split(b',"ok":true,"result":', 1)
    del frame
    incoming, outgoing = sys.stdin.buffer, sys.stdout.buffer
    hello = json.dumps(
        {"session_protocol": 1, "contract_version": 1, "max_frame_bytes": 1024**3}
    ).encode()
    outgoing.write(struct.pack("<Q", len(hello)))
    outgoing.write(hello)
    outgoing.flush()
    while prefix := incoming.read(8):
        if len(prefix) < 8:
            prefix += exact(incoming, 8 - len(prefix))
        size = struct.unpack("<Q", prefix)[0]
        if not 0 < size <= 1024**3:
            raise ValueError("invalid request length")
        request = json.loads(exact(incoming, size))
        start = b'{"id":' + str(request["id"]).encode() + b',"ok":true,"result":'
        outgoing.write(struct.pack("<Q", len(start) + len(result)))
        outgoing.write(start)
        outgoing.write(result)
        outgoing.flush()


if __name__ == "__main__":
    main()
