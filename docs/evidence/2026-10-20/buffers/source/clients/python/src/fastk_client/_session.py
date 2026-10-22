"""Private framed stdio transport. No retries and no store ownership here."""

from __future__ import annotations

import json
import queue
import struct
import subprocess
import threading
import time
from collections import deque
from dataclasses import dataclass, field
from pathlib import Path

MAX_FRAME_BYTES = 1024 * 1024 * 1024


class SessionProtocolError(Exception):
    pass


class SessionBroken(Exception):
    pass


class SessionTimeout(Exception):
    def __init__(self, *, before_dispatch: bool = False):
        self.before_dispatch = before_dispatch


@dataclass
class _Exchange:
    frame: bytes = b""
    done: threading.Event = field(default_factory=threading.Event)
    response: bytes | bytearray = b""
    error: Exception | None = None


def _read_exact(stream, size):
    data = bytearray(size)
    view = memoryview(data)
    offset = 0
    while offset < size:
        amount = stream.readinto(view[offset : offset + 64 * 1024])
        if not amount:
            raise SessionProtocolError("worker closed a session frame")
        offset += amount
    return data


def _read_frame(stream):
    size = struct.unpack("<Q", _read_exact(stream, 8))[0]
    if not 0 < size <= MAX_FRAME_BYTES:
        raise SessionProtocolError("invalid session frame size")
    return _read_exact(stream, size)


def _write_all(stream, data):
    remaining = memoryview(data)
    while remaining:
        written = stream.write(remaining)
        if not written:
            raise SessionProtocolError("worker stopped accepting requests")
        remaining = remaining[written:]
    stream.flush()


class Session:
    def __init__(self, binary: Path, timeout: float):
        self._serial = threading.Lock()
        self._cleanup = threading.Lock()
        self._stop = threading.Event()
        self._tasks: queue.Queue[_Exchange | None] = queue.Queue(maxsize=1)
        self._ready = _Exchange()
        self._failure: str | None = None
        self._stderr: deque[bytes] = deque(maxlen=8)
        self._sequence = 0
        self._closed = False
        self.process = subprocess.Popen(
            [str(binary), "--session-stdio"],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            bufsize=0,
            creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
        )
        self._reader = threading.Thread(
            target=self._io, name=f"fastk-session-io-{self.process.pid}", daemon=True
        )
        self._drainer = threading.Thread(
            target=self._drain_stderr, name=f"fastk-session-stderr-{self.process.pid}", daemon=True
        )
        try:
            self._drainer.start()
            self._reader.start()
            if not self._ready.done.wait(timeout):
                raise SessionTimeout()
            if self._ready.error:
                raise SessionProtocolError(str(self._ready.error))
            try:
                hello = json.loads(self._ready.response)
            except (ValueError, RecursionError) as exc:
                raise SessionProtocolError("invalid session handshake") from exc
            expected = {
                "session_protocol": 1,
                "contract_version": 1,
                "max_frame_bytes": MAX_FRAME_BYTES,
            }
            if (
                not isinstance(hello, dict)
                or hello != expected
                or any(type(hello[k]) is not int for k in expected)
            ):
                raise SessionProtocolError("unsupported session handshake")
        except BaseException as exc:
            self.close()
            if isinstance(exc, Exception) and not isinstance(
                exc, (SessionProtocolError, SessionTimeout)
            ):
                raise SessionProtocolError(f"session startup failed: {exc}") from exc
            raise

    def _drain_stderr(self):
        try:
            while chunk := self.process.stderr.read(8192):
                self._stderr.append(chunk)
        except OSError:
            pass

    def _io(self):
        current = self._ready
        try:
            current.response = _read_frame(self.process.stdout)
            current.done.set()
            while not self._stop.is_set():
                current = self._tasks.get()
                if current is None:
                    return
                if self._stop.is_set():
                    raise SessionBroken("session closed before dispatch")
                _write_all(self.process.stdin, current.frame)
                current.response = _read_frame(self.process.stdout)
                current.done.set()
        except Exception as exc:
            self._failure = str(exc)
            if current is not None:
                current.error = exc
                current.done.set()

    def request(self, args: list[str], payload: bytes | None, timeout: float):
        deadline = time.monotonic() + timeout
        if not self._serial.acquire(timeout=timeout):
            # This request never reached the worker. Do not kill another call.
            raise SessionTimeout(before_dispatch=True)
        enqueued = False
        try:
            if self._closed or self._failure or self.process.poll() is not None:
                self.close()
                raise SessionBroken("session is closed or failed; create a new Client")
            self._sequence += 1
            prefix = json.dumps({"id": self._sequence, "args": args}).encode()[:-1]
            body = prefix + b', "payload":' + (payload if payload is not None else b"null") + b"}"
            if len(body) > MAX_FRAME_BYTES:
                raise ValueError("session request exceeds the 1GiB frame limit")
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise SessionTimeout(before_dispatch=True)
            exchange = _Exchange(struct.pack("<Q", len(body)) + body)
            # close() may race with frame construction. Enqueue under its
            # cleanup lock so a closed worker never receives a new task.
            with self._cleanup:
                if self._closed:
                    raise SessionBroken("session closed before dispatch")
                self._tasks.put_nowait(exchange)
                enqueued = True
            if not exchange.done.wait(remaining):
                self._failure = "request timed out"
                self.close()
                raise SessionTimeout()
            if exchange.error:
                self.close()
                if isinstance(exchange.error, SessionBroken):
                    raise exchange.error
                raise SessionProtocolError(str(exchange.error))
            try:
                response = json.loads(exchange.response)
                if (
                    not isinstance(response, dict)
                    or type(response.get("id")) is not int
                    or response["id"] != self._sequence
                    or type(response.get("ok")) is not bool
                ):
                    raise ValueError("invalid response identity")
                if response["ok"]:
                    if set(response) != {"id", "ok", "result"} or not isinstance(
                        response["result"], dict
                    ):
                        raise ValueError("invalid result envelope")
                else:
                    error = response.get("error")
                    if (
                        set(response) != {"id", "ok", "error"}
                        or not isinstance(error, dict)
                        or set(error) != {"code", "message"}
                        or not all(isinstance(error[k], str) for k in error)
                    ):
                        raise ValueError("invalid error envelope")
                return response
            except (ValueError, TypeError, KeyError, RecursionError) as exc:
                self._failure = str(exc)
                self.close()
                raise SessionProtocolError(str(exc)) from exc
        except BaseException:
            if enqueued and not self._closed:
                self._failure = "request interrupted"
                self.close()
            raise
        finally:
            self._serial.release()

    def close(self):
        # Separate from the request lock so close/timeout can interrupt IO.
        with self._cleanup:
            if self._closed:
                return
            self._closed = True
            self._stop.set()
            if self.process.poll() is None:
                self.process.kill()
            self.process.wait()
            # A close between enqueue and the IO thread taking the task must
            # wake its caller immediately, rather than wait for its deadline.
            try:
                pending = self._tasks.get_nowait()
                if pending is not None:
                    pending.error = SessionBroken("session closed before dispatch")
                    pending.done.set()
            except queue.Empty:
                pass
            try:
                self._tasks.put_nowait(None)
            except queue.Full:
                pass
            for thread in (self._reader, self._drainer):
                if thread.ident is None:
                    continue
                thread.join(timeout=5)
                if thread.is_alive():
                    raise SessionProtocolError("worker IO did not stop after termination")
            for stream in (self.process.stdin, self.process.stdout, self.process.stderr):
                stream.close()
