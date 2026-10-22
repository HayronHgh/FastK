from __future__ import annotations

import json
import math
import os
import subprocess
import threading
from pathlib import Path
from typing import Any, Iterable, Mapping


class FastKError(RuntimeError):
    def __init__(self, code: str, message: str, *, returncode: int | None = None):
        self.code = code
        self.returncode = returncode
        super().__init__(message)


class FastKProtocolError(FastKError):
    def __init__(self, message: str):
        super().__init__("PROTOCOL", message)


class FastKTimeout(FastKError):
    def __init__(self, command: str, *, message: str | None = None):
        super().__init__(
            "TIMEOUT",
            message
            or f"{command} timed out; the child was reaped. A write may have committed months; inspect inventory before retrying.",
        )


class Client:
    """One subprocess per operation, or a scoped worker with ``with Client(...)``.

    `root` is absolute at construction, so later working-directory changes cannot
    redirect operations. An explicit executable takes precedence over the bundled
    one. No PATH lookup, implicit compilation, network access, or retry occurs.
    """

    def __init__(
        self,
        root: str | os.PathLike[str],
        *,
        binary: str | os.PathLike[str] | None = None,
        timeout: float = 30.0,
    ):
        if not math.isfinite(timeout) or timeout <= 0:
            raise ValueError("timeout must be a positive finite number")
        self.root = Path(root).resolve()
        bundled = (
            Path(__file__).parent
            / "_bin"
            / ("fastk_bridge.exe" if os.name == "nt" else "fastk_bridge")
        )
        self.binary = Path(binary).resolve() if binary is not None else bundled
        self.timeout = timeout
        self._lifecycle = threading.Lock()
        self._session = None
        self._closed = False
        if not self.binary.is_file():
            raise FastKError(
                "EXECUTABLE_NOT_FOUND",
                "Install the platform wheel or pass binary='/absolute/path/to/fastk_bridge'.",
            )
        self.version = self._run("version", raw_args=["--json"])
        if (
            self.version.get("name") != "fastk"
            or type(self.version.get("contract_version")) is not int
            or self.version["contract_version"] != 1
        ):
            raise FastKProtocolError(f"Unsupported bridge contract: {self.version!r}")

    def __enter__(self):
        from ._session import Session, SessionProtocolError, SessionTimeout

        with self._lifecycle:
            if self._closed:
                raise FastKError("CLIENT_CLOSED", "Client is closed; create a new Client")
            if self._session is not None:
                raise FastKError("SESSION_ACTIVE", "Client already has an active context")
            try:
                self._session = Session(self.binary, self.timeout)
            except SessionTimeout as exc:
                self._closed = True
                raise FastKTimeout("session startup") from exc
            except (SessionProtocolError, ValueError) as exc:
                self._closed = True
                raise FastKProtocolError(str(exc)) from exc
            except OSError as exc:
                self._closed = True
                raise FastKError("EXECUTABLE_START_FAILED", str(exc)) from exc
            except BaseException:
                self._closed = True
                raise
        return self

    def __exit__(self, exc_type, exc, traceback):
        self.close()

    def close(self):
        with self._lifecycle:
            self._closed = True
            session = self._session
        if session is not None:
            from ._session import SessionProtocolError

            try:
                session.close()
            except SessionProtocolError as exc:
                raise FastKProtocolError(str(exc)) from exc

    def _run(
        self,
        command: str,
        *,
        payload: Mapping[str, Any] | None = None,
        raw_args: list[str] | None = None,
        **options: Any,
    ) -> dict[str, Any]:
        with self._lifecycle:
            if self._closed:
                raise FastKError("CLIENT_CLOSED", "Client is closed; create a new Client")
            session = self._session
        args = [str(self.binary), command]
        if raw_args is not None:
            args.extend(raw_args)
        else:
            args.extend(["--root", str(self.root)])
            for key, value in options.items():
                if value is None or value is False:
                    continue
                args.append("--" + key.replace("_", "-"))
                if value is not True:
                    args.append(
                        ",".join(map(str, value))
                        if isinstance(value, (list, tuple))
                        else str(value)
                    )
        args.append("--error-format=json")
        request = None if payload is None else json.dumps(payload, allow_nan=False).encode("utf-8")
        if session is not None:
            from ._session import SessionBroken, SessionProtocolError, SessionTimeout

            try:
                response = session.request(args[1:], request, self.timeout)
            except SessionTimeout as exc:
                message = None
                if exc.before_dispatch:
                    message = f"{command} timed out before dispatch; request not sent and the active call was not interrupted."
                raise FastKTimeout(command, message=message) from exc
            except SessionBroken as exc:
                raise FastKError("SESSION_BROKEN", str(exc)) from exc
            except SessionProtocolError as exc:
                raise FastKProtocolError(str(exc)) from exc
            if not response["ok"]:
                error = response["error"]
                raise FastKError(error["code"], error["message"], returncode=1)
            return response["result"]
        try:
            child = subprocess.Popen(
                args,
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
            )
        except OSError as exc:
            raise FastKError("EXECUTABLE_START_FAILED", str(exc)) from exc
        try:
            stdout, stderr = child.communicate(request, timeout=self.timeout)
        except subprocess.TimeoutExpired as exc:
            child.kill()
            child.communicate()
            raise FastKTimeout(command) from exc
        if child.returncode:
            try:
                error = json.loads(stderr)["error"]
                if not isinstance(error["code"], str) or not isinstance(error["message"], str):
                    raise ValueError("invalid error envelope")
            except (ValueError, KeyError, TypeError) as exc:
                raise FastKProtocolError(
                    f"Bridge failed without a valid error envelope (exit {child.returncode}): {stderr}"
                ) from exc
            raise FastKError(error["code"], error["message"], returncode=child.returncode)
        try:
            result = json.loads(stdout)
        except ValueError as exc:
            raise FastKProtocolError("Bridge returned invalid JSON") from exc
        if not isinstance(result, dict):
            raise FastKProtocolError("Bridge response must be a JSON object")
        return result

    def init(self) -> dict[str, Any]:
        return self._run("init")

    def read_kline_range(
        self, symbol: str, timeframe: str, start_ts: int, end_ts: int
    ) -> dict[str, Any]:
        return self._run(
            "read-kline-range",
            symbol=symbol,
            timeframe=timeframe,
            start_ts=start_ts,
            end_ts=end_ts,
        )

    def write_kline_range(
        self,
        symbol: str,
        timeframe: str,
        records: Iterable[Mapping[str, int]],
        *,
        timeframe_ms: int,
        price_scale: int,
        volume_scale: int,
    ) -> dict[str, Any]:
        return self._run(
            "write-kline-range",
            symbol=symbol,
            timeframe=timeframe,
            payload=dict(
                records=list(records),
                timeframe_ms=timeframe_ms,
                price_scale=price_scale,
                volume_scale=volume_scale,
            ),
        )

    def kline_inventory(self, symbol: str, timeframe: str) -> dict[str, Any]:
        return self._run("kline-inventory", symbol=symbol, timeframe=timeframe)

    def read_kline_latest(self, symbol: str, timeframe: str, limit: int = 128) -> dict[str, Any]:
        return self._run("read-kline-latest", symbol=symbol, timeframe=timeframe, limit=limit)

    def read_scalar_range(
        self,
        symbol: str,
        timeframe: str,
        category: str,
        name: str,
        start_ts: int,
        end_ts: int,
    ) -> dict[str, Any]:
        return self._run(
            "read-scalar-range",
            symbol=symbol,
            timeframe=timeframe,
            category=category,
            name=name,
            start_ts=start_ts,
            end_ts=end_ts,
        )

    def write_scalar_range(
        self,
        symbol: str,
        timeframe: str,
        category: str,
        name: str,
        records: Iterable[Mapping[str, int]],
        *,
        timeframe_ms: int | None = None,
    ) -> dict[str, Any]:
        payload: dict[str, Any] = {"records": list(records)}
        if timeframe_ms is not None:
            payload["timeframe_ms"] = timeframe_ms
        return self._run(
            "write-scalar-range",
            symbol=symbol,
            timeframe=timeframe,
            category=category,
            name=name,
            payload=payload,
        )

    def scalar_inventory(
        self, symbol: str, timeframe: str, category: str, name: str
    ) -> dict[str, Any]:
        return self._run(
            "scalar-inventory",
            symbol=symbol,
            timeframe=timeframe,
            category=category,
            name=name,
        )

    def query_scalar_predicate(
        self,
        symbol: str,
        timeframe: str,
        category: str,
        name: str,
        start_ts: int,
        end_ts: int,
        *,
        predicate: str,
        value: int | None = None,
        min: int | None = None,
        max: int | None = None,
        values: Iterable[int] | None = None,
        return_values: bool = True,
    ) -> dict[str, Any]:
        return self._run(
            "query-scalar-predicate",
            symbol=symbol,
            timeframe=timeframe,
            category=category,
            name=name,
            start_ts=start_ts,
            end_ts=end_ts,
            predicate=predicate,
            value=value,
            min=min,
            max=max,
            values=None if values is None else list(values),
            return_values=return_values,
        )

    def read_indicator_range(
        self,
        symbol: str,
        timeframe: str,
        indicator_name: str,
        start_ts: int,
        end_ts: int,
    ) -> dict[str, Any]:
        return self._run(
            "read-indicator-range",
            symbol=symbol,
            timeframe=timeframe,
            indicator_name=indicator_name,
            start_ts=start_ts,
            end_ts=end_ts,
        )

    def write_indicator_range(
        self,
        symbol: str,
        timeframe: str,
        indicator_name: str,
        records: Iterable[Mapping[str, int]],
    ) -> dict[str, Any]:
        return self._run(
            "write-indicator-range",
            symbol=symbol,
            timeframe=timeframe,
            indicator_name=indicator_name,
            payload={"records": list(records)},
        )

    def indicator_inventory(
        self, symbol: str, timeframe: str, indicator_name: str
    ) -> dict[str, Any]:
        return self._run(
            "indicator-inventory",
            symbol=symbol,
            timeframe=timeframe,
            indicator_name=indicator_name,
        )

    def seal_snapshot(self) -> dict[str, Any]:
        return self._run("seal-snapshot")

    def verify_snapshot(self, digest: str | None = None) -> dict[str, Any]:
        return self._run("verify-snapshot", digest=digest)
