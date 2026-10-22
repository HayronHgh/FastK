from os import PathLike
from typing import Any, Literal

import numpy as np

Batch = np.ndarray[Any, np.dtype[np.void]]
KLINE_DTYPE: np.dtype[np.void]
SCALAR_DTYPE: np.dtype[np.void]
__version__: str

class FastKError(RuntimeError):
    code: str

class Reader:
    def __init__(self, root: str | PathLike[str], *, threads: int = 1) -> None: ...
    def read_kline_range(
        self, symbol: str, timeframe: str, start_ts: int, end_ts: int
    ) -> Batch: ...
    def read_kline_latest(self, symbol: str, timeframe: str, limit: int) -> Batch: ...
    def read_scalar_range(
        self, symbol: str, timeframe: str, category: str, name: str, start_ts: int, end_ts: int
    ) -> Batch: ...
    def query_scalar_predicate(
        self,
        symbol: str,
        timeframe: str,
        category: str,
        name: str,
        start_ts: int,
        end_ts: int,
        *,
        predicate: Literal["eq", "ne", "gt", "gte", "lt", "lte", "between", "in", "not-in"],
        value: int | None = None,
        min: int | None = None,
        max: int | None = None,
        values: list[int] | None = None,
        inclusive: bool = True,
    ) -> Batch: ...
    def kline_info(self, symbol: str, timeframe: str) -> dict[str, Any]: ...
    def scalar_info(
        self, symbol: str, timeframe: str, category: str, name: str
    ) -> dict[str, Any]: ...
    def close(self) -> None: ...
    def __enter__(self) -> Reader: ...
    def __exit__(self, kind: Any, value: Any, traceback: Any) -> None: ...

class Writer:
    def __init__(self, root: str | PathLike[str]) -> None: ...
    def write_kline_range(
        self,
        symbol: str,
        timeframe: str,
        rows: Batch,
        *,
        timeframe_ms: int,
        price_scale: int,
        volume_scale: int,
    ) -> dict[str, Any]: ...
    def write_scalar_range(
        self,
        symbol: str,
        timeframe: str,
        category: str,
        name: str,
        rows: Batch,
        *,
        timeframe_ms: int,
    ) -> dict[str, Any]: ...
    def close(self) -> None: ...
    def __enter__(self) -> Writer: ...
    def __exit__(self, kind: Any, value: Any, traceback: Any) -> None: ...
