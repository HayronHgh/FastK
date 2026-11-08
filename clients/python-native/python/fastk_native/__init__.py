"""FastK's in-process interface; independent of the subprocess client."""

from ._native import (
    KLINE_ACTIVITY_DTYPE,
    KLINE_DTYPE,
    KLINE_PRICE_DTYPE,
    KLINE_FLOW_DTYPE,
    KLINE_FLOW_MISSING,
    SCALAR_DTYPE,
    FastKError,
    Reader,
    Writer,
    __version__,
)

__all__ = [
    "KLINE_ACTIVITY_DTYPE",
    "KLINE_DTYPE",
    "KLINE_PRICE_DTYPE",
    "KLINE_FLOW_DTYPE",
    "KLINE_FLOW_MISSING",
    "SCALAR_DTYPE",
    "Reader",
    "Writer",
    "FastKError",
    "__version__",
]
