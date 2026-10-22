"""FastK's in-process interface; independent of the subprocess client."""

from ._native import KLINE_DTYPE, SCALAR_DTYPE, FastKError, Reader, Writer, __version__

__all__ = ["KLINE_DTYPE", "SCALAR_DTYPE", "Reader", "Writer", "FastKError", "__version__"]
