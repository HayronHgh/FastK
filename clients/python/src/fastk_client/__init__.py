"""FastK storage client. No fetching, feature calculation, or automatic retries."""

from .client import Client, FastKError, FastKProtocolError, FastKTimeout

__all__ = ["Client", "FastKError", "FastKProtocolError", "FastKTimeout"]
__version__ = "0.1.0rc1"
