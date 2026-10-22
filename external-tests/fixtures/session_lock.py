"""Hold the same whole-file OS lock used by Rust, for integration faults."""

import os
from contextlib import contextmanager


@contextmanager
def hold_lock(path, *, shared=False):
    with open(path, "r+b", buffering=0) as file:
        if os.name == "nt":
            import ctypes
            import msvcrt
            from ctypes import wintypes

            class Overlapped(ctypes.Structure):
                _fields_ = [
                    ("internal", ctypes.c_size_t),
                    ("internal_high", ctypes.c_size_t),
                    ("offset", wintypes.DWORD),
                    ("offset_high", wintypes.DWORD),
                    ("event", wintypes.HANDLE),
                ]

            kernel = ctypes.WinDLL("kernel32", use_last_error=True)
            handle = wintypes.HANDLE(msvcrt.get_osfhandle(file.fileno()))
            overlapped = Overlapped()
            flags = 1 if shared else 3
            if not kernel.LockFileEx(
                handle, flags, 0, 0xFFFFFFFF, 0xFFFFFFFF, ctypes.byref(overlapped)
            ):
                raise ctypes.WinError(ctypes.get_last_error())
            try:
                yield
            finally:
                if not kernel.UnlockFileEx(
                    handle, 0, 0xFFFFFFFF, 0xFFFFFFFF, ctypes.byref(overlapped)
                ):
                    raise ctypes.WinError(ctypes.get_last_error())
        else:
            import fcntl

            fcntl.flock(file, (fcntl.LOCK_SH if shared else fcntl.LOCK_EX) | fcntl.LOCK_NB)
            try:
                yield
            finally:
                fcntl.flock(file, fcntl.LOCK_UN)
