"""The fully materialized NumPy delivery contract shared by every backend."""

import numpy as np

from .data import FIELDS


def dtype(fields):
    return np.dtype([(name, "<i8") for name in fields])


def columns_for(query):
    return (
        ["ts", "value"]
        if "feature" in query
        else (["ts", "close"] if query["kind"] == "projection" else FIELDS)
    )


def from_table(table, fields=None):
    fields = fields or table.column_names
    return from_columns(
        {name: table[name].to_numpy(zero_copy_only=False) for name in fields}, fields
    )


def from_columns(columns, fields):
    result = np.empty(len(columns[fields[0]]), dtype=dtype(fields))
    for name in fields:
        result[name] = columns[name]
    result.flags.writeable = False
    return result


def select(batch, fields):
    return (
        batch
        if batch.dtype == dtype(fields)
        else from_columns({name: batch[name] for name in fields}, fields)
    )


def input_batch(table):
    return table if isinstance(table, np.ndarray) else from_table(table)


def validate(batch, fields):
    if (
        batch.ndim != 1
        or batch.dtype != dtype(fields)
        or not batch.flags.c_contiguous
        or batch.flags.writeable
    ):
        raise AssertionError("expected a packed read-only int64 result batch")
    if len(batch) > 1 and np.any(batch["ts"][1:] <= batch["ts"][:-1]):
        raise AssertionError("timestamps must be strictly ascending")


def exact_sum(values):
    """Checksum totals can exceed int64 even though each stored value fits."""
    total = 0
    for start in range(0, len(values), 1024):
        block = values[start : start + 1024]
        bound = max(abs(int(block.min())), abs(int(block.max())))
        total += (
            int(block.sum(dtype=np.int64))
            if bound * len(block) <= 2**63 - 1
            else sum(map(int, block))
        )
    return total
