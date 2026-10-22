"""Canonical inputs, integer arithmetic, provenance and independent query oracle."""

from __future__ import annotations

import csv
import hashlib
import io
import json
import random
import urllib.request
import zipfile
from collections import deque
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

FIELDS = ["ts", "open", "high", "low", "close", "volume"]
FEATURES = {
    "ma5": ("close", 5),
    "ma20": ("close", 20),
    "ma60": ("close", 60),
    "volume20": ("volume", 20),
}
SCALE = 100_000_000
SEED = 20261020


def hash_rows(rows):
    digest = hashlib.sha256()
    count = 0
    for row in rows:
        digest.update(
            json.dumps(list(row), separators=(",", ":"), allow_nan=False).encode() + b"\n"
        )
        count += 1
    return {"rows": count, "sha256": digest.hexdigest()}


def save_table(path, rows, names):
    import pyarrow as pa

    path.parent.mkdir(parents=True, exist_ok=True)
    schema = pa.schema([(name, pa.int64()) for name in names])
    table = pa.Table.from_pylist([dict(zip(names, row)) for row in rows], schema=schema)
    with pa.OSFile(str(path), "wb") as sink, pa.ipc.new_file(sink, schema) as writer:
        writer.write_table(table)
    return table


def read_table(path):
    import pyarrow as pa

    with pa.memory_map(str(path), "r") as source:
        return pa.ipc.open_file(source).read_all()


def iter_parts(root, manifest):
    for part in manifest["parts"]:
        yield part, read_table(Path(root) / part["path"])


def verify_inputs(root, manifest):
    for part in manifest["parts"]:
        if hashlib.sha256((Path(root) / part["path"]).read_bytes()).hexdigest() != part["sha256"]:
            raise ValueError("input checksum mismatch: " + part["path"])


def _download_month(cache, symbol, month):
    name = f"{symbol}-1m-{month}.zip"
    url = f"https://data.binance.vision/data/spot/monthly/klines/{symbol}/1m/{name}"
    path = cache / name
    checksum_path = cache / (name + ".CHECKSUM")
    for target, remote in [(path, url), (checksum_path, url + ".CHECKSUM")]:
        if not target.exists():
            with urllib.request.urlopen(remote, timeout=60) as response:
                target.write_bytes(response.read())
    expected = checksum_path.read_text().split()[0].lower()
    if hashlib.sha256(path.read_bytes()).hexdigest() != expected:
        raise ValueError("upstream archive checksum mismatch: " + url)
    with zipfile.ZipFile(path) as archive:
        names = archive.namelist()
        if len(names) != 1:
            raise ValueError("expected one CSV per archive")
        rows = []
        with archive.open(names[0]) as raw:
            for record in csv.reader(io.TextIOWrapper(raw, encoding="utf-8")):
                if not record[0].isdigit():
                    continue
                rows.append(
                    (
                        int(record[0]),
                        *[int(Decimal(value) * SCALE) for value in record[1:6]],
                    )
                )
    return rows, {"url": url, "sha256": expected, "archive": name}


def prepare(root: Path, profile: str):
    root.mkdir(parents=True, exist_ok=False)
    cache = root / "archives"
    cache.mkdir()
    symbols = (
        ["BTCUSDT", "ETHUSDT"]
        if profile == "real"
        else [f"S{index:02}" for index in range(2 if profile == "smoke" else 24)]
    )
    month_count = {"smoke": 2, "standard": 10, "real": 12}[profile]
    parts = []
    sources = []
    for symbol_index, symbol in enumerate(symbols):
        windows = {name: deque(maxlen=length) for name, (_, length) in FEATURES.items()}
        for month in range(1, month_count + 1):
            month_key = f"2024-{month:02}"
            start = int(datetime(2024, month, 1, tzinfo=timezone.utc).timestamp() * 1000)
            end = int(
                datetime(2024 + (month == 12), month % 12 + 1, 1, tzinfo=timezone.utc).timestamp()
                * 1000
            )
            if profile == "real":
                rows, source = _download_month(cache, symbol, month_key)
                sources.append(source)
            else:
                count = (
                    min(1024, (end - start) // 60000)
                    if profile == "smoke"
                    else (end - start) // 60000
                )
                rows = []
                for index in range(count):
                    if index % 211 == 0:
                        continue
                    close = (
                        5_000_000_000_000
                        + symbol_index * 100_000_000
                        + month * 17
                        + (index * 13) % 10007
                    )
                    rows.append(
                        (
                            start + index * 60000,
                            close - 5,
                            close + 10,
                            close - 15,
                            close,
                            100_000_000 + index * 31,
                        )
                    )
            if not rows or any(a[0] >= b[0] for a, b in zip(rows, rows[1:])):
                raise ValueError("source timestamps must be unique and sorted")
            if any(
                high < max(open, close) or low > min(open, close) or high < low
                for _, open, high, low, close, _ in rows
            ):
                raise ValueError("invalid source OHLC range")
            typed = {"bars": (rows, FIELDS)}
            feature_rows = {name: [] for name in FEATURES}
            for row in rows:
                for name, (column, _) in FEATURES.items():
                    window = windows[name]
                    window.append(row[FIELDS.index(column)])
                    feature_rows[name].append((row[0], sum(window) // len(window)))
            typed.update({name: (values, ["ts", "value"]) for name, values in feature_rows.items()})
            for kind, (values, names) in typed.items():
                relative = f"{symbol}/{month_key}/{kind}.arrow"
                save_table(root / relative, values, names)
                parts.append(
                    {
                        "symbol": symbol,
                        "month": month_key,
                        "kind": kind,
                        "path": relative,
                        "rows": len(values),
                        "start_ts": values[0][0],
                        "end_ts": values[-1][0],
                        "sha256": hashlib.sha256((root / relative).read_bytes()).hexdigest(),
                    }
                )
    manifest = {
        "format_version": 1,
        "profile": profile,
        "seed": SEED,
        "scale": SCALE,
        "timeframe_ms": 60000,
        "gap_policy": "preserve; never forward-fill",
        "synthetic_gap_every": None if profile == "real" else 211,
        "normalization": "strict ordered UTC epoch milliseconds; Decimal OHLCV to signed i64 at scale 1e8",
        "feature_policy": "integer floor mean over preceding available rows, including partial initial windows",
        "sources": sources,
        "parts": parts,
    }
    (root / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    return manifest


def query_rows(root, manifest, query):
    import pyarrow.compute as pc

    kind = query.get("feature", "bars")
    selected = [
        part
        for part in manifest["parts"]
        if part["symbol"] == query["symbol"]
        and part["kind"] == kind
        and part["end_ts"] >= query["start_ts"]
        and part["start_ts"] <= query["end_ts"]
    ]
    rows = []
    for part in selected:
        table = read_table(Path(root) / part["path"])
        mask = pc.and_(
            pc.greater_equal(table["ts"], query["start_ts"]),
            pc.less_equal(table["ts"], query["end_ts"]),
        )
        if "value" in query:
            mask = pc.and_(mask, pc.equal(table["value"], query["value"]))
        if "min" in query:
            mask = pc.and_(
                mask,
                pc.and_(
                    pc.greater_equal(table["value"], query["min"]),
                    pc.less_equal(table["value"], query["max"]),
                ),
            )
        table = table.filter(mask)
        if query["kind"] == "projection":
            table = table.select(["ts", "close"])
        rows.extend(tuple(row.values()) for row in table.to_pylist())
    if query["kind"] == "latest":
        rows = rows[-query["limit"] :]
    return rows


def queries(root, manifest, short_ops, full_scans, seed):
    import pyarrow.compute as pc

    rng = random.Random(seed)
    bars = [part for part in manifest["parts"] if part["kind"] == "bars"]
    symbols = sorted({part["symbol"] for part in bars})
    result = []
    kinds = [
        "point",
        "point-miss",
        "range",
        "latest",
        "projection",
        "predicate-eq",
        "predicate-between",
    ]
    for index in range(short_ops):
        part = rng.choice(bars)
        kind = kinds[index % len(kinds)]
        position = rng.randrange((part["end_ts"] - part["start_ts"]) // 60000 + 1)
        start = part["start_ts"] + position * 60000
        query = {
            "symbol": part["symbol"],
            "start_ts": start,
            "end_ts": min(part["end_ts"], start + (127 if kind != "point" else 0) * 60000),
            "kind": kind,
        }
        if kind == "point":
            table = read_table(Path(root) / part["path"])
            timestamp = table["ts"][rng.randrange(len(table))].as_py()
            query.update(start_ts=timestamp, end_ts=timestamp)
        if kind == "point-miss":
            query.update(start_ts=part["start_ts"] - 1, end_ts=part["start_ts"] - 1)
        if kind == "latest":
            query.update(
                start_ts=min(p["start_ts"] for p in bars if p["symbol"] == part["symbol"]),
                end_ts=max(p["end_ts"] for p in bars if p["symbol"] == part["symbol"]),
                limit=128,
            )
        if kind.startswith("predicate-"):
            feature = next(
                p
                for p in manifest["parts"]
                if p["kind"] == "ma20"
                and p["symbol"] == part["symbol"]
                and p["month"] == part["month"]
            )
            table = read_table(Path(root) / feature["path"])
            selected = table.filter(
                pc.and_(
                    pc.greater_equal(table["ts"], query["start_ts"]),
                    pc.less_equal(table["ts"], query["end_ts"]),
                )
            )
            values = sorted(selected["value"].to_pylist()) or [0]
            query.update(feature="ma20")
            if kind == "predicate-eq":
                query["value"] = values[len(values) // 2]
            else:
                query.update(min=values[len(values) // 4], max=values[3 * len(values) // 4])
        result.append(query)
    for index in range(full_scans):
        symbol = symbols[index % len(symbols)]
        result.append(
            {
                "symbol": symbol,
                "start_ts": min(p["start_ts"] for p in bars if p["symbol"] == symbol),
                "end_ts": max(p["end_ts"] for p in bars if p["symbol"] == symbol),
                "kind": "scan",
            }
        )
    for query in result:
        query["expected"] = hash_rows(query_rows(root, manifest, query))
    return result


def maintenance_inputs(source, manifest, root):
    """Revision is a full new version; extension is one later synthetic month.

    All feature recalculation belongs here, outside every storage adapter.
    """
    revised = root / "revised-input"
    extension = root / "append-input"
    revised.mkdir()
    extension.mkdir()
    revised_parts = []
    extension_parts = []

    def write_parts(destination, symbol, month, rows, windows):
        tables = {"bars": (rows, FIELDS)}
        values = {name: [] for name in FEATURES}
        for row in rows:
            for name, (column, _) in FEATURES.items():
                window = windows[name]
                window.append(row[FIELDS.index(column)])
                values[name].append((row[0], sum(window) // len(window)))
        tables.update({name: (data, ["ts", "value"]) for name, data in values.items()})
        output = []
        for kind, (data, fields) in tables.items():
            relative = f"{symbol}/{month}/{kind}.arrow"
            save_table(destination / relative, data, fields)
            output.append(
                {
                    "symbol": symbol,
                    "month": month,
                    "kind": kind,
                    "path": relative,
                    "rows": len(data),
                    "start_ts": data[0][0],
                    "end_ts": data[-1][0],
                    "sha256": hashlib.sha256((destination / relative).read_bytes()).hexdigest(),
                }
            )
        return output

    symbols = sorted({part["symbol"] for part in manifest["parts"]})
    for symbol in symbols:
        revised_windows = {name: deque(maxlen=length) for name, (_, length) in FEATURES.items()}
        original_windows = {name: deque(maxlen=length) for name, (_, length) in FEATURES.items()}
        original = []
        bar_parts = [
            part
            for part in manifest["parts"]
            if part["kind"] == "bars" and part["symbol"] == symbol
        ]
        for part in bar_parts:
            original = [
                tuple(row.values()) for row in read_table(Path(source) / part["path"]).to_pylist()
            ]
            for row in original:
                for name, (column, _) in FEATURES.items():
                    original_windows[name].append(row[FIELDS.index(column)])
            changed = list(original)
            last = list(changed[-1])
            last[4] += 1
            last[2] = max(last[2], last[4])
            changed[-1] = tuple(last)
            revised_parts.extend(
                write_parts(revised, symbol, part["month"], changed, revised_windows)
            )
        last_month = bar_parts[-1]["month"]
        year, month = map(int, last_month.split("-"))
        year += month == 12
        month = month % 12 + 1
        start = int(datetime(year, month, 1, tzinfo=timezone.utc).timestamp() * 1000)
        close = original[-1][4]
        rows = [
            (
                start + i * 60000,
                close + i - 5,
                close + i + 10,
                close + i - 15,
                close + i,
                original[-1][5],
            )
            for i in range(128)
        ]
        extension_parts.extend(
            write_parts(extension, symbol, f"{year}-{month:02}", rows, original_windows)
        )
    parent = hashlib.sha256((Path(source) / "manifest.json").read_bytes()).hexdigest()
    revision_manifest = {
        **manifest,
        "parts": revised_parts,
        "parent_manifest_sha256": parent,
        "revision": "last close of each original month +1; recompute all four rolling features; full rebuild into new root",
    }
    extension_manifest = {
        **manifest,
        "parts": extension_parts,
        "parent_manifest_sha256": parent,
        "extension": "128 synthetic later bars per symbol with rolling features continued from original history",
    }
    for destination, data in [
        (revised, revision_manifest),
        (extension, extension_manifest),
    ]:
        (destination / "manifest.json").write_text(json.dumps(data, indent=2) + "\n")
    return revised, revision_manifest, extension, extension_manifest
