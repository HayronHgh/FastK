"""Public in-process adapters, all delivering the same NumPy batch layout."""

from __future__ import annotations

import os
import sqlite3
from collections import OrderedDict
from pathlib import Path

import numpy as np

from .batches import columns_for, dtype, from_columns, from_table, input_batch, select
from .data import iter_parts

NAMES = ["fastk", "sqlite", "duckdb", "parquet", "arrow"]


def ingest(backend, source):
    for part, table in iter_parts(source, backend.manifest):
        backend.append(part, input_batch(table))


class FastK:
    def __init__(self, root, manifest, *, threads=1, write=False):
        from fastk_native import Reader, Writer

        self.manifest = manifest
        self.handle = Writer(root) if write else Reader(root, threads=threads)

    def setup(self, source):
        ingest(self, source)

    def append(self, part, table):
        batch = input_batch(table)
        if part["kind"] == "bars":
            self.handle.write_kline_range(
                part["symbol"],
                "1m",
                batch,
                timeframe_ms=60_000,
                price_scale=self.manifest["scale"],
                volume_scale=self.manifest["scale"],
            )
        else:
            self.handle.write_scalar_range(
                part["symbol"], "1m", "feature", part["kind"], batch, timeframe_ms=60_000
            )

    def query(self, q):
        if q["kind"].startswith("predicate-"):
            result = self.handle.query_scalar_predicate(
                q["symbol"],
                "1m",
                "feature",
                q["feature"],
                q["start_ts"],
                q["end_ts"],
                predicate="eq" if "value" in q else "between",
                value=q.get("value"),
                min=q.get("min"),
                max=q.get("max"),
            )
        elif "feature" in q:
            result = self.handle.read_scalar_range(
                q["symbol"], "1m", "feature", q["feature"], q["start_ts"], q["end_ts"]
            )
        elif q["kind"] == "latest":
            result = self.handle.read_kline_latest(q["symbol"], "1m", q["limit"])
        else:
            result = self.handle.read_kline_range(q["symbol"], "1m", q["start_ts"], q["end_ts"])
        return select(result, columns_for(q))

    def close(self):
        self.handle.close()


class Sql:
    def __init__(self, root, manifest, *, duck=False, threads=1, write=False):
        self.root = Path(root)
        if write:
            self.root.mkdir(parents=True, exist_ok=True)
        self.duck = duck
        self.manifest = manifest
        if duck:
            import duckdb

            self.connection = duckdb.connect(
                str(self.root / "data.duckdb"), read_only=not write, config={"threads": threads}
            )
        else:
            path = (self.root / "data.sqlite").resolve()
            self.connection = sqlite3.connect(
                str(path) if write else path.as_uri() + "?mode=ro", uri=not write
            )
            self.connection.execute("PRAGMA cache_size=-65536")
            if write:
                self.connection.executescript("PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL;")
        if write:
            tail = "" if duck else " WITHOUT ROWID"
            integer = "BIGINT" if duck else "INTEGER"
            self.connection.execute(
                f"CREATE TABLE IF NOT EXISTS bars(symbol TEXT NOT NULL,ts {integer} NOT NULL,open {integer} NOT NULL,high {integer} NOT NULL,low {integer} NOT NULL,close {integer} NOT NULL,volume {integer} NOT NULL,PRIMARY KEY(symbol,ts)){tail}"
            )
            self.connection.execute(
                f"CREATE TABLE IF NOT EXISTS features(symbol TEXT NOT NULL,name TEXT NOT NULL,ts {integer} NOT NULL,value {integer} NOT NULL,PRIMARY KEY(symbol,name,ts)){tail}"
            )
            self.connection.commit()

    def setup(self, source):
        ingest(self, source)
        self.connection.execute(
            "CREATE INDEX IF NOT EXISTS feature_value ON features(symbol,name,value,ts)"
        )
        self.connection.commit()
        self.connection.execute("CHECKPOINT" if self.duck else "PRAGMA wal_checkpoint(TRUNCATE)")

    def append(self, part, table):
        batch = input_batch(table)
        if self.duck:
            import pyarrow as pa

            incoming = pa.Table.from_arrays(
                [pa.array(batch[name]) for name in batch.dtype.names], names=list(batch.dtype.names)
            )
            self.connection.register("incoming", incoming)
            try:
                self.connection.execute("BEGIN TRANSACTION")
                if part["kind"] == "bars":
                    self.connection.execute(
                        "INSERT INTO bars SELECT ?,ts,open,high,low,close,volume FROM incoming",
                        [part["symbol"]],
                    )
                else:
                    self.connection.execute(
                        "INSERT INTO features SELECT ?,?,ts,value FROM incoming",
                        [part["symbol"], part["kind"]],
                    )
                self.connection.commit()
            except BaseException:
                self.connection.rollback()
                raise
            finally:
                self.connection.unregister("incoming")
        else:
            # sqlite3 requires Python integers; numpy.int64 can bind as BLOB.
            rows = batch.tolist()
            if part["kind"] == "bars":
                self.connection.executemany(
                    "INSERT INTO bars VALUES (?,?,?,?,?,?,?)",
                    [(part["symbol"], *row) for row in rows],
                )
            else:
                self.connection.executemany(
                    "INSERT INTO features VALUES (?,?,?,?)",
                    [(part["symbol"], part["kind"], *row) for row in rows],
                )
            self.connection.commit()

    def query(self, q):
        fields = columns_for(q)
        feature = q.get("feature")
        sql = f"SELECT {','.join(fields)} FROM {'features' if feature else 'bars'} WHERE symbol=? AND ts>=? AND ts<=?"
        params = [q["symbol"], q["start_ts"], q["end_ts"]]
        if feature:
            sql += " AND name=?"
            params.append(feature)
        if "value" in q:
            sql += " AND value=?"
            params.append(q["value"])
        if "min" in q:
            sql += " AND value>=? AND value<=?"
            params.extend([q["min"], q["max"]])
        sql += " ORDER BY ts " + ("DESC LIMIT ?" if q["kind"] == "latest" else "ASC")
        if q["kind"] == "latest":
            params.append(q["limit"])
        cursor = self.connection.execute(sql, params)
        result = (
            from_columns(cursor.fetchnumpy(), fields)
            if self.duck
            else np.array(cursor.fetchall(), dtype=dtype(fields))
        )
        if q["kind"] == "latest":
            result = result[::-1].copy()
        result.flags.writeable = False
        return result

    def close(self):
        self.connection.close()


class Files:
    def __init__(self, root, manifest, *, parquet=False, threads=1, write=False):
        import pyarrow as pa

        self.root = Path(root)
        self.parquet = parquet
        self.manifest = manifest
        self.datasets = {}
        self.ipc_batches = OrderedDict()
        self.parts = {}
        for part in manifest["parts"]:
            self.parts.setdefault((part["symbol"], part["kind"]), []).append(part)
        for parts in self.parts.values():
            parts.sort(key=lambda part: part["start_ts"])
        self.threads = threads
        pa.set_cpu_count(threads)
        pa.set_io_thread_count(threads)

    def path(self, part):
        return self.root / Path(part["path"]).with_suffix(".parquet" if self.parquet else ".arrow")

    def setup(self, source):
        ingest(self, source)

    def append(self, part, table):
        import pyarrow as pa

        batch = input_batch(table)
        table = pa.Table.from_arrays(
            [pa.array(batch[name]) for name in batch.dtype.names], names=list(batch.dtype.names)
        )
        path = self.path(part)
        path.parent.mkdir(parents=True, exist_ok=True)
        if self.parquet:
            import pyarrow.parquet as pq

            pq.write_table(
                table, path, compression="zstd", row_group_size=8192, write_statistics=True
            )
        else:
            with pa.OSFile(str(path), "wb") as sink, pa.ipc.new_file(sink, table.schema) as writer:
                writer.write_table(table)
        with path.open("rb+") as file:
            os.fsync(file.fileno())
        if os.name == "posix":
            descriptor = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
            try:
                os.fsync(descriptor)
            finally:
                os.close(descriptor)
        self.datasets.clear()
        self.ipc_batches.clear()

    def query(self, q):
        import pyarrow.dataset as ds

        kind = q.get("feature", "bars")
        parts = [
            p
            for p in self.parts.get((q["symbol"], kind), [])
            if p["end_ts"] >= q["start_ts"] and p["start_ts"] <= q["end_ts"]
        ]
        if q["kind"] == "latest":
            selected = []
            count = 0
            for part in reversed(parts):
                selected.append(part)
                if q["start_ts"] <= part["start_ts"] and q["end_ts"] >= part["end_ts"]:
                    count += part["rows"]
                if count >= q["limit"]:
                    break
            parts = list(reversed(selected))
        fields = columns_for(q)
        if not parts:
            return from_columns({name: np.empty(0, dtype="i8") for name in fields}, fields)
        if not self.parquet:
            return self.query_ipc(parts, q, fields)
        paths = tuple(str(self.path(part)) for part in parts)
        if paths not in self.datasets:
            self.datasets[paths] = ds.dataset(
                list(paths), format="parquet" if self.parquet else "ipc"
            )
        mask = (ds.field("ts") >= q["start_ts"]) & (ds.field("ts") <= q["end_ts"])
        if "value" in q:
            mask = mask & (ds.field("value") == q["value"])
        if "min" in q:
            mask = mask & (ds.field("value") >= q["min"]) & (ds.field("value") <= q["max"])
        table = self.datasets[paths].to_table(
            columns=fields, filter=mask, use_threads=self.threads > 1
        )
        result = from_table(table, fields)
        return result[-q["limit"] :] if q["kind"] == "latest" else result

    def query_ipc(self, parts, q, fields):
        import pyarrow as pa

        pieces = []
        for part in parts:
            path = str(self.path(part))
            if path not in self.ipc_batches:
                source = pa.memory_map(path, "r")
                table = pa.ipc.open_file(source).read_all()
                columns = {
                    name: table[name].chunk(0).to_numpy(zero_copy_only=True)
                    if table[name].num_chunks == 1
                    else table[name].to_numpy()
                    for name in table.column_names
                }
                self.ipc_batches[path] = source, columns
                # Match FastK's default open-file budget; views retain their owners.
                if len(self.ipc_batches) > 64:
                    self.ipc_batches.popitem(last=False)
            self.ipc_batches.move_to_end(path)
            _, columns = self.ipc_batches[path]
            start = np.searchsorted(columns["ts"], q["start_ts"], side="left")
            end = np.searchsorted(columns["ts"], q["end_ts"], side="right")
            piece = {name: columns[name][start:end] for name in fields}
            if "value" in q:
                mask = piece["value"] == q["value"]
                piece = {name: values[mask] for name, values in piece.items()}
            elif "min" in q:
                mask = (piece["value"] >= q["min"]) & (piece["value"] <= q["max"])
                piece = {name: values[mask] for name, values in piece.items()}
            pieces.append(piece)
        if q["kind"] == "latest":
            remaining = q["limit"]
            selected = []
            for piece in reversed(pieces):
                take = min(remaining, len(piece["ts"]))
                if take:
                    selected.append({name: values[-take:] for name, values in piece.items()})
                remaining -= take
                if not remaining:
                    break
            pieces = list(reversed(selected))
        result = np.empty(sum(len(piece["ts"]) for piece in pieces), dtype=dtype(fields))
        offset = 0
        for piece in pieces:
            count = len(piece["ts"])
            for name in fields:
                result[name][offset : offset + count] = piece[name]
            offset += count
        result.flags.writeable = False
        return result

    def close(self):
        self.datasets.clear()
        self.ipc_batches.clear()


def connect(name, root, manifest, binary=None, *, threads=1, write=False):
    if threads < 1:
        raise ValueError("thread budget must be positive")
    if name == "fastk":
        return FastK(root, manifest, threads=threads, write=write)
    if name in {"sqlite", "duckdb"}:
        return Sql(root, manifest, duck=name == "duckdb", threads=threads, write=write)
    if name in {"arrow", "parquet"}:
        return Files(root, manifest, parquet=name == "parquet", threads=threads, write=write)
    raise ValueError(name)
