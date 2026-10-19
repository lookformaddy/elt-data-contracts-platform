"""Warehouse loader: a local SQLite warehouse with a file export layout.

SQLite is the local stand-in for a cloud warehouse (Snowflake/BigQuery):
same contract — typed tables created from contracts, replace/append
loads, SQL transforms against it. Every load also mirrors the table to
``<root>/<table>/<table>.json`` (a parquet-style partitioned layout in
spirit: one self-describing file per table) so results are inspectable
without a database client.

Idempotency lives here: a ``replace`` load for a table is atomic
(drop + recreate + insert in one transaction), so re-running a pipeline
with the same inputs converges to the same warehouse state instead of
duplicating rows.
"""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any

from .contracts import Contract

_SQLITE_TYPES = {"int": "INTEGER", "float": "REAL", "bool": "INTEGER",
                 "string": "TEXT", "date": "TEXT", "datetime": "TEXT"}


def _sqlite_value(value: Any) -> Any:
    if isinstance(value, bool):
        return int(value)
    return value


class Warehouse:
    def __init__(self, db_path: str | Path = ":memory:", export_root: str | Path | None = None) -> None:
        self.db_path = str(db_path)
        if self.db_path != ":memory:":
            Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(self.db_path)
        self.conn.row_factory = sqlite3.Row
        self.export_root = Path(export_root) if export_root else None

    # -- schema ---------------------------------------------------------
    def create_table(self, contract: Contract, *, replace: bool = False) -> None:
        cols = ", ".join(
            f'"{c.name}" {_SQLITE_TYPES[c.type]}' + ("" if c.nullable else " NOT NULL")
            for c in contract.columns
        )
        pk = ""
        if contract.primary_key:
            pk = ", PRIMARY KEY (" + ", ".join(f'"{c}"' for c in contract.primary_key) + ")"
        verb = "CREATE TABLE"
        if replace:
            self.conn.execute(f'DROP TABLE IF EXISTS "{contract.table}"')
        self.conn.execute(f'{verb} IF NOT EXISTS "{contract.table}" ({cols}{pk})')
        self.conn.commit()

    def create_generic_table(self, table: str, columns: list[str], *, replace: bool = False) -> None:
        if replace:
            self.conn.execute(f'DROP TABLE IF EXISTS "{table}"')
        cols = ", ".join(f'"{c}"' for c in columns) or '"value"'
        self.conn.execute(f'CREATE TABLE IF NOT EXISTS "{table}" ({cols})')
        self.conn.commit()

    # -- loading --------------------------------------------------------
    def load_rows(self, table: str, rows: list[dict[str, Any]], *, mode: str = "replace") -> int:
        if mode not in {"replace", "append"}:
            raise ValueError(f"unsupported load mode {mode!r}")
        if mode == "replace":
            self.conn.execute(f'DELETE FROM "{table}"')
        if not rows:
            self.conn.commit()
            return 0
        columns = list(rows[0].keys())
        placeholders = ", ".join("?" for _ in columns)
        col_sql = ", ".join(f'"{c}"' for c in columns)
        self.conn.executemany(
            f'INSERT INTO "{table}" ({col_sql}) VALUES ({placeholders})',
            [tuple(_sqlite_value(row.get(c)) for c in columns) for row in rows],
        )
        self.conn.commit()
        if self.export_root is not None:
            self.export_table(table)
        return len(rows)

    def load_quarantine(self, table: str, quarantined: list[tuple[dict[str, Any], list[str]]]) -> int:
        qtable = f"{table}_quarantine"
        self.conn.execute(
            f'CREATE TABLE IF NOT EXISTS "{qtable}" '
            '("payload" TEXT, "reasons" TEXT)'
        )
        self.conn.executemany(
            f'INSERT INTO "{qtable}" ("payload", "reasons") VALUES (?, ?)',
            [(json.dumps(row, default=str), json.dumps(reasons)) for row, reasons in quarantined],
        )
        self.conn.commit()
        return len(quarantined)

    # -- reading --------------------------------------------------------
    def table_exists(self, table: str) -> bool:
        cur = self.conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)
        )
        return cur.fetchone() is not None

    def read_table(self, table: str) -> list[dict[str, Any]]:
        if not self.table_exists(table):
            return []
        cur = self.conn.execute(f'SELECT * FROM "{table}"')
        return [dict(row) for row in cur.fetchall()]

    def row_count(self, table: str) -> int:
        if not self.table_exists(table):
            return 0
        return int(self.conn.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()[0])

    def query(self, sql: str, params: tuple[Any, ...] = ()) -> list[dict[str, Any]]:
        cur = self.conn.execute(sql, params)
        if cur.description is None:
            self.conn.commit()
            return []
        return [dict(row) for row in cur.fetchall()]

    def execute(self, sql: str) -> None:
        self.conn.execute(sql)
        self.conn.commit()

    def list_tables(self) -> list[str]:
        cur = self.conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name"
        )
        return [row[0] for row in cur.fetchall()]

    # -- file layout ------------------------------------------------------
    def export_table(self, table: str) -> Path | None:
        if self.export_root is None:
            return None
        out_dir = self.export_root / table
        out_dir.mkdir(parents=True, exist_ok=True)
        out = out_dir / f"{table}.json"
        out.write_text(json.dumps(self.read_table(table), indent=2, default=str), encoding="utf-8")
        return out

    def close(self) -> None:
        self.conn.close()

    def __enter__(self) -> "Warehouse":
        return self

    def __exit__(self, *exc: Any) -> None:
        self.close()
