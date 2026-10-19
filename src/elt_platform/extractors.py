"""Declarative source extraction: CSV / JSON / JSONL files -> rows.

Sources are *declared*, not coded: a :class:`SourceSpec` names the file,
its format, and the warehouse table (and therefore contract) it feeds.
Adding a source is a config change. Extraction itself is deliberately
dumb — no business logic, no silent type fixing; coercion and validation
belong to the contract layer so breaches stay visible.
"""

from __future__ import annotations

import csv
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class SourceSpec:
    name: str
    path: str
    format: str = "csv"  # csv | json | jsonl
    table: str = ""

    def __post_init__(self) -> None:
        if self.format not in {"csv", "json", "jsonl"}:
            raise ValueError(f"source {self.name!r}: unsupported format {self.format!r}")
        if not self.table:
            object.__setattr__(self, "table", self.name)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "SourceSpec":
        return cls(
            name=str(data["name"]),
            path=str(data["path"]),
            format=str(data.get("format", "csv")),
            table=str(data.get("table", data["name"])),
        )


def extract_csv(path: str | Path) -> list[dict[str, Any]]:
    with Path(path).open(newline="", encoding="utf-8") as fh:
        return [dict(row) for row in csv.DictReader(fh)]


def extract_json(path: str | Path) -> list[dict[str, Any]]:
    data = json.loads(Path(path).read_text(encoding="utf-8"))
    if isinstance(data, dict) and "records" in data:
        data = data["records"]
    if not isinstance(data, list):
        raise ValueError(f"{path}: expected a JSON list of objects")
    return [dict(item) for item in data]


def extract_jsonl(path: str | Path) -> list[dict[str, Any]]:
    rows = []
    for line in Path(path).read_text(encoding="utf-8").splitlines():
        if line.strip():
            rows.append(json.loads(line))
    return rows


_EXTRACTORS = {"csv": extract_csv, "json": extract_json, "jsonl": extract_jsonl}


class Extractor:
    """Resolve a SourceSpec (relative to an optional base dir) to rows."""

    def __init__(self, base_dir: str | Path | None = None) -> None:
        self.base_dir = Path(base_dir) if base_dir else None

    def extract(self, spec: SourceSpec) -> list[dict[str, Any]]:
        path = Path(spec.path)
        if not path.is_absolute() and self.base_dir is not None:
            path = self.base_dir / path
        if not path.exists():
            raise FileNotFoundError(f"source {spec.name!r}: file not found: {path}")
        return _EXTRACTORS[spec.format](path)
