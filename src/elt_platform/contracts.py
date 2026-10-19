"""Data contracts: declarative schema definitions and validation.

A contract is the agreement between a producer and this platform. It is
declared in YAML or JSON (see ``examples/contracts.yml``)::

    name: customers
    table: customers
    freshness_sla_hours: 48
    columns:
      - {name: customer_id, type: int, nullable: false, unique: true}
      - {name: email, type: string}
    primary_key: [customer_id]

Validation is *fail-closed*: a row that breaches the contract never
reaches the warehouse as trusted data. ``split_rows`` separates valid
rows from quarantined rows (each with machine-readable reasons) so the
orchestrator can quarantine the bad and load the good — and account for
every row.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

SUPPORTED_TYPES = {"string", "int", "float", "bool", "date", "datetime"}


@dataclass(frozen=True)
class ColumnContract:
    name: str
    type: str = "string"
    nullable: bool = True
    unique: bool = False
    min_value: float | None = None
    max_value: float | None = None
    min_length: int | None = None
    allowed_values: tuple[Any, ...] | None = None
    pattern: str | None = None

    def __post_init__(self) -> None:
        if self.type not in SUPPORTED_TYPES:
            raise ValueError(f"column {self.name!r}: unsupported type {self.type!r}")

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "ColumnContract":
        allowed = data.get("allowed_values")
        return cls(
            name=str(data["name"]),
            type=str(data.get("type", "string")),
            nullable=bool(data.get("nullable", True)),
            unique=bool(data.get("unique", False)),
            min_value=data.get("min_value"),
            max_value=data.get("max_value"),
            min_length=data.get("min_length"),
            allowed_values=tuple(allowed) if allowed is not None else None,
            pattern=data.get("pattern"),
        )


@dataclass(frozen=True)
class Contract:
    name: str
    table: str
    columns: tuple[ColumnContract, ...]
    primary_key: tuple[str, ...] = ()
    freshness_sla_hours: float | None = None
    freshness_column: str | None = None
    description: str = ""

    def column(self, name: str) -> ColumnContract | None:
        return next((c for c in self.columns if c.name == name), None)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "Contract":
        return cls(
            name=str(data["name"]),
            table=str(data.get("table", data["name"])),
            columns=tuple(ColumnContract.from_dict(c) for c in data["columns"]),
            primary_key=tuple(data.get("primary_key", ())),
            freshness_sla_hours=data.get("freshness_sla_hours"),
            freshness_column=data.get("freshness_column"),
            description=str(data.get("description", "")),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name, "table": self.table,
            "primary_key": list(self.primary_key),
            "freshness_sla_hours": self.freshness_sla_hours,
            "freshness_column": self.freshness_column,
            "description": self.description,
            "columns": [
                {"name": c.name, "type": c.type, "nullable": c.nullable,
                 "unique": c.unique, "min_value": c.min_value,
                 "max_value": c.max_value, "min_length": c.min_length,
                 "allowed_values": list(c.allowed_values) if c.allowed_values else None,
                 "pattern": c.pattern}
                for c in self.columns
            ],
        }


def load_contracts(path: str | Path) -> list[Contract]:
    """Load one or more contracts from a YAML or JSON file."""
    path = Path(path)
    text = path.read_text(encoding="utf-8")
    if path.suffix.lower() == ".json":
        data: Any = json.loads(text)
    else:
        try:
            import yaml
        except ImportError as exc:  # pragma: no cover
            raise RuntimeError("PyYAML is required to load .yml/.yaml contracts") from exc
        data = yaml.safe_load(text)
    if isinstance(data, dict) and "contracts" in data:
        data = data["contracts"]
    if isinstance(data, dict):
        data = [data]
    return [Contract.from_dict(item) for item in data]


def _parse_temporal(value: Any, kind: str) -> bool:
    if isinstance(value, (date, datetime)):
        return True
    if not isinstance(value, str):
        return False
    try:
        if kind == "date":
            date.fromisoformat(value)
        else:
            datetime.fromisoformat(value.replace("Z", "+00:00"))
        return True
    except ValueError:
        return False


def check_type(value: Any, type_name: str) -> bool:
    if type_name == "int":
        return isinstance(value, int) and not isinstance(value, bool)
    if type_name == "float":
        return isinstance(value, (int, float)) and not isinstance(value, bool)
    if type_name == "string":
        return isinstance(value, str)
    if type_name == "bool":
        return isinstance(value, bool)
    if type_name in {"date", "datetime"}:
        return _parse_temporal(value, type_name)
    return False


def coerce_value(value: Any, type_name: str) -> Any:
    """Best-effort coercion of raw (often string) input to a declared type.

    Un-coercible values are returned unchanged so validation can flag
    them rather than the coercion layer silently hiding a breach.
    """
    if value is None or isinstance(value, bool) and type_name != "bool":
        return value
    if type_name == "int" and isinstance(value, str):
        try:
            return int(value)
        except ValueError:
            return value
    if type_name == "float" and isinstance(value, str):
        try:
            return float(value)
        except ValueError:
            return value
    if type_name == "bool" and isinstance(value, str):
        if value.strip().lower() in {"true", "1", "yes"}:
            return True
        if value.strip().lower() in {"false", "0", "no"}:
            return False
    return value


def coerce_rows(rows: list[dict[str, Any]], contract: Contract) -> list[dict[str, Any]]:
    out = []
    for row in rows:
        new = dict(row)
        for col in contract.columns:
            if col.name in new:
                new[col.name] = coerce_value(new[col.name], col.type)
        out.append(new)
    return out


@dataclass
class ValidationResult:
    contract: str
    checked_rows: int
    errors: list[str] = field(default_factory=list)

    @property
    def passed(self) -> bool:
        return not self.errors

    def to_dict(self) -> dict[str, Any]:
        return {"contract": self.contract, "checked_rows": self.checked_rows,
                "passed": self.passed, "errors": list(self.errors)}


def validate_row(row: dict[str, Any], contract: Contract) -> list[str]:
    """Return the list of contract breaches for a single row (empty = valid)."""
    errors: list[str] = []
    for col in contract.columns:
        if col.name not in row or row[col.name] is None or row[col.name] == "":
            if not col.nullable:
                errors.append(f"{col.name}: missing or null but contract is non-nullable")
            continue
        value = row[col.name]
        if not check_type(value, col.type):
            errors.append(f"{col.name}: expected {col.type}, got {type(value).__name__} ({value!r})")
            continue
        if col.min_value is not None and isinstance(value, (int, float)) and value < col.min_value:
            errors.append(f"{col.name}: {value} below min_value {col.min_value}")
        if col.max_value is not None and isinstance(value, (int, float)) and value > col.max_value:
            errors.append(f"{col.name}: {value} above max_value {col.max_value}")
        if col.min_length is not None and isinstance(value, str) and len(value) < col.min_length:
            errors.append(f"{col.name}: shorter than min_length {col.min_length}")
        if col.allowed_values is not None and value not in col.allowed_values:
            errors.append(f"{col.name}: {value!r} not in allowed values {list(col.allowed_values)}")
        if col.pattern is not None and isinstance(value, str) and not re.search(col.pattern, value):
            errors.append(f"{col.name}: {value!r} does not match pattern {col.pattern!r}")
    return errors


def split_rows(
    rows: list[dict[str, Any]], contract: Contract
) -> tuple[list[dict[str, Any]], list[tuple[dict[str, Any], list[str]]]]:
    """Split rows into (valid, quarantined-with-reasons), enforcing uniqueness too."""
    valid: list[dict[str, Any]] = []
    quarantined: list[tuple[dict[str, Any], list[str]]] = []
    seen_unique: dict[str, set[Any]] = {
        c.name: set() for c in contract.columns if c.unique or c.name in contract.primary_key
    }
    for row in rows:
        errors = validate_row(row, contract)
        for col_name, seen in seen_unique.items():
            value = row.get(col_name)
            if value is not None:
                if value in seen:
                    errors.append(f"{col_name}: duplicate value {value!r} breaches uniqueness")
                else:
                    seen.add(value)
        if errors:
            quarantined.append((row, errors))
        else:
            valid.append(row)
    return valid, quarantined


def validate_rows(rows: list[dict[str, Any]], contract: Contract) -> ValidationResult:
    _, quarantined = split_rows(rows, contract)
    errors = [f"row {i}: {err}" for row, errs in quarantined for err in errs
              for i in [rows.index(row) if row in rows else -1]]
    # Simpler deterministic messages without index lookup pitfalls:
    if quarantined:
        errors = [err for _, errs in quarantined for err in errs]
    return ValidationResult(contract=contract.name, checked_rows=len(rows), errors=errors)


def validate_freshness(
    rows: list[dict[str, Any]],
    contract: Contract,
    *,
    now: datetime | None = None,
) -> ValidationResult:
    """Check the contract's freshness SLA against its timestamp column."""
    result = ValidationResult(contract=contract.name, checked_rows=len(rows))
    if contract.freshness_sla_hours is None or not contract.freshness_column:
        return result
    now = now or datetime.now(timezone.utc)
    stamps: list[datetime] = []
    for row in rows:
        raw = row.get(contract.freshness_column)
        if raw is None:
            continue
        if isinstance(raw, datetime):
            stamps.append(raw if raw.tzinfo else raw.replace(tzinfo=timezone.utc))
        elif isinstance(raw, date):
            stamps.append(datetime(raw.year, raw.month, raw.day, tzinfo=timezone.utc))
        elif isinstance(raw, str):
            try:
                parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
                stamps.append(parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc))
            except ValueError:
                continue
    if not stamps:
        result.errors.append(f"freshness: no parseable {contract.freshness_column} values")
        return result
    age_hours = (now - max(stamps)).total_seconds() / 3600.0
    if age_hours > contract.freshness_sla_hours:
        result.errors.append(
            f"freshness: newest {contract.freshness_column} is {age_hours:.1f}h old, "
            f"SLA is {contract.freshness_sla_hours}h"
        )
    return result
