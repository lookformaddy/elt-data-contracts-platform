"""Data tests: row counts, uniqueness, referential integrity, and friends.

Contracts validate *shape*; data tests validate *content relationships*
that only make sense across a whole table or between tables — the checks
a warehouse test framework (dbt tests, Great Expectations) runs after a
transform. Every check returns a :class:`CheckResult`; a suite aggregates
them into a report the orchestrator embeds in the run manifest.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class CheckResult:
    name: str
    passed: bool
    detail: str = ""
    actual: Any = None

    def to_dict(self) -> dict[str, Any]:
        return {"name": self.name, "passed": self.passed,
                "detail": self.detail, "actual": self.actual}


@dataclass
class DataQualityReport:
    checks: list[CheckResult] = field(default_factory=list)

    @property
    def passed(self) -> bool:
        return all(c.passed for c in self.checks)

    @property
    def failed_checks(self) -> list[CheckResult]:
        return [c for c in self.checks if not c.passed]

    def to_dict(self) -> dict[str, Any]:
        return {"passed": self.passed,
                "checks": [c.to_dict() for c in self.checks]}


def check_row_count(rows: list[dict[str, Any]], *, table: str, min_rows: int = 1,
                    max_rows: int | None = None) -> CheckResult:
    ok = len(rows) >= min_rows and (max_rows is None or len(rows) <= max_rows)
    return CheckResult(
        name=f"{table}.row_count", passed=ok, actual=len(rows),
        detail=f"{table} has {len(rows)} rows (expected >= {min_rows}"
               + (f", <= {max_rows}" if max_rows is not None else "") + ")")


def check_uniqueness(rows: list[dict[str, Any]], *, table: str, column: str) -> CheckResult:
    values = [r.get(column) for r in rows if r.get(column) is not None]
    dupes = sorted({v for v in values if values.count(v) > 1}, key=str)
    return CheckResult(
        name=f"{table}.{column}.unique", passed=not dupes, actual=len(values),
        detail="no duplicates" if not dupes else f"duplicate values: {dupes[:5]}")


def check_not_null(rows: list[dict[str, Any]], *, table: str, column: str) -> CheckResult:
    missing = sum(1 for r in rows if r.get(column) is None)
    return CheckResult(
        name=f"{table}.{column}.not_null", passed=missing == 0, actual=missing,
        detail=f"{missing} null values in {table}.{column}")


def check_referential_integrity(child_rows: list[dict[str, Any]], *, child_table: str,
                                child_column: str, parent_rows: list[dict[str, Any]],
                                parent_table: str, parent_column: str) -> CheckResult:
    parent_keys = {r.get(parent_column) for r in parent_rows}
    orphans = sorted(
        {r.get(child_column) for r in child_rows
         if r.get(child_column) is not None and r.get(child_column) not in parent_keys},
        key=str)
    return CheckResult(
        name=f"{child_table}.{child_column}-> {parent_table}.{parent_column}".replace(" ", ""),
        passed=not orphans, actual=len(orphans),
        detail="all foreign keys resolve" if not orphans else f"orphan keys: {orphans[:5]}")


def check_accepted_values(rows: list[dict[str, Any]], *, table: str, column: str,
                          allowed: list[Any]) -> CheckResult:
    bad = sorted({r.get(column) for r in rows if r.get(column) not in allowed}, key=str)
    return CheckResult(
        name=f"{table}.{column}.accepted_values", passed=not bad, actual=len(bad),
        detail="all values accepted" if not bad else f"unexpected values: {bad[:5]}")


def check_sum_equals(a_rows: list[dict[str, Any]], a_column: str,
                     b_rows: list[dict[str, Any]], b_column: str, *,
                     name: str, tolerance: float = 0.01) -> CheckResult:
    a_total = round(sum(float(r[a_column]) for r in a_rows if r.get(a_column) is not None), 2)
    b_total = round(sum(float(r[b_column]) for r in b_rows if r.get(b_column) is not None), 2)
    return CheckResult(
        name=name, passed=abs(a_total - b_total) <= tolerance,
        actual={"a": a_total, "b": b_total},
        detail=f"sum {a_column}={a_total} vs sum {b_column}={b_total}")
