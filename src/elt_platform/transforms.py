"""Transform models and their dependency DAG.

A transform produces one warehouse table, either from a SQL ``SELECT``
(the warehouse materialises it as ``DROP + CREATE TABLE AS``) or from a
Python callable (for logic SQL expresses badly). Transforms declare
``depends_on`` — the tables / other transforms they read — and the DAG
runner executes them in topological order, detects cycles before any
work happens, and reports exactly which transform failed.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable

from .loader import Warehouse

PythonTransform = Callable[[Warehouse], list[dict[str, Any]]]


@dataclass
class Transform:
    name: str  # also the target table name
    depends_on: list[str] = field(default_factory=list)
    sql: str | None = None
    python_fn: PythonTransform | None = None
    description: str = ""

    def __post_init__(self) -> None:
        if bool(self.sql) == bool(self.python_fn):
            raise ValueError(f"transform {self.name!r}: set exactly one of sql / python_fn")


@dataclass
class TransformResult:
    name: str
    rows: int
    attempts: int = 1


class TransformDAG:
    def __init__(self, transforms: list[Transform] | None = None) -> None:
        self._transforms: dict[str, Transform] = {}
        for t in transforms or []:
            self.add(t)

    def add(self, transform: Transform) -> None:
        if transform.name in self._transforms:
            raise ValueError(f"duplicate transform {transform.name!r}")
        self._transforms[transform.name] = transform

    def get(self, name: str) -> Transform:
        return self._transforms[name]

    def names(self) -> list[str]:
        return list(self._transforms)

    def __len__(self) -> int:
        return len(self._transforms)

    def topological_order(self) -> list[str]:
        """Kahn's algorithm; dependencies on raw (non-transform) tables are leaves."""
        indegree: dict[str, int] = {name: 0 for name in self._transforms}
        edges: dict[str, list[str]] = {name: [] for name in self._transforms}
        for name, t in self._transforms.items():
            for dep in t.depends_on:
                if dep in self._transforms:
                    edges[dep].append(name)
                    indegree[name] += 1
        queue = sorted(n for n, d in indegree.items() if d == 0)
        order: list[str] = []
        while queue:
            node = queue.pop(0)
            order.append(node)
            for nxt in sorted(edges[node]):
                indegree[nxt] -= 1
                if indegree[nxt] == 0:
                    queue.append(nxt)
                    queue.sort()
        if len(order) != len(self._transforms):
            remaining = sorted(set(self._transforms) - set(order))
            raise ValueError(f"cycle detected in transform DAG involving: {remaining}")
        return order

    def run(self, warehouse: Warehouse, *, max_retries: int = 0) -> list[TransformResult]:
        results: list[TransformResult] = []
        for name in self.topological_order():
            transform = self._transforms[name]
            attempts = 0
            while True:
                attempts += 1
                try:
                    rows = self._run_one(transform, warehouse)
                    break
                except Exception:
                    if attempts > max_retries:
                        raise
            results.append(TransformResult(name=name, rows=rows, attempts=attempts))
        return results

    @staticmethod
    def _run_one(transform: Transform, warehouse: Warehouse) -> int:
        if transform.sql is not None:
            warehouse.execute(f'DROP TABLE IF EXISTS "{transform.name}"')
            warehouse.execute(f'CREATE TABLE "{transform.name}" AS {transform.sql}')
            return warehouse.row_count(transform.name)
        assert transform.python_fn is not None
        rows = transform.python_fn(warehouse)
        if rows:
            warehouse.create_generic_table(transform.name, list(rows[0].keys()), replace=True)
        else:
            warehouse.create_generic_table(transform.name, ["value"], replace=True)
        return warehouse.load_rows(transform.name, rows, mode="replace")
