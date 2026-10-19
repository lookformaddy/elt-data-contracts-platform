"""Run manifests: the durable record of every pipeline run.

A manifest answers, for any run: what ran, in what order, how many rows
moved where, what was quarantined and why, which data tests passed, and
whether the run succeeded. Manifests are persisted as one JSON file per
run plus an append-only ``index.jsonl`` — the same shape a production
orchestrator (Airflow/Dagster metadata, a run-registry table) keeps,
without the infrastructure.

The store is also the idempotency ledger: a successful run recorded
under an idempotency key is returned instead of re-executing.
"""

from __future__ import annotations

import json
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass
class StepRecord:
    name: str
    status: str = "pending"  # pending | success | failed | skipped
    rows_in: int = 0
    rows_out: int = 0
    rows_quarantined: int = 0
    attempts: int = 1
    detail: str = ""
    started_at: str = ""
    finished_at: str = ""

    def to_dict(self) -> dict[str, Any]:
        return vars(self).copy()


@dataclass
class RunManifest:
    pipeline: str
    run_id: str = field(default_factory=lambda: uuid.uuid4().hex[:12])
    idempotency_key: str | None = None
    status: str = "running"  # running | success | failed
    started_at: str = field(default_factory=utcnow_iso)
    finished_at: str = ""
    steps: list[StepRecord] = field(default_factory=list)
    quality: dict[str, Any] = field(default_factory=dict)
    error: str = ""

    def step(self, name: str) -> StepRecord:
        record = StepRecord(name=name, started_at=utcnow_iso())
        self.steps.append(record)
        return record

    @property
    def total_quarantined(self) -> int:
        return sum(s.rows_quarantined for s in self.steps)

    @property
    def total_loaded(self) -> int:
        return sum(s.rows_out for s in self.steps if s.name.startswith("load:"))

    def finish(self, status: str, error: str = "") -> None:
        self.status = status
        self.error = error
        self.finished_at = utcnow_iso()

    def to_dict(self) -> dict[str, Any]:
        return {
            "pipeline": self.pipeline, "run_id": self.run_id,
            "idempotency_key": self.idempotency_key, "status": self.status,
            "started_at": self.started_at, "finished_at": self.finished_at,
            "steps": [s.to_dict() for s in self.steps],
            "quality": self.quality, "error": self.error,
            "total_loaded": self.total_loaded,
            "total_quarantined": self.total_quarantined,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "RunManifest":
        manifest = cls(
            pipeline=data["pipeline"], run_id=data["run_id"],
            idempotency_key=data.get("idempotency_key"),
            status=data.get("status", "running"),
            started_at=data.get("started_at", ""), finished_at=data.get("finished_at", ""),
            quality=data.get("quality", {}), error=data.get("error", ""),
        )
        manifest.steps = [StepRecord(**s) for s in data.get("steps", [])]
        return manifest


class ManifestStore:
    """File-backed manifest store. ``root=None`` keeps manifests in memory."""

    def __init__(self, root: str | Path | None = None) -> None:
        self.root = Path(root) if root else None
        self._memory: dict[str, RunManifest] = {}
        if self.root is not None:
            self.root.mkdir(parents=True, exist_ok=True)

    def save(self, manifest: RunManifest) -> None:
        self._memory[manifest.run_id] = manifest
        if self.root is not None:
            (self.root / f"{manifest.run_id}.json").write_text(
                json.dumps(manifest.to_dict(), indent=2), encoding="utf-8")
            with (self.root / "index.jsonl").open("a", encoding="utf-8") as fh:
                fh.write(json.dumps({
                    "run_id": manifest.run_id, "pipeline": manifest.pipeline,
                    "status": manifest.status,
                    "idempotency_key": manifest.idempotency_key,
                    "finished_at": manifest.finished_at,
                }) + "\n")

    def get(self, run_id: str) -> RunManifest | None:
        if run_id in self._memory:
            return self._memory[run_id]
        if self.root is not None:
            path = self.root / f"{run_id}.json"
            if path.exists():
                return RunManifest.from_dict(json.loads(path.read_text(encoding="utf-8")))
        return None

    def list_runs(self) -> list[RunManifest]:
        runs = dict(self._memory)
        if self.root is not None:
            for path in sorted(self.root.glob("*.json")):
                data = json.loads(path.read_text(encoding="utf-8"))
                runs.setdefault(data["run_id"], RunManifest.from_dict(data))
        return sorted(runs.values(), key=lambda m: m.started_at)

    def find_by_idempotency_key(self, key: str) -> RunManifest | None:
        for manifest in self.list_runs():
            if manifest.idempotency_key == key and manifest.status == "success":
                return manifest
        return None
