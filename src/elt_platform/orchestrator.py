"""Pipeline orchestration: extract -> contract gate -> load -> transform
-> post-transform contracts + data tests -> run report.

Guarantees the platform is operated against:

* **Fail-closed contracts.** Breaching rows are quarantined with reasons
  at extract *and* after transform; they never silently land as trusted
  data. A structural breach (a source with zero valid rows, a transform
  target that fails its contract outright) fails the run.
* **Idempotent re-runs.** Loads are replace-mode and transforms are
  DROP+CREATE, so the same inputs converge to the same warehouse. An
  ``idempotency_key`` short-circuits a duplicate successful run entirely.
* **Retries with accounting.** Transform steps retry up to
  ``max_retries``; attempts are recorded in the manifest.
* **A manifest for everything.** Success or failure, the run leaves a
  manifest with per-step counts and the full data-test report.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from .contracts import Contract, coerce_rows, split_rows, validate_freshness, validate_rows
from .extractors import Extractor, SourceSpec
from .loader import Warehouse
from .manifest import ManifestStore, RunManifest, utcnow_iso
from .quality import (
    DataQualityReport,
    check_not_null,
    check_referential_integrity,
    check_row_count,
    check_uniqueness,
)
from .transforms import TransformDAG


@dataclass
class ReferentialCheck:
    child_table: str
    child_column: str
    parent_table: str
    parent_column: str


@dataclass
class Pipeline:
    name: str
    sources: list[SourceSpec]
    contracts: dict[str, Contract]  # keyed by table
    dag: TransformDAG = field(default_factory=TransformDAG)
    referential_checks: list[ReferentialCheck] = field(default_factory=list)
    base_dir: str | Path | None = None
    max_retries: int = 1

    # ------------------------------------------------------------------
    def run(self, warehouse: Warehouse, store: ManifestStore, *,
            idempotency_key: str | None = None) -> RunManifest:
        if idempotency_key is not None:
            existing = store.find_by_idempotency_key(idempotency_key)
            if existing is not None:
                return existing

        manifest = RunManifest(pipeline=self.name, idempotency_key=idempotency_key)
        extractor = Extractor(self.base_dir)
        try:
            self._extract_and_load(manifest, extractor, warehouse)
            self._run_transforms(manifest, warehouse)
            self._validate_transform_outputs(manifest, warehouse)
            report = self._run_data_tests(warehouse)
            manifest.quality = report.to_dict()
            if not report.passed:
                failed = ", ".join(c.name for c in report.failed_checks)
                raise RuntimeError(f"data tests failed: {failed}")
            manifest.finish("success")
        except Exception as exc:
            manifest.finish("failed", error=str(exc))
        store.save(manifest)
        return manifest

    # -- stages ---------------------------------------------------------
    def _extract_and_load(self, manifest: RunManifest, extractor: Extractor,
                          warehouse: Warehouse) -> None:
        for spec in self.sources:
            step = manifest.step(f"extract:{spec.name}")
            rows = extractor.extract(spec)
            step.rows_in = len(rows)
            step.status = "success"
            step.finished_at = utcnow_iso()

            contract = self.contracts.get(spec.table)
            if contract is None:
                raise RuntimeError(f"source {spec.name!r}: no contract for table {spec.table!r}")

            load_step = manifest.step(f"load:{spec.table}")
            typed = coerce_rows(rows, contract)
            valid, quarantined = split_rows(typed, contract)
            fresh = validate_freshness(valid, contract)
            if not fresh.passed:
                # Freshness is a table-level breach: quarantine everything.
                quarantined = quarantined + [(r, fresh.errors) for r in valid]
                valid = []
            if quarantined:
                warehouse.load_quarantine(spec.table, quarantined)
            if not valid:
                load_step.status = "failed"
                load_step.rows_quarantined = len(quarantined)
                raise RuntimeError(
                    f"table {spec.table!r}: 0 valid rows out of {len(rows)} — "
                    f"contract breach quarantined the whole source")
            warehouse.create_table(contract, replace=True)
            load_step.rows_out = warehouse.load_rows(spec.table, valid, mode="replace")
            load_step.rows_quarantined = len(quarantined)
            load_step.status = "success"
            load_step.detail = (
                f"{load_step.rows_out} loaded, {len(quarantined)} quarantined")
            load_step.finished_at = utcnow_iso()

    def _run_transforms(self, manifest: RunManifest, warehouse: Warehouse) -> None:
        if len(self.dag) == 0:
            return
        step = manifest.step("transform")
        results = self.dag.run(warehouse, max_retries=self.max_retries)
        step.rows_out = sum(r.rows for r in results)
        step.attempts = max((r.attempts for r in results), default=1)
        step.detail = ", ".join(f"{r.name}={r.rows} rows" for r in results)
        step.status = "success"
        step.finished_at = utcnow_iso()

    def _validate_transform_outputs(self, manifest: RunManifest,
                                    warehouse: Warehouse) -> None:
        for name in self.dag.names():
            contract = self.contracts.get(name)
            if contract is None:
                continue
            step = manifest.step(f"contract:{name}")
            rows = warehouse.read_table(name)
            result = validate_rows(coerce_rows(rows, contract), contract)
            step.rows_in = len(rows)
            if not result.passed:
                warehouse.load_quarantine(
                    name, [(r, result.errors) for r in rows[:1]])
                step.status = "failed"
                step.detail = "; ".join(result.errors[:3])
                raise RuntimeError(
                    f"transform output {name!r} breaches its contract: {result.errors[:3]}")
            step.status = "success"
            step.finished_at = utcnow_iso()

    def _run_data_tests(self, warehouse: Warehouse) -> DataQualityReport:
        report = DataQualityReport()
        tables = set(self.contracts) | set(self.dag.names())
        for table in sorted(tables):
            if not warehouse.table_exists(table):
                continue
            rows = warehouse.read_table(table)
            report.checks.append(check_row_count(rows, table=table, min_rows=0))
            contract = self.contracts.get(table)
            if contract is not None:
                for pk in contract.primary_key:
                    report.checks.append(check_uniqueness(rows, table=table, column=pk))
                    report.checks.append(check_not_null(rows, table=table, column=pk))
        for ref in self.referential_checks:
            report.checks.append(check_referential_integrity(
                warehouse.read_table(ref.child_table), child_table=ref.child_table,
                child_column=ref.child_column,
                parent_rows=warehouse.read_table(ref.parent_table),
                parent_table=ref.parent_table, parent_column=ref.parent_column))
        return report
