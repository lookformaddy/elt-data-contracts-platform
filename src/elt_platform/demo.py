"""End-to-end demo pipeline: synthetic retail-style sources, two contracts,
SQL + Python transforms, referential data tests, quarantine, and a report.

Used identically by the CLI, ``examples/run_demo.py``, and the API's
demo endpoint, so every entry point demonstrates the same run.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from .contracts import ColumnContract, Contract
from .extractors import SourceSpec
from .generator import write_sources
from .loader import Warehouse
from .manifest import ManifestStore
from .orchestrator import Pipeline, ReferentialCheck
from .transforms import Transform, TransformDAG


def build_contracts() -> dict[str, Contract]:
    customers = Contract(
        name="customers", table="customers",
        description="One row per customer; producer-owned dimension.",
        primary_key=("customer_id",), freshness_sla_hours=24 * 365 * 5,
        freshness_column="signup_date",
        columns=(
            ColumnContract("customer_id", "int", nullable=False, unique=True),
            ColumnContract("name", "string", nullable=False, min_length=1),
            ColumnContract("email", "string", nullable=False, pattern=r"@"),
            ColumnContract("region", "string", nullable=False,
                           allowed_values=("north", "south", "east", "west", "central")),
            ColumnContract("signup_date", "date", nullable=False),
            ColumnContract("lifetime_value", "float", nullable=False, min_value=0),
        ),
    )
    orders = Contract(
        name="orders", table="orders",
        description="One row per order; amounts are non-negative by contract.",
        primary_key=("order_id",),
        columns=(
            ColumnContract("order_id", "int", nullable=False, unique=True),
            ColumnContract("customer_id", "int", nullable=False),
            ColumnContract("amount", "float", nullable=False, min_value=0),
            ColumnContract("quantity", "int", nullable=False, min_value=1),
            ColumnContract("status", "string", nullable=False,
                           allowed_values=("placed", "shipped", "delivered", "cancelled")),
            ColumnContract("order_date", "date", nullable=False),
        ),
    )
    summary = Contract(
        name="customer_summary", table="customer_summary",
        description="Transform output: per-customer order aggregates.",
        primary_key=("customer_id",),
        columns=(
            ColumnContract("customer_id", "int", nullable=False, unique=True),
            ColumnContract("order_count", "int", nullable=False, min_value=0),
            ColumnContract("total_spend", "float", nullable=False, min_value=0),
        ),
    )
    return {c.table: c for c in (customers, orders, summary)}


def _high_value_customers(warehouse: Warehouse) -> list[dict[str, Any]]:
    """Python transform: customers whose total spend is >= 500 (demo threshold)."""
    rows = warehouse.read_table("customer_summary")
    return [{"customer_id": r["customer_id"], "total_spend": r["total_spend"],
             "segment": "high_value"} for r in rows if float(r["total_spend"]) >= 500]


def build_pipeline(source_dir: str | Path) -> Pipeline:
    dag = TransformDAG([
        Transform(
            name="orders_enriched", depends_on=["orders", "customers"],
            sql=("SELECT o.order_id, o.customer_id, c.name AS customer_name, "
                 "c.region, o.amount, o.quantity, o.status, o.order_date "
                 "FROM orders o JOIN customers c ON c.customer_id = o.customer_id"),
            description="Orders joined to customer dimension."),
        Transform(
            name="customer_summary", depends_on=["orders"],
            sql=("SELECT customer_id, COUNT(*) AS order_count, "
                 "ROUND(SUM(amount), 2) AS total_spend "
                 "FROM orders GROUP BY customer_id"),
            description="Per-customer aggregates."),
        Transform(
            name="high_value_customers", depends_on=["customer_summary"],
            python_fn=_high_value_customers,
            description="Python transform: high-value segment from the summary."),
    ])
    return Pipeline(
        name="retail-elt-demo",
        sources=[
            SourceSpec(name="customers", path="customers.csv", format="csv", table="customers"),
            SourceSpec(name="orders", path="orders.json", format="json", table="orders"),
        ],
        contracts=build_contracts(),
        dag=dag,
        referential_checks=[ReferentialCheck(
            child_table="orders", child_column="customer_id",
            parent_table="customers", parent_column="customer_id")],
        base_dir=source_dir,
    )


def run_demo(work_dir: str | Path, *, customers: int = 20, orders: int = 60,
             corrupt: bool = True, idempotency_key: str | None = None) -> dict[str, Any]:
    """Run the demo in ``work_dir`` and return the run report as a dict."""
    work_dir = Path(work_dir)
    source_dir = work_dir / "sources"
    write_sources(source_dir, customers=customers, orders=orders, corrupt=corrupt)
    warehouse = Warehouse(work_dir / "warehouse.db", export_root=work_dir / "warehouse_files")
    store = ManifestStore(work_dir / "manifests")
    try:
        pipeline = build_pipeline(source_dir)
        manifest = pipeline.run(warehouse, store, idempotency_key=idempotency_key)
        report: dict[str, Any] = {
            "run": manifest.to_dict(),
            "tables": {t: warehouse.row_count(t) for t in warehouse.list_tables()},
            "customer_summary_sample": warehouse.read_table("customer_summary")[:3],
        }
        return report
    finally:
        warehouse.close()
