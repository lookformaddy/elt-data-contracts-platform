import csv
import json

from elt_platform.demo import run_demo
from elt_platform.extractors import SourceSpec
from elt_platform.orchestrator import Pipeline, ReferentialCheck
from elt_platform.transforms import Transform, TransformDAG


def _write_sources(tmp_path, customers, orders):
    with (tmp_path / "customers.csv").open("w", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(customers[0].keys()))
        writer.writeheader()
        writer.writerows(customers)
    (tmp_path / "orders.json").write_text(json.dumps(orders))


def _pipeline(tmp_path, contracts):
    return Pipeline(
        name="test-pipeline",
        sources=[
            SourceSpec(name="customers", path="customers.csv", format="csv", table="customers"),
            SourceSpec(name="orders", path="orders.json", format="json", table="orders"),
        ],
        contracts=contracts,
        dag=TransformDAG([Transform(
            name="customer_summary", depends_on=["orders"],
            sql=("SELECT customer_id, COUNT(*) AS order_count, "
                 "ROUND(SUM(amount), 2) AS total_spend FROM orders GROUP BY customer_id"))]),
        referential_checks=[ReferentialCheck(
            child_table="orders", child_column="customer_id",
            parent_table="customers", parent_column="customer_id")],
        base_dir=tmp_path,
    )


def test_end_to_end_run_succeeds(tmp_path, warehouse, store, customers_contract,
                                 orders_contract, valid_customers, valid_orders):
    _write_sources(tmp_path, valid_customers, valid_orders)
    contracts = {"customers": customers_contract, "orders": orders_contract}
    manifest = _pipeline(tmp_path, contracts).run(warehouse, store)
    assert manifest.status == "success", manifest.error
    assert warehouse.row_count("customers") == 2
    assert warehouse.row_count("orders") == 3
    assert warehouse.row_count("customer_summary") == 2
    assert manifest.quality["passed"] is True
    assert store.get(manifest.run_id) is not None


def test_contract_breach_is_quarantined_not_loaded(tmp_path, warehouse, store,
                                                   customers_contract, orders_contract,
                                                   valid_customers, valid_orders):
    bad_orders = valid_orders + [
        {"order_id": 1999, "customer_id": 1, "amount": -5.0, "quantity": 1,
         "status": "delivered", "order_date": "2026-10-04"}]
    _write_sources(tmp_path, valid_customers, bad_orders)
    contracts = {"customers": customers_contract, "orders": orders_contract}
    manifest = _pipeline(tmp_path, contracts).run(warehouse, store)
    assert manifest.status == "success", manifest.error
    assert warehouse.row_count("orders") == 3  # bad row never landed as trusted data
    assert warehouse.row_count("orders_quarantine") == 1
    assert manifest.total_quarantined == 1
    reasons = warehouse.read_table("orders_quarantine")[0]["reasons"]
    assert "min_value" in reasons


def test_idempotent_rerun_converges_and_key_short_circuits(
        tmp_path, warehouse, store, customers_contract, orders_contract,
        valid_customers, valid_orders):
    _write_sources(tmp_path, valid_customers, valid_orders)
    contracts = {"customers": customers_contract, "orders": orders_contract}
    pipeline = _pipeline(tmp_path, contracts)
    first = pipeline.run(warehouse, store, idempotency_key="batch-2026-10-19")
    counts_first = (warehouse.row_count("orders"), warehouse.row_count("customer_summary"))

    second = pipeline.run(warehouse, store, idempotency_key="batch-2026-10-19")
    assert second.run_id == first.run_id  # duplicate successful run was not re-executed

    third = pipeline.run(warehouse, store)  # no key: re-executes but converges
    assert third.status == "success"
    assert (warehouse.row_count("orders"),
            warehouse.row_count("customer_summary")) == counts_first


def test_orphan_foreign_key_fails_data_tests(tmp_path, warehouse, store,
                                             customers_contract, orders_contract,
                                             valid_customers, valid_orders):
    orphan_orders = valid_orders + [
        {"order_id": 1777, "customer_id": 99, "amount": 10.0, "quantity": 1,
         "status": "placed", "order_date": "2026-10-05"}]
    _write_sources(tmp_path, valid_customers, orphan_orders)
    contracts = {"customers": customers_contract, "orders": orders_contract}
    manifest = _pipeline(tmp_path, contracts).run(warehouse, store)
    assert manifest.status == "failed"
    assert "data tests failed" in manifest.error


def test_all_rows_breaching_fails_closed(tmp_path, warehouse, store,
                                         customers_contract, orders_contract,
                                         valid_customers, valid_orders):
    bad_customers = [dict(c, region="atlantis") for c in valid_customers]
    _write_sources(tmp_path, bad_customers, valid_orders)
    contracts = {"customers": customers_contract, "orders": orders_contract}
    manifest = _pipeline(tmp_path, contracts).run(warehouse, store)
    assert manifest.status == "failed"
    assert warehouse.row_count("customers") == 0


def test_demo_report_shape(tmp_path):
    report = run_demo(tmp_path / "demo", corrupt=True)
    assert report["run"]["status"] == "success", report["run"]["error"]
    assert report["tables"]["customers"] == 20
    assert report["tables"]["orders"] == 60
    assert report["tables"]["orders_quarantine"] == 1
    assert report["run"]["total_quarantined"] == 1
    assert report["tables"]["customer_summary"] >= 1
