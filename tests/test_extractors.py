import json

import pytest

from elt_platform.extractors import Extractor, SourceSpec, extract_csv, extract_jsonl
from elt_platform.generator import generate_customers, generate_orders, write_sources
from elt_platform.loader import Warehouse


def test_extract_csv_and_json_via_specs(tmp_path):
    paths = write_sources(tmp_path, customers=5, orders=7)
    extractor = Extractor(tmp_path)
    customers = extractor.extract(
        SourceSpec(name="customers", path="customers.csv", format="csv", table="customers"))
    orders = extractor.extract(
        SourceSpec(name="orders", path="orders.json", format="json", table="orders"))
    assert len(customers) == 5
    assert len(orders) == 7
    assert paths["customers"].exists()


def test_extract_jsonl(tmp_path):
    path = tmp_path / "rows.jsonl"
    path.write_text('{"a": 1}\n{"a": 2}\n')
    assert extract_jsonl(path) == [{"a": 1}, {"a": 2}]


def test_missing_source_file_raises(tmp_path):
    with pytest.raises(FileNotFoundError):
        Extractor(tmp_path).extract(SourceSpec(name="nope", path="nope.csv"))


def test_invalid_format_rejected():
    with pytest.raises(ValueError):
        SourceSpec(name="x", path="x.parquet", format="parquet")


def test_generator_is_deterministic():
    assert generate_customers(5, seed=7) == generate_customers(5, seed=7)
    assert generate_orders(5, seed=7) != generate_orders(5, seed=8)


def test_generator_corrupt_row_breaches_contract(orders_contract):
    from elt_platform.contracts import validate_rows

    orders = generate_orders(10, corrupt=True)
    assert not validate_rows(orders, orders_contract).passed
    assert validate_rows(generate_orders(10), orders_contract).passed


def test_warehouse_load_replace_is_idempotent(warehouse, customers_contract, valid_customers):
    warehouse.create_table(customers_contract, replace=True)
    warehouse.load_rows("customers", valid_customers, mode="replace")
    warehouse.load_rows("customers", valid_customers, mode="replace")
    assert warehouse.row_count("customers") == 2
    assert warehouse.read_table("customers")[0]["name"] == "Ava Reyes"


def test_warehouse_export_layout(warehouse, customers_contract, valid_customers, tmp_path):
    warehouse.create_table(customers_contract, replace=True)
    warehouse.load_rows("customers", valid_customers)
    exported = tmp_path / "files" / "customers" / "customers.json"
    assert exported.exists()
    assert json.loads(exported.read_text())[1]["customer_id"] == 2
