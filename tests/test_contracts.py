import json
from datetime import datetime, timezone

import pytest

from elt_platform.contracts import (
    coerce_rows,
    load_contracts,
    split_rows,
    validate_freshness,
    validate_row,
    validate_rows,
)


def test_valid_rows_pass(customers_contract, valid_customers):
    result = validate_rows(valid_customers, customers_contract)
    assert result.passed
    assert result.checked_rows == 2


def test_missing_non_nullable_fails(customers_contract, valid_customers):
    rows = [dict(valid_customers[0], email=None)]
    assert not validate_rows(rows, customers_contract).passed


def test_wrong_type_fails(customers_contract, valid_customers):
    rows = [dict(valid_customers[0], customer_id="not-an-int")]
    errors = validate_row(rows[0], customers_contract)
    assert any("expected int" in e for e in errors)


def test_min_value_and_allowed_values_fail(orders_contract, valid_orders):
    bad = dict(valid_orders[0], amount=-1.0, status="lost")
    errors = validate_row(bad, orders_contract)
    assert any("below min_value" in e for e in errors)
    assert any("not in allowed values" in e for e in errors)


def test_duplicate_unique_value_is_quarantined(customers_contract, valid_customers):
    rows = valid_customers + [dict(valid_customers[1], customer_id=1)]
    valid, quarantined = split_rows(rows, customers_contract)
    assert len(valid) == 2
    assert len(quarantined) == 1
    assert "duplicate" in quarantined[0][1][0]


def test_coerce_rows_converts_csv_strings(customers_contract):
    raw = [{"customer_id": "7", "name": "Zoe Frost", "email": "zoe@example.com",
            "region": "east", "signup_date": "2025-03-01", "lifetime_value": "42.5"}]
    typed = coerce_rows(raw, customers_contract)
    assert typed[0]["customer_id"] == 7
    assert typed[0]["lifetime_value"] == 42.5
    assert validate_rows(typed, customers_contract).passed


def test_pattern_constraint(customers_contract, valid_customers):
    bad = dict(valid_customers[0], email="not-an-email")
    assert any("pattern" in e for e in validate_row(bad, customers_contract))


def test_load_contracts_from_json_and_yaml(tmp_path, customers_contract):
    payload = {"contracts": [customers_contract.to_dict()]}
    json_path = tmp_path / "contracts.json"
    json_path.write_text(json.dumps(payload))
    loaded = load_contracts(json_path)
    assert loaded[0].name == "customers"
    assert loaded[0].column("region").allowed_values == (
        "north", "south", "east", "west", "central")

    yaml_path = tmp_path / "contracts.yml"
    yaml_path.write_text(
        "name: simple\ntable: simple\ncolumns:\n"
        "  - {name: id, type: int, nullable: false}\n")
    loaded_yaml = load_contracts(yaml_path)
    assert loaded_yaml[0].columns[0].name == "id"


def test_freshness_sla_breach_and_pass(customers_contract, valid_customers):
    contract = type(customers_contract)(
        name=customers_contract.name, table=customers_contract.table,
        columns=customers_contract.columns, primary_key=customers_contract.primary_key,
        freshness_sla_hours=24, freshness_column="signup_date")
    now = datetime(2026, 10, 19, tzinfo=timezone.utc)
    assert not validate_freshness(valid_customers, contract, now=now).passed
    recent = [dict(valid_customers[0], signup_date="2026-10-18")]
    assert validate_freshness(recent, contract, now=now).passed


def test_unsupported_column_type_rejected():
    from elt_platform.contracts import ColumnContract

    with pytest.raises(ValueError):
        ColumnContract("x", "blob")
