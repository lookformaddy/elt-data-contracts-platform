import pytest

from elt_platform.contracts import ColumnContract, Contract
from elt_platform.loader import Warehouse
from elt_platform.manifest import ManifestStore


@pytest.fixture()
def customers_contract() -> Contract:
    return Contract(
        name="customers", table="customers", primary_key=("customer_id",),
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


@pytest.fixture()
def orders_contract() -> Contract:
    return Contract(
        name="orders", table="orders", primary_key=("order_id",),
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


@pytest.fixture()
def valid_customers() -> list[dict]:
    return [
        {"customer_id": 1, "name": "Ava Reyes", "email": "ava@example.com",
         "region": "north", "signup_date": "2025-01-10", "lifetime_value": 120.5},
        {"customer_id": 2, "name": "Liam Khan", "email": "liam@example.com",
         "region": "south", "signup_date": "2025-02-11", "lifetime_value": 0.0},
    ]


@pytest.fixture()
def valid_orders() -> list[dict]:
    return [
        {"order_id": 1001, "customer_id": 1, "amount": 50.0, "quantity": 1,
         "status": "delivered", "order_date": "2026-10-01"},
        {"order_id": 1002, "customer_id": 1, "amount": 75.25, "quantity": 2,
         "status": "shipped", "order_date": "2026-10-02"},
        {"order_id": 1003, "customer_id": 2, "amount": 20.0, "quantity": 1,
         "status": "placed", "order_date": "2026-10-03"},
    ]


@pytest.fixture()
def warehouse(tmp_path):
    wh = Warehouse(tmp_path / "warehouse.db", export_root=tmp_path / "files")
    yield wh
    wh.close()


@pytest.fixture()
def store(tmp_path) -> ManifestStore:
    return ManifestStore(tmp_path / "manifests")
