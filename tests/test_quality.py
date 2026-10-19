from elt_platform.quality import (
    check_accepted_values,
    check_not_null,
    check_referential_integrity,
    check_row_count,
    check_sum_equals,
    check_uniqueness,
)


def test_row_count_bounds(valid_orders):
    assert check_row_count(valid_orders, table="orders", min_rows=1).passed
    assert not check_row_count(valid_orders, table="orders", min_rows=10).passed
    assert not check_row_count(valid_orders, table="orders", max_rows=2).passed


def test_uniqueness(valid_orders):
    assert check_uniqueness(valid_orders, table="orders", column="order_id").passed
    dupes = valid_orders + [dict(valid_orders[0])]
    assert not check_uniqueness(dupes, table="orders", column="order_id").passed


def test_not_null():
    rows = [{"id": 1}, {"id": None}]
    assert not check_not_null(rows, table="t", column="id").passed
    assert check_not_null(rows[:1], table="t", column="id").passed


def test_referential_integrity(valid_orders, valid_customers):
    result = check_referential_integrity(
        valid_orders, child_table="orders", child_column="customer_id",
        parent_rows=valid_customers, parent_table="customers", parent_column="customer_id")
    assert result.passed
    orphan = valid_orders + [dict(valid_orders[0], order_id=2001, customer_id=99)]
    failed = check_referential_integrity(
        orphan, child_table="orders", child_column="customer_id",
        parent_rows=valid_customers, parent_table="customers", parent_column="customer_id")
    assert not failed.passed
    assert failed.actual == 1


def test_accepted_values(valid_orders):
    allowed = ["placed", "shipped", "delivered", "cancelled"]
    assert check_accepted_values(valid_orders, table="orders", column="status",
                                 allowed=allowed).passed
    bad = [dict(valid_orders[0], status="lost")]
    assert not check_accepted_values(bad, table="orders", column="status",
                                     allowed=allowed).passed


def test_sum_equals(valid_orders):
    summary = [{"total": 125.25}, {"total": 20.0}]
    result = check_sum_equals(valid_orders, "amount", summary, "total",
                              name="orders_vs_summary")
    assert result.passed
