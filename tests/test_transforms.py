import pytest

from elt_platform.transforms import Transform, TransformDAG


def _seed(warehouse, customers_contract, orders_contract, valid_customers, valid_orders):
    warehouse.create_table(customers_contract, replace=True)
    warehouse.load_rows("customers", valid_customers)
    warehouse.create_table(orders_contract, replace=True)
    warehouse.load_rows("orders", valid_orders)


def test_dag_orders_dependencies_first():
    dag = TransformDAG([
        Transform(name="c", depends_on=["b"], sql="SELECT 1 AS x"),
        Transform(name="a", sql="SELECT 1 AS x"),
        Transform(name="b", depends_on=["a"], sql="SELECT 1 AS x"),
    ])
    assert dag.topological_order() == ["a", "b", "c"]


def test_dag_cycle_detected():
    dag = TransformDAG([
        Transform(name="a", depends_on=["b"], sql="SELECT 1 AS x"),
        Transform(name="b", depends_on=["a"], sql="SELECT 1 AS x"),
    ])
    with pytest.raises(ValueError, match="cycle"):
        dag.topological_order()


def test_transform_requires_exactly_one_kind():
    with pytest.raises(ValueError):
        Transform(name="bad")
    with pytest.raises(ValueError):
        Transform(name="bad", sql="SELECT 1", python_fn=lambda wh: [])


def test_sql_transform_aggregates(warehouse, customers_contract, orders_contract,
                                 valid_customers, valid_orders):
    _seed(warehouse, customers_contract, orders_contract, valid_customers, valid_orders)
    dag = TransformDAG([Transform(
        name="customer_summary", depends_on=["orders"],
        sql=("SELECT customer_id, COUNT(*) AS order_count, SUM(amount) AS total_spend "
             "FROM orders GROUP BY customer_id"))])
    results = dag.run(warehouse)
    assert results[0].rows == 2
    summary = {r["customer_id"]: r for r in warehouse.read_table("customer_summary")}
    assert summary[1]["order_count"] == 2
    assert summary[1]["total_spend"] == pytest.approx(125.25)


def test_python_transform_runs_after_sql_dependency(warehouse, customers_contract,
                                                    orders_contract, valid_customers,
                                                    valid_orders):
    _seed(warehouse, customers_contract, orders_contract, valid_customers, valid_orders)

    def big_spenders(wh):
        return [{"customer_id": r["customer_id"], "total_spend": r["total_spend"]}
                for r in wh.read_table("customer_summary") if r["total_spend"] >= 100]

    dag = TransformDAG([
        Transform(name="big_spenders", depends_on=["customer_summary"], python_fn=big_spenders),
        Transform(name="customer_summary", depends_on=["orders"],
                  sql=("SELECT customer_id, SUM(amount) AS total_spend "
                       "FROM orders GROUP BY customer_id")),
    ])
    dag.run(warehouse)
    rows = warehouse.read_table("big_spenders")
    assert len(rows) == 1 and rows[0]["customer_id"] == 1


def test_transform_retry_recovers(warehouse):
    calls = {"n": 0}

    def flaky(wh):
        calls["n"] += 1
        if calls["n"] == 1:
            raise RuntimeError("transient")
        return [{"x": 1}]

    dag = TransformDAG([Transform(name="flaky_table", python_fn=flaky)])
    results = dag.run(warehouse, max_retries=1)
    assert results[0].attempts == 2
    assert warehouse.row_count("flaky_table") == 1
