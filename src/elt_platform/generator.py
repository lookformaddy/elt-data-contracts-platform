"""Deterministic synthetic data generator (generic retail-style domain).

All data is invented: customer names are assembled from word lists,
amounts come from a seeded RNG, and no real person, company, or employer
system appears anywhere. ``corrupt=True`` injects a small number of
contract-breaching rows so the quarantine path is exercised end-to-end.
"""

from __future__ import annotations

import csv
import json
import random
from pathlib import Path
from typing import Any

_FIRST = ["Ava", "Liam", "Maya", "Noah", "Zoe", "Eli", "Ivy", "Omar", "Nina", "Leo",
          "Ruby", "Sam", "Tara", "Hugo", "Lena", "Cole", "Mira", "Jude", "Ana", "Ezra"]
_LAST = ["Reyes", "Khan", "Brooks", "Patel", "Nguyen", "Garcia", "Smith", "Ali",
         "Brown", "Kaur", "Lopez", "Kim", "Sharma", "Novak", "Frost", "Mehta"]
_REGIONS = ["north", "south", "east", "west", "central"]
_STATUSES = ["placed", "shipped", "delivered", "cancelled"]


def generate_customers(n: int = 20, *, seed: int = 42) -> list[dict[str, Any]]:
    rng = random.Random(seed)
    customers = []
    for i in range(1, n + 1):
        name = f"{rng.choice(_FIRST)} {rng.choice(_LAST)}"
        customers.append({
            "customer_id": i,
            "name": name,
            "email": f"customer{i}@example.com",
            "region": rng.choice(_REGIONS),
            "signup_date": f"2025-{rng.randint(1, 12):02d}-{rng.randint(1, 28):02d}",
            "lifetime_value": round(rng.uniform(0, 5000), 2),
        })
    return customers


def generate_orders(n: int = 60, *, customer_count: int = 20, seed: int = 42,
                    corrupt: bool = False) -> list[dict[str, Any]]:
    rng = random.Random(seed + 1)
    orders = []
    for i in range(1, n + 1):
        orders.append({
            "order_id": 1000 + i,
            "customer_id": rng.randint(1, customer_count),
            "amount": round(rng.uniform(5, 400), 2),
            "quantity": rng.randint(1, 5),
            "status": rng.choice(_STATUSES),
            "order_date": f"2026-10-{rng.randint(1, 18):02d}",
        })
    if corrupt:
        bad = dict(orders[0])
        bad.update({"order_id": 9999, "amount": -25.0, "status": "unknown-status"})
        orders.append(bad)
    return orders


def write_sources(out_dir: str | Path, *, customers: int = 20, orders: int = 60,
                  seed: int = 42, corrupt: bool = False) -> dict[str, Path]:
    """Write customers.csv + orders.json under ``out_dir``; return their paths."""
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    cust_rows = generate_customers(customers, seed=seed)
    order_rows = generate_orders(orders, customer_count=customers, seed=seed, corrupt=corrupt)
    csv_path = out_dir / "customers.csv"
    with csv_path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(cust_rows[0].keys()))
        writer.writeheader()
        writer.writerows(cust_rows)
    json_path = out_dir / "orders.json"
    json_path.write_text(json.dumps(order_rows, indent=2), encoding="utf-8")
    return {"customers": csv_path, "orders": json_path}
