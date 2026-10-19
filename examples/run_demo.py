#!/usr/bin/env python3
"""End-to-end offline demo: synthetic sources -> contracts -> ELT -> report.

    python examples/run_demo.py

Writes sources, the SQLite warehouse, its JSON file layout, and run
manifests under ./elt-demo-data (override with --work-dir), then prints
the run report. One deliberately corrupt order is generated so the
quarantine path is visible in the report.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from elt_platform.demo import run_demo  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--work-dir", default="./elt-demo-data")
    parser.add_argument("--customers", type=int, default=20)
    parser.add_argument("--orders", type=int, default=60)
    args = parser.parse_args()

    report = run_demo(args.work_dir, customers=args.customers, orders=args.orders)
    print(json.dumps(report, indent=2, default=str))
    print(f"\nWarehouse + manifests written under: {Path(args.work_dir).resolve()}")
    return 0 if report["run"]["status"] == "success" else 1


if __name__ == "__main__":
    raise SystemExit(main())
