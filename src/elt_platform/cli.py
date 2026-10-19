"""Command-line interface.

    elt-demo demo [--work-dir DIR] [--orders N] [--clean]
    elt-demo contracts          print the demo pipeline's contracts as JSON
    elt-demo serve [--port N]   run the FastAPI service (needs the api extra)
"""

from __future__ import annotations

import argparse
import json
import shutil
from pathlib import Path

from .demo import build_contracts, run_demo


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="elt-demo", description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)

    demo = sub.add_parser("demo", help="run the end-to-end demo pipeline")
    demo.add_argument("--work-dir", default="./elt-demo-data")
    demo.add_argument("--orders", type=int, default=60)
    demo.add_argument("--customers", type=int, default=20)
    demo.add_argument("--clean", action="store_true", help="start from an empty work dir")
    demo.add_argument("--idempotency-key", default=None)

    sub.add_parser("contracts", help="print the demo contracts as JSON")

    serve = sub.add_parser("serve", help="run the FastAPI service")
    serve.add_argument("--host", default="0.0.0.0")
    serve.add_argument("--port", type=int, default=8000)

    args = parser.parse_args(argv)

    if args.command == "contracts":
        print(json.dumps([c.to_dict() for c in build_contracts().values()], indent=2))
        return 0

    if args.command == "serve":  # pragma: no cover
        import uvicorn

        from .api import create_app

        uvicorn.run(create_app(), host=args.host, port=args.port)
        return 0

    work_dir = Path(args.work_dir)
    if args.clean and work_dir.exists():
        shutil.rmtree(work_dir)
    report = run_demo(work_dir, customers=args.customers, orders=args.orders,
                      idempotency_key=args.idempotency_key)
    print(json.dumps(report, indent=2, default=str))
    return 0 if report["run"]["status"] == "success" else 1


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
