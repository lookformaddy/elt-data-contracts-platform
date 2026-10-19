"""FastAPI service: pipeline runs and contract status over HTTP.

    GET  /health            liveness
    GET  /contracts         contracts known to this service (+ their tables)
    GET  /runs              run manifests recorded in the manifest store
    GET  /runs/{run_id}     one manifest
    POST /runs/demo         run the demo pipeline in a temp workspace

The core pipeline has no web dependency; this module is the only place
FastAPI is imported, and it is imported lazily inside ``create_app`` so
``import elt_platform`` works without the ``api`` extra installed.
"""

from __future__ import annotations

import tempfile
from pathlib import Path
from typing import Any

from .demo import build_contracts, run_demo
from .manifest import ManifestStore


def create_app(manifest_root: str | Path | None = None):  # type: ignore[no-untyped-def]
    try:
        from fastapi import FastAPI, HTTPException
    except ImportError as exc:  # pragma: no cover
        raise RuntimeError("install the api extra: pip install -e '.[api]'") from exc

    store = ManifestStore(manifest_root)
    contracts = build_contracts()
    app = FastAPI(title="ELT Data Contracts Platform", version="0.1.0")

    @app.get("/health")
    def health() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/contracts")
    def list_contracts() -> dict[str, Any]:
        return {"contracts": [c.to_dict() for c in contracts.values()]}

    @app.get("/runs")
    def list_runs() -> dict[str, Any]:
        return {"runs": [m.to_dict() for m in store.list_runs()]}

    @app.get("/runs/{run_id}")
    def get_run(run_id: str) -> dict[str, Any]:
        manifest = store.get(run_id)
        if manifest is None:
            raise HTTPException(status_code=404, detail=f"run {run_id} not found")
        return manifest.to_dict()

    @app.post("/runs/demo")
    def demo_run() -> dict[str, Any]:
        with tempfile.TemporaryDirectory() as tmp:
            return run_demo(Path(tmp) / "demo")

    return app


def app():  # pragma: no cover - uvicorn entry point factory
    return create_app()
