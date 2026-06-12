"""FastAPI surface — thin routing layer that delegates to the pipeline."""
from __future__ import annotations

import logging

from fastapi import FastAPI, HTTPException

from .config import get_settings
from .pipeline import get_run_status, preview, run_pipeline
from .schemas import (
    PreviewResponse,
    RunCreatedResponse,
    RunRequest,
    RunStatusResponse,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)

app = FastAPI(title="Risk Alert Service", version="1.0.0")


@app.get("/health")
def health():
    return {"ok": True}


@app.post("/runs", response_model=RunCreatedResponse)
def create_run(req: RunRequest) -> RunCreatedResponse:
    """Process a run synchronously (blocks until complete) and return its id."""
    settings = get_settings()
    run_id = run_pipeline(req, settings)
    return RunCreatedResponse(run_id=run_id)


@app.get("/runs/{run_id}", response_model=RunStatusResponse)
def read_run(run_id: str) -> RunStatusResponse:
    settings = get_settings()
    result = get_run_status(run_id, settings)
    if result is None:
        raise HTTPException(status_code=404, detail=f"run {run_id} not found")
    return result


@app.post("/preview", response_model=PreviewResponse)
def preview_run(req: RunRequest) -> PreviewResponse:
    """Compute alerts for the month without sending Slack or persisting outcomes."""
    settings = get_settings()
    return preview(req, settings)
