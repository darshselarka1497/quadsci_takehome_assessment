"""Pydantic request/response models for the FastAPI surface."""
from __future__ import annotations

from datetime import date
from typing import List, Optional

from pydantic import BaseModel, Field, field_validator


class RunRequest(BaseModel):
    source_uri: str = Field(..., examples=["file:///data/monthly_account_status.parquet"])
    month: str = Field(..., examples=["2026-01-01"], description="First day of month, YYYY-MM-01")
    dry_run: bool = False

    @field_validator("month")
    @classmethod
    def _validate_month(cls, v: str) -> str:
        d = date.fromisoformat(v)
        if d.day != 1:
            raise ValueError("month must be the first day of the month (YYYY-MM-01)")
        return v

    def month_date(self) -> date:
        return date.fromisoformat(self.month)


class RunCreatedResponse(BaseModel):
    run_id: str


class RunCounts(BaseModel):
    rows_scanned: int
    duplicate_rows: int
    alerts_sent: int
    skipped_replay: int
    failed_deliveries: int


class AlertOutcomeOut(BaseModel):
    account_id: str
    channel: Optional[str]
    status: str
    reason: Optional[str] = None
    error: Optional[str] = None


class RunStatusResponse(BaseModel):
    run_id: str
    status: str
    month: str
    dry_run: bool
    counts: RunCounts
    sample_alerts: List[AlertOutcomeOut]
    sample_errors: List[AlertOutcomeOut]


class PreviewAlert(BaseModel):
    account_id: str
    account_name: str
    account_region: Optional[str]
    channel: Optional[str]
    duration_months: int
    risk_start_month: str
    arr: Optional[int]
    routable: bool
    reason: Optional[str] = None
    message: str


class PreviewResponse(BaseModel):
    month: str
    rows_scanned: int
    duplicate_rows: int
    at_risk_count: int
    unknown_region_count: int
    alerts: List[PreviewAlert]
