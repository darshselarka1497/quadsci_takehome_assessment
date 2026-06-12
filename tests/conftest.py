"""Shared test fixtures: an in-memory Parquet file and base settings."""
from __future__ import annotations

from datetime import date, datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from app.config import Settings


def _row(account_id, name, region, month, status, updated_at, arr=50000,
         renewal=date(2026, 6, 1), owner="owner@example.com"):
    return {
        "account_id": account_id,
        "account_name": name,
        "account_region": region,
        "month": month,
        "status": status,
        "renewal_date": renewal,
        "account_owner": owner,
        "arr": arr,
        "updated_at": updated_at,
    }


@pytest.fixture
def sample_parquet(tmp_path: Path) -> str:
    """Build a small Parquet covering the gap-reset case, dupes, and unknown region."""
    rows = [
        # a1: At Risk Oct, Nov; Healthy Dec; At Risk Jan -> duration 1 (gap reset)
        _row("a1", "Acct 1", "AMER", date(2025, 10, 1), "At Risk", datetime(2025, 10, 5)),
        _row("a1", "Acct 1", "AMER", date(2025, 11, 1), "At Risk", datetime(2025, 11, 5)),
        _row("a1", "Acct 1", "AMER", date(2025, 12, 1), "Healthy", datetime(2025, 12, 5)),
        _row("a1", "Acct 1", "AMER", date(2026, 1, 1), "At Risk", datetime(2026, 1, 5)),
        # a2: At Risk Nov, Dec, Jan -> duration 3
        _row("a2", "Acct 2", "EMEA", date(2025, 11, 1), "At Risk", datetime(2025, 11, 5)),
        _row("a2", "Acct 2", "EMEA", date(2025, 12, 1), "At Risk", datetime(2025, 12, 5)),
        _row("a2", "Acct 2", "EMEA", date(2026, 1, 1), "At Risk", datetime(2026, 1, 5)),
        # a3: unknown (null) region, At Risk Jan
        _row("a3", "Acct 3", None, date(2026, 1, 1), "At Risk", datetime(2026, 1, 5)),
        # a4: At Risk Jan with a DUPLICATE row; later updated_at flips to Healthy
        _row("a4", "Acct 4", "APAC", date(2026, 1, 1), "At Risk", datetime(2026, 1, 1)),
        _row("a4", "Acct 4", "APAC", date(2026, 1, 1), "Healthy", datetime(2026, 1, 9)),
        # a5: Healthy Jan -> not a candidate
        _row("a5", "Acct 5", "AMER", date(2026, 1, 1), "Healthy", datetime(2026, 1, 5)),
    ]
    table = pa.Table.from_pylist(rows)
    out = tmp_path / "sample.parquet"
    pq.write_table(table, out)
    return f"file://{out}"


@pytest.fixture
def settings(tmp_path: Path) -> Settings:
    return Settings(
        region_channel_map={
            "AMER": "amer-risk-alerts",
            "EMEA": "emea-risk-alerts",
            "APAC": "apac-risk-alerts",
        },
        arr_threshold=0,
        db_url=f"sqlite:///{tmp_path/'test.db'}",
        slack_webhook_base_url="http://localhost:9000/slack/webhook",
        retry_base_seconds=0.0,
        retry_cap_seconds=0.0,
    )
