"""End-to-end pipeline: preview, run, replay idempotency, unknown region."""
import app.slack as slack_mod
from app.db import AlertOutcome, get_session_factory
from app.pipeline import get_run_status, preview, run_pipeline
from app.schemas import RunRequest
from sqlalchemy import select


class FakeResp:
    status_code = 200
    headers: dict = {}


def _patch_slack_ok(monkeypatch):
    monkeypatch.setattr(slack_mod.requests, "post", lambda url, **k: FakeResp())


def test_preview_counts_candidates_and_unknown_region(sample_parquet, settings):
    req = RunRequest(source_uri=sample_parquet, month="2026-01-01")
    result = preview(req, settings)

    # Candidates: a1, a2, a3 (a4 deduped to Healthy, a5 Healthy) => 3 At Risk.
    assert result.at_risk_count == 3
    assert result.unknown_region_count == 1  # a3 has null region
    assert result.duplicate_rows == 1        # a4 had a duplicate Jan row

    by_id = {a.account_id: a for a in result.alerts}
    assert by_id["a1"].duration_months == 1  # gap reset
    assert by_id["a2"].duration_months == 3
    assert by_id["a3"].routable is False
    assert by_id["a3"].reason == "unknown_region"


def test_run_sends_and_replay_is_idempotent(sample_parquet, settings, monkeypatch):
    _patch_slack_ok(monkeypatch)
    req = RunRequest(source_uri=sample_parquet, month="2026-01-01")

    run_id = run_pipeline(req, settings)
    status = get_run_status(run_id, settings)
    assert status.counts.alerts_sent == 2          # a1, a2 (a3 unknown region)
    assert status.counts.failed_deliveries == 1    # a3 unknown_region
    assert status.counts.skipped_replay == 0

    # Re-run the same month -> no new sends, all skipped_replay.
    replay_id = run_pipeline(req, settings)
    replay = get_run_status(replay_id, settings)
    assert replay.counts.alerts_sent == 0
    assert replay.counts.skipped_replay == 2

    # Only one outcome row per (account, month, alert_type) survives.
    Session = get_session_factory(settings.db_url)
    with Session() as s:
        rows = s.scalars(
            select(AlertOutcome).where(AlertOutcome.account_id == "a1")
        ).all()
    assert len(rows) == 1
    assert rows[0].status == "sent"


def test_dry_run_persists_no_outcomes(sample_parquet, settings, monkeypatch):
    posts = {"n": 0}

    def counting_post(url, **k):
        posts["n"] += 1
        return FakeResp()

    monkeypatch.setattr(slack_mod.requests, "post", counting_post)
    req = RunRequest(source_uri=sample_parquet, month="2026-01-01", dry_run=True)

    run_id = run_pipeline(req, settings)
    assert posts["n"] == 0  # nothing actually sent

    Session = get_session_factory(settings.db_url)
    with Session() as s:
        outcomes = s.scalars(select(AlertOutcome)).all()
    assert outcomes == []  # dry run never occupies the idempotency slot
