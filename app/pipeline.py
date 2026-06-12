"""Pipeline orchestration — the service layer that ties everything together.

Flow for a run:
  1. open_uri -> load month window (predicate pushdown) -> deduplicate.
  2. Select At Risk candidates for the target month above ARR_THRESHOLD.
  3. For each candidate: compute continuous-risk duration, resolve region->channel.
       - Unknown/missing region  -> failed/unknown_region, no Slack, collect for digest.
       - Already sent this month  -> skipped_replay (idempotency, no Slack).
       - Otherwise                -> send via Slack with retry; record outcome.
  4. After the loop: aggregate unknown-region accounts into one email digest.
  5. Persist the Run with final counts. The run completes even if sends fail.

``preview`` reuses the same computation with sending disabled.
"""
from __future__ import annotations

import logging
import uuid
from datetime import date, datetime, timezone
from typing import Dict, List, Optional, Tuple

from sqlalchemy import select

from . import notifications
from .config import Settings
from .data import AT_RISK, AccountMonth, load_and_deduplicate
from .db import AlertOutcome, Run, get_session_factory
from .duration import compute_duration
from .schemas import (
    AlertOutcomeOut,
    PreviewAlert,
    PreviewResponse,
    RunCounts,
    RunRequest,
    RunStatusResponse,
)
from .slack import SlackClient, format_message
from .storage import open_uri

logger = logging.getLogger("risk_alerts.pipeline")

ALERT_TYPE = "at_risk"


def _load_and_dedup(
    req: RunRequest, settings: Settings
) -> Tuple[List[AccountMonth], int, int]:
    """Return (deduped_records, rows_scanned, duplicate_rows).

    Streams the month window as record batches and deduplicates incrementally, so
    the full file is never materialized (see data.load_and_deduplicate).
    """
    fs, path = open_uri(req.source_uri)
    return load_and_deduplicate(
        fs,
        path,
        req.month_date(),
        settings.history_lookback_months,
        settings.scan_batch_size,
    )


def _candidates_and_history(
    records: List[AccountMonth], target_month: date, settings: Settings
) -> Tuple[List[AccountMonth], Dict[str, Dict[date, str]]]:
    """Split deduped records into (At Risk candidates for target month, history map).

    History map: account_id -> {month: status}, used for duration computation.
    Candidates are filtered by ARR_THRESHOLD.
    """
    history: Dict[str, Dict[date, str]] = {}
    for r in records:
        history.setdefault(r.account_id, {})[r.month] = r.status

    candidates = [
        r
        for r in records
        if r.month == target_month
        and r.status == AT_RISK
        and (r.arr is None or r.arr >= settings.arr_threshold)
    ]
    # Stable, deterministic ordering for reproducible runs / samples.
    candidates.sort(key=lambda r: r.account_id)
    return candidates, history


def _build_alert(
    account: AccountMonth,
    history: Dict[str, Dict[date, str]],
    target_month: date,
    settings: Settings,
) -> Tuple[int, date, Optional[str], dict]:
    """Compute duration, channel, and Slack payload for a candidate."""
    duration, risk_start = compute_duration(
        history.get(account.account_id, {}), target_month
    )
    channel = settings.channel_for_region(account.account_region)
    payload = format_message(account, duration, risk_start, settings.details_base_url)
    return duration, risk_start, channel, payload


def run_pipeline(req: RunRequest, settings: Settings) -> str:
    """Execute a run end-to-end and persist results. Returns run_id.

    When ``req.dry_run`` is set we compute and persist run-level metadata/counts
    but post nothing to Slack and write no ``alert_outcomes`` rows — so a dry run
    never occupies the UNIQUE idempotency slot and a later real run is unaffected.
    """
    Session = get_session_factory(settings.db_url)
    run_id = str(uuid.uuid4())
    month_str = req.month
    target_month = req.month_date()

    run = Run(
        id=run_id,
        source_uri=req.source_uri,
        month=month_str,
        dry_run=req.dry_run,
        status="succeeded",
        # Initialize counters explicitly — SQLAlchemy column defaults only apply
        # on flush, but we accumulate these in memory before commit.
        rows_scanned=0,
        duplicate_rows=0,
        alerts_sent=0,
        skipped_replay=0,
        failed_deliveries=0,
    )

    try:
        records, rows_scanned, duplicate_rows = _load_and_dedup(req, settings)
    except Exception as exc:  # surface load failures as a failed run
        logger.exception("Run %s failed during load", run_id)
        run.status = "failed"
        run.error = f"{type(exc).__name__}: {exc}"
        with Session() as s:
            s.add(run)
            s.commit()
        return run_id

    run.rows_scanned = rows_scanned
    run.duplicate_rows = duplicate_rows

    candidates, history = _candidates_and_history(records, target_month, settings)
    slack = SlackClient(settings)
    unknown_region: List[AccountMonth] = []

    with Session() as s:
        # Idempotency: which accounts already have a 'sent' outcome for this month?
        already_sent = set(
            s.scalars(
                select(AlertOutcome.account_id).where(
                    AlertOutcome.month == month_str,
                    AlertOutcome.alert_type == ALERT_TYPE,
                    AlertOutcome.status == "sent",
                )
            ).all()
        )

        for account in candidates:
            duration, risk_start, channel, payload = _build_alert(
                account, history, target_month, settings
            )

            # Unknown / missing region -> failed outcome, no Slack send.
            if channel is None:
                unknown_region.append(account)
                if not req.dry_run:
                    _record(
                        s, run_id, account, channel=None, status="failed",
                        reason="unknown_region",
                    )
                run.failed_deliveries += 1
                continue

            # Dry run: count what *would* be sent, persist nothing per-account.
            if req.dry_run:
                run.alerts_sent += 1
                continue

            # Idempotent replay: already delivered for this account+month.
            if account.account_id in already_sent:
                _record(s, run_id, account, channel=channel, status="skipped_replay")
                run.skipped_replay += 1
                continue

            result = slack.send_alert(channel, payload)
            if result.ok:
                _record(
                    s, run_id, account, channel=channel, status="sent",
                    sent_at=datetime.now(timezone.utc),
                )
                run.alerts_sent += 1
                already_sent.add(account.account_id)
            else:
                _record(
                    s, run_id, account, channel=channel, status="failed",
                    reason="delivery_failed", error=result.error,
                )
                run.failed_deliveries += 1

        s.add(run)
        s.commit()

    # One aggregated digest for all unknown-region accounts (skip on dry runs).
    if not req.dry_run:
        notifications.send_unknown_region_digest(
            unknown_region, settings.support_email, run_id
        )

    return run_id


def _record(
    session,
    run_id: str,
    account: AccountMonth,
    *,
    channel: Optional[str],
    status: str,
    reason: Optional[str] = None,
    error: Optional[str] = None,
    sent_at: Optional[datetime] = None,
) -> None:
    """Upsert an AlertOutcome keyed by (account_id, month, alert_type).

    The DB-level UNIQUE constraint guarantees one row per (account, month,
    alert_type). On a fresh decision we insert; when a row already exists (e.g.
    retrying a previously *failed* delivery) we update it in place so the latest
    outcome is the source of truth. A 'sent' row is terminal and never downgraded.
    """
    month_str = account.month.isoformat()
    existing = session.scalar(
        select(AlertOutcome).where(
            AlertOutcome.account_id == account.account_id,
            AlertOutcome.month == month_str,
            AlertOutcome.alert_type == ALERT_TYPE,
        )
    )

    if existing is None:
        session.add(
            AlertOutcome(
                run_id=run_id,
                account_id=account.account_id,
                month=month_str,
                alert_type=ALERT_TYPE,
                channel=channel,
                status=status,
                reason=reason,
                error=error,
                sent_at=sent_at,
            )
        )
        return

    if existing.status == "sent":
        return  # terminal — don't overwrite a successful delivery

    existing.run_id = run_id
    existing.channel = channel
    existing.status = status
    existing.reason = reason
    existing.error = error
    existing.sent_at = sent_at


def preview(req: RunRequest, settings: Settings) -> PreviewResponse:
    """Compute alerts for the month without sending Slack or persisting outcomes."""
    records, rows_scanned, duplicate_rows = _load_and_dedup(req, settings)
    target_month = req.month_date()
    candidates, history = _candidates_and_history(records, target_month, settings)

    alerts: List[PreviewAlert] = []
    unknown = 0
    for account in candidates:
        duration, risk_start, channel, payload = _build_alert(
            account, history, target_month, settings
        )
        routable = channel is not None
        if not routable:
            unknown += 1
        alerts.append(
            PreviewAlert(
                account_id=account.account_id,
                account_name=account.account_name,
                account_region=account.account_region,
                channel=channel,
                duration_months=duration,
                risk_start_month=risk_start.isoformat(),
                arr=account.arr,
                routable=routable,
                reason=None if routable else "unknown_region",
                message=payload["text"],
            )
        )

    return PreviewResponse(
        month=req.month,
        rows_scanned=rows_scanned,
        duplicate_rows=duplicate_rows,
        at_risk_count=len(candidates),
        unknown_region_count=unknown,
        alerts=alerts,
    )


def get_run_status(run_id: str, settings: Settings) -> Optional[RunStatusResponse]:
    """Load a persisted run + a sample of its outcomes."""
    Session = get_session_factory(settings.db_url)
    with Session() as s:
        run = s.get(Run, run_id)
        if run is None:
            return None

        outcomes = s.scalars(
            select(AlertOutcome).where(AlertOutcome.run_id == run_id)
        ).all()

    sample_alerts = [
        AlertOutcomeOut(
            account_id=o.account_id, channel=o.channel, status=o.status,
            reason=o.reason, error=o.error,
        )
        for o in outcomes
        if o.status in ("sent", "skipped_replay")
    ][:10]
    sample_errors = [
        AlertOutcomeOut(
            account_id=o.account_id, channel=o.channel, status=o.status,
            reason=o.reason, error=o.error,
        )
        for o in outcomes
        if o.status == "failed"
    ][:10]

    return RunStatusResponse(
        run_id=run.id,
        status=run.status,
        month=run.month,
        dry_run=run.dry_run,
        counts=RunCounts(
            rows_scanned=run.rows_scanned,
            duplicate_rows=run.duplicate_rows,
            alerts_sent=run.alerts_sent,
            skipped_replay=run.skipped_replay,
            failed_deliveries=run.failed_deliveries,
        ),
        sample_alerts=sample_alerts,
        sample_errors=sample_errors,
    )
