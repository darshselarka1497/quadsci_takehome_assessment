"""Operator notifications.

After a run, accounts whose region could not be routed (missing/unknown region)
are aggregated into a single notification to the support inbox. This is a
documented STUB: it logs a structured digest and exposes a clean interface.

In production, swap ``_deliver`` for a real provider (SendGrid / Amazon SES /
internal mail service) — the interface and aggregation logic stay the same.
"""
from __future__ import annotations

import logging
from typing import List

from .data import AccountMonth

logger = logging.getLogger("risk_alerts.notifications")


def send_unknown_region_digest(
    accounts: List[AccountMonth],
    support_email: str,
    run_id: str,
) -> None:
    """Send one aggregated digest for all unknown-region accounts in a run.

    No-op when there are no unknown-region accounts.
    """
    if not accounts:
        return

    rows = "\n".join(
        f"  - {a.account_id} ({a.account_name}) region={a.account_region!r}"
        for a in accounts
    )
    body = (
        f"Run {run_id}: {len(accounts)} account(s) could not be routed to a Slack "
        f"channel (missing/unknown region). No alert was sent for these:\n{rows}"
    )
    _deliver(to=support_email, subject="[Risk Alerts] Unknown-region accounts", body=body)


def _deliver(to: str, subject: str, body: str) -> None:
    """STUB email delivery — replace with SendGrid/SES in production."""
    logger.warning("EMAIL STUB -> %s | %s\n%s", to, subject, body)
