"""Slack webhook client with resilient delivery.

Retries on HTTP 429 and 5xx using exponential backoff, honoring the
``Retry-After`` header when present. Synchronous and blocking by design — the
spec requires ``POST /runs`` to block until the run completes. In production this
would move behind an async queue (Cloud Tasks / Celery) for fan-out at scale.
"""
from __future__ import annotations

import random
import time
from dataclasses import dataclass
from datetime import date
from typing import Callable, Optional

import requests

from .config import Settings
from .data import AccountMonth


@dataclass
class SendResult:
    ok: bool
    status_code: Optional[int]
    attempts: int
    error: Optional[str] = None


def format_message(
    account: AccountMonth,
    duration_months: int,
    risk_start: date,
    details_base_url: str,
) -> dict:
    """Build the Slack message payload for an At Risk account."""
    renewal = account.renewal_date.isoformat() if account.renewal_date else "Unknown"
    arr_str = f"${account.arr:,}" if account.arr is not None else "Unknown"

    lines = [
        f"🚩 At Risk: {account.account_name} ({account.account_id})",
        f"Region: {account.account_region}",
        f"At Risk for: {duration_months} months (since {risk_start.isoformat()})",
        f"ARR: {arr_str}",
        f"Renewal date: {renewal}",
    ]
    if account.account_owner:
        lines.append(f"Owner: {account.account_owner}")
    lines.append(f"Details: {details_base_url}/{account.account_id}")

    text = "\n".join(lines)
    return {"text": text}


class SlackClient:
    """Posts alerts to Slack (or the mock webhook) with retry/backoff."""

    def __init__(self, settings: Settings, sleep: Callable[[float], None] = time.sleep):
        self.settings = settings
        self._sleep = sleep  # injectable for fast tests

    def _url_for(self, channel: str) -> str:
        # Base-URL mode takes precedence over single-webhook mode.
        if self.settings.slack_webhook_base_url:
            return f"{self.settings.slack_webhook_base_url.rstrip('/')}/{channel}"
        if self.settings.slack_webhook_url:
            return self.settings.slack_webhook_url
        raise RuntimeError(
            "No Slack target configured: set SLACK_WEBHOOK_BASE_URL or SLACK_WEBHOOK_URL."
        )

    def _backoff_seconds(self, attempt: int, retry_after: Optional[str]) -> float:
        if retry_after:
            try:
                return float(retry_after)
            except ValueError:
                pass  # ignore malformed header, fall through to exponential
        base = self.settings.retry_base_seconds
        cap = self.settings.retry_cap_seconds
        return min(cap, base * (2 ** attempt)) + random.uniform(0, base)

    def send_alert(self, channel: str, payload: dict) -> SendResult:
        url = self._url_for(channel)
        headers = {"Content-Type": "application/json"}
        if self.settings.slack_auth_token:
            headers["X-Mock-Slack-Token"] = self.settings.slack_auth_token

        last_status: Optional[int] = None
        last_error: Optional[str] = None

        for attempt in range(self.settings.retry_max_attempts):
            try:
                resp = requests.post(url, json=payload, headers=headers, timeout=10)
            except requests.RequestException as exc:
                last_error = str(exc)
                self._sleep(self._backoff_seconds(attempt, None))
                continue

            last_status = resp.status_code
            if resp.status_code < 300:
                return SendResult(ok=True, status_code=resp.status_code, attempts=attempt + 1)

            # Retry on 429 and 5xx; other 4xx are non-retryable.
            if resp.status_code == 429 or 500 <= resp.status_code < 600:
                last_error = f"HTTP {resp.status_code}"
                is_last = attempt == self.settings.retry_max_attempts - 1
                if not is_last:
                    self._sleep(
                        self._backoff_seconds(attempt, resp.headers.get("Retry-After"))
                    )
                continue

            # Non-retryable client error.
            return SendResult(
                ok=False,
                status_code=resp.status_code,
                attempts=attempt + 1,
                error=f"HTTP {resp.status_code} (non-retryable)",
            )

        return SendResult(
            ok=False,
            status_code=last_status,
            attempts=self.settings.retry_max_attempts,
            error=last_error or "exhausted retries",
        )
