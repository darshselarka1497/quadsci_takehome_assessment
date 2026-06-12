"""Slack client retry/backoff behavior against a fake transport."""
from datetime import date

import app.slack as slack_mod
from app.config import Settings
from app.data import AccountMonth
from app.slack import SlackClient, format_message


class FakeResp:
    def __init__(self, status_code, headers=None):
        self.status_code = status_code
        self.headers = headers or {}


def _settings():
    return Settings(
        slack_webhook_base_url="http://x/slack/webhook",
        retry_max_attempts=5,
        retry_base_seconds=1.0,
        retry_cap_seconds=60.0,
    )


def test_retries_on_429_then_succeeds(monkeypatch):
    responses = [FakeResp(429, {"Retry-After": "2"}), FakeResp(500), FakeResp(200)]
    calls = {"n": 0}

    def fake_post(url, **kwargs):
        r = responses[calls["n"]]
        calls["n"] += 1
        return r

    sleeps = []
    monkeypatch.setattr(slack_mod.requests, "post", fake_post)
    client = SlackClient(_settings(), sleep=sleeps.append)

    result = client.send_alert("amer-risk-alerts", {"text": "hi"})
    assert result.ok is True
    assert result.attempts == 3
    # First backoff honors the Retry-After header (2s) rather than exponential.
    assert sleeps[0] == 2.0


def test_exhausts_retries_and_fails(monkeypatch):
    monkeypatch.setattr(slack_mod.requests, "post", lambda url, **k: FakeResp(503))
    client = SlackClient(_settings(), sleep=lambda s: None)

    result = client.send_alert("amer-risk-alerts", {"text": "hi"})
    assert result.ok is False
    assert result.attempts == 5
    assert "503" in (result.error or "")


def test_non_retryable_4xx_stops_immediately(monkeypatch):
    monkeypatch.setattr(slack_mod.requests, "post", lambda url, **k: FakeResp(404))
    client = SlackClient(_settings(), sleep=lambda s: None)

    result = client.send_alert("amer-risk-alerts", {"text": "hi"})
    assert result.ok is False
    assert result.attempts == 1


def test_base_url_takes_precedence_over_single_webhook():
    s = Settings(
        slack_webhook_base_url="http://base/slack/webhook",
        slack_webhook_url="http://single/hook",
    )
    client = SlackClient(s)
    assert client._url_for("amer-risk-alerts") == "http://base/slack/webhook/amer-risk-alerts"


def test_message_format_contains_required_fields():
    acct = AccountMonth(
        account_id="a1", account_name="Acct 1", account_region="AMER",
        month=date(2026, 1, 1), status="At Risk", renewal_date=date(2026, 6, 1),
        account_owner="owner@example.com", arr=50000, updated_at=None,
    )
    msg = format_message(acct, 3, date(2025, 11, 1), "https://app.example.com/accounts")
    text = msg["text"]
    assert "🚩 At Risk: Acct 1 (a1)" in text
    assert "AMER" in text
    assert "At Risk for: 3 months (since 2025-11-01)" in text
    assert "$50,000" in text
    assert "2026-06-01" in text
    assert "owner@example.com" in text
    assert "https://app.example.com/accounts/a1" in text
