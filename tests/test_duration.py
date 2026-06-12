"""Duration calculation, including the spec's gap-reset edge case."""
from datetime import date

from app.duration import compute_duration

AR = "At Risk"
H = "Healthy"


def test_gap_resets_streak():
    # Oct/Nov At Risk, Dec Healthy, Jan At Risk -> duration 1 (NOT 3)
    history = {
        date(2025, 10, 1): AR,
        date(2025, 11, 1): AR,
        date(2025, 12, 1): H,
        date(2026, 1, 1): AR,
    }
    duration, start = compute_duration(history, date(2026, 1, 1))
    assert duration == 1
    assert start == date(2026, 1, 1)


def test_continuous_streak():
    history = {
        date(2025, 11, 1): AR,
        date(2025, 12, 1): AR,
        date(2026, 1, 1): AR,
    }
    duration, start = compute_duration(history, date(2026, 1, 1))
    assert duration == 3
    assert start == date(2025, 11, 1)


def test_missing_prior_month_stops_count():
    # Jan At Risk, no December row -> streak stops, duration 1
    history = {date(2026, 1, 1): AR, date(2025, 10, 1): AR}
    duration, start = compute_duration(history, date(2026, 1, 1))
    assert duration == 1
    assert start == date(2026, 1, 1)


def test_year_boundary():
    history = {
        date(2024, 12, 1): AR,
        date(2025, 1, 1): AR,
    }
    duration, start = compute_duration(history, date(2025, 1, 1))
    assert duration == 2
    assert start == date(2024, 12, 1)


def test_no_at_risk_returns_one():
    history = {date(2026, 1, 1): H}
    duration, start = compute_duration(history, date(2026, 1, 1))
    assert duration == 1
