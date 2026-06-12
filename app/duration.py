"""Continuous At-Risk duration — the core business logic.

A *gap* in At Risk status resets the counter. Walking backward from the target
month, we count consecutive At Risk months and stop at the first non-At-Risk
month, a missing month, or the start of history.

Spec example (the deliberate edge case):
    2025-10 At Risk
    2025-11 At Risk
    2025-12 Healthy   <- gap breaks the streak
    2026-01 At Risk   => duration = 1 (NOT 3)
"""
from __future__ import annotations

from datetime import date
from typing import Dict, Tuple

from .data import AT_RISK


def _prev_month(d: date) -> date:
    if d.month == 1:
        return date(d.year - 1, 12, 1)
    return date(d.year, d.month - 1, 1)


def compute_duration(
    status_by_month: Dict[date, str],
    target_month: date,
) -> Tuple[int, date]:
    """Return (duration_months, risk_start_month) for an account.

    ``status_by_month`` maps first-of-month dates to that account's resolved
    status. The target month is assumed At Risk by the caller (it's an alert
    candidate); duration is at least 1.
    """
    duration = 0
    cursor = target_month
    risk_start = target_month

    while status_by_month.get(cursor) == AT_RISK:
        duration += 1
        risk_start = cursor
        cursor = _prev_month(cursor)
        # Stop conditions are implicit: if the previous month is missing or not
        # At Risk, the loop guard fails on the next iteration.

    # If the target month itself isn't At Risk in the map (shouldn't happen for a
    # candidate), fall back to duration = 1 per spec ("no prior At Risk => 1").
    if duration == 0:
        return 1, target_month

    return duration, risk_start
