"""Data access layer: scale-aware Parquet loading and row-level deduplication.

Two concerns live here, both about *data*:
  1. Loading only the rows we need (predicate pushdown over a month window).
  2. Resolving duplicate (account_id, month) rows by latest ``updated_at``.

Deduplication is a *data-quality* concern and is deliberately kept separate from
*idempotency* (the operational-safety concern handled in db.py / pipeline.py).
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import List, Tuple

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.dataset as ds
import pyarrow.fs as pafs

# Only the columns we actually need — projection avoids reading the rest.
NEEDED_COLUMNS = [
    "account_id",
    "account_name",
    "account_region",
    "month",
    "status",
    "renewal_date",
    "account_owner",
    "arr",
    "updated_at",
]

AT_RISK = "At Risk"


@dataclass
class AccountMonth:
    """One deduplicated account-month record."""

    account_id: str
    account_name: str
    account_region: str | None
    month: date
    status: str
    renewal_date: date | None
    account_owner: str | None
    arr: int | None
    updated_at: object  # pandas/py datetime; only used for dedup ordering


def _month_minus(d: date, months: int) -> date:
    """Return the first-of-month ``months`` before ``d``."""
    total = (d.year * 12 + (d.month - 1)) - months
    return date(total // 12, total % 12 + 1, 1)


def load_month_window(
    fs: pafs.FileSystem,
    path: str,
    target_month: date,
    lookback_months: int,
) -> Tuple[pa.Table, date]:
    """Read only the target month plus the lookback history window.

    Uses a PyArrow dataset filtered scan so the ``month`` predicate is pushed to
    the Parquet reader — only relevant row groups are materialized, never the
    whole file. Returns (table, window_start).
    """
    window_start = _month_minus(target_month, lookback_months)
    dataset = ds.dataset(path, filesystem=fs, format="parquet")

    month_field = ds.field("month")
    # Predicate pushdown: month >= window_start AND month <= target_month.
    predicate = (month_field >= pa.scalar(window_start)) & (
        month_field <= pa.scalar(target_month)
    )
    table = dataset.to_table(columns=NEEDED_COLUMNS, filter=predicate)
    return table, window_start


def deduplicate(table: pa.Table) -> Tuple[List[AccountMonth], int]:
    """Collapse duplicate (account_id, month) rows, keeping latest updated_at.

    Returns (records, duplicate_count) where duplicate_count is how many rows were
    superseded (i.e. dropped) during resolution — reported as a data-quality signal.
    """
    if table.num_rows == 0:
        return [], 0

    # Sort by updated_at ascending so the last occurrence per key wins.
    sort_idx = pc.sort_indices(table, sort_keys=[("updated_at", "ascending")])
    sorted_tbl = table.take(sort_idx)

    cols = {name: sorted_tbl.column(name).to_pylist() for name in NEEDED_COLUMNS}
    n = sorted_tbl.num_rows

    latest: dict[tuple[str, date], AccountMonth] = {}
    for i in range(n):
        key = (cols["account_id"][i], cols["month"][i])
        latest[key] = AccountMonth(
            account_id=cols["account_id"][i],
            account_name=cols["account_name"][i],
            account_region=cols["account_region"][i],
            month=cols["month"][i],
            status=cols["status"][i],
            renewal_date=cols["renewal_date"][i],
            account_owner=cols["account_owner"][i],
            arr=cols["arr"][i],
            updated_at=cols["updated_at"][i],
        )

    duplicate_count = n - len(latest)
    return list(latest.values()), duplicate_count
