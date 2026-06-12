"""Data access layer: scale-aware Parquet loading and row-level deduplication.

Two concerns live here, both about *data*:
  1. Loading only the rows we need (predicate pushdown over a month window).
  2. Resolving duplicate (account_id, month) rows by latest ``updated_at``.

Scale strategy: the month predicate is pushed to the Parquet reader (only relevant
row groups are touched) AND results are streamed as **record batches**, deduplicated
incrementally. Peak memory is therefore bounded by one batch plus the set of unique
(account_id, month) keys in the window — never the full file or even the full window.

Deduplication is a *data-quality* concern and is deliberately kept separate from
*idempotency* (the operational-safety concern handled in db.py / pipeline.py).
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Dict, Iterator, List, Tuple

import pyarrow as pa
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

# Rows per record batch. Caps peak materialization regardless of file size.
DEFAULT_BATCH_SIZE = 65_536

# Key into the dedup map: (account_id, month).
_Key = Tuple[str, date]


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
    updated_at: object  # py datetime; only used for dedup ordering


def _month_minus(d: date, months: int) -> date:
    """Return the first-of-month ``months`` before ``d``."""
    total = (d.year * 12 + (d.month - 1)) - months
    return date(total // 12, total % 12 + 1, 1)


def scan_month_window(
    fs: pafs.FileSystem,
    path: str,
    target_month: date,
    lookback_months: int,
    batch_size: int = DEFAULT_BATCH_SIZE,
) -> Iterator[pa.RecordBatch]:
    """Yield record batches for the target month + lookback window only.

    The ``month`` predicate is pushed to the Parquet reader (predicate pushdown), so
    only relevant row groups are read; the scanner then streams them in batches of
    ``batch_size`` rows instead of materializing the whole window into one table.
    """
    window_start = _month_minus(target_month, lookback_months)
    dataset = ds.dataset(path, filesystem=fs, format="parquet")

    month_field = ds.field("month")
    predicate = (month_field >= pa.scalar(window_start)) & (
        month_field <= pa.scalar(target_month)
    )
    scanner = dataset.scanner(
        columns=NEEDED_COLUMNS, filter=predicate, batch_size=batch_size
    )
    yield from scanner.to_batches()


def _merge_batch(latest: Dict[_Key, AccountMonth], batch: pa.RecordBatch) -> int:
    """Fold one record batch into the latest-by-key map. Returns rows processed.

    For each (account_id, month) we keep the row with the greatest ``updated_at``,
    so the freshest late-arriving update wins regardless of batch order.
    """
    cols = {name: batch.column(name).to_pylist() for name in NEEDED_COLUMNS}
    n = batch.num_rows
    for i in range(n):
        key = (cols["account_id"][i], cols["month"][i])
        updated_at = cols["updated_at"][i]
        existing = latest.get(key)
        if existing is None or _is_newer(updated_at, existing.updated_at):
            latest[key] = AccountMonth(
                account_id=cols["account_id"][i],
                account_name=cols["account_name"][i],
                account_region=cols["account_region"][i],
                month=cols["month"][i],
                status=cols["status"][i],
                renewal_date=cols["renewal_date"][i],
                account_owner=cols["account_owner"][i],
                arr=cols["arr"][i],
                updated_at=updated_at,
            )
    return n


def _is_newer(candidate, current) -> bool:
    """True if ``candidate`` updated_at should replace ``current`` (>= wins)."""
    if candidate is None:
        return False
    if current is None:
        return True
    return candidate >= current


def load_and_deduplicate(
    fs: pafs.FileSystem,
    path: str,
    target_month: date,
    lookback_months: int,
    batch_size: int = DEFAULT_BATCH_SIZE,
) -> Tuple[List[AccountMonth], int, int]:
    """Stream the month window and deduplicate incrementally.

    Returns (records, rows_scanned, duplicate_count):
      * records         — one AccountMonth per (account_id, month), latest updated_at
      * rows_scanned     — total rows read across all batches
      * duplicate_count  — rows superseded during dedup (data-quality signal)
    """
    latest: Dict[_Key, AccountMonth] = {}
    rows_scanned = 0
    for batch in scan_month_window(fs, path, target_month, lookback_months, batch_size):
        rows_scanned += _merge_batch(latest, batch)
    duplicate_count = rows_scanned - len(latest)
    return list(latest.values()), rows_scanned, duplicate_count


def deduplicate(table: pa.Table) -> Tuple[List[AccountMonth], int]:
    """Deduplicate an in-memory table (same logic as the streaming path).

    Kept as a convenience for tests and ad-hoc use; the production path uses
    ``load_and_deduplicate`` which never materializes the full table.
    """
    latest: Dict[_Key, AccountMonth] = {}
    rows = 0
    for batch in table.to_batches():
        rows += _merge_batch(latest, batch)
    return list(latest.values()), rows - len(latest)
