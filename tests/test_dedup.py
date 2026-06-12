"""Row-level deduplication: latest updated_at wins; duplicates are counted."""
from datetime import date, datetime

import pyarrow as pa

from app.data import deduplicate, load_and_deduplicate
from app.storage import open_uri


def _row(account_id, month, status, updated_at):
    return {
        "account_id": account_id,
        "account_name": "n",
        "account_region": "AMER",
        "month": month,
        "status": status,
        "renewal_date": None,
        "account_owner": None,
        "arr": 1,
        "updated_at": updated_at,
    }


def test_latest_updated_at_wins_and_counts_duplicates():
    table = pa.Table.from_pylist([
        _row("a1", date(2026, 1, 1), "At Risk", datetime(2026, 1, 1)),
        _row("a1", date(2026, 1, 1), "Healthy", datetime(2026, 1, 9)),  # newer -> wins
        _row("a2", date(2026, 1, 1), "At Risk", datetime(2026, 1, 5)),
    ])
    records, dup_count = deduplicate(table)

    assert dup_count == 1
    by_id = {r.account_id: r for r in records}
    assert by_id["a1"].status == "Healthy"   # newer row survived
    assert by_id["a2"].status == "At Risk"


def test_empty_table():
    table = pa.Table.from_pylist([], schema=pa.schema([
        ("account_id", pa.string()),
        ("account_name", pa.string()),
        ("account_region", pa.string()),
        ("month", pa.date32()),
        ("status", pa.string()),
        ("renewal_date", pa.date32()),
        ("account_owner", pa.string()),
        ("arr", pa.int64()),
        ("updated_at", pa.timestamp("ns")),
    ]))
    records, dup_count = deduplicate(table)
    assert records == []
    assert dup_count == 0


def test_streaming_dedup_across_batch_boundaries(sample_parquet):
    # batch_size=1 forces one row per record batch, so dedup must work *across*
    # batches — proving the streaming path doesn't rely on a single materialized table.
    fs, path = open_uri(sample_parquet)
    records, rows_scanned, dup = load_and_deduplicate(
        fs, path, date(2026, 1, 1), 24, batch_size=1
    )
    by_id = {r.account_id: r for r in records}
    # a4 has two Jan rows; the later updated_at (Healthy) must win across batches.
    assert by_id["a4"].status == "Healthy"
    assert dup == 1
    assert rows_scanned == 11  # total rows in the fixture window
