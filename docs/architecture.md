# Architecture

Plain-text diagrams (monospace) — render in any Markdown/PDF viewer, no tooling needed.

## Components

```text
        Client / Batch trigger
                │
                │   POST /runs · POST /preview
                ▼
   ┌─────────────────────────────────────────────┐
   │  API LAYER        app/main.py (FastAPI)       │
   │  /runs   /runs/{id}   /preview   /health      │
   └─────────────────────────────────────────────┘
                │
                ▼
   ┌─────────────────────────────────────────────┐
   │  SERVICE LAYER    app/pipeline.py             │
   │  orchestrates one run, end to end             │
   └─────────────────────────────────────────────┘
                │
                ▼   the pipeline calls each layer:


   LAYER         DOES                       TALKS TO
   ─────────     ──────────────────────     ────────────────────────────────────
   storage       open_uri()                 file:// · gs:// · s3:// (stub)
   data          load + deduplicate         Parquet (predicate pushdown)
   duration      compute_duration           continuous At-Risk months (gap resets)
   slack         SlackClient.send_alert     Slack webhook (retry 429/5xx, Retry-After)
   db            SQLAlchemy models          SQLite: runs · alert_outcomes (UNIQUE key)
   notify        unknown-region digest      support@quadsci.ai (email stub)
```

## Sequence — `POST /runs`

```text
Synchronous: the request blocks until the whole run finishes.

  Client  ──▶  FastAPI  ──▶  Pipeline


  STEP 1   load the data
  ─────────────────────────────────────────────────────────────────────
    open_uri(source_uri)            → (filesystem, path)
    load_month_window               → target month + lookback only
                                      (predicate pushdown on `month`)
    deduplicate (latest updated_at) → records  +  duplicate_count


  STEP 2   for each At Risk account (arr ≥ ARR_THRESHOLD)
  ─────────────────────────────────────────────────────────────────────
    compute_duration                  (a gap in At Risk status resets it)
    region → channel, then:

        unknown / missing region  →  failed (unknown_region), no send
        already sent this month   →  skipped_replay, no send
        otherwise                 →  Slack POST {base}/{channel}
                                       retry 429/5xx, honor Retry-After
                                       → sent  (or failed after retries)


  STEP 3   finish the run
  ─────────────────────────────────────────────────────────────────────
    one aggregated email for all unknown-region accounts (stub)
    persist Run + counts:
        rows_scanned · duplicate_rows · alerts_sent
        skipped_replay · failed_deliveries


  Pipeline  ──▶  FastAPI  ──▶  Client      { "run_id": "<uuid>" }
```

## Key invariants
- **Predicate pushdown** — only the target month + lookback window is read from Parquet.
- **Dedup ≠ idempotency** — dedup resolves duplicate input rows (data quality); the UNIQUE
  constraint on `(account_id, month, alert_type)` prevents duplicate Slack posts on replay
  (operational safety).
- **No default channel** — unknown/missing regions never send; they fail with
  `unknown_region` and are surfaced in one aggregated email.
- **Run completes on partial failure** — a failed Slack delivery is recorded, not fatal.
- **Dry run** computes + persists run-level counts but writes no `alert_outcomes`, so it
  never occupies the idempotency slot.
