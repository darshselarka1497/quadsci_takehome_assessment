# Risk Alert Service

A cloud-deployable Python service that ingests monthly account-health Parquet data,
identifies **At Risk** accounts for a target month, computes how long each has been
**continuously** at risk, routes Slack alerts by region, and is **safe to re-run**
(no duplicate alerts). Exposed behind a small FastAPI interface.

---

## Architecture

Layered, with a clear interface at each boundary (see [docs/architecture.md](docs/architecture.md)
for component + sequence diagrams):

| Layer | Module | Responsibility |
|-------|--------|----------------|
| Storage | [`app/storage.py`](app/storage.py) | `open_uri()` — single interface for `file://`, `gs://`, `s3://` |
| Data | [`app/data.py`](app/data.py) | Parquet load with **predicate pushdown** + **record-batch streaming**; row-level dedup by latest `updated_at` |
| Business logic | [`app/duration.py`](app/duration.py) | Continuous At-Risk duration (gap resets the streak) |
| Notification | [`app/slack.py`](app/slack.py) | Slack client: exponential backoff, 429/5xx retry, `Retry-After` |
| Notification | [`app/notifications.py`](app/notifications.py) | Aggregated unknown-region email digest (stub) |
| Persistence | [`app/db.py`](app/db.py) | SQLAlchemy `runs` + `alert_outcomes`; UNIQUE idempotency |
| Service | [`app/pipeline.py`](app/pipeline.py) | Orchestrates a run end-to-end |
| API | [`app/main.py`](app/main.py) | FastAPI routes |

### Two concerns, kept separate
- **Deduplication** (data quality): multiple rows for `(account_id, month)` are resolved
  by latest `updated_at` and the dropped count is reported.
- **Idempotency** (operational safety): a UNIQUE constraint on
  `(account_id, month, alert_type)` in SQLite means re-running a month produces
  `skipped_replay`, never a second Slack post.

---

## Business logic: continuous At-Risk duration

Starting from the target month, walk **backward** month-by-month and count consecutive
`At Risk` months. Stop when status changes, a month is missing, or history ends.
If there is no prior At-Risk month, duration = 1.

```
2025-10 At Risk
2025-11 At Risk
2025-12 Healthy   <- gap breaks the streak
2026-01 At Risk   => duration = 1 (NOT 3)
```

---

## Scale awareness

The Parquet file may be large, so the service (`load_and_deduplicate` in
[`app/data.py`](app/data.py)):
- Pushes the `month` predicate to the Parquet reader via a **PyArrow dataset filtered
  scan**, reading only the target month plus the `HISTORY_LOOKBACK_MONTHS` window
  needed for duration — never the whole file.
- Projects only the columns it needs.
- **Streams the result as record batches** (`SCAN_BATCH_SIZE` rows each) and
  deduplicates incrementally, so peak memory is bounded by one batch plus the set of
  unique `(account_id, month)` keys — not the file, nor even the full window.
- Is runnable as a containerized batch job (see Dockerfile).

---

## Configuration (environment variables)

| Var | Default | Notes |
|-----|---------|-------|
| `REGION_CHANNEL_MAP` | `{"AMER":"amer-risk-alerts","EMEA":"emea-risk-alerts","APAC":"apac-risk-alerts"}` | JSON. **No default channel** — unknown regions fail. |
| `ARR_THRESHOLD` | `0` | Floor to suppress noise on small accounts. Default `0` = alert on all; raise per-customer to tune signal quality. |
| `DETAILS_BASE_URL` | `https://app.yourcompany.com/accounts` | Account details link base. |
| `HISTORY_LOOKBACK_MONTHS` | `24` | How far back to scan for duration. Caps the read window. |
| `SCAN_BATCH_SIZE` | `65536` | Rows per Parquet record batch. Caps peak memory during the streamed scan. |
| `SLACK_WEBHOOK_BASE_URL` | – | Base-URL mode: POST to `{base}/{channel}`. **Takes precedence** if both set. |
| `SLACK_WEBHOOK_URL` | – | Single-webhook mode (real Slack). |
| `RETRY_MAX_ATTEMPTS` / `RETRY_BASE_SECONDS` / `RETRY_CAP_SECONDS` | `5` / `1.0` / `60.0` | Backoff: `min(cap, base*2^attempt) + jitter`. |
| `DB_URL` | `sqlite:///./risk_alerts.db` | SQLAlchemy URL. Point at Postgres for concurrent/multi-tenant. |
| `SUPPORT_EMAIL` | `support@quadsci.ai` | Recipient of the unknown-region digest. |
| `GOOGLE_APPLICATION_CREDENTIALS` | – | GCS auth for local dev (see below). |

### Why `ARR_THRESHOLD=0` by default
The default surfaces **all** At-Risk accounts rather than silently hiding any — a
conservative choice for a brand-new integration where missing a real risk signal is
worse than an extra alert. It's a single env var to raise once a customer knows their
noise floor (e.g. suppress accounts under $10k ARR).

### GCS auth
- **Production:** prefer **Workload Identity** (GKE/Cloud Run) — no credentials file on disk.
- **Local dev:** set `GOOGLE_APPLICATION_CREDENTIALS=/path/to/sa.json` (Application
  Default Credentials). S3 would use IAM roles / instance profiles the same way.

---

## Quickstart (local)

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
```

**1. Start the mock Slack server** (injects 429/500 to exercise retries):
```bash
MOCK_SLACK_FAIL_RATE_429=0.2 MOCK_SLACK_FAIL_RATE_500=0.1 \
  uvicorn mock_slack.server:app --port 9000
```

**2. Start the service:**
```bash
export SLACK_WEBHOOK_BASE_URL="http://localhost:9000/slack/webhook"
uvicorn app.main:app --port 8000
```

**3. Exercise it:**
```bash
PARQUET="file://$PWD/monthly_account_status.parquet"

# Preview (no Slack, no persistence)
curl -s -X POST localhost:8000/preview -H 'Content-Type: application/json' \
  -d "{\"source_uri\":\"$PARQUET\",\"month\":\"2026-01-01\"}"

# Real run (synchronous; blocks until complete)
curl -s -X POST localhost:8000/runs -H 'Content-Type: application/json' \
  -d "{\"source_uri\":\"$PARQUET\",\"month\":\"2026-01-01\"}"

# Inspect a run
curl -s localhost:8000/runs/<run_id>
```

---

## API

| Method | Path | Behavior |
|--------|------|----------|
| `POST` | `/runs` | Processes synchronously (blocks), sends Slack unless `dry_run`, persists run + outcomes, returns `{"run_id": ...}`. Completes even if some sends fail. |
| `GET` | `/runs/{run_id}` | Persisted status + counts (`rows_scanned`, `duplicate_rows`, `alerts_sent`, `skipped_replay`, `failed_deliveries`) + sample alerts/errors. |
| `POST` | `/preview` | Same request, computes alerts, **no Slack, no persistence**. |
| `GET` | `/health` | Liveness. |

Request body: `{"source_uri": "...", "month": "YYYY-MM-01", "dry_run": false}`.

### Example: `POST /preview` (truncated)
```json
{
  "month": "2026-01-01",
  "rows_scanned": 10587,
  "duplicate_rows": 308,
  "at_risk_count": 151,
  "unknown_region_count": 4,
  "alerts": [
    {
      "account_id": "a00012",
      "account_name": "Account 0012",
      "account_region": "AMER",
      "channel": "amer-risk-alerts",
      "duration_months": 1,
      "risk_start_month": "2026-01-01",
      "arr": 15702,
      "routable": true,
      "reason": null,
      "message": "🚩 At Risk: Account 0012 (a00012)\nRegion: AMER\nAt Risk for: 1 months (since 2026-01-01)\nARR: $15,702\nRenewal date: Unknown\nOwner: owner12@example.com\nDetails: https://app.yourcompany.com/accounts/a00012"
    }
  ]
}
```

### Example: `GET /runs/{id}` (real run, then replay)
```json
// first run
{"status":"succeeded","counts":{"rows_scanned":10587,"duplicate_rows":308,
 "alerts_sent":147,"skipped_replay":0,"failed_deliveries":4}}

// re-running the same month -> idempotent, no new Slack posts
{"status":"succeeded","counts":{"rows_scanned":10587,"duplicate_rows":308,
 "alerts_sent":0,"skipped_replay":147,"failed_deliveries":4}}
```
(147 = 151 At Risk − 4 unknown-region; the 4 unknown regions are reported as
`failed_deliveries` with reason `unknown_region` and trigger one aggregated email digest.)

---

## Replay & failure semantics
- **Already sent** for `(account_id, month, at_risk)` → `skipped_replay`, no Slack post.
- **Previously failed** (e.g. transient delivery failure) → retried on the next run;
  the outcome row is updated in place (a `sent` row is terminal and never downgraded).
- **Unknown / missing region** → `failed` / `unknown_region`, no Slack; aggregated into a
  single email to `SUPPORT_EMAIL`. To force a resend after fixing data, delete the relevant
  `alert_outcomes` row (a `force_resend` flag would be the productized version).

---

## Slack integration
Two modes (base-URL wins if both set):
1. **Base-URL mode** (mock/local): `SLACK_WEBHOOK_BASE_URL` → POST `{base}/{channel}`.
2. **Single-webhook mode** (real Slack): `SLACK_WEBHOOK_URL` → POST that URL.

Retries on 429 and 5xx with exponential backoff and honors `Retry-After`. Sending is
synchronous because the spec requires `POST /runs` to block; in production this moves
behind a queue (Cloud Tasks / Celery) for async fan-out.

---

## Testing

```bash
pytest -q
```
Covers the gap-reset duration edge case, dedup (latest `updated_at` wins), region
routing incl. `unknown_region`, Slack retry/`Retry-After`, and the full pipeline
(send → replay idempotency → dry-run persists nothing). For CI, these unit/integration
tests run against an in-memory Parquet fixture and a fake Slack transport; a smoke test
would run against a real GCS bucket + mock Slack in staging.

---

## Docker

```bash
docker build -t risk-alert-service .
docker run -p 8000:8000 \
  -e SLACK_WEBHOOK_BASE_URL="http://host.docker.internal:9000/slack/webhook" \
  -e DB_URL="sqlite:////data/risk_alerts.db" \
  risk-alert-service
```

---

## Production evolution (designed, not built here)
- **Async `POST /runs`**: enqueue + return `run_id` immediately; `GET /runs/{id}` polls.
  Avoids HTTP timeouts and enables region fan-out.
- **Postgres** instead of SQLite for concurrent writes / multi-tenant (per-customer
  schema or DB). ORM models are unchanged.
- **Real email** (SendGrid / SES) behind the existing `notifications` interface.
- **S3 read**: enable `pyarrow.fs.S3FileSystem()` in `open_uri` — a one-function change.
- **Observability**: structured JSON logs to Cloud Logging/Datadog; counter metrics on
  `alerts_sent` / `failed_deliveries` / `duplicate_rows`; alert on `failure_rate`.
