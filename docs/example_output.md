# Example Output

Real responses captured from the running service against the provided
`monthly_account_status.parquet`, target month `2026-01-01`.

- Total rows scanned: **10,587**
- Duplicate `(account_id, month)` rows resolved: **308**
- At Risk accounts: **151** (4 of them in an unknown/missing region)

---

## `POST /preview`

Computes alerts but sends nothing and persists nothing. (Alerts array trimmed to the
first two for readability; the real response contains all 151.)

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
    },
    {
      "account_id": "a00015",
      "account_name": "Account 0015",
      "account_region": "AMER",
      "channel": "amer-risk-alerts",
      "duration_months": 3,
      "risk_start_month": "2025-11-01",
      "arr": 0,
      "routable": true,
      "reason": null,
      "message": "🚩 At Risk: Account 0015 (a00015)\nRegion: AMER\nAt Risk for: 3 months (since 2025-11-01)\nARR: $0\nRenewal date: 2026-05-01\nOwner: owner15@example.com\nDetails: https://app.yourcompany.com/accounts/a00015"
    }
    // ... 149 more alerts
  ]
}
```

The `message` field rendered (this is the exact text POSTed to Slack):

```
🚩 At Risk: Account 0015 (a00015)
Region: AMER
At Risk for: 3 months (since 2025-11-01)
ARR: $0
Renewal date: 2026-05-01
Owner: owner15@example.com
Details: https://app.yourcompany.com/accounts/a00015
```

---

## `POST /runs`

Processes synchronously and returns the id once complete.

```json
{ "run_id": "f6debdbe-59e7-4880-9c8a-f7f998833906" }
```

## `GET /runs/{run_id}`

Persisted result: status, the four required counts, and sample alerts/errors.
(Sample arrays trimmed for readability.)

```json
{
  "run_id": "f6debdbe-59e7-4880-9c8a-f7f998833906",
  "status": "succeeded",
  "month": "2026-01-01",
  "dry_run": false,
  "counts": {
    "rows_scanned": 10587,
    "duplicate_rows": 308,
    "alerts_sent": 147,
    "skipped_replay": 0,
    "failed_deliveries": 4
  },
  "sample_alerts": [
    { "account_id": "a00012", "channel": "amer-risk-alerts", "status": "sent", "reason": null, "error": null },
    { "account_id": "a00022", "channel": "apac-risk-alerts", "status": "sent", "reason": null, "error": null },
    { "account_id": "a00054", "channel": "emea-risk-alerts", "status": "sent", "reason": null, "error": null }
    // ...
  ],
  "sample_errors": [
    { "account_id": "a00090", "channel": null, "status": "failed", "reason": "unknown_region", "error": null },
    { "account_id": "a00559", "channel": null, "status": "failed", "reason": "unknown_region", "error": null },
    { "account_id": "a00593", "channel": null, "status": "failed", "reason": "unknown_region", "error": null },
    { "account_id": "a00769", "channel": null, "status": "failed", "reason": "unknown_region", "error": null }
  ]
}
```

`alerts_sent` (147) + `failed_deliveries` (4 unknown-region) = 151 At Risk accounts.
The 4 unknown-region accounts are also aggregated into a single email digest to
`support@quadsci.ai` after the run.

---

## Replay (re-running the same month) — idempotency

Running `POST /runs` again for `2026-01-01` posts **nothing new** to Slack; every
already-sent alert is reported as `skipped_replay`:

```json
{
  "rows_scanned": 10587,
  "duplicate_rows": 308,
  "alerts_sent": 0,
  "skipped_replay": 147,
  "failed_deliveries": 4
}
```

This is enforced by a UNIQUE constraint on `(account_id, month, alert_type)` in SQLite —
duplicate Slack posts are impossible at the database level, not just in application code.
