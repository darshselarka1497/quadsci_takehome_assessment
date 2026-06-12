# Architecture

## Component diagram

```mermaid
flowchart TD
    Client[Client / Batch trigger] -->|POST /runs, /preview| API[FastAPI app.main]
    API --> Pipeline[pipeline.run_pipeline / preview]

    Pipeline --> Storage[storage.open_uri]
    Storage -->|file://| Local[(Local FS)]
    Storage -->|gs://| GCS[(Google Cloud Storage)]
    Storage -.->|s3:// designed| S3[(S3 — stub)]

    Pipeline --> Data[data.load_month_window + deduplicate]
    Data -->|predicate pushdown| Parquet[(Parquet)]

    Pipeline --> Duration[duration.compute_duration]
    Pipeline --> Slack[slack.SlackClient]
    Slack -->|retry 429/5xx + Retry-After| Webhook[Slack webhook / mock]

    Pipeline --> DB[(SQLite: runs + alert_outcomes)]
    Pipeline --> Email[notifications.send_unknown_region_digest]
    Email -.->|stub| Support[support@quadsci.ai]
```

## Sequence: `POST /runs`

```mermaid
sequenceDiagram
    participant C as Client
    participant API as FastAPI
    participant P as Pipeline
    participant ST as Storage
    participant D as Data
    participant DB as SQLite
    participant SL as Slack

    C->>API: POST /runs {source_uri, month, dry_run}
    API->>P: run_pipeline(req)
    P->>ST: open_uri(source_uri)
    ST-->>P: (filesystem, path)
    P->>D: load_month_window (predicate pushdown)
    D-->>P: table (target month + lookback only)
    P->>D: deduplicate (latest updated_at)
    D-->>P: records, duplicate_count

    P->>DB: which accounts already 'sent' this month?
    DB-->>P: already_sent set

    loop each At Risk candidate (ARR >= threshold)
        P->>P: compute_duration (gap resets streak)
        alt unknown / missing region
            P->>DB: outcome failed / unknown_region
        else already sent (replay)
            P->>DB: outcome skipped_replay
        else send
            P->>SL: POST {base}/{channel} (retry 429/5xx)
            SL-->>P: ok / failed
            P->>DB: outcome sent | failed
        end
    end

    P->>DB: persist Run + counts
    P->>P: aggregate unknown-region -> one email digest
    P-->>API: run_id
    API-->>C: {run_id}
```

## Key invariants
- **Predicate pushdown**: only the target month + lookback window is read from Parquet.
- **Dedup ≠ idempotency**: dedup resolves duplicate input rows (data quality); the UNIQUE
  constraint on `(account_id, month, alert_type)` prevents duplicate Slack posts on replay
  (operational safety).
- **No default channel**: unknown/missing regions never send; they fail with
  `unknown_region` and are surfaced in one aggregated email.
- **Run completes on partial failure**: a failed Slack delivery is recorded, not fatal.
- **Dry run** computes + persists run-level counts but writes no `alert_outcomes`, so it
  never occupies the idempotency slot.
