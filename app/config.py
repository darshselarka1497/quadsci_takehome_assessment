"""Application configuration loaded from environment variables.

All cloud access, Slack routing, the details base URL, and tuning knobs are
configurable via env vars (with sane defaults) so the service is portable across
customer environments without code changes.
"""
from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from typing import Dict, Optional


def _get_bool(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def _default_region_map() -> Dict[str, str]:
    return {
        "AMER": "amer-risk-alerts",
        "EMEA": "emea-risk-alerts",
        "APAC": "apac-risk-alerts",
    }


@dataclass
class Settings:
    # --- Region routing (NO default channel; unknown regions are failures) ---
    region_channel_map: Dict[str, str] = field(default_factory=_default_region_map)

    # --- Alert tuning ---
    # ARR floor to suppress noise on small accounts. Default 0 => alert on all
    # At Risk accounts; raise per-customer to tune signal quality (see README).
    arr_threshold: int = 0
    details_base_url: str = "https://app.yourcompany.com/accounts"

    # How far back to scan when computing continuous-risk duration. Caps the
    # predicate-pushdown window so we never read the whole file.
    history_lookback_months: int = 24

    # Rows per Parquet record batch. Caps peak memory during the streamed scan.
    scan_batch_size: int = 65_536

    # --- Slack ---
    # Base-URL mode (mock/local) takes precedence over single-webhook mode.
    slack_webhook_base_url: Optional[str] = None
    slack_webhook_url: Optional[str] = None
    slack_auth_token: Optional[str] = None  # sent as X-Mock-Slack-Token if set

    # Retry knobs: delay = min(cap, base * 2**attempt) + jitter
    retry_max_attempts: int = 5
    retry_base_seconds: float = 1.0
    retry_cap_seconds: float = 60.0

    # --- Persistence ---
    db_url: str = "sqlite:///./risk_alerts.db"

    # --- Notifications ---
    support_email: str = "support@quadsci.ai"

    # --- Cloud auth (GCS) ---
    google_application_credentials: Optional[str] = None

    @classmethod
    def from_env(cls) -> "Settings":
        region_map_raw = os.getenv("REGION_CHANNEL_MAP")
        if region_map_raw:
            parsed = json.loads(region_map_raw)
            # Accept either {"AMER": "..."} or {"regions": {"AMER": "..."}}
            region_map = parsed.get("regions", parsed) if isinstance(parsed, dict) else {}
        else:
            region_map = _default_region_map()

        return cls(
            region_channel_map=region_map,
            arr_threshold=int(os.getenv("ARR_THRESHOLD", "0")),
            details_base_url=os.getenv(
                "DETAILS_BASE_URL", "https://app.yourcompany.com/accounts"
            ).rstrip("/"),
            history_lookback_months=int(os.getenv("HISTORY_LOOKBACK_MONTHS", "24")),
            scan_batch_size=int(os.getenv("SCAN_BATCH_SIZE", "65536")),
            slack_webhook_base_url=(os.getenv("SLACK_WEBHOOK_BASE_URL") or None),
            slack_webhook_url=(os.getenv("SLACK_WEBHOOK_URL") or None),
            slack_auth_token=(os.getenv("MOCK_SLACK_AUTH_TOKEN") or None),
            retry_max_attempts=int(os.getenv("RETRY_MAX_ATTEMPTS", "5")),
            retry_base_seconds=float(os.getenv("RETRY_BASE_SECONDS", "1.0")),
            retry_cap_seconds=float(os.getenv("RETRY_CAP_SECONDS", "60.0")),
            db_url=os.getenv("DB_URL", "sqlite:///./risk_alerts.db"),
            support_email=os.getenv("SUPPORT_EMAIL", "support@quadsci.ai"),
            google_application_credentials=(
                os.getenv("GOOGLE_APPLICATION_CREDENTIALS") or None
            ),
        )

    def channel_for_region(self, region: Optional[str]) -> Optional[str]:
        """Resolve a Slack channel for a region, or None if unroutable.

        Returns None when the region is missing/null or absent from config — the
        caller must treat this as an ``unknown_region`` failure (no default channel).
        """
        if not region:
            return None
        return self.region_channel_map.get(region)


def get_settings() -> Settings:
    """Return settings freshly read from the environment.

    Not cached so tests (and per-request overrides) see current env values.
    """
    return Settings.from_env()
