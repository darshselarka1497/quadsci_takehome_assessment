"""Persistence layer (SQLAlchemy ORM over SQLite).

Two tables:
  * ``runs``           — one row per pipeline invocation, with rolled-up counts.
  * ``alert_outcomes`` — one row per (account, month, alert_type) decision.

The UNIQUE constraint on (account_id, month, alert_type) enforces *idempotency*
at the database level — re-running a month cannot create a second alert row, so
replays surface as ``skipped_replay`` instead of duplicate Slack posts. This is
the operational-safety concern, distinct from row-level deduplication.

SQLite is appropriate for a single-node batch job. For concurrent writes or
multi-customer deployments, point ``DB_URL`` at Postgres — the ORM models are
unchanged.
"""
from __future__ import annotations

from datetime import datetime, timezone

from sqlalchemy import (
    Boolean,
    DateTime,
    ForeignKey,
    Integer,
    String,
    Text,
    UniqueConstraint,
    create_engine,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship, sessionmaker


class Base(DeclarativeBase):
    pass


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


class Run(Base):
    __tablename__ = "runs"

    id: Mapped[str] = mapped_column(String, primary_key=True)
    source_uri: Mapped[str] = mapped_column(String, nullable=False)
    month: Mapped[str] = mapped_column(String, nullable=False)  # YYYY-MM-01
    dry_run: Mapped[bool] = mapped_column(Boolean, default=False)
    status: Mapped[str] = mapped_column(String, default="succeeded")  # succeeded/failed

    rows_scanned: Mapped[int] = mapped_column(Integer, default=0)
    duplicate_rows: Mapped[int] = mapped_column(Integer, default=0)
    alerts_sent: Mapped[int] = mapped_column(Integer, default=0)
    skipped_replay: Mapped[int] = mapped_column(Integer, default=0)
    failed_deliveries: Mapped[int] = mapped_column(Integer, default=0)

    error: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=_utcnow)

    outcomes: Mapped[list["AlertOutcome"]] = relationship(
        back_populates="run", cascade="all, delete-orphan"
    )


class AlertOutcome(Base):
    __tablename__ = "alert_outcomes"
    __table_args__ = (
        UniqueConstraint(
            "account_id", "month", "alert_type", name="uq_account_month_alert_type"
        ),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    run_id: Mapped[str] = mapped_column(ForeignKey("runs.id"), nullable=False)

    account_id: Mapped[str] = mapped_column(String, nullable=False)
    month: Mapped[str] = mapped_column(String, nullable=False)  # YYYY-MM-01
    alert_type: Mapped[str] = mapped_column(String, default="at_risk")
    channel: Mapped[str | None] = mapped_column(String, nullable=True)

    status: Mapped[str] = mapped_column(String, nullable=False)  # sent/skipped_replay/failed
    reason: Mapped[str | None] = mapped_column(String, nullable=True)  # e.g. unknown_region
    sent_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    error: Mapped[str | None] = mapped_column(Text, nullable=True)

    run: Mapped["Run"] = relationship(back_populates="outcomes")


# Cache one session factory per db_url (one in prod; distinct per test db).
_FACTORIES: dict[str, "sessionmaker"] = {}


def init_engine(db_url: str):
    """Create (once per url) the engine + tables and return a session factory."""
    factory = _FACTORIES.get(db_url)
    if factory is None:
        connect_args = {"check_same_thread": False} if db_url.startswith("sqlite") else {}
        engine = create_engine(db_url, connect_args=connect_args, future=True)
        Base.metadata.create_all(engine)
        factory = sessionmaker(bind=engine, expire_on_commit=False, future=True)
        _FACTORIES[db_url] = factory
    return factory


def get_session_factory(db_url: str):
    return init_engine(db_url)
