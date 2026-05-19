import uuid
from datetime import datetime

from sqlalchemy import (
    BigInteger,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy import DateTime
from sqlalchemy.dialects.postgresql import JSONB, UUID

# Convenience alias — emits TIMESTAMPTZ DDL on PostgreSQL
TIMESTAMPTZ = DateTime(timezone=True)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from sqlalchemy.sql import func


class Base(DeclarativeBase):
    pass


class RawEvent(Base):
    """
    Append-only event log.  Partitioned by occurred_at (daily).
    Never updated, never deleted — it is the source of truth and replay buffer.
    """

    __tablename__ = "raw_events"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    event_type: Mapped[str] = mapped_column(Text, nullable=False)
    merchant_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    amount_cents: Mapped[int] = mapped_column(Integer, nullable=False)
    currency: Mapped[str] = mapped_column(String(3), nullable=False, default="EUR")
    idempotency_key: Mapped[str] = mapped_column(Text, nullable=False, unique=True)
    occurred_at: Mapped[datetime] = mapped_column(TIMESTAMPTZ, nullable=False)
    client_timestamp: Mapped[datetime | None] = mapped_column(TIMESTAMPTZ, nullable=True)
    metadata_: Mapped[dict | None] = mapped_column("metadata", JSONB, nullable=True)
    created_at: Mapped[datetime] = mapped_column(TIMESTAMPTZ, nullable=False, server_default=func.now())

    __table_args__ = (
        Index("ix_raw_events_merchant_occurred", "merchant_id", "occurred_at"),
        Index("ix_raw_events_occurred_at", "occurred_at"),
        Index("ix_raw_events_created_at", "created_at"),
        {
            "postgresql_partition_by": "RANGE (occurred_at)",
        },
    )


class RollupMinute(Base):
    """Pre-aggregated counts and sums per merchant per minute bucket."""

    __tablename__ = "rollup_minute"

    merchant_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True)
    bucket: Mapped[datetime] = mapped_column(TIMESTAMPTZ, primary_key=True)
    event_type: Mapped[str] = mapped_column(Text, primary_key=True)
    count: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0)
    amount_sum_cents: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0)
    amount_min_cents: Mapped[int] = mapped_column(Integer, nullable=True)
    amount_max_cents: Mapped[int] = mapped_column(Integer, nullable=True)
    updated_at: Mapped[datetime] = mapped_column(TIMESTAMPTZ, nullable=False, server_default=func.now())

    __table_args__ = (
        Index("ix_rollup_minute_bucket", "bucket"),
        Index("ix_rollup_minute_merchant_bucket", "merchant_id", "bucket"),
    )


class RollupHour(Base):
    """Pre-aggregated counts and sums per merchant per hour bucket."""

    __tablename__ = "rollup_hour"

    merchant_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True)
    bucket: Mapped[datetime] = mapped_column(TIMESTAMPTZ, primary_key=True)
    event_type: Mapped[str] = mapped_column(Text, primary_key=True)
    count: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0)
    amount_sum_cents: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0)
    amount_min_cents: Mapped[int] = mapped_column(Integer, nullable=True)
    amount_max_cents: Mapped[int] = mapped_column(Integer, nullable=True)
    updated_at: Mapped[datetime] = mapped_column(TIMESTAMPTZ, nullable=False, server_default=func.now())

    __table_args__ = (
        Index("ix_rollup_hour_bucket", "bucket"),
        Index("ix_rollup_hour_merchant_bucket", "merchant_id", "bucket"),
    )


class RollupDay(Base):
    """Pre-aggregated counts and sums per merchant per day bucket."""

    __tablename__ = "rollup_day"

    merchant_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True)
    bucket: Mapped[datetime] = mapped_column(TIMESTAMPTZ, primary_key=True)
    event_type: Mapped[str] = mapped_column(Text, primary_key=True)
    count: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0)
    amount_sum_cents: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0)
    amount_min_cents: Mapped[int] = mapped_column(Integer, nullable=True)
    amount_max_cents: Mapped[int] = mapped_column(Integer, nullable=True)
    updated_at: Mapped[datetime] = mapped_column(TIMESTAMPTZ, nullable=False, server_default=func.now())

    __table_args__ = (
        Index("ix_rollup_day_bucket", "bucket"),
        Index("ix_rollup_day_merchant_bucket", "merchant_id", "bucket"),
    )


class ConsumerOffset(Base):
    """
    Tracks rollup worker position in the event stream.
    Used for crash recovery: worker replays from last committed offset.
    """

    __tablename__ = "consumer_offsets"

    consumer_id: Mapped[str] = mapped_column(Text, primary_key=True)
    last_event_id: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True), nullable=True)
    last_event_at: Mapped[datetime | None] = mapped_column(TIMESTAMPTZ, nullable=True)
    updated_at: Mapped[datetime] = mapped_column(TIMESTAMPTZ, nullable=False, server_default=func.now(), onupdate=func.now())


class LeaderboardSnapshot(Base):
    """Point-in-time leaderboard snapshots for historical comparison."""

    __tablename__ = "leaderboard_snapshots"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    period: Mapped[str] = mapped_column(Text, nullable=False)  # daily | weekly | monthly
    period_start: Mapped[datetime] = mapped_column(TIMESTAMPTZ, nullable=False)
    merchant_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    rank: Mapped[int] = mapped_column(Integer, nullable=False)
    amount_sum_cents: Mapped[int] = mapped_column(BigInteger, nullable=False)
    created_at: Mapped[datetime] = mapped_column(TIMESTAMPTZ, nullable=False, server_default=func.now())

    __table_args__ = (
        Index("ix_leaderboard_snapshots_period", "period", "period_start"),
        UniqueConstraint("period", "period_start", "merchant_id", name="uq_leaderboard_snapshot"),
    )


class EventIdempotency(Base):
    """
    Global deduplication guard for raw_events idempotency keys.

    PostgreSQL does not enforce unique constraints across partitions on
    partitioned tables, so a non-partitioned table is required to guarantee
    globally unique idempotency keys regardless of which day-partition an
    event lands in.

    Insert flow:
        INSERT INTO event_idempotency (idempotency_key, event_id)
        ON CONFLICT (idempotency_key) DO NOTHING RETURNING event_id
        → row returned: new event, proceed to insert raw_events
        → no row returned: duplicate, return the existing event_id
    """

    __tablename__ = "event_idempotency"

    idempotency_key: Mapped[str] = mapped_column(Text, primary_key=True)
    event_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMPTZ, nullable=False, server_default=func.now()
    )

    __table_args__ = (
        Index("ix_event_idempotency_event_id", "event_id"),
    )


class Merchant(Base):
    """
    Merchants referenced by events.
    Kept minimal — just enough to validate merchant_id existence on ingestion.
    """

    __tablename__ = "merchants"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    name: Mapped[str] = mapped_column(Text, nullable=False)
    created_at: Mapped[datetime] = mapped_column(TIMESTAMPTZ, nullable=False, server_default=func.now())
