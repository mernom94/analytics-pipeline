"""Initial schema: raw_events, rollups, consumer_offsets, leaderboard_snapshots, merchants

Revision ID: 0001_initial_schema
Revises:
Create Date: 2026-05-12
"""
from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision = "0001_initial_schema"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    # ── merchants ──────────────────────────────────────────────────────────────
    op.create_table(
        "merchants",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("name", sa.Text, nullable=False),
        sa.Column("created_at", sa.TIMESTAMP(timezone=True), server_default=sa.text("NOW()"), nullable=False),
    )

    # ── raw_events (partitioned by occurred_at) ───────────────────────────────
    # Note: SQLAlchemy doesn't natively create partitioned tables, so we use raw DDL.
    op.execute("""
        CREATE TABLE raw_events (
            id               UUID         NOT NULL,
            event_type       TEXT         NOT NULL,
            merchant_id      UUID         NOT NULL,
            amount_cents     INTEGER      NOT NULL,
            currency         CHAR(3)      NOT NULL DEFAULT 'EUR',
            idempotency_key  TEXT         NOT NULL,
            occurred_at      TIMESTAMPTZ  NOT NULL,
            client_timestamp TIMESTAMPTZ,
            metadata         JSONB,
            created_at       TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
            PRIMARY KEY (id, occurred_at)
        ) PARTITION BY RANGE (occurred_at)
    """)

    # Unique constraint on idempotency_key for deduplication
    op.execute("""
        CREATE UNIQUE INDEX uq_raw_events_idempotency_key ON raw_events (idempotency_key, occurred_at)
    """)

    op.execute("CREATE INDEX ix_raw_events_merchant_occurred ON raw_events (merchant_id, occurred_at)")
    op.execute("CREATE INDEX ix_raw_events_occurred_at      ON raw_events (occurred_at)")
    op.execute("CREATE INDEX ix_raw_events_created_at       ON raw_events (created_at)")

    # ── Default partition (catch-all for data outside explicit partitions) ─────
    op.execute("""
        CREATE TABLE raw_events_default
        PARTITION OF raw_events DEFAULT
    """)

    # ── rollup_minute ─────────────────────────────────────────────────────────
    op.create_table(
        "rollup_minute",
        sa.Column("merchant_id",      postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("bucket",           sa.TIMESTAMP(timezone=True),   nullable=False),
        sa.Column("event_type",       sa.Text,                       nullable=False),
        sa.Column("count",            sa.BigInteger,                 nullable=False, server_default="0"),
        sa.Column("amount_sum_cents", sa.BigInteger,                 nullable=False, server_default="0"),
        sa.Column("amount_min_cents", sa.Integer,                    nullable=True),
        sa.Column("amount_max_cents", sa.Integer,                    nullable=True),
        sa.Column("updated_at",       sa.TIMESTAMP(timezone=True),   server_default=sa.text("NOW()"), nullable=False),
        sa.PrimaryKeyConstraint("merchant_id", "bucket", "event_type"),
    )
    op.create_index("ix_rollup_minute_bucket",          "rollup_minute", ["bucket"])
    op.create_index("ix_rollup_minute_merchant_bucket", "rollup_minute", ["merchant_id", "bucket"])

    # ── rollup_hour ───────────────────────────────────────────────────────────
    op.create_table(
        "rollup_hour",
        sa.Column("merchant_id",      postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("bucket",           sa.TIMESTAMP(timezone=True),   nullable=False),
        sa.Column("event_type",       sa.Text,                       nullable=False),
        sa.Column("count",            sa.BigInteger,                 nullable=False, server_default="0"),
        sa.Column("amount_sum_cents", sa.BigInteger,                 nullable=False, server_default="0"),
        sa.Column("amount_min_cents", sa.Integer,                    nullable=True),
        sa.Column("amount_max_cents", sa.Integer,                    nullable=True),
        sa.Column("updated_at",       sa.TIMESTAMP(timezone=True),   server_default=sa.text("NOW()"), nullable=False),
        sa.PrimaryKeyConstraint("merchant_id", "bucket", "event_type"),
    )
    op.create_index("ix_rollup_hour_bucket",          "rollup_hour", ["bucket"])
    op.create_index("ix_rollup_hour_merchant_bucket", "rollup_hour", ["merchant_id", "bucket"])

    # ── rollup_day ────────────────────────────────────────────────────────────
    op.create_table(
        "rollup_day",
        sa.Column("merchant_id",      postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("bucket",           sa.TIMESTAMP(timezone=True),   nullable=False),
        sa.Column("event_type",       sa.Text,                       nullable=False),
        sa.Column("count",            sa.BigInteger,                 nullable=False, server_default="0"),
        sa.Column("amount_sum_cents", sa.BigInteger,                 nullable=False, server_default="0"),
        sa.Column("amount_min_cents", sa.Integer,                    nullable=True),
        sa.Column("amount_max_cents", sa.Integer,                    nullable=True),
        sa.Column("updated_at",       sa.TIMESTAMP(timezone=True),   server_default=sa.text("NOW()"), nullable=False),
        sa.PrimaryKeyConstraint("merchant_id", "bucket", "event_type"),
    )
    op.create_index("ix_rollup_day_bucket",          "rollup_day", ["bucket"])
    op.create_index("ix_rollup_day_merchant_bucket", "rollup_day", ["merchant_id", "bucket"])

    # ── consumer_offsets ──────────────────────────────────────────────────────
    op.create_table(
        "consumer_offsets",
        sa.Column("consumer_id",   sa.Text,                    primary_key=True),
        sa.Column("last_event_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("last_event_at", sa.TIMESTAMP(timezone=True),   nullable=True),
        sa.Column("updated_at",    sa.TIMESTAMP(timezone=True),   server_default=sa.text("NOW()"), nullable=False),
    )

    # ── leaderboard_snapshots ─────────────────────────────────────────────────
    op.create_table(
        "leaderboard_snapshots",
        sa.Column("id",              postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("period",          sa.Text,                       nullable=False),
        sa.Column("period_start",    sa.TIMESTAMP(timezone=True),   nullable=False),
        sa.Column("merchant_id",     postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("rank",            sa.Integer,                    nullable=False),
        sa.Column("amount_sum_cents",sa.BigInteger,                 nullable=False),
        sa.Column("created_at",      sa.TIMESTAMP(timezone=True),   server_default=sa.text("NOW()"), nullable=False),
        sa.UniqueConstraint("period", "period_start", "merchant_id", name="uq_leaderboard_snapshot"),
    )
    op.create_index("ix_leaderboard_snapshots_period", "leaderboard_snapshots", ["period", "period_start"])


def downgrade() -> None:
    op.drop_table("leaderboard_snapshots")
    op.drop_table("consumer_offsets")
    op.drop_table("rollup_day")
    op.drop_table("rollup_hour")
    op.drop_table("rollup_minute")
    op.execute("DROP TABLE IF EXISTS raw_events CASCADE")
    op.drop_table("merchants")
