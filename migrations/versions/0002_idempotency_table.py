"""
Fix cross-partition idempotency enforcement for raw_events.

Revision ID: 0002_idempotency_table
Revises: 0001_initial_schema
Create Date: 2026-05-14

Problem being fixed
-------------------
PostgreSQL does not support global unique indexes across partitioned tables
(as of PG 16).  The unique index on raw_events(idempotency_key) created in
the initial migration is only enforced *within* each partition — two events
with the same idempotency_key arriving on different days land in different
partitions and both are stored, silently double-counting rollups.

Solution
--------
A non-partitioned `event_idempotency` table acts as the global deduplication
guard.  The application INSERT flow becomes:

    1. INSERT INTO event_idempotency (idempotency_key, event_id)
       ON CONFLICT (idempotency_key) DO NOTHING RETURNING event_id
    2. If RETURNING returns a row → new event; INSERT into raw_events.
    3. If RETURNING returns nothing → duplicate; return existing event_id.

The non-partitioned table has a true primary key uniqueness guarantee
regardless of how raw_events is partitioned.

The old (ineffective) unique index on raw_events is dropped as it gave a
false sense of security without providing the guarantee.
"""
from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision = "0002_idempotency_table"
down_revision = "0001_initial_schema"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Drop the ineffective per-partition unique index from the partitioned table.
    op.execute("DROP INDEX IF EXISTS uq_raw_events_idempotency_key")

    # Create a non-partitioned deduplication table.
    # This is the single source of truth for idempotency_key uniqueness.
    op.create_table(
        "event_idempotency",
        sa.Column(
            "idempotency_key",
            sa.Text,
            primary_key=True,
            comment="Caller-supplied idempotency key; globally unique across all partitions.",
        ),
        sa.Column(
            "event_id",
            postgresql.UUID(as_uuid=True),
            nullable=False,
            comment="The UUID assigned to the canonical raw_events row for this key.",
        ),
        sa.Column(
            "created_at",
            sa.TIMESTAMP(timezone=True),
            server_default=sa.text("NOW()"),
            nullable=False,
        ),
    )
    # Index for reverse lookups (event_id → idempotency_key), used in health/debug tooling.
    op.create_index("ix_event_idempotency_event_id", "event_idempotency", ["event_id"])


def downgrade() -> None:
    op.drop_index("ix_event_idempotency_event_id", table_name="event_idempotency")
    op.drop_table("event_idempotency")
    # Restore the (ineffective) index so downgrade doesn't leave the schema
    # in a state that differs from the original 0001 migration output.
    op.execute(
        "CREATE UNIQUE INDEX uq_raw_events_idempotency_key ON raw_events (idempotency_key, occurred_at)"
    )
