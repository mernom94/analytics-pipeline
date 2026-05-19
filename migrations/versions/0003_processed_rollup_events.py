"""
Add processed_rollup_events idempotency table.

Revision ID: 0003_processed_rollup_events
Revises: 0002_idempotency_table
Create Date: 2026-05-16

Purpose
───────
Enables exactly-once aggregation semantics in the rollup worker.

Previously the worker used additive ON CONFLICT DO UPDATE upserts.
Replaying the same batch (e.g. after a crash with offset reset) would
add the delta again, double-counting counts and sums.

This table records (consumer_id, event_id) pairs that have been fully
aggregated.  On replay, the worker inserts markers with ON CONFLICT DO
NOTHING and inspects which rows were actually inserted (RETURNING).
Only newly-inserted-marker events are aggregated; already-marked events
are skipped.  Rollup values are therefore computed exactly once per event.

Schema
──────
    PRIMARY KEY (consumer_id, event_id)

    consumer_id  — supports multiple independent workers reading the same
                   event stream with separate aggregation namespaces.
    event_id     — FK into raw_events (informational; not enforced because
                   raw_events is partitioned and PG can't FK into partitioned
                   tables without listing every child partition).
    processed_at — monotonic timestamp for debugging / compaction.
"""
from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision = "0003_processed_rollup_events"
down_revision = "0002_idempotency_table"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "processed_rollup_events",
        sa.Column("consumer_id", sa.Text, nullable=False),
        sa.Column(
            "event_id",
            postgresql.UUID(as_uuid=True),
            nullable=False,
        ),
        sa.Column(
            "processed_at",
            sa.TIMESTAMP(timezone=True),
            server_default=sa.text("NOW()"),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("consumer_id", "event_id",
                                name="pk_processed_rollup_events"),
    )
    # Support forward-scan: "give me all events this consumer has processed
    # after offset X" — used for compaction / audit tooling.
    op.create_index(
        "ix_processed_rollup_events_consumer_processed_at",
        "processed_rollup_events",
        ["consumer_id", "processed_at"],
    )


def downgrade() -> None:
    op.drop_index(
        "ix_processed_rollup_events_consumer_processed_at",
        table_name="processed_rollup_events",
    )
    op.drop_table("processed_rollup_events")