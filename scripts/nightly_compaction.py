#!/usr/bin/env python3
"""
Nightly compaction job.

Two responsibilities:
1. Re-derive rollup_day from rollup_hour for the previous day (canonical source)
2. Snapshot the leaderboard into leaderboard_snapshots for historical comparison

Run daily at 00:05 UTC so previous day's rollup_hour is fully settled.

Usage:
    python scripts/nightly_compaction.py [--date 2026-05-12]
"""
from __future__ import annotations

import argparse
import asyncio
import uuid
from datetime import date, datetime, timedelta, timezone

import asyncpg

from app.core.config import get_settings


async def compact_day_rollup(conn: asyncpg.Connection, target_date: date) -> dict:
    """
    Re-derive rollup_day from rollup_hour for a specific date.
    Uses INSERT ... ON CONFLICT DO UPDATE so it's safe to re-run.
    """
    start = datetime(target_date.year, target_date.month, target_date.day, tzinfo=timezone.utc)
    end = start + timedelta(days=1)

    result = await conn.execute("""
        INSERT INTO rollup_day (merchant_id, bucket, event_type, count, amount_sum_cents, amount_min_cents, amount_max_cents, updated_at)
        SELECT
            merchant_id,
            date_trunc('day', bucket)  AS bucket,
            event_type,
            SUM(count)                 AS count,
            SUM(amount_sum_cents)      AS amount_sum_cents,
            MIN(amount_min_cents)      AS amount_min_cents,
            MAX(amount_max_cents)      AS amount_max_cents,
            NOW()                      AS updated_at
        FROM rollup_hour
        WHERE bucket >= $1 AND bucket < $2
        GROUP BY merchant_id, date_trunc('day', bucket), event_type
        ON CONFLICT (merchant_id, bucket, event_type) DO UPDATE
        SET
            count            = EXCLUDED.count,
            amount_sum_cents = EXCLUDED.amount_sum_cents,
            amount_min_cents = EXCLUDED.amount_min_cents,
            amount_max_cents = EXCLUDED.amount_max_cents,
            updated_at       = NOW()
    """, start, end)

    # Check for discrepancy between streaming and compacted values
    discrepancy = await conn.fetch("""
        SELECT
            rd.merchant_id,
            rd.event_type,
            rd.count            AS day_count,
            COALESCE(h.hour_count, 0) AS hour_count,
            ABS(rd.count - COALESCE(h.hour_count, 0)) AS delta
        FROM rollup_day rd
        LEFT JOIN (
            SELECT merchant_id, event_type, SUM(count) AS hour_count
            FROM rollup_hour
            WHERE bucket >= $1 AND bucket < $2
            GROUP BY merchant_id, event_type
        ) h USING (merchant_id, event_type)
        WHERE rd.bucket = $1
          AND ABS(rd.count - COALESCE(h.hour_count, 0)) > 0
        ORDER BY delta DESC
        LIMIT 10
    """, start, end)

    return {
        "date": target_date.isoformat(),
        "rows_upserted": result,
        "discrepancies": [dict(r) for r in discrepancy],
    }


async def snapshot_leaderboard(conn: asyncpg.Connection, target_date: date) -> int:
    """
    Take a point-in-time leaderboard snapshot for the completed day.
    """
    start = datetime(target_date.year, target_date.month, target_date.day, tzinfo=timezone.utc)
    end = start + timedelta(days=1)

    top_merchants = await conn.fetch("""
        SELECT
            merchant_id,
            SUM(amount_sum_cents) AS volume,
            RANK() OVER (ORDER BY SUM(amount_sum_cents) DESC) AS rank
        FROM rollup_day
        WHERE bucket >= $1 AND bucket < $2
          AND event_type = 'PAYMENT_CONFIRMED'
        GROUP BY merchant_id
        ORDER BY volume DESC
        LIMIT 100
    """, start, end)

    if not top_merchants:
        return 0

    await conn.executemany("""
        INSERT INTO leaderboard_snapshots
            (id, period, period_start, merchant_id, rank, amount_sum_cents)
        VALUES ($1, 'daily', $2, $3, $4, $5)
        ON CONFLICT (period, period_start, merchant_id) DO UPDATE
        SET rank            = EXCLUDED.rank,
            amount_sum_cents = EXCLUDED.amount_sum_cents
    """, [
        (uuid.uuid4(), start, row["merchant_id"], row["rank"], row["volume"])
        for row in top_merchants
    ])

    return len(top_merchants)


async def run_compaction(target_date: date) -> None:
    settings = get_settings()
    dsn = settings.database_url.replace("+asyncpg", "")
    conn = await asyncpg.connect(dsn)

    print(f"Starting compaction for {target_date}")

    try:
        async with conn.transaction():
            result = await compact_day_rollup(conn, target_date)
            print(f"Rollup compaction: {result}")

            if result["discrepancies"]:
                print(f"WARNING: {len(result['discrepancies'])} discrepancies detected:")
                for d in result["discrepancies"]:
                    print(f"  merchant={d['merchant_id']} type={d['event_type']} delta={d['delta']}")

            snapshot_count = await snapshot_leaderboard(conn, target_date)
            print(f"Leaderboard snapshot: {snapshot_count} merchants")

    finally:
        await conn.close()

    print("Compaction complete")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Nightly rollup compaction")
    parser.add_argument(
        "--date",
        type=date.fromisoformat,
        default=(date.today() - timedelta(days=1)),
        help="Date to compact (default: yesterday)",
    )
    args = parser.parse_args()
    asyncio.run(run_compaction(args.date))
