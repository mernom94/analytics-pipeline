#!/usr/bin/env python3
"""
Partition pre-creation script.

Creates daily partitions for raw_events for the next 7 days.
Run nightly via cron or as a k8s CronJob.

Usage:
    python scripts/create_partitions.py [--days 7]
"""
from __future__ import annotations

import argparse
import asyncio
from datetime import date, timedelta

import asyncpg

from app.core.config import get_settings


async def create_partitions(days_ahead: int = 7) -> None:
    settings = get_settings()
    dsn = settings.database_url.replace("+asyncpg", "")
    conn = await asyncpg.connect(dsn)

    today = date.today()
    created = []
    skipped = []

    for i in range(-1, days_ahead + 1):  # Include yesterday for safety
        target_date = today + timedelta(days=i)
        partition_name = f"raw_events_{target_date.strftime('%Y_%m_%d')}"
        start = target_date.isoformat()
        end = (target_date + timedelta(days=1)).isoformat()

        # Check if partition exists
        exists = await conn.fetchval(
            "SELECT EXISTS(SELECT 1 FROM pg_tables WHERE tablename = $1)",
            partition_name,
        )

        if exists:
            skipped.append(partition_name)
            continue

        await conn.execute(f"""
            CREATE TABLE IF NOT EXISTS {partition_name}
            PARTITION OF raw_events
            FOR VALUES FROM ('{start}') TO ('{end}')
        """)
        created.append(partition_name)
        print(f"Created partition: {partition_name}")

    await conn.close()
    print(f"\nDone: {len(created)} created, {len(skipped)} skipped")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Pre-create raw_events daily partitions")
    parser.add_argument("--days", type=int, default=7, help="Days ahead to create")
    args = parser.parse_args()
    asyncio.run(create_partitions(args.days))
