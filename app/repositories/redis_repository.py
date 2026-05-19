"""
Redis repository.

Responsibilities:
- Leaderboard sorted sets (ZINCRBY / ZREVRANGE)
- Metric query cache (GET/SET with TTL)
- Consumer lag tracking
"""
from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone

from redis.asyncio import Redis

from app.core.config import get_settings
from app.core.logging import get_logger

logger = get_logger(__name__)
settings = get_settings()


def _leaderboard_key(period: str) -> str:
    return f"leaderboard:volume:{period}"


def _metric_cache_key(merchant_id: uuid.UUID, start: datetime, end: datetime, granularity: str) -> str:
    return f"metrics:{merchant_id}:{start.isoformat()}:{end.isoformat()}:{granularity}"


def _sparkline_cache_key(merchant_id: uuid.UUID, window_minutes: int) -> str:
    return f"sparkline:{merchant_id}:{window_minutes}"


class RedisRepository:
    def __init__(self, redis: Redis) -> None:
        self._r = redis

    # ─── Leaderboard ──────────────────────────────────────────────────────────

    async def leaderboard_increment(
        self,
        period: str,
        merchant_id: uuid.UUID,
        amount_cents: int,
    ) -> None:
        """
        ZINCRBY O(log N) — add payment volume to merchant's score.
        Refreshes the key TTL on every write so the current-period key never
        expires while payments are actively flowing.
        """
        key = _leaderboard_key(period)
        async with self._r.pipeline(transaction=False) as pipe:
            pipe.zincrby(key, amount_cents, str(merchant_id))
            # 48-hour TTL, refreshed on every payment — current-period key
            # stays alive as long as payments flow; old periods expire naturally.
            pipe.expire(key, 48 * 3600)
            await pipe.execute()

    async def leaderboard_increment_batch(
        self,
        period: str,
        merchant_increments: dict[str, int],
    ) -> None:
        """
        Pipeline multiple ZINCRBY calls for one period in a single round-trip.

        Used by the rollup worker to apply an entire batch's leaderboard
        updates in one Redis call rather than one per event.
        """
        if not merchant_increments:
            return
        key = _leaderboard_key(period)
        async with self._r.pipeline(transaction=False) as pipe:
            for merchant_id_str, amount in merchant_increments.items():
                pipe.zincrby(key, amount, merchant_id_str)
            pipe.expire(key, 48 * 3600)
            await pipe.execute()

    async def leaderboard_top_n(
        self, period: str, top_n: int = 10
    ) -> list[tuple[str, float]]:
        """
        ZREVRANGE O(log N + K) — fetch top-N merchants by score descending.
        Returns list of (merchant_id_str, score).
        """
        results = await self._r.zrevrange(
            _leaderboard_key(period), 0, top_n - 1, withscores=True
        )
        return results  # type: ignore[return-value]

    async def leaderboard_exists(self, period: str) -> bool:
        return await self._r.exists(_leaderboard_key(period)) > 0

    async def leaderboard_rebuild(
        self, period: str, entries: list[dict]
    ) -> None:
        """
        Rebuild leaderboard sorted set from rollup_day data.
        Uses a pipeline for atomicity and efficiency.
        """
        key = _leaderboard_key(period)
        async with self._r.pipeline(transaction=True) as pipe:
            pipe.delete(key)
            for entry in entries:
                pipe.zadd(key, {str(entry["merchant_id"]): float(entry["volume_cents"])})
            pipe.expire(key, settings.leaderboard_rebuild_ttl)
            await pipe.execute()

        logger.info("leaderboard_rebuilt", period=period, entries=len(entries))

    async def leaderboard_delete(self, period: str) -> None:
        await self._r.delete(_leaderboard_key(period))

    # ─── Metric Cache ─────────────────────────────────────────────────────────

    async def cache_get(self, key: str) -> dict | list | None:
        raw = await self._r.get(key)
        if raw is None:
            return None
        try:
            return json.loads(raw)
        except json.JSONDecodeError:
            logger.warning("cache_decode_error", key=key)
            return None

    async def cache_set(self, key: str, value: dict | list, ttl: int) -> None:
        await self._r.set(key, json.dumps(value, default=str), ex=ttl)

    async def metric_cache_get(
        self, merchant_id: uuid.UUID, start: datetime, end: datetime, granularity: str
    ) -> dict | None:
        key = _metric_cache_key(merchant_id, start, end, granularity)
        result = await self.cache_get(key)
        return result  # type: ignore[return-value]

    async def metric_cache_set(
        self,
        merchant_id: uuid.UUID,
        start: datetime,
        end: datetime,
        granularity: str,
        value: dict,
        ttl: int,
    ) -> None:
        key = _metric_cache_key(merchant_id, start, end, granularity)
        await self.cache_set(key, value, ttl)

    async def sparkline_cache_get(
        self, merchant_id: uuid.UUID, window_minutes: int
    ) -> list | None:
        key = _sparkline_cache_key(merchant_id, window_minutes)
        result = await self.cache_get(key)
        return result  # type: ignore[return-value]

    async def sparkline_cache_set(
        self, merchant_id: uuid.UUID, window_minutes: int, value: list, ttl: int = 5
    ) -> None:
        key = _sparkline_cache_key(merchant_id, window_minutes)
        await self.cache_set(key, value, ttl)

    # ─── Health / Lag ─────────────────────────────────────────────────────────

    async def ping(self) -> float:
        """Returns round-trip latency in ms."""
        import time
        start = time.monotonic()
        await self._r.ping()
        return round((time.monotonic() - start) * 1000, 2)

    async def set_worker_heartbeat(self, worker_id: str) -> None:
        await self._r.set(
            f"worker:heartbeat:{worker_id}",
            datetime.now(timezone.utc).isoformat(),
            ex=60,
        )

    async def get_worker_heartbeat(self, worker_id: str) -> str | None:
        return await self._r.get(f"worker:heartbeat:{worker_id}")

    async def increment_metric(self, key: str, amount: int = 1) -> None:
        """Increment a simple counter metric."""
        await self._r.incrby(key, amount)

    async def get_ingestion_rate(self) -> int:
        """Events ingested in the last second (approximate)."""
        val = await self._r.get("metrics:ingestion_rate")
        return int(val) if val else 0

    async def record_ingestion(self, count: int = 1) -> None:
        """Sliding window counter for ingestion rate display."""
        key = "metrics:ingestion_rate"
        async with self._r.pipeline() as pipe:
            pipe.incrby(key, count)
            pipe.expire(key, 2)
            await pipe.execute()

    async def get_cache_stats(self) -> dict:
        """Return hit/miss counters for dashboard."""
        hits = await self._r.get("metrics:cache_hits") or "0"
        misses = await self._r.get("metrics:cache_misses") or "0"
        return {"hits": int(hits), "misses": int(misses)}

    async def record_cache_hit(self) -> None:
        async with self._r.pipeline() as pipe:
            pipe.incr("metrics:cache_hits")
            pipe.expire("metrics:cache_hits", 3600)
            await pipe.execute()

    async def record_cache_miss(self) -> None:
        async with self._r.pipeline() as pipe:
            pipe.incr("metrics:cache_misses")
            pipe.expire("metrics:cache_misses", 3600)
            await pipe.execute()

    async def incr_leaderboard_rebuild_count(self) -> None:
        await self._r.incr("metrics:leaderboard_rebuilds")
