from __future__ import annotations

import uuid
from datetime import datetime
from enum import StrEnum
from typing import Any

from pydantic import BaseModel, Field, field_validator, model_validator


class EventType(StrEnum):
    PAYMENT_INITIATED = "PAYMENT_INITIATED"
    PAYMENT_CONFIRMED = "PAYMENT_CONFIRMED"
    PAYMENT_FAILED = "PAYMENT_FAILED"


class RollupGranularity(StrEnum):
    MINUTE = "minute"
    HOUR = "hour"
    DAY = "day"


class LeaderboardPeriod(StrEnum):
    DAILY = "daily"
    WEEKLY = "weekly"
    MONTHLY = "monthly"


# ─── Ingestion ────────────────────────────────────────────────────────────────


class EventIngest(BaseModel):
    event_type: EventType
    merchant_id: uuid.UUID
    amount_cents: int = Field(..., ge=0, description="Amount in minor currency units (cents)")
    currency: str = Field(default="EUR", min_length=3, max_length=3)
    idempotency_key: str = Field(..., min_length=1, max_length=255)
    client_timestamp: datetime | None = Field(
        default=None,
        description="Client-side event time. Used for late-arrival bucketing.",
    )
    metadata: dict[str, Any] | None = Field(default=None)

    @field_validator("currency")
    @classmethod
    def currency_uppercase(cls, v: str) -> str:
        return v.upper()


class BatchEventIngest(BaseModel):
    events: list[EventIngest] = Field(..., min_length=1, max_length=500)


class EventIngestResponse(BaseModel):
    event_id: uuid.UUID
    idempotency_key: str
    occurred_at: datetime
    duplicate: bool = False
    """True if this idempotency_key was already recorded."""


class BatchIngestResponse(BaseModel):
    accepted: int
    duplicates: int
    events: list[EventIngestResponse]


# ─── Metrics ──────────────────────────────────────────────────────────────────


class BucketDataPoint(BaseModel):
    bucket: datetime
    volume_cents: int
    count: int


class MetricsResponse(BaseModel):
    merchant_id: uuid.UUID
    start: datetime
    end: datetime
    granularity: RollupGranularity
    total_volume_cents: int
    total_count: int
    success_rate: float | None = None
    data: list[BucketDataPoint]
    cache_hit: bool = False
    query_latency_ms: float | None = None


class SparklineResponse(BaseModel):
    merchant_id: uuid.UUID
    window_minutes: int
    data: list[BucketDataPoint]
    cache_hit: bool = False


# ─── Leaderboard ──────────────────────────────────────────────────────────────


class LeaderboardEntry(BaseModel):
    rank: int
    merchant_id: uuid.UUID
    merchant_name: str | None = None
    volume_cents: int
    transaction_count: int | None = None


class LeaderboardResponse(BaseModel):
    period: str
    period_start: datetime
    period_end: datetime
    entries: list[LeaderboardEntry]
    source: str = Field(description="'redis' | 'rollup_day' | 'snapshot'")
    generated_at: datetime


class LeaderboardHistoryResponse(BaseModel):
    period: LeaderboardPeriod
    snapshots: list[LeaderboardResponse]


# ─── Health ───────────────────────────────────────────────────────────────────


class ComponentHealth(BaseModel):
    status: str  # ok | degraded | down
    detail: str | None = None
    latency_ms: float | None = None


class HealthResponse(BaseModel):
    status: str  # ok | degraded | down
    version: str
    components: dict[str, ComponentHealth]
    rollup_lag_seconds: float | None = None
    consumer_offset: dict[str, Any] | None = None
    timestamp: datetime
