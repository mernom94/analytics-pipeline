# Analytics Pipeline

A production-grade event ingestion and analytics backend built around one constraint: **read queries must be O(1) regardless of event volume**. Built with query latency as the primary design constraint — not developer convenience, not write simplicity.

The system implements the performance primitives that analytics infrastructure demands: pre-aggregated rollup tables at three granularities, a crash-safe consumer with durable offset tracking, cross-partition idempotency enforcement, cache-aside query routing, and exactly-once aggregation semantics. These are not optional features — they are load-bearing correctness and performance guarantees without which the system degrades to full table scans under production load.

---

## Why This Exists

Querying raw events appears simple. It becomes untenable the moment volume grows:

- At 10,000 events/minute, `COUNT(*) WHERE merchant_id = X AND created_at > NOW() - INTERVAL '24 hours'` takes **4 seconds**. A pre-aggregated rollup returns the same answer in **2ms**.
- What happens if the rollup worker crashes mid-batch? Do events get aggregated twice on replay? Once? Not at all?
- What if two producers send the same event with the same idempotency key across different day partitions? PostgreSQL does not enforce UNIQUE constraints across partition boundaries.
- What if Redis is flushed and the leaderboard key is gone? Does the next request fail or rebuild?
- What if a client's clock is wrong and events arrive 5 minutes late? Do they land in the correct historical bucket?

Every one of these scenarios produces either incorrect aggregates (double-counted rollups), missing data (dropped late events), or query failures (cold cache with no fallback). This project addresses each failure mode explicitly, with corresponding tests.

---

## Performance & Correctness Goals

| Goal | Mechanism |
|---|---|
| Sub-millisecond metric reads | Pre-aggregated `rollup_minute`, `rollup_hour`, `rollup_day` tables — read path never touches `raw_events` |
| O(log N) leaderboard reads | Redis sorted sets (`ZINCRBY` on write, `ZREVRANGE` on read) |
| No duplicate aggregation on replay | `processed_rollup_events` marker table + `ON CONFLICT DO NOTHING RETURNING` filters already-processed events |
| No double-insert on cross-partition idempotency | Non-partitioned `event_idempotency` table — the only table that can enforce a true global UNIQUE constraint |
| Crash-safe consumer | `consumer_offsets` tracks last committed event; worker replays from this cursor on restart |
| Correct bucketing of late events | Events bucket by `client_timestamp` (not server `occurred_at`) so late arrivals land in the right historical window |
| Cache fallback on Redis flush | Leaderboard rebuilds from `rollup_day` on cache miss; metric cache falls through to rollup tables |
| Graceful tracing degradation | OTLP tracing optional — `get_tracer()` returns a no-op tracer if unconfigured; app runs without an OTEL collector |

---

## Features

### Event Ingestion
- Accepts single events via `POST /events` and batches up to 500 via `POST /events/batch`
- Server-assigns `occurred_at` — client-supplied timestamps are accepted as `client_timestamp` for bucketing but never trusted for ordering
- Returns `202 Accepted` for new events, `200 OK` for duplicates — callers can distinguish the two cases without inspecting the body
- Validates `merchant_id` existence before insert: single query for single-event path, one `WHERE id = ANY(:ids)` query for the batch path regardless of batch size

### Cross-Partition Idempotency
- PostgreSQL does not enforce UNIQUE constraints across partitioned tables — a naive `ON CONFLICT DO NOTHING` on `raw_events` would silently allow cross-partition duplicates
- Two-phase write enforces global uniqueness:
  1. **Phase 1** — `INSERT INTO event_idempotency ON CONFLICT DO NOTHING RETURNING event_id`. If the row was newly inserted, the key is unclaimed — proceed. If nothing returned, the key already exists — fetch and return the original.
  2. **Phase 2** — `INSERT INTO raw_events` with the same `event_id`. Only reached for newly claimed keys.
- This pattern is safe across partition boundaries because `event_idempotency` is a non-partitioned table with a real primary key.

### Exactly-Once Rollup Aggregation
- The original additive `ON CONFLICT DO UPDATE` pattern is replay-unsafe: replaying the same batch increments counters again, violating exactly-once guarantees
- A `processed_rollup_events` marker table records which events each consumer has already aggregated. On replay, events with existing markers are skipped — the aggregation and rollup upsert are never re-executed
- Worker transaction sequence per batch:
  1. Fetch events after last committed offset
  2. `INSERT INTO processed_rollup_events ON CONFLICT DO NOTHING RETURNING event_id` — returns only newly processed IDs
  3. Aggregate only the new events in Python (`O(N)` dict accumulation)
  4. Upsert aggregated deltas into `rollup_minute`, `rollup_hour`, `rollup_day`
  5. Advance `consumer_offsets` — all in a single atomic transaction

### Pre-Aggregated Rollup Tables
- Three granularities maintained simultaneously: per-minute, per-hour, per-day
- Each rollup row holds `(merchant_id, bucket, event_type)` as a composite key, with `count`, `amount_sum_cents`, `amount_min_cents`, `amount_max_cents`
- Read path queries rollup tables exclusively — `raw_events` is never touched for analytics queries
- Rollup upserts are additive deltas: safe under concurrent workers because the marker table prevents the same event from contributing to a delta more than once

### Query Routing by Range
- Range ≤ 3h → `rollup_minute` (Redis TTL: 5s)
- Range ≤ 30d → `rollup_hour` (Redis TTL: 60s)
- Range > 30d → `rollup_day` (Redis TTL: 300s)
- Routing is automatic: callers supply `start` and `end`; the query service selects the granularity and cache TTL without exposing this to the caller

### Cache-Aside with Automatic Fallback
- Every metric query checks Redis before touching PostgreSQL
- On hit: return immediately, increment hit counter
- On miss: query rollup table, populate cache, return result — the write-through happens before the response is returned
- Leaderboard: Redis sorted set is the fast path (`ZREVRANGE O(log N + K)`). If the key is absent (Redis flush, first request), the query service rebuilds from `rollup_day` and repopulates Redis. The endpoint is never a cache-miss dead-end.

### Crash-Safe Consumer with Offset Tracking
- `consumer_offsets` table stores `(consumer_id, last_event_id, last_event_at)` — updated atomically with each batch commit
- On crash or restart, the worker reads this cursor and replays from that position. No events are lost, and the `processed_rollup_events` marker table prevents double-aggregation on replay.
- Multiple worker instances are supported: each has an independent `consumer_id` and offset. Workers do not share state beyond the PostgreSQL row cursor.

### Late Event Handling
- Events carry an optional `client_timestamp` representing when the event occurred on the client
- Rollup bucketing always uses `client_timestamp` when present, falling back to server-assigned `occurred_at`
- A 5-minute-late event lands in the correct historical minute/hour/day bucket — not the bucket at ingestion time
- Late events beyond `worker_late_event_threshold_s` are logged with structured `lag_seconds` for observability

### Redis Leaderboard
- `PAYMENT_CONFIRMED` events increment a Redis sorted set keyed by `leaderboard:volume:{period}` via `ZINCRBY`
- Reads use `ZREVRANGE` which is `O(log N + K)` — performance does not degrade with merchant count
- Leaderboard increment is best-effort: failures are logged and the DB rollup remains authoritative. A cold leaderboard key is rebuilt from `rollup_day` on the next request.
- Historical leaderboard snapshots are stored in `leaderboard_snapshots` via nightly compaction and queryable by period

### Postgres LISTEN/NOTIFY Worker Wake-Up
- The API sends a `pg_notify('new_events', event_id)` after each successful insert — a best-effort signal to reduce worker poll latency
- The worker maintains a dedicated asyncpg connection pool (`min_size=1, max_size=2`) for LISTEN/NOTIFY, separate from the SQLAlchemy session pool
- NOTIFY is strictly a latency optimisation. The polling loop is the correctness path: a missed notification is handled by the next poll cycle without any data loss.
- Pool-acquired connections are properly released on close. The original raw `asyncpg.connect()` approach leaked connections on GC and had no keepalive semantics.

### Daily Partition Management
- `raw_events` is partitioned by `occurred_at` (daily) — essential for keeping partition pruning effective at high volume
- `event_idempotency` is intentionally non-partitioned — it must enforce a global UNIQUE constraint that partitioning would break
- `scripts/create_partitions.py` pre-creates partitions for the next N days. Without pre-creation, PostgreSQL will reject inserts into a missing partition rather than auto-create one.
- `scripts/nightly_compaction.py` collapses per-minute rollups into day-level snapshots and writes leaderboard snapshots for historical comparison

### Observability
- Structured JSON logging via `structlog` — every log line is machine-parseable
- Request ID injected via pure-ASGI `RequestContextMiddleware` (not `BaseHTTPMiddleware`) — avoids the streaming task leak that causes `RuntimeError: Task was destroyed` in test environments
- Prometheus metrics at `/metrics`:
  - `events_ingested_total` — raw events accepted, by `event_type`
  - `duplicate_events_total` — idempotency-key collisions, by `event_type`
  - `rollup_batches_total` — worker batch cycles completed, by `worker_id`
  - `rollup_lag_seconds` — histogram of seconds between last event timestamp and wall clock at batch end
- OTLP tracing via `opentelemetry-sdk` + `opentelemetry-exporter-otlp-proto-grpc`. Tracing is optional: if `OTLP_ENDPOINT` is empty or the packages are absent, `get_tracer()` returns a no-op tracer and the app runs without change.

---

## System Architecture

### High-Level Overview

```
┌─────────────────────────────────────────────────────────────────────┐
│                          Client / Producer                          │
└───────────────────────────────┬─────────────────────────────────────┘
                                │ POST /events   POST /events/batch
                                ▼
┌─────────────────────────────────────────────────────────────────────┐
│                        FastAPI  (main.py)                           │
│   RequestContextMiddleware → CORS                                   │
│   /events   /metrics   /leaderboard   /health   /metrics (Prometheus│
└───────┬──────────────────────────────────────┬───────────────────────┘
        │                                      │
        ▼                                      ▼
┌──────────────────┐                  ┌─────────────────────┐
│  Ingestion       │                  │  Query Service      │
│  Service         │                  │                     │
│                  │                  │  Range routing:     │
│  Two-phase       │                  │  ≤3h  → rollup_min  │
│  idempotency     │                  │  ≤30d → rollup_hour │
│  write           │                  │  >30d → rollup_day  │
└──────┬───────────┘                  └────────┬────────────┘
       │  INSERT raw_events                    │  Cache-aside
       │  INSERT event_idempotency             │
       │  pg_notify (best-effort)         ┌────▼──────────────┐
       ▼                                  │  Redis            │
┌─────────────────────────────────────────│                   │
│  PostgreSQL                             │  metric cache     │
│  raw_events      (partitioned by day)   │  leaderboard sets │
│  event_idempotency (non-partitioned)    └───────────────────┘
│  rollup_minute / rollup_hour / rollup_day
│  consumer_offsets
│  processed_rollup_events
│  leaderboard_snapshots
└───────────────────────┬──────────────────────────────────────┘
                        │ poll + LISTEN/NOTIFY
                        ▼
┌─────────────────────────────────────────────────────────────────────┐
│                        Rollup Worker                                │
│                       (separate process)                            │
│                                                                     │
│  1. Get consumer offset                                             │
│  2. Fetch events after offset                                       │
│  3. Insert processed markers (ON CONFLICT DO NOTHING RETURNING)     │
│  4. Aggregate new events in Python (O(N))                           │
│  5. Upsert rollup deltas + advance offset — single transaction      │
│  6. Update Redis leaderboard (best-effort, outside transaction)     │
└─────────────────────────────────────────────────────────────────────┘
```

### Write Path (POST /events)

```
Client
  │
  │  POST /events  {event_type, merchant_id, amount_cents, currency, idempotency_key, client_timestamp?}
  ▼
RequestContextMiddleware  (inject request_id, bind structlog context)
  │
  ▼
IngestionService.ingest_event()
  │
  ├─ Validate merchant_id exists
  │    → 404 if missing
  │
  ├─ Server-assign occurred_at = datetime.now(UTC)
  │    (client_timestamp accepted separately for bucketing, never for ordering)
  │
  ├─ Phase 1: INSERT INTO event_idempotency ON CONFLICT DO NOTHING RETURNING event_id
  │    → row returned: key is new, proceed
  │    → no row returned: duplicate, fetch existing raw_event → return 200 OK
  │
  ├─ Phase 2: INSERT INTO raw_events (id from Phase 1)
  │
  ├─ pg_notify('new_events', event_id)  [best-effort — polling is the correctness path]
  │
  ├─ Redis INCR ingestion rate counter
  │
  └─ return 202 Accepted
```

### Rollup Worker Lifecycle

```
Each tick():
  1. _process_batch()
     │
     ├─ SELECT consumer_offsets WHERE consumer_id = ?
     │    → last_event_id (None on first run)
     │
     ├─ SELECT raw_events WHERE created_at > last_event_at
     │    ORDER BY created_at ASC LIMIT 1000
     │    → if empty: wait for notify_event or poll_interval_ms timeout
     │
     ├─ INSERT INTO processed_rollup_events (consumer_id, event_id)
     │    SELECT unnest(:event_ids) ON CONFLICT DO NOTHING RETURNING event_id
     │    → returns only newly inserted IDs (replay filters already-processed)
     │
     ├─ _aggregate_batch(new_events)
     │    → O(N) Python dict accumulation
     │    → (merchant_id, minute_bucket, event_type) → _BucketAccum
     │    → same for hour and day granularities
     │
     ├─ batch_upsert_minute / batch_upsert_hour / batch_upsert_day
     │    INSERT ... ON CONFLICT DO UPDATE SET count = count + EXCLUDED.count, ...
     │
     ├─ upsert_consumer_offset (last_event_id, last_event_at)
     │
     └─ COMMIT  ← markers + rollup upserts + offset all atomic
     
  2. _update_leaderboard(new_events)  [outside DB transaction — best-effort]
     └─ PAYMENT_CONFIRMED events: ZINCRBY leaderboard:volume:{period} amount merchant_id

  3. set_worker_heartbeat() / _log_late_events() / emit Prometheus metrics
```

### Read Path (GET /metrics/{merchant_id})

```
Client
  │
  │  GET /metrics/{merchant_id}?start=...&end=...
  ▼
QueryService.get_merchant_metrics()
  │
  ├─ _select_granularity(start, end)
  │    range ≤ 3h   → MINUTE, TTL 5s
  │    range ≤ 30d  → HOUR,   TTL 60s
  │    range > 30d  → DAY,    TTL 300s
  │
  ├─ Redis GET metric:{merchant_id}:{start}:{end}:{granularity}
  │    hit  → return immediately (record hit counter)
  │    miss → continue
  │
  ├─ SELECT rollup_{granularity} WHERE merchant_id = ? AND bucket BETWEEN ? AND ?
  │
  ├─ SELECT success_rate from rollup (PAYMENT_CONFIRMED / total)
  │
  ├─ Redis SET metric:{...} = result  (TTL per granularity)
  │
  └─ return MetricsResponse {total_volume_cents, total_count, success_rate, data[], cache_hit}
```

### Leaderboard Read Path

```
QueryService.get_leaderboard(period, top_n)
  │
  ├─ Redis EXISTS leaderboard:volume:{period}
  │    hit  → ZREVRANGE leaderboard:volume:{period} 0 (top_n - 1) WITHSCORES
  │            → O(log N + K) — stays fast regardless of merchant count
  │    miss → leaderboard_rebuild_from_db()
  │              SELECT rollup_day GROUP BY merchant_id
  │              SUM(amount_sum_cents) ORDER BY sum DESC LIMIT top_n
  │              → Redis ZADD leaderboard:volume:{period} {merchant_id: score, ...}
  │
  └─ return LeaderboardResponse {entries[], period, source: "redis" | "rollup_day"}
```

### Event Bucketing (Late Events)

```
Rollup worker receives event:

  client_timestamp present?
    YES → bucket_ts = client_timestamp
           event arriving 5 minutes late lands in the minute bucket
           corresponding to when it actually happened on the client
    NO  → bucket_ts = occurred_at (server-assigned at ingestion)

  minute_bucket = bucket_ts.replace(second=0, microsecond=0)
  hour_bucket   = bucket_ts.replace(minute=0, second=0, microsecond=0)
  day_bucket    = bucket_ts.replace(hour=0, minute=0, second=0, microsecond=0)
```

---

## Database Design

### Tables

#### `raw_events`
Append-only event log, partitioned by `occurred_at` (daily). Never updated, never deleted — it is the source of truth and replay buffer. All analytics reads bypass this table entirely.

```sql
CREATE TABLE raw_events (
    id               UUID          NOT NULL,
    event_type       TEXT          NOT NULL,
    merchant_id      UUID          NOT NULL,
    amount_cents     INTEGER       NOT NULL,
    currency         VARCHAR(3)    NOT NULL DEFAULT 'EUR',
    idempotency_key  TEXT          NOT NULL UNIQUE,
    occurred_at      TIMESTAMPTZ   NOT NULL,          -- server-assigned
    client_timestamp TIMESTAMPTZ,                     -- client-reported (optional)
    metadata         JSONB,
    created_at       TIMESTAMPTZ   NOT NULL DEFAULT now()
) PARTITION BY RANGE (occurred_at);

-- Required indexes on each partition:
CREATE INDEX ix_raw_events_merchant_occurred ON raw_events (merchant_id, occurred_at);
CREATE INDEX ix_raw_events_occurred_at       ON raw_events (occurred_at);
CREATE INDEX ix_raw_events_created_at        ON raw_events (created_at);
```

#### `event_idempotency`
Global deduplication guard. Non-partitioned. PostgreSQL cannot enforce UNIQUE constraints across partition boundaries on `raw_events`, so this table carries the authoritative uniqueness guarantee.

```sql
CREATE TABLE event_idempotency (
    idempotency_key TEXT PRIMARY KEY,
    event_id        UUID          NOT NULL,
    created_at      TIMESTAMPTZ   NOT NULL DEFAULT now()
);
CREATE INDEX ix_event_idempotency_event_id ON event_idempotency (event_id);
```

Insert flow: `INSERT INTO event_idempotency ON CONFLICT DO NOTHING RETURNING event_id`. A returned row means the key is newly claimed; no row means it existed — look up the existing `event_id` for the response.

#### `rollup_minute` / `rollup_hour` / `rollup_day`
Pre-aggregated counts and sums per `(merchant_id, bucket, event_type)`. Never queried for writes from the read path — reads go here, `raw_events` does not participate.

```sql
CREATE TABLE rollup_minute (
    merchant_id      UUID       NOT NULL,
    bucket           TIMESTAMPTZ NOT NULL,
    event_type       TEXT       NOT NULL,
    count            BIGINT     NOT NULL DEFAULT 0,
    amount_sum_cents BIGINT     NOT NULL DEFAULT 0,
    amount_min_cents INTEGER,
    amount_max_cents INTEGER,
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (merchant_id, bucket, event_type)
);
-- rollup_hour and rollup_day are identical in structure
```

#### `consumer_offsets`
Crash-recovery cursor. One row per worker. Updated atomically with each batch commit.

```sql
CREATE TABLE consumer_offsets (
    consumer_id   TEXT        PRIMARY KEY,
    last_event_id UUID,
    last_event_at TIMESTAMPTZ,
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);
```

#### `processed_rollup_events`
Exactly-once aggregation guard. Records which `(consumer_id, event_id)` pairs have been aggregated. On replay, `ON CONFLICT DO NOTHING RETURNING event_id` filters already-processed events so rollup tables are not incremented twice.

```sql
CREATE TABLE processed_rollup_events (
    consumer_id TEXT NOT NULL,
    event_id    UUID NOT NULL,
    PRIMARY KEY (consumer_id, event_id)
);
```

#### `leaderboard_snapshots`
Point-in-time leaderboard records written by `nightly_compaction.py`. Enables historical leaderboard comparison without replaying the full event log.

```sql
CREATE TABLE leaderboard_snapshots (
    id              UUID        PRIMARY KEY,
    period          TEXT        NOT NULL,   -- 'daily' | 'weekly' | 'monthly'
    period_start    TIMESTAMPTZ NOT NULL,
    merchant_id     UUID        NOT NULL,
    rank            INTEGER     NOT NULL,
    amount_sum_cents BIGINT     NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (period, period_start, merchant_id)
);
```

---

## API Reference

| Method | Path | Description |
|---|---|---|
| `POST` | `/events` | Ingest a single event. `202` new, `200` duplicate. |
| `POST` | `/events/batch` | Ingest up to 500 events. Per-event status in response. |
| `GET` | `/metrics/{merchant_id}` | Volume, count, success rate. Auto-routes to correct rollup granularity. |
| `GET` | `/metrics/{merchant_id}/sparkline` | Per-minute live feed for the last N minutes. |
| `GET` | `/leaderboard` | Top-N merchants by volume. Redis fast path with DB fallback. |
| `GET` | `/leaderboard/history` | Historical snapshots from `leaderboard_snapshots`. |
| `GET` | `/health` | Rollup lag, consumer offset age, Redis connectivity. |
| `GET` | `/health/ready` | Kubernetes readiness probe. |
| `GET` | `/health/live` | Kubernetes liveness probe. |
| `GET` | `/metrics` | Prometheus scrape endpoint. |

### Event Ingestion Schema

```json
POST /events
{
  "event_type":       "PAYMENT_CONFIRMED",
  "merchant_id":      "a0000000-0000-0000-0000-000000000001",
  "amount_cents":     4999,
  "currency":         "EUR",
  "idempotency_key":  "order-7829-attempt-1",
  "client_timestamp": "2026-05-19T10:32:00Z"   // optional
}

// 202 Accepted (new event)
{
  "event_id":        "c1234567-...",
  "idempotency_key": "order-7829-attempt-1",
  "occurred_at":     "2026-05-19T10:32:01.123Z",
  "duplicate":       false
}

// 200 OK (duplicate — same idempotency_key)
{
  "event_id":        "c1234567-...",   // original event_id
  "idempotency_key": "order-7829-attempt-1",
  "occurred_at":     "2026-05-19T10:32:01.123Z",
  "duplicate":       true
}
```

### Metrics Query Schema

```
GET /metrics/{merchant_id}?start=2026-05-19T00:00:00Z&end=2026-05-19T03:00:00Z

// Range ≤ 3h → rollup_minute, TTL 5s
{
  "merchant_id":        "a0000000-...",
  "start":              "2026-05-19T00:00:00Z",
  "end":                "2026-05-19T03:00:00Z",
  "granularity":        "minute",
  "total_volume_cents": 1204500,
  "total_count":        4820,
  "success_rate":       0.9873,
  "data": [
    { "bucket": "2026-05-19T00:00:00Z", "volume_cents": 38200, "count": 143 },
    ...
  ],
  "cache_hit":          false,
  "query_latency_ms":   2.1
}
```

---

## Tech Stack

| Component | Choice | Reasoning |
|---|---|---|
| **API framework** | FastAPI | Native async, automatic OpenAPI, Pydantic integration |
| **Database** | PostgreSQL 16 | Range partitioning for `raw_events`, `ON CONFLICT DO NOTHING RETURNING` for idempotency, `TIMESTAMPTZ` throughout |
| **ORM** | SQLAlchemy 2.x async | True async I/O with asyncpg, `mapped_column` type annotations, async context manager sessions |
| **Cache / leaderboard** | Redis 7 | Sorted sets for O(log N) leaderboard, metric query cache with per-granularity TTLs |
| **Worker protocol** | PostgreSQL LISTEN/NOTIFY | Low-latency wake-up without a separate message broker; polling is the correctness path |
| **Logging** | structlog | JSON output, context propagation, machine-parseable in production |
| **Metrics** | prometheus-client | Counter and histogram export at `/metrics`; zero additional infrastructure |
| **Tracing** | OpenTelemetry OTLP | Optional GRPC export to any OTEL collector; no-op tracer if unconfigured |
| **Migrations** | Alembic | Autogenerate from ORM metadata; same `Base` used by application and migration env |
| **Settings** | pydantic-settings | Env-var validation at startup; misconfiguration is a boot failure, not a runtime surprise |

**Why partition `raw_events` by day?**

At sustained ingestion rates, an unpartitioned `raw_events` table grows unboundedly and degrades index scans, vacuuming, and partition pruning. Partitioning by day keeps each child table to a bounded size, allows dropping old partitions without a full-table DELETE, and enables PostgreSQL to prune irrelevant partitions from query plans automatically.

**Why a separate `event_idempotency` table?**

PostgreSQL UNIQUE constraints on partitioned tables are partition-local — they only prevent duplicates within a single partition. An event with `occurred_at` spanning midnight would bypass a UNIQUE constraint on `raw_events` entirely. The non-partitioned `event_idempotency` table carries the only constraint that is globally enforced.

**Why exactly-once aggregation instead of idempotent upserts?**

Idempotent `ON CONFLICT DO UPDATE SET count = count + EXCLUDED.count` is only safe on the first execution. On replay (offset reset), the same event contributes its delta again — counts double, sums drift. The `processed_rollup_events` marker table makes replay provably correct: if a marker exists, the event is skipped entirely; if it does not, the marker is inserted atomically with the rollup delta.

**Why is the rollup worker a separate process?**

Embedding the worker inside the API process couples two very different resource profiles: bursty HTTP handling and steady-state database batch processing. Separate processes allow independent scaling, prevent worker GC pressure from affecting API latency, and ensure that a worker crash does not take down the API. The API process remains operational even if the rollup worker is restarted or paused.

---

## Project Structure

```
analytics-pipeline/
├── app/
│   ├── api/routes/
│   │   ├── events.py          # POST /events, POST /events/batch
│   │   ├── metrics.py         # GET /metrics/{id}, GET /metrics/{id}/sparkline
│   │   ├── leaderboard.py     # GET /leaderboard, GET /leaderboard/history
│   │   ├── health.py          # GET /health, /health/ready, /health/live
│   │   └── prometheus.py      # GET /metrics (Prometheus scrape)
│   ├── core/
│   │   ├── config.py          # pydantic-settings; all env vars validated at startup
│   │   ├── exceptions.py      # Domain exceptions (MerchantNotFoundError, DuplicateEventError, ...)
│   │   └── logging.py         # structlog JSON configuration
│   ├── db/
│   │   ├── engine.py          # build_engine, build_session_factory, get_db dependency
│   │   ├── session_factory.py # configure/get/reset session factory (injectable for tests)
│   │   └── redis.py           # configure_redis, get_redis, close_redis
│   ├── models/
│   │   ├── orm.py             # SQLAlchemy table definitions (RawEvent, Rollup*, ConsumerOffset, ...)
│   │   └── schemas.py         # Pydantic request/response schemas + EventType enum
│   ├── repositories/
│   │   ├── event_repository.py   # raw_events writes, idempotency two-phase insert, NOTIFY
│   │   ├── rollup_repository.py  # rollup upserts, consumer offset management, leaderboard DB rebuild
│   │   └── redis_repository.py   # metric cache, leaderboard sorted set, ingestion rate, heartbeat
│   ├── services/
│   │   ├── ingestion_service.py  # Validation, two-phase idempotency, Prometheus counters, OTLP spans
│   │   └── query_service.py      # Range routing, cache-aside, leaderboard with DB fallback
│   ├── workers/
│   │   └── rollup_worker.py      # RollupWorker: offset tracking, marker filter, aggregation, upsert
│   ├── observability.py          # Prometheus metric singletons, OTLP tracer init/shutdown, no-op fallback
│   └── main.py                   # FastAPI app + lifespan (engine, Redis, migrations, demo seed)
├── migrations/
│   └── versions/
│       ├── 0001_initial_schema.py
│       ├── 0002_idempotency_table.py
│       └── 0003_processed_rollup_events.py
├── scripts/
│   ├── create_partitions.py   # Pre-create daily partitions for next N days
│   ├── init_partitions.sql    # Bootstrap partition DDL (run once by docker-compose)
│   ├── nightly_compaction.py  # Compact minute rollups → day snapshots, write leaderboard_snapshots
│   └── load_simulator.py      # Traffic generator with fault injection support
├── tests/
│   ├── unit/
│   │   ├── test_rollup_logic.py  # _aggregate_batch, bucket helpers, _BucketAccum — no I/O
│   │   └── test_schemas.py       # Pydantic schema validation edge cases
│   └── integration/
│       ├── conftest.py           # Async engine, test session factory, Redis mock
│       ├── test_ingestion.py     # Full HTTP → DB: single event, batch, duplicates, missing merchant
│       └── test_rollup_worker.py # Worker: offset tracking, exactly-once replay, late events
├── dashboard/
│   └── index.html             # Live dashboard (nginx-served; connects to API)
├── worker_rollup.py           # Rollup worker entrypoint (standalone process)
├── docker-compose.yml
├── Dockerfile
├── alembic.ini
├── requirements.txt
└── .env.example
```

---

## Configuration

All configuration is via environment variables (or `.env` file). Validated at startup via pydantic-settings — a missing or malformed required variable raises at boot, not at first use.

```bash
# Database
DATABASE_URL=postgresql+asyncpg://analytics:analytics@localhost:5432/analytics
DB_POOL_SIZE=20
DB_MAX_OVERFLOW=10
DB_POOL_TIMEOUT=30

# Redis
REDIS_URL=redis://localhost:6379/0
REDIS_POOL_SIZE=20

# Worker
WORKER_ID=rollup-worker-1
WORKER_BATCH_SIZE=1000          # events per batch cycle
WORKER_POLL_INTERVAL_MS=100     # fallback poll interval when stream is empty
WORKER_LAG_THRESHOLD_S=30       # lag above this triggers log warning
WORKER_LATE_EVENT_THRESHOLD_S=60  # client_timestamp delta above this is logged

# Cache TTLs (seconds)
CACHE_TTL_LIVE=5                # ≤3h range → rollup_minute
CACHE_TTL_HISTORICAL=60         # ≤30d range → rollup_hour
CACHE_TTL_ARCHIVE=300           # >30d range → rollup_day

# Leaderboard
LEADERBOARD_TOP_N=10
LEADERBOARD_REBUILD_TTL=3600    # Redis key TTL for rebuilt leaderboards

# App
APP_NAME=Analytics Pipeline
APP_VERSION=1.0.0
DEBUG=false
LOG_LEVEL=INFO
SKIP_MIGRATIONS=false           # set true when migrations run as a separate init-container

# Observability
OTLP_ENDPOINT=                  # leave empty to disable tracing (e.g. http://otel-collector:4317)
```

---

## Quick Start

```bash
# ── Start infrastructure
docker-compose up -d

# ── Verify connectivity
curl http://localhost:8000/health

# ── Ingest a single event
curl -X POST http://localhost:8000/events \
  -H "Content-Type: application/json" \
  -d '{"event_type":"PAYMENT_CONFIRMED","merchant_id":"a0000000-0000-0000-0000-000000000001","amount_cents":4999,"currency":"EUR","idempotency_key":"test-001"}'

# ── Query metrics (auto-routes to rollup_minute for ≤3h range)
curl "http://localhost:8000/metrics/a0000000-0000-0000-0000-000000000001?start=2026-05-19T00:00:00Z&end=2026-05-19T03:00:00Z"

# ── Live sparkline (last 60 minutes, per-minute buckets)
curl "http://localhost:8000/metrics/a0000000-0000-0000-0000-000000000001/sparkline?window_minutes=60"

# ── Leaderboard (current month, Redis fast path)
curl http://localhost:8000/leaderboard

# ── Run the load simulator (50 rps for 60 seconds)
python scripts/load_simulator.py --rps 50 --duration 60

# ── Open the live dashboard
open http://localhost:3000
```

## Example Commands

```bash
# ── Infrastructure
docker-compose up -d
docker-compose down -v   # destroy volumes (full reset)

# ── Database migrations
alembic upgrade head
alembic downgrade -1
alembic history --verbose

# ── API
uvicorn app.main:app --host 0.0.0.0 --port 8000
LOG_LEVEL=DEBUG uvicorn app.main:app --reload   # development

# ── Rollup worker (separate process)
python worker_rollup.py
WORKER_ID=rollup-worker-2 python worker_rollup.py   # second instance

# ── Partition management
python scripts/create_partitions.py --days 7    # pre-create next 7 days
python scripts/nightly_compaction.py            # compact + snapshot

# ── Load simulation
python scripts/load_simulator.py --rps 100 --duration 120
python scripts/load_simulator.py --fault-injection   # inject duplicate and late events

# ── Tests
pytest tests/unit/ -v                             # no infrastructure required
pytest tests/integration/ -v -m integration       # requires postgres + redis
pytest --cov=app --cov-report=html -q
pytest tests/unit/test_rollup_logic.py -v -k "test_late_event"

# ── Linting / formatting
ruff check app/ tests/
ruff format app/ tests/
mypy app/ --strict

# ── Operational queries
psql $DATABASE_URL -c "SELECT consumer_id, last_event_at, updated_at FROM consumer_offsets;"
psql $DATABASE_URL -c "SELECT date_trunc('day', occurred_at) AS day, COUNT(*) FROM raw_events GROUP BY 1 ORDER BY 1 DESC LIMIT 7;"
psql $DATABASE_URL -c "SELECT tablename FROM pg_tables WHERE tablename LIKE 'raw_events_%' ORDER BY tablename;"

# ── Redis inspection
redis-cli ZREVRANGE "leaderboard:volume:2026-05" 0 9 WITHSCORES
redis-cli DBSIZE
redis-cli INFO memory
```

---

## Failure Modes

| Failure | Detection | Recovery |
|---|---|---|
| Worker crash mid-batch | `consumer_offsets.last_event_at` stale | Worker replays from last committed offset; `processed_rollup_events` prevents double-aggregation |
| Redis flush (leaderboard gone) | `EXISTS leaderboard:volume:{period}` returns 0 | Query service rebuilds from `rollup_day` and repopulates Redis on next request |
| Redis flush (metric cache gone) | Cache miss | Falls through to rollup table query; cache repopulated on response |
| Duplicate event (same idempotency_key) | Phase 1 `INSERT INTO event_idempotency` returns no row | Returns existing `event_id` with `duplicate: true` |
| Cross-partition duplicate | Would bypass `raw_events` UNIQUE constraint | Caught by `event_idempotency` (non-partitioned, globally enforced) |
| Missing daily partition | `raw_events` INSERT fails | `scripts/create_partitions.py` pre-creates partitions; `init_partitions.sql` bootstraps initial set |
| Late event (wrong bucket) | `client_timestamp` delta > threshold logged | Event bucketed by `client_timestamp`, not `occurred_at` — lands in correct historical window |
| pg_notify delivery failure | N/A — NOTIFY is best-effort | Worker falls back to polling every `WORKER_POLL_INTERVAL_MS` milliseconds |
| OTLP collector unavailable | `init_tracing()` logs warning | `get_tracer()` returns no-op tracer; app runs without tracing — no spans, no errors |

---

## Scalability Considerations

### Horizontal Scaling

Multiple API instances are safe today — handlers are stateless, sharing only PostgreSQL and Redis. Multiple rollup worker instances are also safe: each has an independent `consumer_id` and offset cursor, and `processed_rollup_events` deduplicated by `(consumer_id, event_id)` allows workers to process the event stream in parallel without producing duplicate rollup increments.

### Kubernetes Deployment

The separate-process design (API + worker) maps naturally to independent Kubernetes Deployments with independent HPA scaling. The API can scale to N replicas based on request latency; the worker can scale based on `rollup_lag_seconds` as a custom metric from the Prometheus endpoint. A worker crash does not affect API availability.

### Partition Maintenance

Partitions must be pre-created before events arrive — PostgreSQL will reject inserts into a missing partition rather than creating one automatically. `scripts/create_partitions.py` should be scheduled as a daily cron job (or Kubernetes CronJob) that pre-creates the next N days of partitions. Old partitions can be dropped by day with zero table-lock cost, unlike DELETE-based retention.

### Leaderboard at Scale

The Redis sorted set (`ZINCRBY` / `ZREVRANGE`) stays O(log N) regardless of merchant count. The bottleneck at very high merchant counts is the DB rebuild path — a `SELECT rollup_day GROUP BY merchant_id` across all merchants for the period. This can be bounded by maintaining a `leaderboard_snapshots` row per period (written by nightly compaction) so rebuilds query the snapshot table rather than re-aggregating the full rollup.

### Nightly Compaction

`scripts/nightly_compaction.py` collapses per-minute rollup rows into day-level snapshots and writes leaderboard snapshots for historical period comparison. Without compaction, `rollup_minute` grows unboundedly — 1,440 rows per merchant per event type per day. Compaction converts these to a single `rollup_day` row and drops the minute rows outside the retention window.

---

## Tradeoffs

### LISTEN/NOTIFY vs. a Message Broker

The rollup worker uses PostgreSQL LISTEN/NOTIFY for low-latency wake-up. This avoids a separate Kafka or RabbitMQ dependency at the cost of delivery semantics: NOTIFY is best-effort and not durable — a notification sent while no worker is connected is lost. The polling loop is the correctness path; NOTIFY only reduces latency between ingestion and rollup.

For throughput beyond approximately 10,000 events/second, a message broker becomes necessary: pg_notify serialises through a single notification channel and can become a bottleneck. The current architecture's write path (`INSERT raw_events → pg_notify`) could be replaced with a Kafka producer without changing the rollup worker's aggregation logic.

### Additive Deltas vs. Full Recomputation

The worker computes aggregation deltas in Python and upserts them into rollup tables additively (`count = count + EXCLUDED.count`). This is efficient but means the rollup tables cannot be trivially recomputed from first principles without resetting all offsets and clearing `processed_rollup_events`. An alternative — full recomputation from `raw_events` on each batch — would be simpler to reason about but would scale as `O(events in window)` rather than `O(batch_size)`.

### Separate Idempotency Table vs. Partition-Aware Deduplication

A non-partitioned `event_idempotency` table is simpler and correct, but it grows independently of the `raw_events` partitions — old rows accumulate and must be explicitly pruned. The alternative, tracking idempotency keys inside each partition, would require a cross-partition check on ingestion (a multi-partition scan) and would not be expressible as a single UNIQUE constraint. The current design trades storage growth for query simplicity and correctness.

### Server-Assigned Timestamps

`occurred_at` is assigned by the server at ingestion time. Client clocks are accepted via `client_timestamp` for bucketing purposes only. This means `occurred_at` is always monotonically reliable for consumer offset tracking (`created_at` ordering), while `client_timestamp` is used where temporal accuracy of the event matters (rollup bucketing). Trusting `client_timestamp` for offset ordering would produce non-monotonic cursors and break the worker's replay logic.

---

## Engineering Philosophy

**Performance is designed, not measured after the fact.** A system that queries raw events on every dashboard load will degrade linearly with data volume — not because of bugs, but because of the fundamental design. Pre-aggregation on the write path is the only approach that keeps query latency constant as the event log grows. Every architectural decision in this project is made with query latency as the primary constraint, and write complexity as a secondary one.

**Correctness under replay is non-negotiable.** Any aggregation system that is not safe to replay is not safe to operate. A worker crash, an offset reset for debugging, or a deployment that restarts mid-batch must produce the same rollup state as a clean run. The `processed_rollup_events` marker table exists solely to make replay a first-class concern — not an afterthought.

**Durability boundaries must be explicit.** The distinction between what is durable (PostgreSQL: `raw_events`, rollup tables, offsets, markers) and what is best-effort (Redis: cache, leaderboard sorted sets, NOTIFY signals) is not an implementation detail — it is the correctness model. Every component that reads from Redis must have a defined behaviour when Redis is empty or unavailable. Treating Redis as durable state is a silent correctness bug.

**Dependencies must degrade gracefully.** Tracing, metrics, and the NOTIFY wake-up mechanism are all optional. If the OTEL collector is unreachable, the app runs without tracing. If Redis is cold, the leaderboard rebuilds from the database. If pg_notify fails, the worker polls. None of these are error conditions that require operator intervention — they are expected operating modes that the system handles automatically.