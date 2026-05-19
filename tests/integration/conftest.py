"""
tests/integration/conftest.py
──────────────────────────────
Production-grade async integration test infrastructure.

CHANGES FROM ORIGINAL
─────────────────────
1. Redis is now wired via configure_redis() / reset_redis() — no more
   auto-initialising singleton.  The test_redis fixture calls
   configure_redis() before yielding and reset_redis() on teardown.

2. The client fixture adds a get_redis dependency override so HTTP routes
   use the same test Redis instance.

3. No other structural changes — the engine/session-factory topology
   (session-scoped engine, function-scoped sessions, TRUNCATE isolation)
   is unchanged because it was already correct.

ARCHITECTURE OVERVIEW
─────────────────────

  Session scope                  Function scope
  ─────────────────────────────  ──────────────────────────────────────────────
  engine (ONE, never recreated)  db_session  (fresh per test, same pool)
  session_factory                worker_session (same pool, different handle)
  schema (created once)          client (lifespan-aware, fresh per test)
  ↑                              clean_db (autouse, truncates all tables)
  Owned here; disposed at end    test_redis (configure_redis / reset_redis)
"""
from __future__ import annotations

import os
import uuid
from datetime import datetime, timezone
from typing import AsyncGenerator

import pytest
import pytest_asyncio
from httpx import AsyncClient, ASGITransport
from sqlalchemy import text
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

# ── Env setup — must happen before any app import because get_settings() is
#    decorated with @lru_cache and will freeze the values on first call. ───────
_TEST_DB_URL: str = (
    os.environ.get("TEST_DATABASE_URL")
    or os.environ.get("DATABASE_URL")
    or "postgresql+asyncpg://localhost/analytics"
)
os.environ["DATABASE_URL"] = _TEST_DB_URL
os.environ.setdefault("REDIS_URL", "redis://localhost:6379/1")  # DB 1 for tests
os.environ["WORKER_ENABLED"] = "false"   # No embedded worker in tests
os.environ["SKIP_MIGRATIONS"] = "true"   # Schema managed by conftest
os.environ.setdefault("DEBUG", "true")

# Bust the lru_cache so settings re-read the env vars we just set.
try:
    from app.core.config import get_settings
    get_settings.cache_clear()
except Exception:
    pass

# ── App imports — after env setup ─────────────────────────────────────────────
from app.db.engine import get_db
from app.db.redis import configure_redis, get_redis, reset_redis
from app.db.session_factory import configure_session_factory, reset_session_factory
from app.main import app
from app.models.orm import Base, Merchant


# ═══════════════════════════════════════════════════════════════════════════════
# SESSION-SCOPED FIXTURES  (created once per pytest session)
# ═══════════════════════════════════════════════════════════════════════════════

@pytest_asyncio.fixture(scope="session", loop_scope="session")
async def engine() -> AsyncGenerator[AsyncEngine, None]:
    """
    Single async engine for the entire test session.

    pool_pre_ping=True  — validates connections before checkout; prevents
                          stale connection errors after DB restarts.
    expire_on_commit=False — set on the sessionmaker below.
    NullPool is intentionally NOT used here.  NullPool creates a new raw
    connection for every session.acquire() call, removing the pooling benefit
    and causing excessive connection churn.  A real pool with pool_pre_ping
    is the correct choice.
    """
    test_engine = create_async_engine(
        _TEST_DB_URL,
        pool_pre_ping=True,
        echo=False,
    )

    # Create schema once for the entire test run.
    async with test_engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    yield test_engine

    # Dispose exactly once — after ALL tests have completed.
    # This runs while the event loop is still alive (session scope ensures this).
    await test_engine.dispose()


@pytest_asyncio.fixture(scope="session", loop_scope="session")
async def session_factory(
    engine: AsyncEngine,
) -> async_sessionmaker[AsyncSession]:
    """
    Single async_sessionmaker shared across all tests.

    Also registers it in the central registry so that:
      - get_db() FastAPI dependency returns sessions from this pool
      - RollupWorker injected with this factory uses this pool
      - all sessions see each other's committed writes
    """
    factory = async_sessionmaker(
        engine,
        class_=AsyncSession,
        expire_on_commit=False,
        autoflush=False,
    )
    configure_session_factory(factory)

    yield factory

    # Clear the registry after the session so subsequent pytest sessions
    # (e.g. watch mode) start clean.
    reset_session_factory()


# ═══════════════════════════════════════════════════════════════════════════════
# ISOLATION FIXTURE  (autouse — runs before EVERY test)
# ═══════════════════════════════════════════════════════════════════════════════

# Tables to truncate between tests.  Order matters for FK constraints;
# RESTART IDENTITY CASCADE handles most dependencies, but raw_events must come
# before tables that reference event_ids.
_MUTABLE_TABLES: tuple[str, ...] = (
    "processed_rollup_events",
    "consumer_offsets",
    "event_idempotency",
    "raw_events",             # partitioned; TRUNCATE cascades to child partitions
    "rollup_minute",
    "rollup_hour",
    "rollup_day",
    "leaderboard_snapshots",
    "merchants",
)


@pytest_asyncio.fixture(autouse=True)
async def clean_db(session_factory: async_sessionmaker[AsyncSession]) -> None:
    """
    Truncate all mutable tables before each test.

    RESTART IDENTITY CASCADE:
      - resets serial/sequence counters (deterministic IDs in tests)
      - cascades to dependent tables (handles FK relationships)

    Why BEFORE not AFTER?
      - A failing test leaves data intact for post-mortem inspection.
      - The next test still gets a clean slate.
      - Teardown errors can't prevent cleanup from running.
    """
    tables = ", ".join(_MUTABLE_TABLES)
    async with session_factory() as session:
        await session.execute(
            text(f"TRUNCATE {tables} RESTART IDENTITY CASCADE")
        )
        await session.commit()


# ═══════════════════════════════════════════════════════════════════════════════
# FUNCTION-SCOPED DB FIXTURES
# ═══════════════════════════════════════════════════════════════════════════════

@pytest_asyncio.fixture
async def db_session(
    session_factory: async_sessionmaker[AsyncSession],
) -> AsyncGenerator[AsyncSession, None]:
    """
    Short-lived AsyncSession for the test body.

    Shares the session-scoped pool — so writes committed here are immediately
    visible to the worker (which also uses this pool) and to worker_session.

    Do NOT share this session with the worker.  The worker must open its own
    sessions via the factory to simulate real transaction boundaries.
    """
    async with session_factory() as session:
        yield session
        # Rollback any uncommitted work the test left behind (e.g. adds without
        # explicit commit).  This is a safety net, not the isolation mechanism.
        await session.rollback()


@pytest_asyncio.fixture
async def worker_session(
    session_factory: async_sessionmaker[AsyncSession],
) -> AsyncGenerator[AsyncSession, None]:
    """
    Separate AsyncSession for reading worker output after it commits.

    Tests that call worker._process_batch() need to see the rows it committed.
    Using the same AsyncSession as db_session would return stale cache entries
    from SQLAlchemy's identity map.  A new session from the same pool always
    sees the latest committed state.
    """
    async with session_factory() as session:
        yield session
        await session.rollback()


# ═══════════════════════════════════════════════════════════════════════════════
# REDIS FIXTURE  (configure/reset triad)
# ═══════════════════════════════════════════════════════════════════════════════

@pytest_asyncio.fixture
async def test_redis():
    """
    Configure a real Redis client for the test, then tear it down cleanly.

    Uses REDIS_URL (default: redis://localhost:6379/1 — DB 1 to isolate
    from any dev data in DB 0).

    Calls configure_redis() so that:
      - get_redis() works inside the test body
      - the worker and routes (if using the get_redis dependency) see this client
    Calls reset_redis() on teardown so the next test starts without a stale
    configured client.
    """
    from redis.asyncio import ConnectionPool, Redis as AioRedis

    redis_url = os.environ.get("REDIS_URL", "redis://localhost:6379/1")
    pool = ConnectionPool.from_url(redis_url, max_connections=5, decode_responses=True)
    redis_client = AioRedis(connection_pool=pool)
    configure_redis(redis_client)

    yield redis_client

    # Flush test keys — DB 1 is dedicated to tests so a full flushdb is safe.
    try:
        await redis_client.flushdb()
    except Exception:
        pass
    await redis_client.aclose()
    reset_redis()


# ═══════════════════════════════════════════════════════════════════════════════
# HTTP CLIENT FIXTURE  (lifespan-aware)
# ═══════════════════════════════════════════════════════════════════════════════

@pytest_asyncio.fixture
async def client(
    session_factory: async_sessionmaker[AsyncSession],
    test_redis,
) -> AsyncGenerator[AsyncClient, None]:
    """
    Lifespan-aware AsyncClient backed by the test ASGI app.

    Uses asgi-lifespan's LifespanManager to run startup/shutdown hooks so:
      - background resources (Redis, health check tasks) are initialised
      - teardown is deterministic (no orphan tasks survive past the test)

    The FastAPI dependency overrides ensure HTTP routes use:
      - the test DB session pool (same as db_session and worker_session)
      - the test Redis client (same as test_redis)

    Note: configure_session_factory() was already called by the session_factory
    fixture, and configure_redis() was called by test_redis, so the app's own
    lifespan startup is pre-empted for both.  SKIP_MIGRATIONS=true prevents
    Alembic from running.  WORKER_ENABLED=false is ignored (worker never
    embedded).
    """
    from asgi_lifespan import LifespanManager

    # Override get_db so route handlers use our test pool.
    async def _override_get_db() -> AsyncGenerator[AsyncSession, None]:
        async with session_factory() as session:
            try:
                yield session
                await session.commit()
            except Exception:
                await session.rollback()
                raise

    # Override get_redis so route handlers use our test Redis client.
    def _override_get_redis():
        return test_redis

    app.dependency_overrides[get_db] = _override_get_db
    app.dependency_overrides[get_redis] = _override_get_redis

    async with LifespanManager(app):
        async with AsyncClient(
            transport=ASGITransport(app=app),
            base_url="http://test",
        ) as ac:
            yield ac

    app.dependency_overrides.clear()


# ═══════════════════════════════════════════════════════════════════════════════
# DOMAIN FIXTURES
# ═══════════════════════════════════════════════════════════════════════════════

@pytest_asyncio.fixture
async def merchant(db_session: AsyncSession) -> Merchant:
    """A single merchant committed to the DB — usable across sessions."""
    m = Merchant(id=uuid.uuid4(), name=f"Test Merchant {uuid.uuid4().hex[:6]}")
    db_session.add(m)
    await db_session.commit()
    return m


@pytest.fixture
def valid_event(merchant: Merchant) -> dict:
    return {
        "event_type": "PAYMENT_CONFIRMED",
        "merchant_id": str(merchant.id),
        "amount_cents": 9900,
        "currency": "EUR",
        "idempotency_key": str(uuid.uuid4()),
        "client_timestamp": datetime.now(timezone.utc).isoformat(),
    }
