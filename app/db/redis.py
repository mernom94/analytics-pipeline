"""
app/db/redis.py
───────────────
Redis client lifecycle with explicit configure / get / reset triad.

DESIGN
──────
The module no longer auto-initialises a Redis client on first call.
Callers must call configure_redis() once (during app lifespan startup or
test setup) before any call to get_redis().  This makes the dependency
explicit and fully injectable:

  Production (app/main.py lifespan):
      configure_redis(Redis(connection_pool=...))

  Tests (conftest.py):
      configure_redis(FakeRedis())   # or any Redis-compatible stub

  Teardown:
      await close_redis()
      reset_redis()   # clears the slot for the next test

WHY configure/get/reset INSTEAD OF A GLOBAL SINGLETON
──────────────────────────────────────────────────────
A module-level `_redis: Redis | None = None` that auto-initialises on
first call is a hidden global dependency — it couples every caller to a
specific Redis URL and makes test isolation impossible without monkey-
patching.  The triad pattern makes the dependency surface explicit and
mirrors the session_factory registry pattern already used for the DB.
"""
from __future__ import annotations

from typing import Final

from redis.asyncio import Redis

from app.core.logging import get_logger

logger = get_logger(__name__)

# ── Module-level slot ─────────────────────────────────────────────────────────
_redis: Redis | None = None

_NOT_CONFIGURED: Final = (
    "Redis client has not been configured. "
    "Call configure_redis() during application startup "
    "before any Redis access is attempted."
)


def configure_redis(client: Redis) -> None:
    """
    Register the application-wide Redis client.

    Must be called once during the FastAPI lifespan startup hook (or at
    the top of conftest.py for tests).  Calling it a second time replaces
    the client — tests may do this intentionally; production code must not.
    """
    global _redis
    _redis = client
    logger.info("redis_client_configured")


def get_redis() -> Redis:
    """
    Return the configured Redis client.

    Raises RuntimeError when called before configure_redis().

    Also usable as a FastAPI dependency — FastAPI will call this and inject
    the return value directly (no yield needed for a singleton client).
    """
    if _redis is None:
        raise RuntimeError(_NOT_CONFIGURED)
    return _redis


def reset_redis() -> None:
    """
    Clear the Redis client reference.

    Intended for test teardown only.  Production code must never call this.
    """
    global _redis
    _redis = None


async def close_redis() -> None:
    """
    Close the active Redis client and clear the slot.

    Safe to call even when no client is configured (e.g. if startup failed
    before configure_redis() was reached).
    """
    global _redis
    if _redis is not None:
        await _redis.aclose()
        _redis = None
        logger.info("redis_client_closed")
