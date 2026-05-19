from functools import lru_cache
from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import field_validator


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    # Database
    database_url: str = "postgresql+asyncpg://analytics:analytics@localhost:5432/analytics"
    db_pool_size: int = 20
    db_max_overflow: int = 10
    db_pool_timeout: int = 30

    # Redis
    redis_url: str = "redis://localhost:6379/0"
    redis_pool_size: int = 20

    # Worker
    worker_enabled: bool = False
    worker_id: str = "rollup-worker-1"
    worker_batch_size: int = 1000
    worker_poll_interval_ms: int = 100
    worker_lag_threshold_s: int = 30
    worker_late_event_threshold_s: int = 60

    # Cache TTLs (seconds)
    cache_ttl_live: int = 5        # ≤3h range
    cache_ttl_historical: int = 60  # ≤30d range
    cache_ttl_archive: int = 300    # >30d range

    # Leaderboard
    leaderboard_top_n: int = 10
    leaderboard_rebuild_ttl: int = 3600  # 1h

    # Rollup windows
    minute_open_window_s: int = 300   # buckets open for 5 min after close
    rollup_batch_lag_trigger: int = 100  # switch to batch mode after this many pending

    # App
    app_name: str = "Analytics Pipeline"
    app_version: str = "1.0.0"
    debug: bool = False
    log_level: str = "INFO"
    # Set to true when migrations are run as a separate init-container step
    # (recommended for production Kubernetes deployments).
    skip_migrations: bool = False

    # Observability
    # OTLP gRPC endpoint for OpenTelemetry tracing (e.g. "http://otel-collector:4317").
    # Leave empty to disable tracing.
    otlp_endpoint: str = ""

    @field_validator("DATABASE_URL", mode="before")
    @classmethod
    def fix_database_url(cls, v: str) -> str:
        v = str(v)
        if v.startswith("postgres://"):
            v = v.replace("postgres://", "postgresql+asyncpg://", 1)
        if v.startswith("postgresql://"):
            v = v.replace("postgresql://", "postgresql+asyncpg://", 1)
        return v


@lru_cache
def get_settings() -> Settings:
    return Settings()
