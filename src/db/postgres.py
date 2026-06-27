"""
asyncpg connection pool + DDL migrations.

All DDL runs idempotently (CREATE TABLE IF NOT EXISTS / CREATE INDEX IF NOT EXISTS).
"""

import asyncpg
from src.config import get_settings

_pool: asyncpg.Pool | None = None


async def get_pool() -> asyncpg.Pool:
    global _pool
    if _pool is None:
        settings = get_settings()
        _pool = await asyncpg.create_pool(
            dsn=settings.asyncpg_dsn,
            min_size=settings.postgres_min_pool,
            max_size=settings.postgres_max_pool,
        )
    return _pool


async def close_pool() -> None:
    global _pool
    if _pool:
        await _pool.close()
        _pool = None


# ------------------------------------------------------------------
# DDL
# ------------------------------------------------------------------

_MIGRATIONS = [
    # 1. Feature views catalog
    """
    CREATE TABLE IF NOT EXISTS feature_views (
        name                VARCHAR(255) PRIMARY KEY,
        entity_column       VARCHAR(255) NOT NULL DEFAULT 'entity_id',
        timestamp_column    VARCHAR(255) NOT NULL DEFAULT 'event_timestamp',
        schema_version      INTEGER NOT NULL DEFAULT 1,
        ttl_seconds         INTEGER,
        tags                JSONB NOT NULL DEFAULT '{}',
        description         TEXT NOT NULL DEFAULT '',
        is_active           BOOLEAN NOT NULL DEFAULT TRUE,
        created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
    """,

    # 2. Schema registry (versioned feature schemas)
    """
    CREATE TABLE IF NOT EXISTS schema_registry (
        id                  SERIAL PRIMARY KEY,
        feature_view        VARCHAR(255) NOT NULL,
        version             INTEGER NOT NULL,
        spec_json           JSONB NOT NULL,
        compatibility_mode  VARCHAR(50) NOT NULL DEFAULT 'BACKWARD',
        is_latest           BOOLEAN NOT NULL DEFAULT TRUE,
        created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        CONSTRAINT uq_schema_view_version UNIQUE (feature_view, version)
    )
    """,

    # 3. Feature values (offline store — append-only time series)
    """
    CREATE TABLE IF NOT EXISTS feature_values (
        id              BIGSERIAL PRIMARY KEY,
        feature_view    VARCHAR(255) NOT NULL,
        entity_id       VARCHAR(255) NOT NULL,
        features        JSONB NOT NULL,
        event_timestamp TIMESTAMPTZ NOT NULL,
        created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
    """,

    # 4. Indexes for fast PIT joins
    """
    CREATE INDEX IF NOT EXISTS idx_fv_view_entity_time
        ON feature_values (feature_view, entity_id, event_timestamp DESC)
    """,

    """
    CREATE INDEX IF NOT EXISTS idx_fv_view_time
        ON feature_values (feature_view, event_timestamp DESC)
    """,

    # 5. Materialization job log
    """
    CREATE TABLE IF NOT EXISTS materialization_jobs (
        job_id                  VARCHAR(36) PRIMARY KEY,
        feature_view            VARCHAR(255) NOT NULL,
        start_time              TIMESTAMPTZ,
        end_time                TIMESTAMPTZ,
        status                  VARCHAR(20) NOT NULL DEFAULT 'pending',
        entities_materialized   INTEGER NOT NULL DEFAULT 0,
        created_at              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        completed_at            TIMESTAMPTZ,
        error                   TEXT
    )
    """,
]


async def run_migrations(pool: asyncpg.Pool | None = None) -> None:
    p = pool or await get_pool()
    async with p.acquire() as conn:
        for stmt in _MIGRATIONS:
            await conn.execute(stmt.strip())
