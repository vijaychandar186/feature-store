"""
Feature view registry backed by PostgreSQL with Redis cache.

The registry is the single source of truth for what feature views exist,
their schemas, and which schema version is current.
"""

import json
from datetime import datetime

import asyncpg

from src.db.postgres import get_pool
from src.db.redis_client import get_redis, schema_latest_key, schema_version_key
from src.models.schemas import FeatureView, FeatureViewSpec


def _parse_jsonb(val):
    if isinstance(val, str):
        return json.loads(val)
    return val


async def register_feature_view(spec: FeatureViewSpec, schema_version: int = 1) -> FeatureView:
    pool = await get_pool()
    now = datetime.utcnow()
    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO feature_views
                (name, entity_column, timestamp_column, schema_version, ttl_seconds, tags, description, is_active, created_at, updated_at)
            VALUES ($1, $2, $3, $4, $5, $6::jsonb, $7, TRUE, $8, $8)
            ON CONFLICT (name) DO UPDATE SET
                entity_column    = EXCLUDED.entity_column,
                timestamp_column = EXCLUDED.timestamp_column,
                schema_version   = EXCLUDED.schema_version,
                ttl_seconds      = EXCLUDED.ttl_seconds,
                tags             = EXCLUDED.tags,
                description      = EXCLUDED.description,
                updated_at       = EXCLUDED.updated_at
            """,
            spec.name,
            spec.entity_column,
            spec.timestamp_column,
            schema_version,
            spec.ttl_seconds,
            json.dumps(spec.tags),
            spec.description,
            now,
        )

    return FeatureView(
        **spec.model_dump(),
        schema_version=schema_version,
        created_at=now,
        updated_at=now,
    )


async def get_feature_view(name: str) -> FeatureView | None:
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT * FROM feature_views WHERE name = $1 AND is_active = TRUE",
            name,
        )
    if not row:
        return None

    # Reconstruct spec from schema registry
    spec = await _load_spec_from_registry(name, row["schema_version"], conn=None)
    if not spec:
        return None

    return FeatureView(
        name=row["name"],
        entity_column=row["entity_column"],
        timestamp_column=row["timestamp_column"],
        features=spec.features,
        ttl_seconds=row["ttl_seconds"],
        tags=_parse_jsonb(row["tags"]) if row["tags"] else {},
        description=row["description"] or "",
        schema_version=row["schema_version"],
        created_at=row["created_at"],
        updated_at=row["updated_at"],
        is_active=row["is_active"],
    )


async def list_feature_views() -> list[FeatureView]:
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT * FROM feature_views WHERE is_active = TRUE ORDER BY name"
        )
    result = []
    for row in rows:
        spec = await _load_spec_from_registry(row["name"], row["schema_version"])
        if spec:
            result.append(FeatureView(
                name=row["name"],
                entity_column=row["entity_column"],
                timestamp_column=row["timestamp_column"],
                features=spec.features,
                ttl_seconds=row["ttl_seconds"],
                tags=_parse_jsonb(row["tags"]) if row["tags"] else {},
                description=row["description"] or "",
                schema_version=row["schema_version"],
                created_at=row["created_at"],
                updated_at=row["updated_at"],
                is_active=row["is_active"],
            ))
    return result


async def deactivate_feature_view(name: str) -> bool:
    pool = await get_pool()
    async with pool.acquire() as conn:
        result = await conn.execute(
            "UPDATE feature_views SET is_active = FALSE, updated_at = NOW() WHERE name = $1",
            name,
        )
    return result.endswith("1")


async def feature_view_exists(name: str) -> bool:
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT 1 FROM feature_views WHERE name = $1 AND is_active = TRUE",
            name,
        )
    return row is not None


async def _load_spec_from_registry(
    feature_view: str,
    version: int,
    conn: asyncpg.Connection | None = None,
) -> FeatureViewSpec | None:
    # Try Redis cache first
    r = get_redis()
    cached = await r.get(schema_version_key(feature_view, version))
    if cached:
        data = json.loads(cached)
        return FeatureViewSpec(**data)

    pool = await get_pool()
    async with pool.acquire() as c:
        row = await c.fetchrow(
            "SELECT spec_json FROM schema_registry WHERE feature_view = $1 AND version = $2",
            feature_view,
            version,
        )
    if not row:
        return None

    spec_data = _parse_jsonb(row["spec_json"])
    spec = FeatureViewSpec(**spec_data)

    # Populate Redis cache (1 hour TTL for schema data)
    await r.set(schema_version_key(feature_view, version), json.dumps(spec_data), ex=3600)
    return spec
