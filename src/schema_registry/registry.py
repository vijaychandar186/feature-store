"""
Schema Registry: versioned feature schemas with compatibility enforcement.

Each registration either creates version 1 or increments the version after
running a compatibility check. The mode determines what changes are allowed:

  BACKWARD  new schema can read data written with old schema
            → may ADD new optional fields (with defaults)
            → may NOT remove required fields
            → may NOT change field types

  FORWARD   old schema can read data written with new schema
            → may REMOVE fields
            → may NOT add new required fields without defaults

  FULL      both BACKWARD and FORWARD

  NONE      no compatibility enforcement
"""

import json
from datetime import datetime

from src.db.postgres import get_pool
from src.db.redis_client import get_redis, schema_latest_key, schema_version_key
from src.models.schemas import (
    FeatureViewSpec,
    SchemaVersion,
    CompatibilityMode,
    CompatibilityCheckResult,
)
from src.schema_registry.compatibility import check_compatibility


def _parse_jsonb(val):
    if isinstance(val, str):
        return json.loads(val)
    return val


async def register_schema(
    spec: FeatureViewSpec,
    compatibility_mode: CompatibilityMode = "BACKWARD",
) -> SchemaVersion:
    pool = await get_pool()

    async with pool.acquire() as conn:
        # Get current latest version
        row = await conn.fetchrow(
            "SELECT version, spec_json FROM schema_registry WHERE feature_view = $1 AND is_latest = TRUE",
            spec.name,
        )

        if row is None:
            new_version = 1
        else:
            old_spec = FeatureViewSpec(**_parse_jsonb(row["spec_json"]))
            compat = check_compatibility(old_spec, spec, compatibility_mode)
            if not compat.compatible:
                raise ValueError(
                    f"Schema not compatible ({compatibility_mode}): {'; '.join(compat.errors)}"
                )
            new_version = row["version"] + 1
            # Mark old version as not-latest
            await conn.execute(
                "UPDATE schema_registry SET is_latest = FALSE WHERE feature_view = $1",
                spec.name,
            )

        spec_json = spec.model_dump_json()
        now = datetime.utcnow()

        await conn.execute(
            """
            INSERT INTO schema_registry (feature_view, version, spec_json, compatibility_mode, is_latest, created_at)
            VALUES ($1, $2, $3::jsonb, $4, TRUE, $5)
            """,
            spec.name,
            new_version,
            spec_json,
            compatibility_mode,
            now,
        )

    # Update Redis cache
    r = get_redis()
    await r.set(schema_version_key(spec.name, new_version), spec.model_dump_json(), ex=3600)
    await r.set(schema_latest_key(spec.name), str(new_version), ex=3600)

    return SchemaVersion(
        feature_view=spec.name,
        version=new_version,
        spec=spec,
        compatibility_mode=compatibility_mode,
        is_latest=True,
        created_at=now,
    )


async def get_schema(feature_view: str, version: int | None = None) -> SchemaVersion | None:
    pool = await get_pool()

    async with pool.acquire() as conn:
        if version is None:
            row = await conn.fetchrow(
                "SELECT * FROM schema_registry WHERE feature_view = $1 AND is_latest = TRUE",
                feature_view,
            )
        else:
            row = await conn.fetchrow(
                "SELECT * FROM schema_registry WHERE feature_view = $1 AND version = $2",
                feature_view,
                version,
            )

    if not row:
        return None

    spec = FeatureViewSpec(**_parse_jsonb(row["spec_json"]))
    return SchemaVersion(
        feature_view=row["feature_view"],
        version=row["version"],
        spec=spec,
        compatibility_mode=row["compatibility_mode"],
        is_latest=row["is_latest"],
        created_at=row["created_at"],
    )


async def list_schema_versions(feature_view: str) -> list[SchemaVersion]:
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT * FROM schema_registry WHERE feature_view = $1 ORDER BY version DESC",
            feature_view,
        )
    return [
        SchemaVersion(
            feature_view=r["feature_view"],
            version=r["version"],
            spec=FeatureViewSpec(**_parse_jsonb(r["spec_json"])),
            compatibility_mode=r["compatibility_mode"],
            is_latest=r["is_latest"],
            created_at=r["created_at"],
        )
        for r in rows
    ]


async def get_latest_version_number(feature_view: str) -> int | None:
    # Fast path: Redis
    r = get_redis()
    cached = await r.get(schema_latest_key(feature_view))
    if cached:
        return int(cached)

    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT version FROM schema_registry WHERE feature_view = $1 AND is_latest = TRUE",
            feature_view,
        )
    if not row:
        return None
    v = row["version"]
    await r.set(schema_latest_key(feature_view), str(v), ex=3600)
    return v
