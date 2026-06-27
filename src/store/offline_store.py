"""
PostgreSQL-backed offline feature store.

Key capability: point-in-time correct historical feature retrieval via
PostgreSQL LATERAL JOIN + UNNEST — a single query that processes all
(entity_id, as_of_timestamp) pairs without N+1 round trips.

PIT correctness guarantee: for each (entity_id, T), we return the feature
row with the LARGEST event_timestamp that is still <= T. This ensures no
future information leaks into training datasets.
"""

import json
from datetime import datetime
from typing import Any

import asyncpg

from src.db.postgres import get_pool
from src.models.schemas import (
    FeatureRow,
    HistoricalFeatureRow,
    HistoricalFeatureResponse,
)


def _parse_jsonb(val):
    if isinstance(val, str):
        return json.loads(val)
    return val


async def write_feature_rows(
    feature_view: str,
    rows: list[FeatureRow],
) -> int:
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.executemany(
            """
            INSERT INTO feature_values (feature_view, entity_id, features, event_timestamp)
            VALUES ($1, $2, $3::jsonb, $4)
            """,
            [
                (feature_view, row.entity_id, json.dumps(row.features), row.event_timestamp)
                for row in rows
            ],
        )
    return len(rows)


async def get_latest_features(
    feature_view: str,
    entity_id: str,
) -> dict[str, Any] | None:
    """Get the most recent feature row for an entity (no PIT filter)."""
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT features, event_timestamp
            FROM feature_values
            WHERE feature_view = $1 AND entity_id = $2
            ORDER BY event_timestamp DESC
            LIMIT 1
            """,
            feature_view,
            entity_id,
        )
    if not row:
        return None
    return _parse_jsonb(row["features"])


async def get_historical_features(
    feature_view: str,
    entity_timestamps: list[tuple[str, datetime]],
    feature_names: list[str] | None = None,
) -> HistoricalFeatureResponse:
    """
    Point-in-time correct retrieval for training datasets.

    Uses UNNEST + LATERAL JOIN to resolve all (entity_id, as_of_ts) pairs
    in a single round-trip. For each pair, returns the LATEST feature row
    with event_timestamp <= as_of_ts.

    This prevents feature leakage: no future data can bleed into training labels.
    """
    if not entity_timestamps:
        return HistoricalFeatureResponse(feature_view=feature_view, rows=[])

    entity_ids = [et[0] for et in entity_timestamps]
    timestamps = [et[1] for et in entity_timestamps]

    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            WITH input AS (
                SELECT
                    unnest($1::text[])        AS entity_id,
                    unnest($2::timestamptz[]) AS label_timestamp,
                    generate_series(1, array_length($1::text[], 1)) AS row_num
            )
            SELECT
                i.entity_id,
                i.label_timestamp,
                fv.features,
                fv.event_timestamp AS feature_timestamp
            FROM input i
            LEFT JOIN LATERAL (
                SELECT features, event_timestamp
                FROM feature_values
                WHERE feature_view    = $3
                  AND entity_id       = i.entity_id
                  AND event_timestamp <= i.label_timestamp
                ORDER BY event_timestamp DESC
                LIMIT 1
            ) fv ON TRUE
            ORDER BY i.row_num
            """,
            entity_ids,
            timestamps,
            feature_view,
        )

    result_rows = []
    for row in rows:
        raw_features: dict[str, Any] | None = None
        if row["features"] is not None:
            raw_features = _parse_jsonb(row["features"])
            if feature_names:
                raw_features = {k: v for k, v in raw_features.items() if k in feature_names}

        result_rows.append(
            HistoricalFeatureRow(
                entity_id=row["entity_id"],
                label_timestamp=row["label_timestamp"],
                features=raw_features,
                feature_timestamp=row["feature_timestamp"],
            )
        )

    return HistoricalFeatureResponse(feature_view=feature_view, rows=result_rows)


async def get_all_latest_for_view(
    feature_view: str,
    batch_size: int = 1000,
) -> list[tuple[str, dict[str, Any], datetime]]:
    """
    For batch materialization: return the latest features per entity.
    Uses DISTINCT ON (entity_id) sorted by event_timestamp DESC.
    Yields rows in batches to avoid loading everything into memory.
    """
    pool = await get_pool()
    results = []
    offset = 0

    while True:
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT DISTINCT ON (entity_id) entity_id, features, event_timestamp
                FROM feature_values
                WHERE feature_view = $1
                ORDER BY entity_id, event_timestamp DESC
                LIMIT $2 OFFSET $3
                """,
                feature_view,
                batch_size,
                offset,
            )

        if not rows:
            break

        for row in rows:
            results.append((
                row["entity_id"],
                _parse_jsonb(row["features"]),
                row["event_timestamp"],
            ))
        offset += len(rows)
        if len(rows) < batch_size:
            break

    return results


async def delete_feature_view_data(feature_view: str) -> int:
    pool = await get_pool()
    async with pool.acquire() as conn:
        result = await conn.execute(
            "DELETE FROM feature_values WHERE feature_view = $1",
            feature_view,
        )
    # "DELETE N" → extract N
    return int(result.split()[-1])


async def offline_stats(feature_view: str) -> dict:
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT
                COUNT(*)                                        AS total_rows,
                COUNT(DISTINCT entity_id)                       AS unique_entities,
                MIN(event_timestamp)                            AS earliest_ts,
                MAX(event_timestamp)                            AS latest_ts
            FROM feature_values
            WHERE feature_view = $1
            """,
            feature_view,
        )
    return dict(row) if row else {}
