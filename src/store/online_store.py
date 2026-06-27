"""
Redis-backed online feature store.

All reads/writes go through Redis pipeline for batch efficiency.
Features are stored as Redis HASHes, with a special __ts__ field for the
event timestamp (milliseconds since epoch).
"""

import json
from datetime import datetime
from typing import Any

from src.db.redis_client import get_redis, feature_key
from src.config import get_settings


def _encode(value: Any) -> str:
    if isinstance(value, (list, dict)):
        return json.dumps(value)
    return str(value)


def _decode(raw: str, dtype_hint: str | None = None) -> Any:
    if raw is None:
        return None
    if dtype_hint in ("list_float64", "list_int64") or (raw.startswith("[") and raw.endswith("]")):
        try:
            return json.loads(raw)
        except Exception:
            pass
    if dtype_hint == "float64":
        try:
            return float(raw)
        except ValueError:
            return raw
    if dtype_hint == "int64":
        try:
            return int(raw)
        except ValueError:
            return raw
    if dtype_hint == "bool":
        return raw.lower() in ("true", "1", "yes")
    return raw


async def write_online_features(
    feature_view: str,
    entity_id: str,
    features: dict[str, Any],
    event_timestamp: datetime,
    ttl_seconds: int | None = None,
) -> None:
    r = get_redis()
    s = get_settings()
    key = feature_key(feature_view, entity_id)
    ts_ms = int(event_timestamp.timestamp() * 1000)

    mapping: dict[str, str] = {k: _encode(v) for k, v in features.items()}
    mapping["__ts__"] = str(ts_ms)
    mapping["__view__"] = feature_view

    pipe = r.pipeline(transaction=False)
    pipe.hset(key, mapping=mapping)
    ttl = ttl_seconds or s.online_feature_ttl_seconds
    pipe.expire(key, ttl)
    await pipe.execute()


async def write_online_batch(
    feature_view: str,
    rows: list[tuple[str, dict[str, Any], datetime]],
    ttl_seconds: int | None = None,
) -> int:
    """Batch write many entities using a single Redis pipeline. Returns count written."""
    if not rows:
        return 0
    r = get_redis()
    s = get_settings()
    ttl = ttl_seconds or s.online_feature_ttl_seconds
    pipe = r.pipeline(transaction=False)

    for entity_id, features, event_timestamp in rows:
        key = feature_key(feature_view, entity_id)
        ts_ms = int(event_timestamp.timestamp() * 1000)
        mapping: dict[str, str] = {k: _encode(v) for k, v in features.items()}
        mapping["__ts__"] = str(ts_ms)
        mapping["__view__"] = feature_view
        pipe.hset(key, mapping=mapping)
        pipe.expire(key, ttl)

    await pipe.execute()
    return len(rows)


async def read_online_features(
    feature_view: str,
    entity_ids: list[str],
    feature_names: list[str] | None = None,
) -> tuple[dict[str, dict[str, Any]], list[str]]:
    """
    Returns:
      results: entity_id → {feature: value}
      missing: list of entity_ids not found in Redis
    """
    r = get_redis()
    pipe = r.pipeline(transaction=False)

    for eid in entity_ids:
        pipe.hgetall(feature_key(feature_view, eid))

    raw_results = await pipe.execute()

    results: dict[str, dict[str, Any]] = {}
    missing: list[str] = []

    for eid, raw in zip(entity_ids, raw_results):
        if not raw:
            missing.append(eid)
            continue
        decoded = {}
        for k, v in raw.items():
            if k.startswith("__"):
                continue
            if feature_names and k not in feature_names:
                continue
            decoded[k] = _decode(v)
        results[eid] = decoded

    return results, missing


async def delete_online_features(feature_view: str, entity_id: str) -> None:
    r = get_redis()
    await r.delete(feature_key(feature_view, entity_id))


async def count_online_entities(feature_view: str) -> int:
    """Approximate count of online entities via SCAN (avoids blocking KEYS)."""
    r = get_redis()
    pattern = f"feature:{feature_view}:*"
    count = 0
    async for _ in r.scan_iter(match=pattern, count=100):
        count += 1
    return count
