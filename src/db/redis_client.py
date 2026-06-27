"""
Redis connection via redis.asyncio.

Key namespace:
  feature:{feature_view}:{entity_id}  →  HASH  {feature_name: value, __ts__: epoch_ms}
  schema:{feature_view}:latest        →  STRING (version number)
  schema:{feature_view}:{version}     →  STRING (JSON schema)
"""

import redis.asyncio as aioredis
from src.config import get_settings

_client: aioredis.Redis | None = None


def get_redis() -> aioredis.Redis:
    global _client
    if _client is None:
        s = get_settings()
        kwargs: dict = {"host": s.redis_host, "port": s.redis_port, "db": s.redis_db, "decode_responses": True}
        if s.redis_password:
            kwargs["password"] = s.redis_password
        _client = aioredis.Redis(**kwargs)
    return _client


async def close_redis() -> None:
    global _client
    if _client:
        await _client.aclose()
        _client = None


# ------------------------------------------------------------------
# Key helpers
# ------------------------------------------------------------------

def feature_key(feature_view: str, entity_id: str) -> str:
    return f"feature:{feature_view}:{entity_id}"


def schema_latest_key(feature_view: str) -> str:
    return f"schema:{feature_view}:latest"


def schema_version_key(feature_view: str, version: int) -> str:
    return f"schema:{feature_view}:{version}"


def materialization_lock_key(feature_view: str) -> str:
    return f"matlock:{feature_view}"
