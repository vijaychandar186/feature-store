"""
Online store (Redis) tests.
Requires a running Redis instance.
"""

import pytest
import pytest_asyncio
from datetime import datetime, timezone
from src.store.online_store import (
    write_online_features,
    read_online_features,
    write_online_batch,
    delete_online_features,
)
import src.db.redis_client as redis_module
from src.db.redis_client import get_redis, feature_key


@pytest_asyncio.fixture(autouse=True)
async def cleanup():
    redis_module._client = None
    yield
    r = get_redis()
    async for key in r.scan_iter("feature:online_test:*"):
        await r.delete(key)
    await r.aclose()
    redis_module._client = None


@pytest.mark.asyncio
async def test_write_and_read_single_entity():
    now = datetime.now(timezone.utc)
    await write_online_features(
        "online_test", "user_1",
        {"score": 0.9, "age": 25, "active": True},
        event_timestamp=now,
        ttl_seconds=60,
    )
    results, missing = await read_online_features("online_test", ["user_1"])
    assert "user_1" in results
    assert missing == []
    feats = results["user_1"]
    assert feats["score"] == "0.9"
    assert feats["age"] == "25"
    assert feats["active"] == "True"


@pytest.mark.asyncio
async def test_missing_entity():
    results, missing = await read_online_features("online_test", ["nonexistent_999"])
    assert results == {}
    assert "nonexistent_999" in missing


@pytest.mark.asyncio
async def test_batch_write_and_read():
    now = datetime.now(timezone.utc)
    batch = [
        ("ent_a", {"val": 1.0, "flag": False}, now),
        ("ent_b", {"val": 2.5, "flag": True}, now),
        ("ent_c", {"val": 0.0, "flag": False}, now),
    ]
    count = await write_online_batch("online_test", batch, ttl_seconds=60)
    assert count == 3

    results, missing = await read_online_features("online_test", ["ent_a", "ent_b", "ent_c"])
    assert set(results.keys()) == {"ent_a", "ent_b", "ent_c"}
    assert missing == []


@pytest.mark.asyncio
async def test_feature_name_filter():
    now = datetime.now(timezone.utc)
    await write_online_features(
        "online_test", "filter_ent",
        {"x": 1, "y": 2, "z": 3},
        event_timestamp=now,
        ttl_seconds=60,
    )
    results, _ = await read_online_features("online_test", ["filter_ent"], feature_names=["x", "z"])
    feats = results["filter_ent"]
    assert "x" in feats
    assert "z" in feats
    assert "y" not in feats


@pytest.mark.asyncio
async def test_overwrite_updates_value():
    t1 = datetime(2024, 1, 1, tzinfo=timezone.utc)
    t2 = datetime(2024, 1, 2, tzinfo=timezone.utc)

    await write_online_features("online_test", "ow_ent", {"score": 0.1}, t1, ttl_seconds=60)
    await write_online_features("online_test", "ow_ent", {"score": 0.9}, t2, ttl_seconds=60)

    results, _ = await read_online_features("online_test", ["ow_ent"])
    assert results["ow_ent"]["score"] == "0.9"


@pytest.mark.asyncio
async def test_delete_entity():
    now = datetime.now(timezone.utc)
    await write_online_features("online_test", "del_ent", {"score": 1.0}, now, ttl_seconds=60)
    await delete_online_features("online_test", "del_ent")
    _, missing = await read_online_features("online_test", ["del_ent"])
    assert "del_ent" in missing


@pytest.mark.asyncio
async def test_timestamp_stored():
    """__ts__ field is stored but not returned in feature dict."""
    now = datetime.now(timezone.utc)
    await write_online_features("online_test", "ts_ent", {"a": 1}, now, ttl_seconds=60)
    r = get_redis()
    raw = await r.hgetall(feature_key("online_test", "ts_ent"))
    assert "__ts__" in raw
    assert "a" in raw
