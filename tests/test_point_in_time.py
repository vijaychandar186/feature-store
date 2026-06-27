"""
Point-in-time correctness tests.
Requires a running PostgreSQL instance.
"""

import pytest
import pytest_asyncio
from datetime import datetime, timedelta, timezone
from src.db.postgres import get_pool, run_migrations, close_pool, _pool
import src.db.postgres as pg_module
from src.store.offline_store import write_feature_rows, get_historical_features
from src.models.schemas import FeatureRow


@pytest_asyncio.fixture(autouse=True)
async def reset_pool():
    pg_module._pool = None
    await run_migrations()
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute("DELETE FROM feature_values WHERE feature_view = 'pit_test'")
    yield
    pg_module._pool = None


@pytest.mark.asyncio
async def test_pit_returns_correct_version():
    now = datetime.now(timezone.utc)
    t1 = now - timedelta(hours=2)
    t2 = now - timedelta(hours=1)

    rows = [
        FeatureRow(entity_id="user_1", features={"score": 0.5, "tier": "free"}, event_timestamp=t1),
        FeatureRow(entity_id="user_1", features={"score": 0.9, "tier": "paid"}, event_timestamp=t2),
    ]
    await write_feature_rows("pit_test", rows)

    label_time = t2 - timedelta(minutes=30)
    result = await get_historical_features("pit_test", [("user_1", label_time)])

    assert len(result.rows) == 1
    row = result.rows[0]
    assert row.entity_id == "user_1"
    assert row.features is not None
    assert row.features["score"] == pytest.approx(0.5)
    assert row.features["tier"] == "free"


@pytest.mark.asyncio
async def test_pit_returns_latest_at_or_before():
    now = datetime.now(timezone.utc)
    t1 = now - timedelta(hours=4)
    t2 = now - timedelta(hours=3)

    rows = [
        FeatureRow(entity_id="user_2", features={"score": 0.1}, event_timestamp=t1),
        FeatureRow(entity_id="user_2", features={"score": 0.8}, event_timestamp=t2),
    ]
    await write_feature_rows("pit_test", rows)

    result = await get_historical_features("pit_test", [("user_2", t2)])
    assert result.rows[0].features["score"] == pytest.approx(0.8)


@pytest.mark.asyncio
async def test_pit_returns_null_before_any_data():
    now = datetime.now(timezone.utc)
    t_past = now - timedelta(days=10)

    rows = [FeatureRow(entity_id="user_3", features={"score": 0.7}, event_timestamp=now)]
    await write_feature_rows("pit_test", rows)

    result = await get_historical_features("pit_test", [("user_3", t_past)])
    assert result.rows[0].features is None


@pytest.mark.asyncio
async def test_pit_batch_different_times():
    now = datetime.now(timezone.utc)
    t1 = now - timedelta(hours=6)
    t2 = now - timedelta(hours=5)
    t3 = now - timedelta(hours=4)

    rows = [
        FeatureRow(entity_id="batch_1", features={"x": 1.0}, event_timestamp=t1),
        FeatureRow(entity_id="batch_1", features={"x": 2.0}, event_timestamp=t3),
        FeatureRow(entity_id="batch_2", features={"x": 3.0}, event_timestamp=t2),
    ]
    await write_feature_rows("pit_test", rows)

    entity_timestamps = [
        ("batch_1", t2),
        ("batch_2", t3),
    ]
    result = await get_historical_features("pit_test", entity_timestamps)
    assert len(result.rows) == 2

    by_entity = {r.entity_id: r for r in result.rows}
    assert by_entity["batch_1"].features["x"] == pytest.approx(1.0)
    assert by_entity["batch_2"].features["x"] == pytest.approx(3.0)


@pytest.mark.asyncio
async def test_pit_feature_name_filter():
    now = datetime.now(timezone.utc)
    t = now - timedelta(hours=7)
    rows = [FeatureRow(entity_id="filter_1", features={"a": 1, "b": 2, "c": 3}, event_timestamp=t)]
    await write_feature_rows("pit_test", rows)

    result = await get_historical_features("pit_test", [("filter_1", now)], feature_names=["a", "c"])
    feats = result.rows[0].features
    assert "a" in feats
    assert "c" in feats
    assert "b" not in feats
