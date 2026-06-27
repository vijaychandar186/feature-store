from fastapi import APIRouter, HTTPException, BackgroundTasks
from src.models.schemas import (
    WriteFeatureRequest,
    OnlineFeatureRequest,
    OnlineFeatureResponse,
    HistoricalFeatureRequest,
    HistoricalFeatureResponse,
)
from src.store.offline_store import write_feature_rows, offline_stats
from src.store.online_store import read_online_features
from src.kafka.producer import produce_feature_updates
from src.store.feature_registry import feature_view_exists

router = APIRouter(prefix="/features", tags=["features"])


@router.post("/write")
async def write_features(request: WriteFeatureRequest):
    """
    Write feature rows to the offline store (PostgreSQL) directly.
    Also pushes to Kafka so the consumer will propagate to the online store.
    """
    if not await feature_view_exists(request.feature_view):
        raise HTTPException(404, f"Feature view '{request.feature_view}' not found")

    # Direct offline write
    n = await write_feature_rows(request.feature_view, request.rows)

    # Async propagation via Kafka → consumer will write to Redis
    try:
        await produce_feature_updates(request.feature_view, request.rows)
    except Exception as exc:
        # Kafka unavailable: offline write still succeeded; log and continue
        import logging
        logging.getLogger(__name__).warning("Kafka produce failed: %s", exc)

    return {"written": n, "feature_view": request.feature_view}


@router.post("/online", response_model=OnlineFeatureResponse)
async def get_online_features(request: OnlineFeatureRequest):
    """
    Low-latency feature lookup from Redis.
    Returns features for all requested entity_ids.
    """
    if not await feature_view_exists(request.feature_view):
        raise HTTPException(404, f"Feature view '{request.feature_view}' not found")

    results, missing = await read_online_features(
        request.feature_view,
        request.entity_ids,
        request.feature_names,
    )
    return OnlineFeatureResponse(
        feature_view=request.feature_view,
        results=results,
        missing_entities=missing,
    )


@router.post("/historical", response_model=HistoricalFeatureResponse)
async def get_historical_features(request: HistoricalFeatureRequest):
    """
    Point-in-time correct feature retrieval for training dataset generation.
    For each (entity_id, as_of_timestamp), returns the latest feature row
    with event_timestamp <= as_of_timestamp.
    """
    if not await feature_view_exists(request.feature_view):
        raise HTTPException(404, f"Feature view '{request.feature_view}' not found")

    from src.store.offline_store import get_historical_features
    return await get_historical_features(
        request.feature_view,
        request.entity_timestamps,
        request.feature_names,
    )


@router.get("/stats/{feature_view}")
async def feature_stats(feature_view: str):
    """Offline store statistics for a feature view."""
    if not await feature_view_exists(feature_view):
        raise HTTPException(404, f"Feature view '{feature_view}' not found")

    from src.store.online_store import count_online_entities
    offline = await offline_stats(feature_view)
    online_count = await count_online_entities(feature_view)
    return {
        "feature_view": feature_view,
        "offline": offline,
        "online_entities": online_count,
    }
