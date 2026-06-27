"""
Batch materialization: offline (PostgreSQL) → online (Redis).

For each feature view, reads the latest-per-entity features from PostgreSQL
and bulk-loads them into Redis using a pipeline. This is the "catch-up" path
used on cold start or when the online store needs a full refresh.

A Redis distributed lock (SETNX with TTL) prevents concurrent materialization
jobs for the same feature view.
"""

import asyncio
import logging
from datetime import datetime, timezone

from src.db.postgres import get_pool
from src.db.redis_client import get_redis, materialization_lock_key
from src.store.offline_store import get_all_latest_for_view
from src.store.online_store import write_online_batch
from src.store.feature_registry import get_feature_view
from src.models.schemas import MaterializationJob
from src.config import get_settings

log = logging.getLogger(__name__)

_LOCK_TTL_SECONDS = 600   # 10-minute max lock
_jobs: dict[str, MaterializationJob] = {}  # in-memory job store (use DB in production)


async def materialize_feature_view(
    feature_view: str,
    start_time: datetime | None = None,
    end_time: datetime | None = None,
) -> MaterializationJob:
    """
    Materialize a feature view from offline → online store.
    Respects a distributed Redis lock to avoid double-materialisation.
    """
    r = get_redis()
    lock_key = materialization_lock_key(feature_view)

    # Acquire distributed lock (SETNX)
    acquired = await r.set(lock_key, "1", nx=True, ex=_LOCK_TTL_SECONDS)
    if not acquired:
        raise RuntimeError(
            f"Materialization for '{feature_view}' is already running. "
            f"Wait for it to complete or check for a stale lock."
        )

    job = MaterializationJob(feature_view=feature_view, start_time=start_time, end_time=end_time)
    job.status = "running"
    _jobs[job.job_id] = job

    try:
        fv = await get_feature_view(feature_view)
        if fv is None:
            raise ValueError(f"Feature view '{feature_view}' not found")

        s = get_settings()
        ttl = fv.ttl_seconds or s.online_feature_ttl_seconds

        rows = await get_all_latest_for_view(feature_view, batch_size=s.materialization_batch_size)

        # Filter by time range if specified
        if start_time or end_time:
            filtered = []
            for entity_id, features, event_ts in rows:
                if start_time and event_ts < start_time:
                    continue
                if end_time and event_ts > end_time:
                    continue
                filtered.append((entity_id, features, event_ts))
            rows = filtered

        count = await write_online_batch(feature_view, rows, ttl_seconds=ttl)

        job.status = "completed"
        job.entities_materialized = count
        job.completed_at = datetime.now(timezone.utc)
        log.info("Materialization complete: view=%s entities=%d", feature_view, count)

    except Exception as exc:
        job.status = "failed"
        job.error = str(exc)
        job.completed_at = datetime.now(timezone.utc)
        log.exception("Materialization failed for '%s'", feature_view)
        raise
    finally:
        await r.delete(lock_key)

    return job


async def get_job(job_id: str) -> MaterializationJob | None:
    return _jobs.get(job_id)


async def list_jobs(feature_view: str | None = None) -> list[MaterializationJob]:
    jobs = list(_jobs.values())
    if feature_view:
        jobs = [j for j in jobs if j.feature_view == feature_view]
    return sorted(jobs, key=lambda j: j.created_at, reverse=True)
