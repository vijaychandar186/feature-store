import asyncio
from datetime import datetime
from fastapi import APIRouter, HTTPException, BackgroundTasks
from src.models.schemas import MaterializationJob
from src.materialization.batch import materialize_feature_view, get_job, list_jobs
from src.store.feature_registry import feature_view_exists

router = APIRouter(prefix="/materialize", tags=["materialization"])


@router.post("/{feature_view}", response_model=MaterializationJob)
async def trigger_materialization(
    feature_view: str,
    background_tasks: BackgroundTasks,
    start_time: datetime | None = None,
    end_time: datetime | None = None,
):
    """
    Trigger batch materialization: offline PostgreSQL → online Redis.
    Runs as a background task and returns the job metadata immediately.
    """
    if not await feature_view_exists(feature_view):
        raise HTTPException(404, f"Feature view '{feature_view}' not found")

    from src.models.schemas import MaterializationJob
    job = MaterializationJob(feature_view=feature_view, start_time=start_time, end_time=end_time)

    async def _run():
        try:
            await materialize_feature_view(feature_view, start_time, end_time)
        except Exception:
            pass  # errors are stored in the job object

    background_tasks.add_task(_run)
    return job


@router.post("/{feature_view}/sync", response_model=MaterializationJob)
async def trigger_materialization_sync(
    feature_view: str,
    start_time: datetime | None = None,
    end_time: datetime | None = None,
):
    """Synchronous materialization — waits for completion before returning."""
    if not await feature_view_exists(feature_view):
        raise HTTPException(404, f"Feature view '{feature_view}' not found")
    try:
        return await materialize_feature_view(feature_view, start_time, end_time)
    except RuntimeError as exc:
        raise HTTPException(409, str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(404, str(exc)) from exc


@router.get("/jobs/{job_id}", response_model=MaterializationJob)
async def get_job_status(job_id: str):
    job = await get_job(job_id)
    if not job:
        raise HTTPException(404, f"Job '{job_id}' not found")
    return job


@router.get("/jobs")
async def list_all_jobs(feature_view: str | None = None):
    return await list_jobs(feature_view)
