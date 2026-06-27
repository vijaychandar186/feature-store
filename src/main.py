"""
Entry point — supports two modes:
  --mode api       runs the FastAPI server (default)
  --mode consumer  runs the Kafka consumer worker
"""

import asyncio
import logging
import click
import uvicorn
from contextlib import asynccontextmanager
from fastapi import FastAPI

from src.config import get_settings
from src.db.postgres import get_pool, run_migrations, close_pool
from src.db.redis_client import get_redis, close_redis
from src.kafka.producer import get_producer, close_producer
from src.api.features import router as features_router
from src.api.schemas import router as schemas_router, views_router
from src.api.materialization import router as mat_router

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
log = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    pool = await get_pool()
    await run_migrations(pool)
    log.info("DB migrations complete")
    await get_redis().ping()
    log.info("Redis connected")
    await get_producer()
    log.info("Kafka producer started")
    yield
    # Shutdown
    await close_producer()
    await close_redis()
    await close_pool()
    log.info("Clean shutdown complete")


app = FastAPI(
    title="ML Feature Store",
    description=(
        "Dual-path feature store: offline (PostgreSQL, point-in-time correct) + "
        "online (Redis, low-latency) with Kafka change propagation and schema registry."
    ),
    version="1.0.0",
    lifespan=lifespan,
)

app.include_router(features_router)
app.include_router(schemas_router)
app.include_router(views_router)
app.include_router(mat_router)


@app.get("/health")
async def health():
    s = get_settings()
    r = get_redis()
    try:
        await r.ping()
        redis_ok = True
    except Exception:
        redis_ok = False

    pool = await get_pool()
    try:
        async with pool.acquire() as conn:
            await conn.fetchval("SELECT 1")
        pg_ok = True
    except Exception:
        pg_ok = False

    return {
        "status": "ok" if (redis_ok and pg_ok) else "degraded",
        "postgres": "ok" if pg_ok else "error",
        "redis": "ok" if redis_ok else "error",
        "kafka_bootstrap": s.kafka_bootstrap_servers,
    }


# ------------------------------------------------------------------
# CLI entry point
# ------------------------------------------------------------------

@click.command()
@click.option("--mode", default="api", type=click.Choice(["api", "consumer"]))
def main(mode: str):
    s = get_settings()
    if mode == "api":
        uvicorn.run("src.main:app", host=s.api_host, port=s.api_port, reload=False)
    else:
        from src.kafka.consumer import run_consumer
        asyncio.run(run_consumer())


if __name__ == "__main__":
    main()
