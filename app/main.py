from contextlib import asynccontextmanager
from datetime import datetime
import logging

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from fastapi import FastAPI


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("scheduler")


scheduler = AsyncIOScheduler(timezone="UTC")


def perform_scheduled_action() -> None:
    """Replace this with your real job logic."""
    logger.info("Running scheduled action at %s", datetime.utcnow().isoformat())


@asynccontextmanager
async def lifespan(app: FastAPI):  # type: ignore[override]
    # Schedule the job to run every 12 hours. It will also schedule the first run
    # for 12 hours from process start. Uncomment the next line to run once at startup.
    # perform_scheduled_action()
    scheduler.add_job(
        perform_scheduled_action,
        trigger="interval",
        hours=12,
        id="twelve_hour_job",
        replace_existing=True,
        max_instances=1,
        coalesce=True,
    )
    scheduler.start()
    try:
        yield
    finally:
        scheduler.shutdown(wait=False)


app = FastAPI(title="DCL Payment Status - Scheduler", lifespan=lifespan)


@app.get("/")
def health() -> dict:
    return {"status": "ok"}


