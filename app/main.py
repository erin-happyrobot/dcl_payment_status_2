from contextlib import asynccontextmanager
import logging
import requests 
from requests.adapters import HTTPAdapter
from datetime import timedelta, datetime, timezone
import os
import pandas as pd
import dotenv



from apscheduler.schedulers.asyncio import AsyncIOScheduler
from fastapi import FastAPI


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("scheduler")

dotenv.load_dotenv()


scheduler = AsyncIOScheduler(timezone="UTC")


def perform_scheduled_action() -> None:
    print(f"[scheduler] Running 12-hour task at {datetime.now(timezone.utc).isoformat()}")
    session = requests.Session()
    session.mount('https://', HTTPAdapter(max_retries=3))
    url = "https://platform.happyrobot.ai/api/v1/runs"
    headers = {
        "Authorization": f"Bearer {os.getenv('PLATFORM_API_KEY')}",
        "x-organization-id": os.getenv('DCL_ORG_ID'),
    }
    start_utc = datetime.now(timezone.utc) - timedelta(hours=12)
    params = {
        "limit": 100,
        "use_case_id": os.getenv('PAYMENT_STATUS_USE_CASE_ID'),
         "start": start_utc.isoformat(timespec="seconds").replace("+00:00", "Z")
    }

    response = session.get(url, headers=headers, params=params, timeout=(3.05, 20))
    response.raise_for_status()
    runs = response.json()
    logger.info(f"[scheduler] Found {len(runs)} runs")
    for run in runs:
        print(f"[scheduler] Run {run['id']} - {run['status']}")


    print(type(response.json()))
    print(response.json()[0])
    
    calls_data = response.json()
    df_total = pd.DataFrame(calls_data)
    total_calls_past_12_hours = len(df_total)
    df = pd.json_normalize(calls_data)
    print(df.columns)
    total_calls_past_12_hours_failed = len(df[df['data.01988326-54c7-7f8c-a055-6b5ea45d6cc8.response.classification'] == 'did_not_find_load'])
    total_calls_past_12_hours_failed_percentage = total_calls_past_12_hours_failed / total_calls_past_12_hours
    logger.info(f"[scheduler] Total calls past 12 hours: {total_calls_past_12_hours}")
    logger.info(f"[scheduler] Total calls past 12 hours failed: {total_calls_past_12_hours_failed}")
    logger.info(f"[scheduler] Total calls past 12 hours failed percentage: {total_calls_past_12_hours_failed_percentage}")

    if total_calls_past_12_hours_failed_percentage > 0.25:
        logger.info("[scheduler] Total calls past 12 hours failed percentage is greater than 25%")
        logger.info(f"[scheduler] Sending email to {os.getenv('EMAIL_TO')}")
        # send_email( [os.getenv('EMAIL_TO')], "Payment Status Audit Happy Robot", f"Total calls past 12 hours failed percentage is greater than 25%. We are seeing a rate of {total_calls_past_12_hours_failed_percentage*100}% of calls failing to find a load id.")
    else:
        logger.info("[scheduler] Total calls past 12 hours failed percentage is less than 25%")



@asynccontextmanager
async def lifespan(app: FastAPI):  # type: ignore[override]
    # Schedule the job to run every 12 hours. It will also schedule the first run
    # for 12 hours from process start. Uncomment the next line to run once at startup.
    # perform_scheduled_action()
    scheduler.add_job(
        perform_scheduled_action,
        trigger="interval",
        hours=0.002,
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


