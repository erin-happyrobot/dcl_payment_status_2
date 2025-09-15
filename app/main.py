from contextlib import asynccontextmanager
import logging
from requests.adapters import HTTPAdapter
from datetime import timedelta, datetime, timezone
import os
import pandas as pd
import dotenv
import json
from typing import List
from fastapi import HTTPException
import boto3
from botocore.exceptions import NoCredentialsError, ClientError
from botocore.config import Config
import uuid
import requests 

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from fastapi import FastAPI


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("scheduler")

dotenv.load_dotenv()


scheduler = AsyncIOScheduler(timezone="UTC")


# Cooldown to prevent duplicate emails within a short window
EMAIL_COOLDOWN_SECONDS = int(os.getenv("EMAIL_COOLDOWN_SECONDS", "180"))
LAST_SENT_AT = {}

# Lambda invocation cooldown (prevents calling Lambda more than once in window)
LAMBDA_COOLDOWN_SECONDS = int(os.getenv("LAMBDA_COOLDOWN_SECONDS", "300"))
LAST_LAMBDA_INVOKE_AT = None

def _should_send(kind: str, now: datetime) -> bool:
    last = LAST_SENT_AT.get(kind)
    if last is not None:
        elapsed = (now - last).total_seconds()
        if elapsed < EMAIL_COOLDOWN_SECONDS:
            logger.info(f"[scheduler] Cooldown active for '{kind}' ({int(elapsed)}s < {EMAIL_COOLDOWN_SECONDS}s); skipping email")
            return False
    LAST_SENT_AT[kind] = now
    return True

def perform_scheduled_action() -> None:
    now = datetime.now(timezone.utc)
    print(f"[scheduler] Running 12-hour task at {now.isoformat()}")
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


    # print(type(response.json()))
    # print(response.json()[0])
    
    calls_data = response.json()
    df_total = pd.DataFrame(calls_data)
    if (len(df_total) == 0):
        logger.info("[scheduler] No runs found past 12 hours")
        if _should_send("no_runs", now):
            send_email(["erin@happyrobot.ai"], "No Runs Found - Payment Status Audit Happy Robot", "No runs found past 12 hours")
        return
    total_calls_past_12_hours = len(df_total)
    df = pd.json_normalize(calls_data)
    print(df.columns)
    total_calls_past_12_hours_failed = len(df[df['data.01990824-79a3-78fd-a338-c55450cd8803.response.classification'] == 'did_not_find_load'])
    total_calls_past_12_hours_failed_percentage = total_calls_past_12_hours_failed / total_calls_past_12_hours
    logger.info(f"[scheduler] Total calls past 12 hours: {total_calls_past_12_hours}")
    logger.info(f"[scheduler] Total calls past 12 hours failed: {total_calls_past_12_hours_failed}")
    logger.info(f"[scheduler] Total calls past 12 hours failed percentage: {total_calls_past_12_hours_failed_percentage}")

    if total_calls_past_12_hours_failed_percentage > 0.25:
        logger.info("[scheduler] Total calls past 12 hours failed percentage is greater than 25%")
        logger.info(f"[scheduler] Sending email to {os.getenv('EMAIL_TO')}")
        if _should_send("alert", now):
            send_email(["erin@happyrobot.ai", "graham.cason@directconnectlogistix.com", "podriscoll@directconnectlogistix.com"], "McLeod Alert - Payment Status Audit Happy Robot", f"Total calls past 12 hours failed percentage is greater than 25%. We are seeing a rate of {total_calls_past_12_hours_failed_percentage*100}% of calls failing to find a load id.")
    else:
        logger.info("[scheduler] Total calls past 12 hours failed percentage is less than 25%")
        if _should_send("ok", now):
            send_email(["erin@happyrobot.ai"], "All Good - Payment Status Audit Happy Robot", f"Total calls past 12 hours failed percentage is less than 25%. We are seeing a rate of {total_calls_past_12_hours_failed_percentage*100}% of calls failing to find a load id.")


def send_email(
    email_addresses: List[str] = [],
    subject: str = "",
    body: str = "",
) -> dict:
    try:

        payload = {
            "orgId": os.getenv("DCL_ORG_ID"),
            "from": os.getenv("SENDER_EMAIL"),
            "to": email_addresses,
            "subject": subject,
            "body": body,
        }
        # Add idempotency/debug metadata
        payload["requestId"] = str(uuid.uuid4())
        payload["requestedAt"] = datetime.now(timezone.utc).isoformat()
        # region is read in invoke_lambda
        lambda_function_name = os.getenv("LAMBDA_FUNCTION_NAME")
        if not lambda_function_name:
            logger.error("Missing LAMBDA_FUNCTION_NAME environment variable")
            raise HTTPException(status_code=500, detail="Missing LAMBDA_FUNCTION_NAME env var")

        # Lambda global cooldown
        global LAST_LAMBDA_INVOKE_AT
        now_ts = datetime.now(timezone.utc)
        if LAST_LAMBDA_INVOKE_AT is not None:
            elapsed = (now_ts - LAST_LAMBDA_INVOKE_AT).total_seconds()
            if elapsed < LAMBDA_COOLDOWN_SECONDS:
                logger.info(
                    f"[scheduler] Lambda cooldown active ({int(elapsed)}s < {LAMBDA_COOLDOWN_SECONDS}s); skipping invoke"
                )
                return {
                    "success": True,
                    "message": "Skipped due to lambda cooldown",
                    "email_addresses": email_addresses,
                }

        invoke_lambda(
            payload=payload,
        )
        LAST_LAMBDA_INVOKE_AT = now_ts
        return {
            "success": True,
            "message": "Email sent successfully",
            "email_addresses": email_addresses,
        }
    except Exception as e:
        logger.error(f"Error sending email: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=500, detail=f"Error sending email: {str(e)}"
        )


def invoke_lambda(payload: dict) -> dict:
    try:
        # Disable client-side retries to avoid duplicate invokes
        client = boto3.client(
            "lambda",
            region_name=os.getenv("AWS_REGION"),
            config=Config(retries={"max_attempts": 0, "mode": "standard"}, connect_timeout=3, read_timeout=10),
        )
        resp = client.invoke(
            FunctionName=os.getenv("LAMBDA_FUNCTION_NAME"),  # or full ARN
            InvocationType="RequestResponse",
            Payload=json.dumps(payload).encode("utf-8"),
        )
        status_code = resp.get("StatusCode")
        function_error = resp.get("FunctionError")
        logger.info(f"Lambda invoke StatusCode={status_code} FunctionError={function_error}")
        with resp["Payload"] as stream:
            return json.loads(stream.read().decode("utf-8"))
    except NoCredentialsError:
        raise HTTPException(status_code=500, detail="Missing AWS credentials. Set AWS_ACCESS_KEY_ID/SECRET (and SESSION_TOKEN if temp) and AWS_REGION.")
    except ClientError as e:
        raise HTTPException(status_code=500, detail=f"Error invoking Lambda: {e}")



@asynccontextmanager
async def lifespan(app: FastAPI):  # type: ignore[override]
    # Gate scheduling so only one instance runs jobs
    run_scheduler_env = os.getenv("RUN_SCHEDULER", "true").lower()
    run_scheduler = run_scheduler_env in ("1", "true", "yes", "on")
    if not run_scheduler:
        logger.info("RUN_SCHEDULER is disabled; skipping job scheduling for this process")
        try:
            yield
        finally:
            pass
        return

    logger.info(f"Starting scheduler in PID {os.getpid()}")
    # Resolve schedule interval from env with safe fallback and log it
    schedule_hours_raw = os.getenv("SCHEDULE_HOURS", "12")
    try:
        schedule_hours = float(schedule_hours_raw)
    except Exception:
        schedule_hours = 12.0
        logger.warning(f"Invalid SCHEDULE_HOURS='{schedule_hours_raw}', defaulting to {schedule_hours}h")
    logger.info(f"Scheduling job every {schedule_hours} hours (SCHEDULE_HOURS='{schedule_hours_raw}')")

    # perform_scheduled_action()
    scheduler.add_job(
        perform_scheduled_action,
        trigger="interval",
        hours=schedule_hours,
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


