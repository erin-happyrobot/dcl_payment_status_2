from contextlib import asynccontextmanager
import logging
from requests.adapters import HTTPAdapter
from datetime import timedelta, datetime, timezone
import os
import pandas as pd
import dotenv
import json
from typing import List, Dict, Any, Optional
from fastapi import HTTPException
import boto3
from botocore.exceptions import NoCredentialsError, ClientError
import requests 

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
        send_email( [os.getenv('EMAIL_TO'), "erin@happyrobot.ai"], "McLeod Alert - Payment Status Audit Happy Robot", f"Total calls past 12 hours failed percentage is greater than 25%. We are seeing a rate of {total_calls_past_12_hours_failed_percentage*100}% of calls failing to find a load id.")
    else:
        logger.info("[scheduler] Total calls past 12 hours failed percentage is less than 25%")
        send_email( ["erin@happyrobot.ai"], "All Good - Payment Status Audit Happy Robot", f"Total calls past 12 hours failed percentage is less than 25%. We are seeing a rate of {total_calls_past_12_hours_failed_percentage*100}% of calls failing to find a load id.")


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
        aws_region = os.getenv("AWS_REGION") or "us-east-2"
        lambda_function_name = os.getenv("LAMBDA_FUNCTION_NAME")
        if not lambda_function_name:
            logger.error("Missing LAMBDA_FUNCTION_NAME environment variable")
            raise HTTPException(status_code=500, detail="Missing LAMBDA_FUNCTION_NAME env var")

        invoke_lambda(
            payload=payload,
        )
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
        client = boto3.client("lambda", region_name=os.getenv("AWS_REGION"))
        resp = client.invoke(
            FunctionName=os.getenv("LAMBDA_FUNCTION_NAME"),  # or full ARN
            InvocationType="RequestResponse",
            Payload=json.dumps(payload).encode("utf-8"),
        )
        with resp["Payload"] as stream:
            return json.loads(stream.read().decode("utf-8"))
    except NoCredentialsError:
        raise HTTPException(status_code=500, detail="Missing AWS credentials. Set AWS_ACCESS_KEY_ID/SECRET (and SESSION_TOKEN if temp) and AWS_REGION.")
    except ClientError as e:
        raise HTTPException(status_code=500, detail=f"Error invoking Lambda: {e}")



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


