# DCL Payment Status - Scheduler Service

FastAPI service with a background job that runs every 12 hours. Ready to deploy on Railway.

## Quickstart (Local)

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# Run the API locally
uvicorn app.main:app --reload
```

Open http://localhost:8000 to see `{ "status": "ok" }`.

## Scheduled Job

The job is configured in `app/main.py` using APScheduler's `AsyncIOScheduler` with an interval trigger of 12 hours.

- Edit `perform_scheduled_action()` to implement your logic.
- To also run once at startup, uncomment the `perform_scheduled_action()` call inside `lifespan`.

## Deploy on Railway

Railway will install from `requirements.txt` and use the `Procfile` to start the server.

```bash
# From the project root
railway up
```

The server binds to `0.0.0.0` and uses `PORT` provided by Railway.

