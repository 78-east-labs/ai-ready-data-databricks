from __future__ import annotations

import asyncio
import json
from pathlib import Path

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from .databricks_client import is_configured
from .jobs import JOBS, create_job, run_job
from .loader import list_profiles

app = FastAPI(title="AI-Ready Data — Databricks")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173"],
    allow_methods=["*"],
    allow_headers=["*"],
)


class AssessRequest(BaseModel):
    catalog: str
    schema_name: str
    profile: str


@app.get("/api/status")
def status():
    return {"mock_mode": not is_configured()}


@app.get("/api/profiles")
def profiles():
    return list_profiles()


@app.post("/api/assessments")
async def start_assessment(req: AssessRequest):
    job = create_job(req.catalog, req.schema_name, req.profile)
    asyncio.create_task(run_job(job))
    return {"id": job.id}


@app.get("/api/assessments/{job_id}/stream")
async def stream(job_id: str):
    job = JOBS.get(job_id)
    if not job:
        raise HTTPException(404, "job not found")

    async def gen():
        while True:
            event = await job.queue.get()
            yield f"data: {json.dumps(event)}\n\n"
            if event["type"] == "done":
                break

    return StreamingResponse(gen(), media_type="text/event-stream")


@app.get("/api/assessments/{job_id}")
def get_job(job_id: str):
    job = JOBS.get(job_id)
    if not job:
        raise HTTPException(404, "job not found")
    return {
        "id": job.id,
        "status": job.status,
        "catalog": job.catalog,
        "schema": job.schema,
        "profile": job.profile,
        "results": job.results,
    }


# --- Frontend static files (built via `npm run build` in webapp/frontend) ---
# Registered last so /api/* routes above always take priority over the catch-all.
STATIC_DIR = Path(__file__).resolve().parent.parent / "static"

if STATIC_DIR.exists():
    app.mount("/assets", StaticFiles(directory=STATIC_DIR / "assets"), name="assets")

    @app.get("/{full_path:path}")
    async def serve_frontend(full_path: str):
        candidate = STATIC_DIR / full_path
        if full_path and candidate.is_file():
            return FileResponse(candidate)
        index = STATIC_DIR / "index.html"
        if index.exists():
            return FileResponse(index)
        raise HTTPException(404, "frontend not built — run `npm run build` in webapp/frontend")

