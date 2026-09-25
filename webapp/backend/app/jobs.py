"""Background job manager: each assessment runs as an asyncio task and streams
progress over an SSE queue so the UI updates live while checks execute."""
from __future__ import annotations

import asyncio
import time
import uuid
from dataclasses import dataclass, field

from .executor import run_check
from .loader import load_profile, load_requirements, profile_requirement_keys


@dataclass
class Job:
    id: str
    catalog: str
    schema: str
    profile: str
    status: str = "pending"  # pending | running | done | error
    queue: asyncio.Queue = field(default_factory=asyncio.Queue)
    results: list = field(default_factory=list)
    created_at: float = field(default_factory=time.time)


JOBS: dict[str, Job] = {}


def create_job(catalog: str, schema: str, profile_name: str) -> Job:
    job = Job(id=str(uuid.uuid4()), catalog=catalog, schema=schema, profile=profile_name)
    JOBS[job.id] = job
    return job


async def run_job(job: Job) -> None:
    job.status = "running"
    loop = asyncio.get_event_loop()
    try:
        profile = load_profile(job.profile)
    except FileNotFoundError as exc:
        job.status = "error"
        await job.queue.put({"type": "error", "detail": str(exc)})
        await job.queue.put({"type": "done", "results": job.results})
        return

    reqs = load_requirements()
    items = profile_requirement_keys(profile)
    await job.queue.put({"type": "start", "total": len(items), "profile": profile.get("name", job.profile)})

    for key, stage, threshold in items:
        entry = reqs.get(key, {})
        await job.queue.put({"type": "progress", "key": key, "stage": stage, "status": "running"})
        try:
            result = await loop.run_in_executor(None, run_check, key, job.catalog, job.schema)
        except Exception as exc:  # keep the run going even if one check errors
            result = {"status": "error", "value": None, "detail": str(exc)}

        value = result.get("value")
        passed = value is not None and value >= threshold
        record = {
            "key": key,
            "stage": stage,
            "description": entry.get("description", ""),
            "threshold": threshold,
            "value": value,
            "passed": passed,
            "status": result.get("status"),
            "detail": result.get("detail"),
        }
        job.results.append(record)
        await job.queue.put({"type": "result", **record})

    job.status = "done"
    await job.queue.put({"type": "done", "results": job.results})
