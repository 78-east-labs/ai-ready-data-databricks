# Web app

A background-execution web UI for the [`ai-ready-data`](../skills/ai-ready-data) skill: pick a catalog/schema and a profile, and it runs the checks as a background job, streaming live progress to a dashboard (score ring, factor radar, pass/fail list). This never forks the framework content — it reads `requirements.yaml`, `profiles/*.yaml` and `check.md` directly from `skills/ai-ready-data/`.

```
backend/   FastAPI service: loads requirements/profiles, renders check.md SQL, executes
           against Databricks (or a deterministic mock when no credentials are set),
           runs assessments as background asyncio jobs, streams progress over SSE.
frontend/  Vite + React + TypeScript + Tailwind dashboard.
```

## Run it

**Backend**

```bash
cd backend
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# Optional — omit these to run in demo/mock mode with synthetic scores
export DATABRICKS_HOST=<workspace-hostname>
export DATABRICKS_HTTP_PATH=<sql-warehouse-http-path>
export DATABRICKS_TOKEN=<personal-access-token>

uvicorn app.main:app --port 8000 --reload
```

**Frontend**

```bash
cd frontend
npm install
npm run dev   # http://localhost:5173, proxies /api to :8000
```

## Automation coverage

Every requirement in a chosen profile is attempted. A check runs automatically when its `check.md` primary SQL only needs `{{ catalog }}`/`{{ schema }}` plus placeholders that have a documented, non-business default (SLA hours, tag key names, size thresholds — see `app/loader.py::DEFAULT_PLACEHOLDERS`). Checks that need a specific table/column or a business judgment call (e.g. `allowed_values`, `consistency_predicate`) are reported as `needs_target` instead of guessed. `access_optimization` is the one probe-mode check (no single SQL query covers it) and is executed as a `DESCRIBE DETAIL` loop over the schema's tables.

The `scan` profile (8 requirements, all schema-scoped) runs fully automatically end to end.
