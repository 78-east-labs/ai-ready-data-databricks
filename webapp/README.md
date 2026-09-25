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

## Deploy as a Databricks App

`backend/` is a self-contained Databricks App: `app.yaml` plus a FastAPI service that serves the built frontend's static files *and* the `/api/*` routes from one process/port, which is what Databricks Apps requires (a single command listening on one port).

**1. Build the frontend into the backend's static dir**

```bash
cd frontend
npm install
npm run build   # outputs to ../backend/static (see vite.config.ts)
```

**2. Create the app and bind a SQL warehouse resource**

In the workspace: **Compute > Apps > Create app** (or `databricks apps create <name>`), then on the app's **Resources** tab add a **SQL warehouse** resource with the key `sql-warehouse` — this is what `backend/app.yaml`'s `valueFrom: sql-warehouse` resolves to `DATABRICKS_WAREHOUSE_ID`. No other env vars are needed: Databricks injects `DATABRICKS_HOST`, `DATABRICKS_CLIENT_ID` and `DATABRICKS_CLIENT_SECRET` for the app's service principal automatically, and `app/databricks_client.py` uses them for OAuth M2M auth (via `databricks-sdk`'s `Config.authenticate`) instead of a personal access token. Grant that service principal `USE SCHEMA`/`SELECT` on whatever catalogs it will assess.

**3. Sync and deploy `backend/`**

```bash
databricks sync --watch backend/ /Workspace/Users/<you>/databricks_apps/<app-name>
databricks apps deploy <app-name> --source-code-path /Workspace/Users/<you>/databricks_apps/<app-name>
```

(Exact commands are also shown on the app's Overview page after you create it — copy them from there. `frontend/` is never uploaded; only `backend/`, including the `static/` folder built in step 1, is deployed.)

**Local dev that mirrors the App runtime**

```bash
databricks apps run-local --prepare-environment --debug
```
runs `backend/` the same way Databricks Apps does (env vars, port binding) for testing before you deploy.
