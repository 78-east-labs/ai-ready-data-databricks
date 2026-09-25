# Fix: propagation_latency_compliance

Get late derived tables caught up, then align the writer's schedule with the SLA.

## Context

Propagation lag has four usual causes, and the diagnostic's `writer_last_state`, `writer_type` and `upstream_count` point at which one applies:

- **The writer failed.** `writer_last_state = 'FAILED'` (or `TIMEDOUT`, `UPSTREAM_FAILED`). Fix the failure, re-run.
- **The writer runs less often than the SLA.** A daily job cannot satisfy a 4-hour SLA. Tighten the schedule, or convert the table to a scheduled materialized view or streaming table so the schedule lives with the object.
- **The writer is not scheduled at all.** `writer_type` is `NOTEBOOK` or `QUERY`. Someone runs it by hand. That is the finding; the remedy is a job.
- **One slow source.** `upstream_count > 1` and the second diagnostic query shows one source written much later than the rest. The derived table's own writer may be fine; the lag belongs to the source, which needs the same treatment one hop upstream.

Nothing here rewrites data. Triggering a run or refresh requires `CAN MANAGE RUN` on the job, or ownership of the pipeline or object.

## Fix: Re-run the writer now

Job writer (`writer_type = 'JOB'`):

```bash
databricks jobs run-now --job-id {{ job_id }}
```

Pipeline writer (`writer_type = 'PIPELINE'`):

```bash
databricks pipelines start-update {{ pipeline_id }}
```

Streaming table or materialized view target:

```sql
REFRESH MATERIALIZED VIEW {{ catalog }}.{{ schema }}.{{ asset }}
```

```sql
REFRESH STREAMING TABLE {{ catalog }}.{{ schema }}.{{ asset }}
```

Verify by re-running the diagnostic after lineage catches up (allow a few hours), or immediately with the table's own log:

```sql
SELECT version, timestamp, operation, job.jobId AS job_id
FROM (DESCRIBE HISTORY {{ catalog }}.{{ schema }}.{{ asset }})
ORDER BY version DESC
LIMIT 3
```

## Fix: Tighten the writer's schedule

For a job, read the current schedule and set a new cron (Quartz syntax, six fields). This is a job settings change; confirm with the job owner because it changes compute cost.

```bash
databricks jobs get {{ job_id }} --output json | jq '.settings.schedule'

databricks jobs update --json '{
  "job_id": {{ job_id }},
  "new_settings": {
    "schedule": {
      "quartz_cron_expression": "0 0 */{{ interval_hours }} * * ?",
      "timezone_id": "UTC",
      "pause_status": "UNPAUSED"
    }
  }
}'
```

`jobs update` merges `new_settings` into the existing settings, so other fields are kept. Pick `{{ interval_hours }}` at or below `{{ sla_hours }}` minus the job's typical duration.

For a materialized view or streaming table (guard: `DESCRIBE EXTENDED` shows a `Refresh Schedule` row; use `ADD SCHEDULE` if it is empty):

```sql
ALTER MATERIALIZED VIEW {{ catalog }}.{{ schema }}.{{ asset }}
ALTER SCHEDULE EVERY {{ interval_hours }} HOURS
```

```sql
ALTER STREAMING TABLE {{ catalog }}.{{ schema }}.{{ asset }}
ALTER SCHEDULE EVERY {{ interval_hours }} HOURS
```

## Fix: Trigger the writer when the source changes instead of on a clock

Two options remove clock lag entirely. A job with a **table update trigger** runs as soon as the source table commits (the job must have `CAN MANAGE` for the user configuring it; the trigger monitors up to ten tables):

```bash
databricks jobs update --json '{
  "job_id": {{ job_id }},
  "new_settings": {
    "trigger": {
      "pause_status": "UNPAUSED",
      "table_update": {
        "table_names": ["{{ source_catalog }}.{{ source_schema }}.{{ source_asset }}"],
        "condition": "ANY_UPDATED",
        "min_time_between_triggers_seconds": 900
      }
    }
  }
}'
```

Or, when the derived table is a SQL transformation, a streaming table over the source with Change Data Feed on the source (see `change_detection/databricks/fix.md`) picks up each source commit on its own schedule:

```sql
CREATE STREAMING TABLE {{ catalog }}.{{ schema }}.{{ asset }}_st
SCHEDULE EVERY 1 HOUR
AS SELECT * FROM STREAM {{ source_catalog }}.{{ source_schema }}.{{ source_asset }}
```

Create under a new name, validate against the existing table, then repoint consumers. Do not replace the existing table in place.

## Fix: Investigate a failing writer

```bash
databricks jobs list-runs --job-id {{ job_id }} --limit 5 --output json \
  | jq '.runs[] | {run_id, state: .state.result_state, msg: .state.state_message}'
databricks jobs get-run-output --run-id {{ run_id }}
```

For pipelines:

```bash
databricks pipelines list-pipeline-events {{ pipeline_id }} --max-results 50
```

## Organizational guidance

Declare the SLA where the dependency is declared. Put `freshness_sla_hours` on derived tables (the same tag `data_freshness` reads) and make each writer's schedule a function of it in the deployment template (Databricks Asset Bundles `schedule` or `trigger.table_update`, dbt `+schedule` in Lakeflow-hosted projects, Terraform `databricks_job`). Prefer table-update triggers and streaming tables over cron for anything with an SLA under a day; cron schedules add up to one full interval of lag on top of runtime. Alert on the diagnostic's `LATE` and `NOT_PROPAGATED` rows with a Databricks SQL alert routed to the writer's owner, resolved from `system.lakeflow.jobs.run_as_user_id` or `system.lakeflow.pipelines.run_as`.
