# Fix: pipeline_execution_audit

Make sure every writer to the schema runs as a Lakeflow job or pipeline and that the Lakeflow system tables recording those runs are enabled and readable.

## Context

The run record is produced automatically for anything that executes as a job run or a pipeline update. The gaps come from three places, and the diagnostic tells them apart:

- **Writers that are not jobs or pipelines** (the variant query). Interactive notebooks, SQL editor sessions, external orchestrators over JDBC. These have no run timeline at all. The fix is to schedule or trigger them as jobs; if an external orchestrator (Airflow, Dagster, ADF) must stay in charge, have it trigger a Databricks job via the Jobs API instead of running SQL directly, so the run still lands in `job_run_timeline`.
- **`NO_RUN_RECORD` writers**. The job exists and lineage saw it, but no timeline rows. Usually `system.lakeflow` is not enabled, or the assessment principal cannot read it, or the job lives in a workspace that shares the metastore but has not had the schema enabled. Lag of an hour or two is also possible for very recent runs.
- **`DELETED_ENTITY`**. The history is intact (system tables are append-only) but the definition is gone. Nothing to fix for the audit; note it for provenance, because the code that produced the data can no longer be inspected.

Nothing here touches table data. `system.lakeflow.*` retention is governed by Databricks (currently one year); export it if you need longer.

## Fix: Enable and grant the Lakeflow system tables

Run as a metastore admin. Enabling an already-enabled schema returns an error that can be ignored.

```bash
databricks system-schemas enable {{ metastore_id }} lakeflow
databricks system-schemas list {{ metastore_id }}
```

```sql
GRANT USE SCHEMA ON SCHEMA system.lakeflow TO `{{ assessment_principal }}`;
GRANT SELECT ON TABLE system.lakeflow.jobs                     TO `{{ assessment_principal }}`;
GRANT SELECT ON TABLE system.lakeflow.job_run_timeline         TO `{{ assessment_principal }}`;
GRANT SELECT ON TABLE system.lakeflow.pipelines                TO `{{ assessment_principal }}`;
GRANT SELECT ON TABLE system.lakeflow.pipeline_update_timeline TO `{{ assessment_principal }}`;
```

## Fix: Turn an interactive writer into a job

Create the job once (check the name first, `jobs create` is not idempotent), then run it on a schedule or on demand. Every run produces `job_run_timeline` rows and lineage with `entity_type = 'JOB'`.

```bash
if [ -z "$(databricks jobs list --name '{{ job_name }}' --output json | jq -r '.[]?.job_id')" ]; then
  databricks jobs create --json '{
    "name": "{{ job_name }}",
    "description": "Loads {{ catalog }}.{{ schema }}.{{ asset }} from {{ source_description }}",
    "tasks": [{
      "task_key": "load",
      "notebook_task": {"notebook_path": "{{ notebook_path }}"},
      "environment_key": "default"
    }],
    "environments": [{"environment_key": "default", "spec": {"client": "2"}}],
    "schedule": {"quartz_cron_expression": "{{ cron }}", "timezone_id": "UTC"}
  }'
fi
```

For a SQL-only step, replace `notebook_task` with `sql_task` (`{"sql_task": {"file": {"path": "{{ sql_file_path }}"}, "warehouse_id": "{{ warehouse_id }}"}}`) so the statement also appears in `system.query.history` with `query_source.job_info` populated.

## Fix: Trigger from an external orchestrator through the Jobs API

Instead of the orchestrator opening a SQL connection and writing directly, have it start the Databricks job and wait. The run is then attributed and recorded regardless of where the schedule lives.

```bash
databricks jobs run-now {{ job_id }} --json '{"job_parameters": {"run_date": "{{ run_date }}"}}'
```

Airflow's `DatabricksRunNowOperator`, Dagster's `databricks_pyspark_step_launcher` and ADF's Databricks Notebook activity all wrap this call.

## Fix: Verify a writer is now audited

After the next run and the system-table lag, confirm the join the check relies on:

```sql
SELECT r.run_id, r.period_start_time, r.period_end_time, r.result_state, r.termination_code
FROM system.lakeflow.job_run_timeline r
WHERE CAST(r.job_id AS STRING) = '{{ job_id }}'
  AND r.period_end_time >= current_timestamp() - INTERVAL 7 DAYS
ORDER BY r.period_end_time DESC
LIMIT 20
```

## Organizational guidance

Make "writes to governed schemas come from jobs or pipelines" a grant policy, not a convention: give `MODIFY` on those schemas only to the service principals that jobs run as, and leave interactive users with `SELECT`. Put a `description` on every job (the same field `transformation_documentation` reads) and a stable naming scheme (`<domain>__<target_table>__load`) so the diagnostic's `entity_name` column is self-explanatory. Export `system.lakeflow.job_run_timeline` and `system.access.table_lineage` to a long-retention table if the audit period the organization needs exceeds the system-table retention.
