# Check: pipeline_execution_audit

Fraction of jobs and pipelines that wrote to the schema in the window whose runs are recorded, with a terminal result, in the Lakeflow system tables (`system.lakeflow.job_run_timeline` / `system.lakeflow.pipeline_update_timeline`).

## Context

An execution audit answers "which run produced this data, when did it start and end, and did it succeed". On Databricks that record is split across two places. `system.access.table_lineage` says which entity wrote a table (`entity_type`, `entity_id`, `entity_run_id`). `system.lakeflow.job_run_timeline` (one row per job run period: `job_id`, `run_id`, `period_start_time`, `period_end_time`, `result_state`, `termination_code`) and `system.lakeflow.pipeline_update_timeline` (`pipeline_id`, `update_id`, `update_type`, `result_state`) hold the immutable run history. The check joins the two: a writer is audited when its id from lineage has at least one run row in the window with a non-null `result_state`.

Population: distinct writer entities of type `JOB` or `PIPELINE` (matched case-insensitively) with a non-null `entity_id` that wrote into `{{ catalog }}.{{ schema }}` in the last `{{ lookback_days }}` days (default 30). Writers of other types (`NOTEBOOK`, `QUERY`, `DASHBOARD`, NULL) are not in the population because they have no run timeline; `agent_attribution` scores them. If the schema is written only by ad hoc notebooks the check returns NULL, and that absence is itself the finding: nothing writing here is auditable.

`entity_id` is a string; `job_id` and `pipeline_id` are stored as strings in the Lakeflow tables, and both sides are cast to `STRING` to be safe. The join is on id only, not workspace, so a job id reused across workspaces attached to the same metastore could match the wrong workspace's runs; this is rare and the diagnostic shows the run names so it can be spotted.

The signal is native. It proves the run record exists in a system table that users cannot edit. It does not prove that every write came from an audited run: a job can write once from a run and once from an interactive notebook. The run-level variant below tightens that.

`table_lineage` lags by up to a few hours, `system.lakeflow.*` by minutes to hours; writers whose only runs are very recent can appear unaudited for a while. Reading needs `SELECT` on `system.access.table_lineage`, `system.lakeflow.job_run_timeline` and `system.lakeflow.pipeline_update_timeline`, and the `lakeflow` schema must be enabled. Returns NULL (N/A) when no job or pipeline wrote to the schema in the window.

## SQL

### Writers with a recorded terminal run (primary)

```sql
WITH writers AS (
    SELECT DISTINCT
        UPPER(entity_type)      AS entity_type,
        CAST(entity_id AS STRING) AS entity_id
    FROM system.access.table_lineage
    WHERE LOWER(target_catalog) = LOWER('{{ catalog }}')
      AND LOWER(target_schema)  = LOWER('{{ schema }}')
      AND target_table_full_name IS NOT NULL
      AND UPPER(entity_type) IN ('JOB', 'PIPELINE')
      AND entity_id IS NOT NULL
      AND event_time >= current_timestamp() - INTERVAL {{ lookback_days }} DAYS
),
job_runs AS (
    SELECT DISTINCT CAST(job_id AS STRING) AS entity_id
    FROM system.lakeflow.job_run_timeline
    WHERE period_end_time >= current_timestamp() - INTERVAL {{ lookback_days }} DAYS
      AND result_state IS NOT NULL
),
pipeline_updates AS (
    SELECT DISTINCT CAST(pipeline_id AS STRING) AS entity_id
    FROM system.lakeflow.pipeline_update_timeline
    WHERE period_end_time >= current_timestamp() - INTERVAL {{ lookback_days }} DAYS
      AND result_state IS NOT NULL
)
SELECT
    COUNT_IF(jr.entity_id IS NOT NULL OR pu.entity_id IS NOT NULL)          AS audited_writers,
    COUNT(*)                                                                AS total_writers,
    COUNT_IF(jr.entity_id IS NOT NULL OR pu.entity_id IS NOT NULL)::DOUBLE
        / NULLIF(COUNT(*), 0)                                               AS value
FROM writers w
LEFT JOIN job_runs          jr ON w.entity_type = 'JOB'      AND w.entity_id = jr.entity_id
LEFT JOIN pipeline_updates  pu ON w.entity_type = 'PIPELINE' AND w.entity_id = pu.entity_id
```

### Write events matched to a specific run (variant)

Stricter and per event: the fraction of job/pipeline write events whose `entity_run_id` matches a `run_id` (jobs) or `update_id` (pipelines) with a terminal state. This catches runs that lineage saw but the timeline has not recorded (lag, or a run in a workspace whose Lakeflow tables are not enabled). Same population filter, different unit.

```sql
WITH write_events AS (
    SELECT DISTINCT
        target_table_full_name,
        event_time,
        UPPER(entity_type)            AS entity_type,
        CAST(entity_id AS STRING)     AS entity_id,
        CAST(entity_run_id AS STRING) AS entity_run_id
    FROM system.access.table_lineage
    WHERE LOWER(target_catalog) = LOWER('{{ catalog }}')
      AND LOWER(target_schema)  = LOWER('{{ schema }}')
      AND target_table_full_name IS NOT NULL
      AND UPPER(entity_type) IN ('JOB', 'PIPELINE')
      AND entity_id IS NOT NULL
      AND event_time >= current_timestamp() - INTERVAL {{ lookback_days }} DAYS
    ORDER BY event_time DESC
    LIMIT 100000
),
job_runs AS (
    SELECT DISTINCT CAST(job_id AS STRING) AS entity_id, CAST(run_id AS STRING) AS run_id
    FROM system.lakeflow.job_run_timeline
    WHERE period_end_time >= current_timestamp() - INTERVAL {{ lookback_days }} DAYS
      AND result_state IS NOT NULL
),
pipeline_updates AS (
    SELECT DISTINCT CAST(pipeline_id AS STRING) AS entity_id, CAST(update_id AS STRING) AS run_id
    FROM system.lakeflow.pipeline_update_timeline
    WHERE period_end_time >= current_timestamp() - INTERVAL {{ lookback_days }} DAYS
      AND result_state IS NOT NULL
)
SELECT
    COUNT_IF(jr.run_id IS NOT NULL OR pu.run_id IS NOT NULL)          AS audited_write_events,
    COUNT(*)                                                          AS total_write_events,
    COUNT_IF(jr.run_id IS NOT NULL OR pu.run_id IS NOT NULL)::DOUBLE
        / NULLIF(COUNT(*), 0)                                         AS value
FROM write_events w
LEFT JOIN job_runs         jr ON w.entity_type = 'JOB'      AND w.entity_id = jr.entity_id AND w.entity_run_id = jr.run_id
LEFT JOIN pipeline_updates pu ON w.entity_type = 'PIPELINE' AND w.entity_id = pu.entity_id AND w.entity_run_id = pu.run_id
```

For multi-task jobs, `entity_run_id` in lineage may be the task run id while `job_run_timeline.run_id` is the parent job run id. If this variant scores far below the primary on a schema written by multi-task jobs, that mismatch is the likely reason; confirm with `SELECT entity_run_id FROM system.access.table_lineage WHERE UPPER(entity_type) = 'JOB' LIMIT 5` against `SELECT run_id FROM system.lakeflow.job_run_timeline WHERE job_id = '<id>' ORDER BY period_start_time DESC LIMIT 5`, and prefer the primary if they do not line up.

### All writes, scored by whether they came from an auditable entity (variant)

Broadest framing: of every distinct write event to the schema (any entity type or none), the fraction produced by a job or pipeline with a recorded terminal run. This penalizes notebook and anonymous writes, which the primary excludes. Use it when the question is "how much of what lands here is auditable", not "are our pipelines recorded".

```sql
WITH write_events AS (
    SELECT DISTINCT
        target_table_full_name,
        event_time,
        UPPER(entity_type)        AS entity_type,
        CAST(entity_id AS STRING) AS entity_id
    FROM system.access.table_lineage
    WHERE LOWER(target_catalog) = LOWER('{{ catalog }}')
      AND LOWER(target_schema)  = LOWER('{{ schema }}')
      AND target_table_full_name IS NOT NULL
      AND event_time >= current_timestamp() - INTERVAL {{ lookback_days }} DAYS
    ORDER BY event_time DESC
    LIMIT 100000
),
audited_entities AS (
    SELECT 'JOB' AS entity_type, CAST(job_id AS STRING) AS entity_id
    FROM system.lakeflow.job_run_timeline
    WHERE period_end_time >= current_timestamp() - INTERVAL {{ lookback_days }} DAYS
      AND result_state IS NOT NULL
    GROUP BY job_id
    UNION ALL
    SELECT 'PIPELINE', CAST(pipeline_id AS STRING)
    FROM system.lakeflow.pipeline_update_timeline
    WHERE period_end_time >= current_timestamp() - INTERVAL {{ lookback_days }} DAYS
      AND result_state IS NOT NULL
    GROUP BY pipeline_id
)
SELECT
    COUNT_IF(a.entity_id IS NOT NULL)            AS audited_write_events,
    COUNT(*)                                      AS total_write_events,
    COUNT_IF(a.entity_id IS NOT NULL)::DOUBLE
        / NULLIF(COUNT(*), 0)                     AS value
FROM write_events w
LEFT JOIN audited_entities a
       ON w.entity_type = a.entity_type AND w.entity_id = a.entity_id
```
