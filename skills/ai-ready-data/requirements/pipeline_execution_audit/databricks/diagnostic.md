# Diagnostic: pipeline_execution_audit

One row per job or pipeline that wrote to the schema in the window, with its name, the tables it wrote, how many runs the Lakeflow tables recorded, the latest result, and an audit status, unrecorded writers first.

## Context

Same writer population as the check (`JOB` / `PIPELINE` entities from `system.access.table_lineage`, `{{ lookback_days }}` default 30). Names come from `system.lakeflow.jobs` and `system.lakeflow.pipelines`, which are slowly changing tables with one row per change; the latest row per id (by `change_time`) is used and deleted objects (`delete_time IS NOT NULL`) are labelled. Run counts and results come from `job_run_timeline` / `pipeline_update_timeline`.

`audit_status`:

- `NO_RUN_RECORD`: lineage saw the writer but no timeline row exists in the window (Lakeflow tables not enabled for that workspace, lag, or a job id from another workspace on the same metastore)
- `RUNNING_ONLY`: timeline rows exist but none has a terminal `result_state` yet
- `DELETED_ENTITY`: runs are recorded but the job or pipeline has since been deleted (history is still immutable, the definition is gone)
- `AUDITED`: at least one terminal run recorded

`failed_runs` and `last_result_state` are shown because an audited pipeline that failed on its last run is a freshness problem worth flagging even though it passes this check. The second query lists the notebook and anonymous writers the check excludes, so the report can say how much of the schema is written by nothing auditable at all.

## SQL

### Writers and their run records (primary)

```sql
WITH writers AS (
    SELECT
        UPPER(entity_type)                                  AS entity_type,
        CAST(entity_id AS STRING)                           AS entity_id,
        COUNT(DISTINCT entity_run_id)                       AS distinct_runs_in_lineage,
        COUNT(*)                                            AS write_events,
        array_sort(collect_set(LOWER(target_table_name)))   AS tables_written,
        MAX(event_time)                                     AS last_write
    FROM system.access.table_lineage
    WHERE LOWER(target_catalog) = LOWER('{{ catalog }}')
      AND LOWER(target_schema)  = LOWER('{{ schema }}')
      AND target_table_full_name IS NOT NULL
      AND UPPER(entity_type) IN ('JOB', 'PIPELINE')
      AND entity_id IS NOT NULL
      AND event_time >= current_timestamp() - INTERVAL {{ lookback_days }} DAYS
    GROUP BY UPPER(entity_type), CAST(entity_id AS STRING)
),
job_names AS (
    SELECT CAST(job_id AS STRING) AS entity_id, name, description, delete_time
    FROM (
        SELECT job_id, name, description, delete_time,
               ROW_NUMBER() OVER (PARTITION BY job_id ORDER BY change_time DESC) AS rn
        FROM system.lakeflow.jobs
    ) WHERE rn = 1
),
pipeline_names AS (
    SELECT CAST(pipeline_id AS STRING) AS entity_id, name, delete_time
    FROM (
        SELECT pipeline_id, name, delete_time,
               ROW_NUMBER() OVER (PARTITION BY pipeline_id ORDER BY change_time DESC) AS rn
        FROM system.lakeflow.pipelines
    ) WHERE rn = 1
),
job_runs AS (
    SELECT CAST(job_id AS STRING)                         AS entity_id,
           COUNT(DISTINCT run_id)                         AS runs_recorded,
           COUNT(DISTINCT CASE WHEN result_state IS NOT NULL THEN run_id END) AS terminal_runs,
           COUNT(DISTINCT CASE WHEN result_state IN ('FAILED','TIMEDOUT','UPSTREAM_FAILED')
                               THEN run_id END)           AS failed_runs,
           MAX(period_end_time)                           AS last_run_end,
           MAX_BY(result_state, period_end_time)          AS last_result_state
    FROM system.lakeflow.job_run_timeline
    WHERE period_end_time >= current_timestamp() - INTERVAL {{ lookback_days }} DAYS
    GROUP BY job_id
),
pipeline_updates AS (
    SELECT CAST(pipeline_id AS STRING)                    AS entity_id,
           COUNT(DISTINCT update_id)                      AS runs_recorded,
           COUNT(DISTINCT CASE WHEN result_state IS NOT NULL THEN update_id END) AS terminal_runs,
           COUNT(DISTINCT CASE WHEN result_state IN ('FAILED','TIMEDOUT','UPSTREAM_FAILED')
                               THEN update_id END)        AS failed_runs,
           MAX(period_end_time)                           AS last_run_end,
           MAX_BY(result_state, period_end_time)          AS last_result_state
    FROM system.lakeflow.pipeline_update_timeline
    WHERE period_end_time >= current_timestamp() - INTERVAL {{ lookback_days }} DAYS
    GROUP BY pipeline_id
)
SELECT
    w.entity_type,
    w.entity_id,
    COALESCE(jn.name, pn.name)                                          AS entity_name,
    COALESCE(jn.delete_time, pn.delete_time) IS NOT NULL                AS entity_deleted,
    w.tables_written,
    w.write_events,
    w.distinct_runs_in_lineage,
    w.last_write,
    COALESCE(jr.runs_recorded,  pu.runs_recorded,  0)                   AS runs_recorded,
    COALESCE(jr.terminal_runs,  pu.terminal_runs,  0)                   AS terminal_runs,
    COALESCE(jr.failed_runs,    pu.failed_runs,    0)                   AS failed_runs,
    COALESCE(jr.last_run_end,   pu.last_run_end)                        AS last_run_end,
    COALESCE(jr.last_result_state, pu.last_result_state)                AS last_result_state,
    CASE
        WHEN COALESCE(jr.runs_recorded, pu.runs_recorded, 0) = 0        THEN 'NO_RUN_RECORD'
        WHEN COALESCE(jr.terminal_runs, pu.terminal_runs, 0) = 0        THEN 'RUNNING_ONLY'
        WHEN COALESCE(jn.delete_time, pn.delete_time) IS NOT NULL       THEN 'DELETED_ENTITY'
        ELSE 'AUDITED'
    END                                                                 AS audit_status
FROM writers w
LEFT JOIN job_names        jn ON w.entity_type = 'JOB'      AND w.entity_id = jn.entity_id
LEFT JOIN pipeline_names   pn ON w.entity_type = 'PIPELINE' AND w.entity_id = pn.entity_id
LEFT JOIN job_runs         jr ON w.entity_type = 'JOB'      AND w.entity_id = jr.entity_id
LEFT JOIN pipeline_updates pu ON w.entity_type = 'PIPELINE' AND w.entity_id = pu.entity_id
ORDER BY
    CASE
        WHEN COALESCE(jr.runs_recorded, pu.runs_recorded, 0) = 0        THEN 0
        WHEN COALESCE(jr.terminal_runs, pu.terminal_runs, 0) = 0        THEN 1
        WHEN COALESCE(jn.delete_time, pn.delete_time) IS NOT NULL       THEN 2
        ELSE 3
    END ASC,
    w.write_events DESC,
    w.entity_id
```

### Writers outside the auditable population (variant)

Notebook, dashboard, query and anonymous writers to the schema, per entity, so the report can quantify what the primary check does not see.

```sql
SELECT
    COALESCE(UPPER(entity_type), 'NONE')                 AS entity_type,
    entity_id,
    user_identity.email                                  AS run_as,
    array_sort(collect_set(LOWER(target_table_name)))    AS tables_written,
    COUNT(*)                                             AS write_events,
    MAX(event_time)                                      AS last_write
FROM system.access.table_lineage
WHERE LOWER(target_catalog) = LOWER('{{ catalog }}')
  AND LOWER(target_schema)  = LOWER('{{ schema }}')
  AND target_table_full_name IS NOT NULL
  AND (entity_type IS NULL OR UPPER(entity_type) NOT IN ('JOB', 'PIPELINE'))
  AND event_time >= current_timestamp() - INTERVAL {{ lookback_days }} DAYS
GROUP BY 1, 2, 3
ORDER BY write_events DESC, last_write DESC
LIMIT 100
```
