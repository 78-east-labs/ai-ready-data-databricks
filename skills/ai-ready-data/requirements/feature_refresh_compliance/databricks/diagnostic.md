# Diagnostic: feature_refresh_compliance

One row per streaming table and materialized view with its pipeline, last successful refresh, latest update outcome, failure streak and hours of staleness.

## Context

Reuses the check's population and pipeline resolution. Adds what the operator needs to decide between "trigger a refresh", "fix the failing pipeline" and "there is no schedule":

- `latest_state` / `latest_update_type`: whether the newest update failed, and whether it was a `REFRESH` or `FULL_REFRESH`.
- `consecutive_failures`: how many of the most recent updates failed in a row (a single failure with an older success is transient; a streak means the definition or its source is broken).
- `updates_7d`: refresh cadence. Zero updates in seven days for a non-continuous pipeline usually means no schedule.
- `pipeline_name` from `system.lakeflow.pipelines` (hidden Databricks SQL pipelines are named after the object).

Status values: `FRESH`, `STALE` (has a success, too old), `FAILING` (latest update failed and no success in window), `NO_PIPELINE_SEEN` (no lineage row from a pipeline in 30 days: never refreshed, or lineage lag). Sorted worst-first.

## SQL

```sql
WITH refreshables AS (
    SELECT LOWER(table_name) AS table_name,
           table_type,
           table_owner,
           created,
           CONCAT(LOWER('{{ catalog }}'), '.', LOWER('{{ schema }}'), '.', LOWER(table_name)) AS full_name
    FROM {{ catalog }}.information_schema.tables
    WHERE LOWER(table_schema) = LOWER('{{ schema }}')
      AND table_type IN ('STREAMING_TABLE', 'MATERIALIZED_VIEW')
),
table_pipeline AS (
    SELECT LOWER(target_table_full_name) AS full_name,
           entity_id                     AS pipeline_id,
           MAX(event_time)               AS last_pipeline_write,
           ROW_NUMBER() OVER (PARTITION BY LOWER(target_table_full_name) ORDER BY MAX(event_time) DESC) AS rn
    FROM system.access.table_lineage
    WHERE LOWER(target_catalog) = LOWER('{{ catalog }}')
      AND LOWER(target_schema)  = LOWER('{{ schema }}')
      AND entity_type = 'PIPELINE'
      AND event_time >= current_timestamp() - INTERVAL 30 DAYS
    GROUP BY LOWER(target_table_full_name), entity_id
),
updates_ranked AS (
    SELECT pipeline_id, update_id, update_type, result_state, period_start_time, period_end_time,
           ROW_NUMBER() OVER (PARTITION BY pipeline_id ORDER BY period_start_time DESC) AS rn
    FROM system.lakeflow.pipeline_update_timeline
    WHERE period_start_time >= current_timestamp() - INTERVAL 30 DAYS
),
updates AS (
    SELECT *,
           MIN(CASE WHEN result_state = 'COMPLETED' THEN rn END)
               OVER (PARTITION BY pipeline_id) AS first_success_rn
    FROM updates_ranked
),
per_pipeline AS (
    SELECT
        pipeline_id,
        MAX(CASE WHEN result_state = 'COMPLETED' THEN period_end_time END)   AS last_completed_at,
        MAX(CASE WHEN rn = 1 THEN result_state END)                          AS latest_state,
        MAX(CASE WHEN rn = 1 THEN update_type END)                           AS latest_update_type,
        MAX(CASE WHEN rn = 1 THEN period_start_time END)                     AS latest_started_at,
        COUNT_IF(period_start_time >= current_timestamp() - INTERVAL 7 DAYS) AS updates_7d,
        COUNT_IF(result_state = 'FAILED' AND rn < COALESCE(first_success_rn, 9999))
                                                                              AS consecutive_failures
    FROM updates
    GROUP BY pipeline_id
),
pipeline_names AS (
    SELECT pipeline_id, MAX(name) AS pipeline_name
    FROM system.lakeflow.pipelines
    WHERE delete_time IS NULL
    GROUP BY pipeline_id
)
SELECT
    r.table_name,
    r.table_type,
    r.table_owner,
    tp.pipeline_id,
    pn.pipeline_name,
    pp.last_completed_at,
    timestampdiff(HOUR, pp.last_completed_at, current_timestamp())   AS hours_since_success,
    pp.latest_state,
    pp.latest_update_type,
    pp.latest_started_at,
    COALESCE(pp.updates_7d, 0)                                        AS updates_7d,
    COALESCE(pp.consecutive_failures, 0)                              AS consecutive_failures,
    r.created,
    CASE
        WHEN tp.pipeline_id IS NULL THEN 'NO_PIPELINE_SEEN'
        WHEN pp.last_completed_at IS NOT NULL
             AND timestampdiff(HOUR, pp.last_completed_at, current_timestamp()) <= {{ staleness_hours }}
             THEN 'FRESH'
        WHEN pp.latest_state IS NULL
             AND timestampdiff(HOUR, pp.latest_started_at, current_timestamp()) <= {{ staleness_hours }}
             THEN 'FRESH'
        WHEN pp.last_completed_at IS NULL THEN 'FAILING'
        ELSE 'STALE'
    END AS status
FROM refreshables r
LEFT JOIN table_pipeline tp ON tp.full_name = r.full_name AND tp.rn = 1
LEFT JOIN per_pipeline   pp USING (pipeline_id)
LEFT JOIN pipeline_names pn USING (pipeline_id)
ORDER BY
    CASE status WHEN 'FAILING' THEN 0 WHEN 'NO_PIPELINE_SEEN' THEN 1 WHEN 'STALE' THEN 2 ELSE 3 END,
    hours_since_success DESC NULLS FIRST,
    r.table_name
```

### Refresh block for one object (per table)

```sql
DESCRIBE EXTENDED {{ catalog }}.{{ schema }}.{{ asset }}
```

Read the rows labelled `Refresh Schedule`, `Last Refresh`, `Latest Refresh Status` and `Latest Refresh Type`. An empty `Refresh Schedule` on a materialized view created in a warehouse means it only refreshes when someone runs `REFRESH MATERIALIZED VIEW`.
