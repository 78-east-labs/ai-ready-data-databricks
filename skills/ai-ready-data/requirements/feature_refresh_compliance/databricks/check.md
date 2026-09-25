# Check: feature_refresh_compliance

Fraction of streaming tables and materialized views in the schema whose last successful refresh completed within `{{ staleness_hours }}` hours.

## Context

On Databricks, served features and other declaratively refreshed assets are streaming tables (`table_type = 'STREAMING_TABLE'`) and materialized views (`table_type = 'MATERIALIZED_VIEW'`). Both are maintained by a Lakeflow pipeline: one the user created, or a hidden one Databricks SQL created when the object was defined in a warehouse. Each refresh is a pipeline update, and updates are recorded in `system.lakeflow.pipeline_update_timeline` with `pipeline_id`, `update_id`, `period_start_time`, `period_end_time`, `update_type` and `result_state`.

The check needs to know which pipeline maintains which table. `information_schema.tables` does not carry the pipeline id. `system.access.table_lineage` does: the pipeline's writes to the table produce rows with `target_table_full_name` = the table, `entity_type = 'PIPELINE'` and `entity_id` = the pipeline id. The primary variant joins through lineage. It inherits lineage lag (up to a few hours) and misses objects that have never completed a refresh (no write, no lineage row), which is itself a failure.

A table passes when its pipeline has at least one update with `result_state = 'COMPLETED'` whose `period_end_time` is within the staleness window. `result_state` values in this table are believed to be `COMPLETED`, `FAILED`, `CANCELED` and a small set of in-progress states; confirm with `SELECT DISTINCT result_state FROM system.lakeflow.pipeline_update_timeline` if the count looks wrong. For a continuous pipeline the latest update may still be running; a running update with a `period_start_time` inside the window also counts as fresh, which the SQL handles by accepting `result_state IS NULL OR result_state = 'COMPLETED'` on the newest row. If your environment uses a different success token the query says so in its output (`observed_states`).

`{{ staleness_hours }}` defaults to 24.

Strength is **native**: pipeline updates are the refresh events themselves. `system.lakeflow.pipeline_update_timeline` may still be in preview in some regions and needs `SELECT` on `system.lakeflow`.

Returns NULL (N/A) when the schema contains no streaming tables or materialized views.

## SQL

### Pipeline update timeline via lineage (primary)

```sql
WITH refreshables AS (
    SELECT LOWER(table_name) AS table_name,
           table_type,
           CONCAT(LOWER('{{ catalog }}'), '.', LOWER('{{ schema }}'), '.', LOWER(table_name)) AS full_name
    FROM {{ catalog }}.information_schema.tables
    WHERE LOWER(table_schema) = LOWER('{{ schema }}')
      AND table_type IN ('STREAMING_TABLE', 'MATERIALIZED_VIEW')
),
table_pipeline AS (
    SELECT LOWER(target_table_full_name) AS full_name,
           entity_id                     AS pipeline_id,
           ROW_NUMBER() OVER (PARTITION BY LOWER(target_table_full_name) ORDER BY MAX(event_time) DESC) AS rn
    FROM system.access.table_lineage
    WHERE LOWER(target_catalog) = LOWER('{{ catalog }}')
      AND LOWER(target_schema)  = LOWER('{{ schema }}')
      AND entity_type = 'PIPELINE'
      AND event_time >= current_timestamp() - INTERVAL 30 DAYS
    GROUP BY LOWER(target_table_full_name), entity_id
),
latest_update AS (
    SELECT pipeline_id, update_type, result_state, period_start_time, period_end_time
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY pipeline_id ORDER BY period_start_time DESC) AS rn
        FROM system.lakeflow.pipeline_update_timeline
        WHERE period_start_time >= current_timestamp() - INTERVAL 30 DAYS
    )
    WHERE rn = 1
),
last_success AS (
    SELECT pipeline_id, MAX(period_end_time) AS last_completed_at
    FROM system.lakeflow.pipeline_update_timeline
    WHERE result_state = 'COMPLETED'
      AND period_start_time >= current_timestamp() - INTERVAL 30 DAYS
    GROUP BY pipeline_id
),
scored AS (
    SELECT
        r.table_name,
        r.table_type,
        tp.pipeline_id,
        ls.last_completed_at,
        lu.result_state AS latest_state,
        lu.period_start_time AS latest_started_at,
        (
            timestampdiff(HOUR, ls.last_completed_at, current_timestamp()) <= {{ staleness_hours }}
            OR (lu.result_state IS NULL
                AND timestampdiff(HOUR, lu.period_start_time, current_timestamp()) <= {{ staleness_hours }})
        ) AS is_fresh
    FROM refreshables r
    LEFT JOIN table_pipeline tp ON tp.full_name = r.full_name AND tp.rn = 1
    LEFT JOIN latest_update lu USING (pipeline_id)
    LEFT JOIN last_success  ls USING (pipeline_id)
)
SELECT
    COUNT_IF(is_fresh)                                  AS fresh_refreshables,
    COUNT(*)                                            AS total_refreshables,
    COUNT_IF(is_fresh)::DOUBLE / NULLIF(COUNT(*), 0)    AS value,
    array_sort(collect_set(latest_state))               AS observed_states
FROM scored
```

`observed_states` is informational: if it contains a success-looking token other than `COMPLETED`, adjust the `last_success` filter.

### Delta history of the backing table (variant, probe mode)

Sees the refresh directly in the object's own transaction log, without lineage lag. Streaming tables and materialized views are backed by Delta tables and accept `DESCRIBE HISTORY` on current warehouses; if a warehouse rejects the statement for a materialized view, use the `DESCRIBE EXTENDED` variant below.

**(a) Enumerate:** the `refreshables` CTE above.

**(b) Probe:**

```sql
DESCRIBE HISTORY {{ catalog }}.{{ schema }}.{{ asset }} LIMIT 20
```

**(c) Predicate.** In words: the newest commit whose operation is a refresh write is within the window. Pipeline refreshes appear as `STREAMING UPDATE` (streaming tables and incremental MV refreshes), `WRITE`, `MERGE`, `CREATE OR REPLACE TABLE AS SELECT`, `REPLACE TABLE AS SELECT` or `CREATE TABLE AS SELECT` (full MV recompute). Over the probe rows:

```sql
timestampdiff(HOUR,
    MAX(CASE WHEN operation IN ('STREAMING UPDATE', 'WRITE', 'MERGE', 'UPDATE', 'DELETE',
                                'CREATE TABLE AS SELECT', 'CREATE OR REPLACE TABLE AS SELECT',
                                'REPLACE TABLE AS SELECT')
             THEN timestamp END),
    current_timestamp()) <= {{ staleness_hours }}
```

A refresh that ran but found no new upstream data may not write a commit at all (incremental MV with nothing to do). Such an object looks stale here while the timeline variant shows a `COMPLETED` update; prefer the primary variant when both are available.

**(d) Aggregation:** `value = passing / probed`, NULL when nothing was probed.

### Refresh information from DESCRIBE EXTENDED (variant, probe mode)

Databricks SQL exposes a refresh block on materialized views and streaming tables through `DESCRIBE EXTENDED`. It is a text table (`col_name`, `data_type`, `comment`), so the row labels are matched by name.

**(b) Probe:**

```sql
DESCRIBE EXTENDED {{ catalog }}.{{ schema }}.{{ asset }}
```

**(c) Predicate.** Find the rows whose `col_name` is `Last Refresh` (a timestamp), `Latest Refresh Status` and `Refresh Schedule`. Pass when the status row is a success value and the timestamp is within the window:

```sql
timestampdiff(HOUR,
    TRY_CAST(MAX(CASE WHEN col_name = 'Last Refresh' THEN data_type END) AS TIMESTAMP),
    current_timestamp()) <= {{ staleness_hours }}
AND LOWER(MAX(CASE WHEN col_name = 'Latest Refresh Status' THEN data_type END)) IN ('completed', 'success', 'succeeded')
```

The exact labels have changed across releases (`Last Refresh`, `Latest Refresh Status`, `Latest Refresh Type`, `Refresh Schedule`); run the probe on one object first and adjust the matched labels to what your workspace prints.

**(d) Aggregation:** as above.
