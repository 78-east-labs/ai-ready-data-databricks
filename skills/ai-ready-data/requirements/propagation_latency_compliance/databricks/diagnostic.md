# Diagnostic: propagation_latency_compliance

One row per derived table with its slowest upstream source, the lag between that source's last write and the table's last write, the writer entity, and the writer's most recent run outcome.

## Context

Reuses the check's edge and last-write logic. The extra fields are what an operator needs to decide whether to re-run the writer, reschedule it, or fix the source:

- `slowest_source` and `source_last_write`: the upstream that most recently changed; lag is measured against it.
- `lag_hours`: hours between that source write and the table's last write. Negative means the table was written after the source (fresh). `NULL` with a non-null source means the table has no write in the window.
- `upstream_count`: how many distinct sources feed the table; tables with many sources are more likely to be late because of one slow feed, and the second query lists every edge for those.
- `writer_type` / `writer_id` / `writer_name`: the JOB or PIPELINE that last wrote the table, resolved from `system.lakeflow.jobs` or `system.lakeflow.pipelines`.
- `writer_last_state`: the result of that writer's most recent run, from `job_run_timeline` or `pipeline_update_timeline`. A `FAILED` state explains the lag by itself.

Status: `WITHIN_SLA`, `LATE` (lag exceeds SLA), `NOT_PROPAGATED` (source written, table not written in window), `SOURCE_IDLE` (no source write in window, passes vacuously). Sorted worst-first, largest lag first.

## SQL

### Per-table lag and writer

```sql
WITH tables_in_scope AS (
    SELECT LOWER(table_name) AS table_name, table_type, table_owner,
           CONCAT(LOWER('{{ catalog }}'), '.', LOWER('{{ schema }}'), '.', LOWER(table_name)) AS full_name
    FROM {{ catalog }}.information_schema.tables
    WHERE LOWER(table_schema) = LOWER('{{ schema }}')
      AND table_type IN ('MANAGED', 'EXTERNAL', 'STREAMING_TABLE', 'MATERIALIZED_VIEW')
),
edges AS (
    SELECT DISTINCT LOWER(target_table_full_name) AS target_full_name,
                    LOWER(source_table_full_name) AS source_full_name
    FROM system.access.table_lineage
    WHERE LOWER(target_catalog) = LOWER('{{ catalog }}')
      AND LOWER(target_schema)  = LOWER('{{ schema }}')
      AND target_table_full_name IS NOT NULL
      AND source_table_full_name IS NOT NULL
      AND LOWER(source_table_full_name) <> LOWER(target_table_full_name)
      AND event_time >= current_timestamp() - INTERVAL {{ lookback_days }} DAYS
),
last_write AS (
    SELECT LOWER(target_table_full_name) AS full_name, MAX(event_time) AS last_write_at
    FROM system.access.table_lineage
    WHERE target_table_full_name IS NOT NULL
      AND event_time >= current_timestamp() - INTERVAL {{ lookback_days }} DAYS
      AND LOWER(target_table_full_name) IN (
            SELECT full_name FROM tables_in_scope
            UNION SELECT source_full_name FROM edges)
    GROUP BY LOWER(target_table_full_name)
),
slowest AS (
    SELECT e.target_full_name AS full_name,
           COUNT(DISTINCT e.source_full_name)                          AS upstream_count,
           MAX(sw.last_write_at)                                       AS source_last_write,
           MAX_BY(e.source_full_name, sw.last_write_at)                AS slowest_source
    FROM edges e
    LEFT JOIN last_write sw ON sw.full_name = e.source_full_name
    GROUP BY e.target_full_name
),
writer AS (
    SELECT LOWER(target_table_full_name) AS full_name, entity_type, entity_id,
           ROW_NUMBER() OVER (PARTITION BY LOWER(target_table_full_name) ORDER BY MAX(event_time) DESC) AS rn
    FROM system.access.table_lineage
    WHERE LOWER(target_catalog) = LOWER('{{ catalog }}')
      AND LOWER(target_schema)  = LOWER('{{ schema }}')
      AND target_table_full_name IS NOT NULL
      AND event_time >= current_timestamp() - INTERVAL {{ lookback_days }} DAYS
    GROUP BY LOWER(target_table_full_name), entity_type, entity_id
),
job_state AS (
    SELECT CAST(job_id AS STRING) AS entity_id, MAX_BY(result_state, period_start_time) AS last_state,
           MAX(period_start_time) AS last_run_at
    FROM system.lakeflow.job_run_timeline
    WHERE period_start_time >= current_timestamp() - INTERVAL {{ lookback_days }} DAYS
    GROUP BY job_id
),
pipeline_state AS (
    SELECT pipeline_id AS entity_id, MAX_BY(result_state, period_start_time) AS last_state,
           MAX(period_start_time) AS last_run_at
    FROM system.lakeflow.pipeline_update_timeline
    WHERE period_start_time >= current_timestamp() - INTERVAL {{ lookback_days }} DAYS
    GROUP BY pipeline_id
),
names AS (
    SELECT CAST(job_id AS STRING) AS entity_id, MAX(name) AS name
    FROM system.lakeflow.jobs WHERE delete_time IS NULL GROUP BY job_id
    UNION ALL
    SELECT pipeline_id, MAX(name)
    FROM system.lakeflow.pipelines WHERE delete_time IS NULL GROUP BY pipeline_id
)
SELECT
    t.table_name,
    t.table_type,
    t.table_owner,
    s.upstream_count,
    s.slowest_source,
    s.source_last_write,
    tw.last_write_at                                                       AS target_last_write,
    timestampdiff(HOUR, s.source_last_write, tw.last_write_at)             AS lag_hours,
    w.entity_type                                                          AS writer_type,
    w.entity_id                                                            AS writer_id,
    n.name                                                                 AS writer_name,
    COALESCE(js.last_state, ps.last_state)                                 AS writer_last_state,
    COALESCE(js.last_run_at, ps.last_run_at)                               AS writer_last_run_at,
    CASE
        WHEN s.source_last_write IS NULL THEN 'SOURCE_IDLE'
        WHEN tw.last_write_at IS NULL    THEN 'NOT_PROPAGATED'
        WHEN tw.last_write_at >= s.source_last_write - INTERVAL {{ sla_hours }} HOURS THEN 'WITHIN_SLA'
        ELSE 'LATE'
    END                                                                    AS status
FROM tables_in_scope t
JOIN slowest s          ON s.full_name = t.full_name
LEFT JOIN last_write tw ON tw.full_name = t.full_name
LEFT JOIN writer w      ON w.full_name = t.full_name AND w.rn = 1
LEFT JOIN job_state js  ON w.entity_type = 'JOB'      AND js.entity_id = w.entity_id
LEFT JOIN pipeline_state ps ON w.entity_type = 'PIPELINE' AND ps.entity_id = w.entity_id
LEFT JOIN names n       ON n.entity_id = w.entity_id
ORDER BY
    CASE status WHEN 'NOT_PROPAGATED' THEN 0 WHEN 'LATE' THEN 1 WHEN 'WITHIN_SLA' THEN 2 ELSE 3 END,
    lag_hours DESC NULLS FIRST,
    t.table_name
```

`MAX_BY` is available on current warehouses. `job_id` in `system.lakeflow.jobs` is a string in recent schema versions; the `CAST` makes the join type-safe either way.

### Every upstream edge for one table

For a `LATE` table with several sources, list them all so the slow feed is obvious:

```sql
WITH edges AS (
    SELECT DISTINCT LOWER(source_table_full_name) AS source_full_name
    FROM system.access.table_lineage
    WHERE LOWER(target_table_full_name) = LOWER('{{ catalog }}.{{ schema }}.{{ asset }}')
      AND source_table_full_name IS NOT NULL
      AND event_time >= current_timestamp() - INTERVAL {{ lookback_days }} DAYS
)
SELECT
    e.source_full_name,
    MAX(l.event_time)                                                AS source_last_write,
    MAX_BY(l.entity_type, l.event_time)                              AS source_writer_type,
    MAX_BY(l.entity_id, l.event_time)                                AS source_writer_id,
    COUNT(l.event_time)                                              AS source_writes_in_window
FROM edges e
LEFT JOIN system.access.table_lineage l
  ON LOWER(l.target_table_full_name) = e.source_full_name
 AND l.event_time >= current_timestamp() - INTERVAL {{ lookback_days }} DAYS
GROUP BY e.source_full_name
ORDER BY source_last_write DESC NULLS LAST
```

Sources with `source_writes_in_window = 0` are either static reference tables or written outside UC compute; either way they do not drive lag.
