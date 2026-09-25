# Check: propagation_latency_compliance

Fraction of derived tables in the schema whose last write is no more than `{{ sla_hours }}` hours behind the last write of their upstream sources.

## Context

A derived table is one that `system.access.table_lineage` shows being written from other tables: rows where `target_table_full_name` is the table and `source_table_full_name` is non-null. Its upstream sources are the distinct `source_table_full_name` values on those rows. Propagation latency is how long after the sources change the derived table catches up. This check approximates it from the same lineage table: for each derived table, take the newest write to it (`MAX(event_time)` where it is the target) and the newest write to any of its sources (`MAX(event_time)` where a source is the target), and pass when the derived table's write is at most `{{ sla_hours }}` older than the source write.

The comparison in words: `target_last_write >= source_last_write - sla_hours`. A derived table written after its sources has negative lag and passes. A source that has not been written in the window gives no lag to measure, and the derived table passes on that source. A derived table with a written source but no write of its own in the window fails.

What this proves and does not prove. It uses write events, not data timestamps, so a job that ran and wrote zero rows counts as propagation. It measures the latest pair of writes only; a table that was 3 days late last week and on time today passes. It is one hop: a two-stage chain is measured stage by stage, and end-to-end latency is the sum the operator computes from the diagnostic. Lineage lags by up to a few hours (affects both sides equally) and only records UC compute; a source written by an external engine looks unwritten and its consumers pass vacuously.

`{{ sla_hours }}` defaults to 24; `{{ lookback_days }}` defaults to 30 for lineage checks. Strength is **native**: the events are the platform's own write records. Tables with no upstream table edge in the window (raw landing tables, tables written from paths or by external systems) are not derived and are excluded from the denominator. Streaming tables and materialized views are included, since pipeline writes appear in lineage like any other.

Returns NULL (N/A) when no table in the schema has an upstream table edge in the window.

## SQL

### Last write of target vs last write of sources (primary)

```sql
WITH tables_in_scope AS (
    SELECT LOWER(table_name) AS table_name,
           CONCAT(LOWER('{{ catalog }}'), '.', LOWER('{{ schema }}'), '.', LOWER(table_name)) AS full_name
    FROM {{ catalog }}.information_schema.tables
    WHERE LOWER(table_schema) = LOWER('{{ schema }}')
      AND table_type IN ('MANAGED', 'EXTERNAL', 'STREAMING_TABLE', 'MATERIALIZED_VIEW')
),
edges AS (
    SELECT DISTINCT
        LOWER(target_table_full_name) AS target_full_name,
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
    SELECT LOWER(target_table_full_name) AS full_name,
           MAX(event_time)               AS last_write_at
    FROM system.access.table_lineage
    WHERE target_table_full_name IS NOT NULL
      AND event_time >= current_timestamp() - INTERVAL {{ lookback_days }} DAYS
      AND LOWER(target_table_full_name) IN (
            SELECT full_name FROM tables_in_scope
            UNION SELECT source_full_name FROM edges)
    GROUP BY LOWER(target_table_full_name)
),
per_table AS (
    SELECT
        t.table_name,
        tw.last_write_at                      AS target_last_write,
        MAX(sw.last_write_at)                 AS source_last_write
    FROM tables_in_scope t
    JOIN edges e            ON e.target_full_name = t.full_name
    LEFT JOIN last_write tw ON tw.full_name = t.full_name
    LEFT JOIN last_write sw ON sw.full_name = e.source_full_name
    GROUP BY t.table_name, tw.last_write_at
)
SELECT
    COUNT_IF(source_last_write IS NULL
             OR (target_last_write IS NOT NULL
                 AND target_last_write >= source_last_write - INTERVAL {{ sla_hours }} HOURS))
                                                                    AS within_sla,
    COUNT(*)                                                        AS derived_tables,
    COUNT_IF(source_last_write IS NULL
             OR (target_last_write IS NOT NULL
                 AND target_last_write >= source_last_write - INTERVAL {{ sla_hours }} HOURS))::DOUBLE
        / NULLIF(COUNT(*), 0)                                       AS value
FROM per_table
```

`INTERVAL {{ sla_hours }} HOURS` requires an integer literal. If the SLA can be fractional, replace the comparison with `timestampdiff(MINUTE, source_last_write, target_last_write) >= -({{ sla_hours }} * 60)`.

### Direct edges only (variant)

Same query with `AND is_direct_lineage = true` added to the `edges` CTE. Excludes edges that lineage inferred through views and intermediate temporary objects, so a table fed through a view is compared against the view's own last write rather than the view's base tables. Use it when a schema has many views and the primary variant is blaming tables for lag that belongs to an upstream view refresh. It misses sources hidden behind views entirely.

### Per-run lag from the writer's job runs (variant)

Where the derived table is written by a scheduled job, the more faithful measure is how long after each source write the next successful job run finished. This variant computes, for the most recent source write, the first subsequent successful run of the job that lineage attributes to the target (`entity_type = 'JOB'`, `entity_id = job_id`), using `system.lakeflow.job_run_timeline`. It only covers JOB writers; PIPELINE writers use `system.lakeflow.pipeline_update_timeline` the same way, and NOTEBOOK or QUERY writers have no run record and fall back to the primary.

```sql
WITH tables_in_scope AS (
    SELECT LOWER(table_name) AS table_name,
           CONCAT(LOWER('{{ catalog }}'), '.', LOWER('{{ schema }}'), '.', LOWER(table_name)) AS full_name
    FROM {{ catalog }}.information_schema.tables
    WHERE LOWER(table_schema) = LOWER('{{ schema }}')
      AND table_type IN ('MANAGED', 'EXTERNAL')
),
writer AS (
    SELECT LOWER(target_table_full_name) AS full_name, entity_id AS job_id,
           ROW_NUMBER() OVER (PARTITION BY LOWER(target_table_full_name) ORDER BY MAX(event_time) DESC) AS rn
    FROM system.access.table_lineage
    WHERE LOWER(target_catalog) = LOWER('{{ catalog }}')
      AND LOWER(target_schema)  = LOWER('{{ schema }}')
      AND entity_type = 'JOB'
      AND event_time >= current_timestamp() - INTERVAL {{ lookback_days }} DAYS
    GROUP BY LOWER(target_table_full_name), entity_id
),
sources AS (
    SELECT DISTINCT LOWER(target_table_full_name) AS full_name, LOWER(source_table_full_name) AS source_full_name
    FROM system.access.table_lineage
    WHERE LOWER(target_catalog) = LOWER('{{ catalog }}')
      AND LOWER(target_schema)  = LOWER('{{ schema }}')
      AND source_table_full_name IS NOT NULL
      AND LOWER(source_table_full_name) <> LOWER(target_table_full_name)
      AND event_time >= current_timestamp() - INTERVAL {{ lookback_days }} DAYS
),
source_last_write AS (
    SELECT s.full_name, MAX(l.event_time) AS source_last_write
    FROM sources s
    JOIN system.access.table_lineage l
      ON LOWER(l.target_table_full_name) = s.source_full_name
    WHERE l.event_time >= current_timestamp() - INTERVAL {{ lookback_days }} DAYS
    GROUP BY s.full_name
),
next_success AS (
    SELECT slw.full_name, slw.source_last_write,
           MIN(r.period_end_time) AS first_success_after
    FROM source_last_write slw
    JOIN writer w ON w.full_name = slw.full_name AND w.rn = 1
    LEFT JOIN system.lakeflow.job_run_timeline r
      ON r.job_id = w.job_id
     AND r.result_state = 'SUCCESS'
     AND r.period_end_time >= slw.source_last_write
     AND r.period_start_time >= current_timestamp() - INTERVAL {{ lookback_days }} DAYS
    GROUP BY slw.full_name, slw.source_last_write
),
per_table AS (
    SELECT t.table_name, ns.source_last_write, ns.first_success_after,
           timestampdiff(HOUR, ns.source_last_write, ns.first_success_after) AS lag_hours
    FROM tables_in_scope t
    JOIN next_success ns USING (full_name)
)
SELECT
    COUNT_IF(lag_hours IS NOT NULL AND lag_hours <= {{ sla_hours }})            AS within_sla,
    COUNT(*)                                                                    AS derived_tables,
    COUNT_IF(lag_hours IS NOT NULL AND lag_hours <= {{ sla_hours }})::DOUBLE
        / NULLIF(COUNT(*), 0)                                                   AS value
FROM per_table
```

A `NULL` lag means no successful run has completed since the source last changed, which fails. The denominator here is tables with a JOB writer and at least one source, so it is smaller than the primary's; report which variant produced the score.
