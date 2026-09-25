# Diagnostic: serving_latency_compliance

The slowest SELECT statements against the schema with their timing breakdown, the tables they read, compute and origin; a per-table latency roll-up; and a per-warehouse queueing view.

## Context

Reuses the check's lineage scoping. Three queries, each answering one question:

1. **Which statements are slow, and why?** Per statement: `total_duration_ms` next to `execution_duration_ms` (the gap is queue plus compile plus fetch), `read_rows`, `read_bytes` where available, `produced_rows`, the tables read, warehouse, client, and job or dashboard id. Status `COMPLIANT` or `EXCEEDS_SLA`. A large `read_rows` with tiny `produced_rows` is a pruning problem (layout); a large total-minus-execution gap is a capacity problem (warehouse).
2. **Which tables carry the slow reads?** Per source table: query count, p50 and p95 duration, compliance fraction, median rows read. Sorted by compliance ascending so the worst table is first.
3. **Is the warehouse queueing?** Per warehouse: query count, p95 total, p95 execution, and the p95 of the difference. If the difference dominates, resizing or autoscaling the warehouse fixes more than any table change.

`read_bytes` and `read_files` exist on current releases of `system.query.history`; if the first query errors on them, remove those two columns (probe: `SELECT read_bytes, read_files FROM system.query.history LIMIT 1`).

Lag: minutes for query history, hours for lineage.

## SQL

### Slowest statements

```sql
WITH schema_reads AS (
    SELECT query_statement_id AS statement_id,
           array_sort(collect_set(LOWER(source_table_name))) AS tables_read
    FROM system.access.table_lineage
    WHERE LOWER(source_catalog) = LOWER('{{ catalog }}')
      AND LOWER(source_schema)  = LOWER('{{ schema }}')
      AND query_statement_id IS NOT NULL
      AND event_time >= current_timestamp() - INTERVAL {{ lookback_days }} DAYS
    GROUP BY query_statement_id
)
SELECT
    qh.statement_id,
    qh.start_time,
    qh.total_duration_ms,
    qh.execution_duration_ms,
    qh.total_duration_ms - COALESCE(qh.execution_duration_ms, 0) AS overhead_ms,
    qh.read_rows,
    qh.read_bytes,
    qh.read_files,
    qh.produced_rows,
    sr.tables_read,
    qh.compute.type                    AS compute_type,
    qh.compute.warehouse_id            AS warehouse_id,
    qh.client_application,
    qh.query_source.job_info.job_id    AS job_id,
    qh.query_source.dashboard_id       AS dashboard_id,
    qh.query_source.genie_space_id     AS genie_space_id,
    qh.executed_by,
    substr(qh.statement_text, 1, 400)  AS statement_text,
    CASE WHEN qh.total_duration_ms <= {{ latency_threshold_ms }} THEN 'COMPLIANT' ELSE 'EXCEEDS_SLA' END AS status
FROM system.query.history qh
JOIN schema_reads sr USING (statement_id)
WHERE qh.statement_type = 'SELECT'
  AND qh.execution_status = 'FINISHED'
  AND qh.start_time >= current_timestamp() - INTERVAL {{ lookback_days }} DAYS
ORDER BY qh.total_duration_ms DESC
LIMIT 200
```

### Latency per table

```sql
WITH schema_reads AS (
    SELECT query_statement_id AS statement_id, LOWER(source_table_name) AS table_name
    FROM system.access.table_lineage
    WHERE LOWER(source_catalog) = LOWER('{{ catalog }}')
      AND LOWER(source_schema)  = LOWER('{{ schema }}')
      AND query_statement_id IS NOT NULL
      AND event_time >= current_timestamp() - INTERVAL {{ lookback_days }} DAYS
),
per_stmt AS (
    SELECT DISTINCT sr.table_name, qh.statement_id, qh.total_duration_ms, qh.read_rows
    FROM system.query.history qh
    JOIN schema_reads sr USING (statement_id)
    WHERE qh.statement_type = 'SELECT'
      AND qh.execution_status = 'FINISHED'
      AND qh.start_time >= current_timestamp() - INTERVAL {{ lookback_days }} DAYS
)
SELECT
    table_name,
    COUNT(*)                                                          AS queries,
    COUNT_IF(total_duration_ms <= {{ latency_threshold_ms }})         AS compliant_queries,
    ROUND(COUNT_IF(total_duration_ms <= {{ latency_threshold_ms }})::DOUBLE
          / NULLIF(COUNT(*), 0), 3)                                   AS compliance,
    percentile_approx(total_duration_ms, 0.5)                         AS p50_ms,
    percentile_approx(total_duration_ms, 0.95)                        AS p95_ms,
    MAX(total_duration_ms)                                            AS max_ms,
    percentile_approx(read_rows, 0.5)                                 AS median_rows_read
FROM per_stmt
GROUP BY table_name
ORDER BY compliance ASC, queries DESC
```

A statement that reads several schema tables is counted once per table, which is the intent: each table shares the blame.

### Queueing per warehouse

```sql
WITH schema_reads AS (
    SELECT DISTINCT query_statement_id AS statement_id
    FROM system.access.table_lineage
    WHERE LOWER(source_catalog) = LOWER('{{ catalog }}')
      AND LOWER(source_schema)  = LOWER('{{ schema }}')
      AND query_statement_id IS NOT NULL
      AND event_time >= current_timestamp() - INTERVAL {{ lookback_days }} DAYS
)
SELECT
    qh.compute.type                                                  AS compute_type,
    qh.compute.warehouse_id                                          AS warehouse_id,
    COUNT(*)                                                         AS queries,
    percentile_approx(qh.total_duration_ms, 0.95)                    AS p95_total_ms,
    percentile_approx(qh.execution_duration_ms, 0.95)                AS p95_execution_ms,
    percentile_approx(qh.total_duration_ms - COALESCE(qh.execution_duration_ms, 0), 0.95)
                                                                     AS p95_overhead_ms,
    ROUND(COUNT_IF(qh.total_duration_ms <= {{ latency_threshold_ms }})::DOUBLE
          / NULLIF(COUNT(*), 0), 3)                                  AS compliance
FROM system.query.history qh
JOIN schema_reads sr USING (statement_id)
WHERE qh.statement_type = 'SELECT'
  AND qh.execution_status = 'FINISHED'
  AND qh.start_time >= current_timestamp() - INTERVAL {{ lookback_days }} DAYS
GROUP BY qh.compute.type, qh.compute.warehouse_id
ORDER BY compliance ASC, queries DESC
```

Warehouse names are not in `system.query.history`; map `warehouse_id` with `databricks warehouses get <id>` or `w.warehouses.get(id).name`.

### Repeated slow query shapes

Statements that recur with the same text after literal removal are candidates for a materialized view or a cache.

```sql
WITH schema_reads AS (
    SELECT DISTINCT query_statement_id AS statement_id
    FROM system.access.table_lineage
    WHERE LOWER(source_catalog) = LOWER('{{ catalog }}')
      AND LOWER(source_schema)  = LOWER('{{ schema }}')
      AND query_statement_id IS NOT NULL
      AND event_time >= current_timestamp() - INTERVAL {{ lookback_days }} DAYS
)
SELECT
    regexp_replace(regexp_replace(LOWER(qh.statement_text), '''[^'']*''', '?'), '\\b\\d+\\b', '?') AS shape,
    COUNT(*)                                                         AS executions,
    percentile_approx(qh.total_duration_ms, 0.95)                    AS p95_ms,
    SUM(qh.total_duration_ms)                                        AS total_ms
FROM system.query.history qh
JOIN schema_reads sr USING (statement_id)
WHERE qh.statement_type = 'SELECT'
  AND qh.execution_status = 'FINISHED'
  AND qh.start_time >= current_timestamp() - INTERVAL {{ lookback_days }} DAYS
  AND qh.total_duration_ms > {{ latency_threshold_ms }}
GROUP BY 1
ORDER BY total_ms DESC
LIMIT 50
```
