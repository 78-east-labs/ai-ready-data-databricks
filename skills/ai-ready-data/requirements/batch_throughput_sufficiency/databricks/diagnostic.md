# Diagnostic: batch_throughput_sufficiency

Per-statement breakdown of recent writes into the schema: target table, statement type, rows written, elapsed time, rows per second, compute, and a status label.

## Context

Reuses the check's lineage scoping so the population matches, then shows the slowest 200 write statements. Each row is labeled `SUFFICIENT`, `SLOW` (finished but below `{{ min_rows_per_second }}`), or `FAILED` (non-finished statements are included here, unlike in the check, because a failed load is often the reason throughput looks fine: nothing was written).

Useful columns: `compute.type` and `compute.warehouse_id` show whether slow writes cluster on one warehouse; `query_source` shows whether a job, notebook or pipeline issued the statement; `written_bytes` next to `written_rows` exposes wide-row tables where a rows-per-second threshold is the wrong yardstick; `read_rows` far above `written_rows` on a `MERGE` points at a merge that rescans the whole target (missing clustering on the merge key).

The second query rolls the same population up per target table so you can see which tables carry the slow loads.

Lag: `system.query.history` minutes, `system.access.table_lineage` hours.

## SQL

### Slowest write statements

```sql
WITH schema_writes AS (
    SELECT query_statement_id AS statement_id,
           array_sort(collect_set(LOWER(target_table_name))) AS target_tables
    FROM system.access.table_lineage
    WHERE LOWER(target_catalog) = LOWER('{{ catalog }}')
      AND LOWER(target_schema)  = LOWER('{{ schema }}')
      AND target_type IN ('TABLE', 'STREAMING_TABLE', 'MATERIALIZED_VIEW')
      AND query_statement_id IS NOT NULL
      AND event_time >= current_timestamp() - INTERVAL {{ lookback_days }} DAYS
    GROUP BY query_statement_id
)
SELECT
    qh.statement_id,
    sw.target_tables,
    qh.statement_type,
    qh.execution_status,
    qh.start_time,
    qh.total_duration_ms,
    qh.execution_duration_ms,
    qh.written_rows,
    qh.written_bytes,
    qh.read_rows,
    ROUND(qh.written_rows / NULLIF(qh.total_duration_ms / 1000.0, 0), 0) AS rows_per_second,
    qh.compute.type                    AS compute_type,
    qh.compute.warehouse_id            AS warehouse_id,
    qh.query_source.job_info.job_id    AS job_id,
    qh.query_source.notebook_id        AS notebook_id,
    qh.executed_by,
    substr(qh.statement_text, 1, 300)  AS statement_text,
    CASE
        WHEN qh.execution_status <> 'FINISHED' THEN 'FAILED'
        WHEN COALESCE(qh.written_rows, 0) = 0 THEN 'EMPTY_WRITE'
        WHEN qh.written_rows / NULLIF(qh.total_duration_ms / 1000.0, 0)
             >= {{ min_rows_per_second }} THEN 'SUFFICIENT'
        ELSE 'SLOW'
    END AS status
FROM system.query.history qh
JOIN schema_writes sw USING (statement_id)
WHERE qh.statement_type IN ('INSERT', 'MERGE', 'COPY', 'CREATE_TABLE_AS_SELECT',
                            'UPDATE', 'DELETE', 'REPLACE')
  AND qh.start_time >= current_timestamp() - INTERVAL {{ lookback_days }} DAYS
ORDER BY
    CASE status WHEN 'FAILED' THEN 0 WHEN 'SLOW' THEN 1 WHEN 'EMPTY_WRITE' THEN 2 ELSE 3 END,
    rows_per_second ASC NULLS FIRST,
    qh.start_time DESC
LIMIT 200
```

### Throughput per target table

```sql
WITH schema_writes AS (
    SELECT query_statement_id AS statement_id, LOWER(target_table_name) AS table_name
    FROM system.access.table_lineage
    WHERE LOWER(target_catalog) = LOWER('{{ catalog }}')
      AND LOWER(target_schema)  = LOWER('{{ schema }}')
      AND target_type IN ('TABLE', 'STREAMING_TABLE', 'MATERIALIZED_VIEW')
      AND query_statement_id IS NOT NULL
      AND event_time >= current_timestamp() - INTERVAL {{ lookback_days }} DAYS
),
per_stmt AS (
    SELECT sw.table_name, qh.statement_id, qh.execution_status, qh.written_rows,
           qh.written_rows / NULLIF(qh.total_duration_ms / 1000.0, 0) AS rps
    FROM system.query.history qh
    JOIN schema_writes sw USING (statement_id)
    WHERE qh.statement_type IN ('INSERT', 'MERGE', 'COPY', 'CREATE_TABLE_AS_SELECT',
                                'UPDATE', 'DELETE', 'REPLACE')
      AND qh.start_time >= current_timestamp() - INTERVAL {{ lookback_days }} DAYS
)
SELECT
    table_name,
    COUNT(*)                                                    AS write_statements,
    COUNT_IF(execution_status <> 'FINISHED')                    AS failed_statements,
    COUNT_IF(execution_status = 'FINISHED' AND written_rows > 0
             AND rps >= {{ min_rows_per_second }})              AS sufficient_statements,
    ROUND(percentile_approx(rps, 0.5), 0)                       AS median_rows_per_second,
    ROUND(MIN(rps), 0)                                          AS min_rows_per_second,
    SUM(written_rows)                                           AS total_rows_written
FROM per_stmt
GROUP BY table_name
ORDER BY sufficient_statements::DOUBLE / NULLIF(COUNT(*), 0) ASC, total_rows_written DESC
```

### Recent commits on one table (covers writes that never reach query history)

```sql
SELECT version, timestamp, operation, userName,
       job.jobId AS job_id, notebook.notebookId AS notebook_id,
       TRY_CAST(operationMetrics['numOutputRows']   AS BIGINT) AS num_output_rows,
       TRY_CAST(operationMetrics['executionTimeMs'] AS BIGINT) AS execution_time_ms,
       ROUND(TRY_CAST(operationMetrics['numOutputRows'] AS BIGINT)
             / NULLIF(TRY_CAST(operationMetrics['executionTimeMs'] AS BIGINT) / 1000.0, 0), 0)
                                                               AS rows_per_second,
       operationParameters['mode'] AS write_mode
FROM (DESCRIBE HISTORY {{ catalog }}.{{ schema }}.{{ asset }})
WHERE operation IN ('WRITE', 'MERGE', 'STREAMING UPDATE', 'COPY INTO', 'CREATE TABLE AS SELECT')
  AND timestamp >= current_timestamp() - INTERVAL {{ lookback_days }} DAYS
ORDER BY timestamp DESC
LIMIT 100
```
