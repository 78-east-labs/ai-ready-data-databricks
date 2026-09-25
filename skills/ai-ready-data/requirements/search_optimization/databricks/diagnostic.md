# Diagnostic: search_optimization

One row per base Delta table with its clustering keys, predictive optimization status and last operation, last manual `OPTIMIZE`, write commits since then, file count and average file size, and a status label.

## Context

Assembled from the check's SQL leg and probes, plus one extra number: `commits_since_optimize`, the count of write commits after the last `OPTIMIZE`. That, with `avg_file_mb`, is what tells you whether maintenance is keeping up:

- `avg_file_mb` well under 64 MB on a table over a few GB means small files are winning.
- `commits_since_optimize` in the hundreds on a streaming target means the optimize cadence is too slow for the write cadence.

Status:

- `PO_ACTIVE`: predictive optimization operated on the table in the window. Maintenance is automatic.
- `MANUAL_MAINTAINED`: clustered, `OPTIMIZE` ran in the window, no predictive optimization. Works until the job breaks; consider enabling predictive optimization.
- `CLUSTERED_STALE`: clustering keys set but no `OPTIMIZE` in the window. The layout is decaying.
- `UNCLUSTERED_OPTIMIZED`: `OPTIMIZE` ran but there are no keys, so it compacts without ordering. Half the benefit.
- `UNMAINTAINED`: nothing ran. Fix candidates, ordered by `write_commits` so the busiest go first.

Whether predictive optimization is *enabled* (as opposed to *ran*) comes from `DESCRIBE EXTENDED` (row `Predictive Optimization`, values like `ENABLE`, `DISABLE`, `INHERIT (ENABLE)`); it is not in `information_schema`. The probe is included so the diagnostic can distinguish "enabled, nothing to do yet" from "not enabled".

## SQL

### Predictive optimization activity (once per schema)

```sql
SELECT LOWER(table_name)                          AS table_name,
       MAX(end_time)                              AS last_po_operation,
       COUNT_IF(operation_type = 'COMPACTION')    AS po_compactions,
       COUNT_IF(operation_type = 'VACUUM')        AS po_vacuums,
       COUNT_IF(operation_type = 'ANALYZE')       AS po_analyzes,
       SUM(TRY_CAST(operation_metrics['number_of_compacted_files'] AS BIGINT)) AS files_compacted
FROM system.storage.predictive_optimization_operations_history
WHERE LOWER(catalog_name) = LOWER('{{ catalog }}')
  AND LOWER(schema_name)  = LOWER('{{ schema }}')
  AND operation_status = 'SUCCESSFUL'
  AND start_time >= current_timestamp() - INTERVAL {{ lookback_days }} DAYS
GROUP BY LOWER(table_name)
```

`operation_metrics` keys vary by operation type; `number_of_compacted_files` is the documented key for `COMPACTION`. If it is absent on your release, probe `SELECT operation_type, map_keys(operation_metrics) FROM system.storage.predictive_optimization_operations_history LIMIT 20`.

### Per-table probes

```sql
DESCRIBE DETAIL {{ catalog }}.{{ schema }}.`{{ asset }}`
```

```sql
WITH h AS (
    SELECT version, timestamp, operation, userName
    FROM (DESCRIBE HISTORY {{ catalog }}.{{ schema }}.`{{ asset }}` LIMIT 500)
),
last_opt AS (
    SELECT MAX(version) AS opt_version, MAX(timestamp) AS last_optimize
    FROM h WHERE operation = 'OPTIMIZE'
)
SELECT
    lo.last_optimize,
    COUNT_IF(h.operation = 'OPTIMIZE'
             AND h.timestamp >= current_timestamp() - INTERVAL {{ lookback_days }} DAYS) AS optimize_commits_window,
    COUNT_IF(h.operation IN ('WRITE','MERGE','UPDATE','DELETE','STREAMING UPDATE','COPY INTO')
             AND h.timestamp >= current_timestamp() - INTERVAL {{ lookback_days }} DAYS) AS write_commits_window,
    COUNT_IF(h.operation IN ('WRITE','MERGE','UPDATE','DELETE','STREAMING UPDATE','COPY INTO')
             AND (lo.opt_version IS NULL OR h.version > lo.opt_version))                   AS commits_since_optimize,
    MAX(CASE WHEN h.operation IN ('WRITE','MERGE','UPDATE','DELETE','STREAMING UPDATE','COPY INTO')
             THEN h.timestamp END)                                                          AS last_write
FROM h CROSS JOIN last_opt lo
GROUP BY lo.last_optimize
```

Predictive optimization enabled state:

```sql
DESCRIBE EXTENDED {{ catalog }}.{{ schema }}.`{{ asset }}`
```

Read the row where `col_name = 'Predictive Optimization'`.

### Assembled output (shape and status rules)

With probe results in `probe_detail(table_name, sizeInBytes, numFiles, clusteringColumns, properties, po_enabled)` and `probe_history(table_name, last_optimize, optimize_commits_window, write_commits_window, commits_since_optimize, last_write)`, and the SQL leg in `po_activity`:

```sql
SELECT
    d.table_name,
    d.clusteringColumns                                            AS clustering_columns,
    LOWER(COALESCE(d.properties['clusterByAuto'], 'false')) = 'true' AS cluster_by_auto,
    d.po_enabled,
    po.last_po_operation,
    po.po_compactions,
    h.last_optimize,
    h.optimize_commits_window,
    h.write_commits_window,
    h.commits_since_optimize,
    h.last_write,
    d.numFiles                                                     AS num_files,
    ROUND(d.sizeInBytes / 1073741824.0, 2)                         AS size_gb,
    ROUND(d.sizeInBytes / NULLIF(d.numFiles, 0) / 1048576.0, 1)    AS avg_file_mb,
    CASE
        WHEN po.last_po_operation IS NOT NULL THEN 'PO_ACTIVE'
        WHEN (size(d.clusteringColumns) > 0 OR LOWER(COALESCE(d.properties['clusterByAuto'], 'false')) = 'true')
             AND h.optimize_commits_window > 0 THEN 'MANUAL_MAINTAINED'
        WHEN size(d.clusteringColumns) > 0 OR LOWER(COALESCE(d.properties['clusterByAuto'], 'false')) = 'true'
             THEN 'CLUSTERED_STALE'
        WHEN h.optimize_commits_window > 0 THEN 'UNCLUSTERED_OPTIMIZED'
        ELSE 'UNMAINTAINED'
    END AS status
FROM probe_detail d
LEFT JOIN probe_history h  USING (table_name)
LEFT JOIN po_activity   po USING (table_name)
ORDER BY
    CASE status
        WHEN 'UNMAINTAINED' THEN 1 WHEN 'CLUSTERED_STALE' THEN 2
        WHEN 'UNCLUSTERED_OPTIMIZED' THEN 3 WHEN 'MANUAL_MAINTAINED' THEN 4 ELSE 5
    END,
    h.write_commits_window DESC NULLS LAST,
    d.sizeInBytes DESC
```

### Scheduled OPTIMIZE jobs in the workspace

To see whether the manual maintenance is a job or someone's notebook:

```sql
SELECT qh.start_time, qh.executed_by, qh.query_source.job_info.job_id AS job_id,
       qh.total_duration_ms, substr(qh.statement_text, 1, 200) AS statement_text
FROM system.query.history qh
WHERE qh.statement_type = 'OPTIMIZE'
  AND qh.start_time >= current_timestamp() - INTERVAL {{ lookback_days }} DAYS
  AND REGEXP_LIKE(LOWER(qh.statement_text), LOWER('{{ catalog }}\\.{{ schema }}\\.'))
ORDER BY qh.start_time DESC
LIMIT 100
```

If `statement_type` for `OPTIMIZE` is reported differently on your release (some show `OTHER`), drop that predicate and rely on the regex.
