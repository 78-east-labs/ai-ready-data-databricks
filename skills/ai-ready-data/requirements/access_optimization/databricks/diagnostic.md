# Diagnostic: access_optimization

One row per base Delta table with its size, file count, average file size, clustering keys, partition columns, last predictive optimization compaction, and a status label.

## Context

Built from the same enumeration and `DESCRIBE DETAIL` probes as the check, plus the predictive optimization history. The orchestrator assembles the rows; the final SELECT below shows the shape and the status rules so the report is consistent with the check.

Status labels:

- `SMALL (OK)`: below `{{ large_table_bytes }}`, excluded from the score.
- `LIQUID`: liquid clustering keys set (or `CLUSTER BY AUTO`).
- `PARTITIONED`: Hive-style partitions, no liquid clustering. Consider migrating to liquid clustering; both cannot coexist on one table.
- `PO_MAINTAINED`: no layout keys, but predictive optimization compacted the table within 30 days.
- `NEEDS LAYOUT`: large, no keys, no recent maintenance. These are the fix candidates.

`avg_file_mb` is `sizeInBytes / numFiles`. Files well under 100 MB on a large table indicate a small-file problem that `OPTIMIZE` or predictive optimization would fix even when keys are already set. `numFiles` counts current files only; `VACUUM` state does not affect it.

Sorted so `NEEDS LAYOUT` comes first, then by size descending.

## SQL

### Per-table probe (run for each table from the check's enumeration)

```sql
DESCRIBE DETAIL {{ catalog }}.{{ schema }}.`{{ asset }}`
```

### Predictive optimization activity (once per schema)

```sql
SELECT LOWER(table_name)                       AS table_name,
       MAX(end_time)                           AS last_po_compaction,
       COUNT_IF(operation_type = 'COMPACTION') AS po_compactions_30d,
       COUNT_IF(operation_type = 'VACUUM')     AS po_vacuums_30d
FROM system.storage.predictive_optimization_operations_history
WHERE LOWER(catalog_name) = LOWER('{{ catalog }}')
  AND LOWER(schema_name)  = LOWER('{{ schema }}')
  AND operation_status = 'SUCCESSFUL'
  AND start_time >= current_timestamp() - INTERVAL 30 DAYS
GROUP BY LOWER(table_name)
```

### Assembled output (shape and status rules)

Load the probe rows into a temp view `probe_detail(table_name, sizeInBytes, numFiles, clusteringColumns, partitionColumns, properties)` and the PO rows into `po_activity`, then:

```sql
SELECT
    p.table_name,
    ROUND(p.sizeInBytes / 1073741824.0, 2)                    AS size_gb,
    p.numFiles                                                AS num_files,
    ROUND(p.sizeInBytes / NULLIF(p.numFiles, 0) / 1048576.0, 1) AS avg_file_mb,
    p.clusteringColumns                                       AS clustering_columns,
    LOWER(COALESCE(p.properties['clusterByAuto'], 'false')) = 'true' AS cluster_by_auto,
    p.partitionColumns                                        AS partition_columns,
    po.last_po_compaction,
    CASE
        WHEN p.sizeInBytes < {{ large_table_bytes }} THEN 'SMALL (OK)'
        WHEN size(p.clusteringColumns) > 0
          OR LOWER(COALESCE(p.properties['clusterByAuto'], 'false')) = 'true' THEN 'LIQUID'
        WHEN size(p.partitionColumns) > 0 THEN 'PARTITIONED'
        WHEN po.last_po_compaction IS NOT NULL THEN 'PO_MAINTAINED'
        ELSE 'NEEDS LAYOUT'
    END AS status
FROM probe_detail p
LEFT JOIN po_activity po USING (table_name)
ORDER BY
    CASE status
        WHEN 'NEEDS LAYOUT'  THEN 1
        WHEN 'PO_MAINTAINED' THEN 2
        WHEN 'PARTITIONED'   THEN 3
        WHEN 'LIQUID'        THEN 4
        ELSE 5
    END,
    p.sizeInBytes DESC
```

### Candidate clustering keys (single table)

Before choosing keys for a `NEEDS LAYOUT` table, look at which columns recent queries filter on. `system.query.history` keeps statement text; this pulls the last 200 statements that mention the table and lets you eyeball the predicates.

```sql
SELECT start_time, executed_by, total_duration_ms, read_rows,
       substr(statement_text, 1, 500) AS statement_text
FROM system.query.history
WHERE statement_type = 'SELECT'
  AND execution_status = 'FINISHED'
  AND start_time >= current_timestamp() - INTERVAL 30 DAYS
  AND REGEXP_LIKE(LOWER(statement_text),
        LOWER('{{ catalog }}\\.{{ schema }}\\.`?{{ asset }}`?'))
ORDER BY start_time DESC
LIMIT 200
```

Columns that appear repeatedly in `WHERE` and `JOIN ... ON` clauses, ordered from most selective to least, are the candidates. Declared primary keys and date columns are the usual first picks.
