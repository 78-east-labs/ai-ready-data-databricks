# Diagnostic: lineage_completeness

Per-table breakdown of upstream lineage: source tables and paths at the table level, traced versus total columns at the column level, the writers involved, and a status, tables with no lineage first.

## Context

Same population and window as the check (base tables, `{{ lookback_days }}` default 30). For each table it shows the distinct upstream tables and external paths from `table_lineage`, how many of the table's columns have at least one source column in `column_lineage`, which entity types wrote it, and when. Combine `writer_entity_types` with `column_coverage` to find the cause: a `NOTEBOOK` writer with zero traced columns usually means a pandas or RDD write; a `JOB` writer with partial coverage usually means UDFs or literals for the untraced columns.

`status` values, worst first:

- `NO_LINEAGE`: no upstream row in either table (external writer, non-UC compute, idle, or lag)
- `TABLE_ONLY`: table-level edges from other tables but no column-level rows
- `PATH_ONLY`: fed only from external paths; column lineage is not possible, judge with `data_provenance` and `record_level_traceability` instead
- `PARTIAL_COLUMNS`: some but not all columns traced
- `COMPLETE`: table edges and every column traced

Both system tables lag by up to a few hours.

## SQL

```sql
WITH tables_in_scope AS (
    SELECT LOWER(table_name) AS table_name, table_owner, last_altered
    FROM {{ catalog }}.information_schema.tables
    WHERE LOWER(table_schema) = LOWER('{{ schema }}')
      AND table_type IN ('MANAGED', 'EXTERNAL')
),
column_counts AS (
    SELECT LOWER(table_name) AS table_name, COUNT(*) AS total_columns
    FROM {{ catalog }}.information_schema.columns
    WHERE LOWER(table_schema) = LOWER('{{ schema }}')
    GROUP BY LOWER(table_name)
),
table_upstream AS (
    SELECT LOWER(target_table_name)                                      AS table_name,
           array_sort(collect_set(source_table_full_name))               AS upstream_tables,
           array_sort(collect_set(source_path))                          AS upstream_paths,
           array_sort(collect_set(UPPER(entity_type)))                   AS writer_entity_types,
           MAX(event_time)                                               AS last_write
    FROM system.access.table_lineage
    WHERE LOWER(target_catalog) = LOWER('{{ catalog }}')
      AND LOWER(target_schema)  = LOWER('{{ schema }}')
      AND target_table_full_name IS NOT NULL
      AND (source_table_full_name IS NOT NULL OR source_path IS NOT NULL)
      AND event_time >= current_timestamp() - INTERVAL {{ lookback_days }} DAYS
    GROUP BY LOWER(target_table_name)
),
column_upstream AS (
    SELECT LOWER(target_table_name)                                      AS table_name,
           COUNT(DISTINCT LOWER(target_column_name))                     AS traced_columns,
           array_sort(collect_set(LOWER(target_column_name)))            AS traced_column_names
    FROM system.access.column_lineage
    WHERE LOWER(target_catalog) = LOWER('{{ catalog }}')
      AND LOWER(target_schema)  = LOWER('{{ schema }}')
      AND target_table_full_name IS NOT NULL
      AND source_column_name IS NOT NULL
      AND event_time >= current_timestamp() - INTERVAL {{ lookback_days }} DAYS
    GROUP BY LOWER(target_table_name)
)
SELECT
    t.table_name,
    t.table_owner,
    t.last_altered,
    tu.upstream_tables,
    tu.upstream_paths,
    tu.writer_entity_types,
    tu.last_write,
    COALESCE(cu.traced_columns, 0)                                       AS traced_columns,
    cc.total_columns,
    COALESCE(cu.traced_columns, 0)::DOUBLE / NULLIF(cc.total_columns, 0) AS column_coverage,
    cu.traced_column_names,
    CASE
        WHEN tu.table_name IS NULL                                                        THEN 'NO_LINEAGE'
        WHEN size(tu.upstream_tables) = 0 AND size(tu.upstream_paths) > 0                 THEN 'PATH_ONLY'
        WHEN cu.table_name IS NULL                                                        THEN 'TABLE_ONLY'
        WHEN cu.traced_columns < cc.total_columns                                         THEN 'PARTIAL_COLUMNS'
        ELSE 'COMPLETE'
    END                                                                  AS status
FROM tables_in_scope t
LEFT JOIN column_counts   cc USING (table_name)
LEFT JOIN table_upstream  tu USING (table_name)
LEFT JOIN column_upstream cu USING (table_name)
ORDER BY
    CASE
        WHEN tu.table_name IS NULL                                                        THEN 0
        WHEN cu.table_name IS NULL AND size(tu.upstream_tables) > 0                       THEN 1
        WHEN size(tu.upstream_tables) = 0 AND size(tu.upstream_paths) > 0                 THEN 2
        WHEN cu.traced_columns < cc.total_columns                                         THEN 3
        ELSE 4
    END ASC,
    column_coverage ASC,
    t.table_name
```
