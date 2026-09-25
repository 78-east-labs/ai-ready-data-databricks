# Diagnostic: record_level_traceability

Per-table view of trace columns, the row-tracking state from the probe, whether the table is loaded from files (so `_metadata.file_path` is available to persist), and a status, untraceable tables first.

## Context

The enumeration query is pure SQL and does most of the work: it lists every base table with its trace columns (matching `{{ trace_column_pattern }}`, default in the check), its primary key columns (a PK gives per-row identity within the table even if it says nothing about the source), and, from `system.access.table_lineage`, whether the table is loaded from external paths in the last `{{ lookback_days }}` days (default 30). A table loaded from files without a `source_file` column is the most common and cheapest gap to close, because Auto Loader already exposes the value.

For tables with `needs_probe = true`, run `DESCRIBE DETAIL` and fill `row_tracking_enabled` from `LOWER(properties['delta.enableRowTracking']) = 'true' OR array_contains(tableFeatures, 'rowTracking')`. Then derive `status`:

- `NOT_TRACEABLE`: no trace column, row tracking off
- `ROW_TRACKING_ONLY`: Delta row identity only; source record unknown
- `TRACE_COLUMN_ONLY`: a correlation column exists but row tracking is off (a `MERGE` can still make row history hard to follow)
- `BOTH`: row tracking and a trace column

Within a status, file-loaded tables sort first because they are the easiest to fix. Lineage lags by up to a few hours.

## SQL

### Enumeration (run first)

```sql
WITH tables_in_scope AS (
    SELECT LOWER(table_name) AS table_name, table_owner, UPPER(data_source_format) AS data_source_format, last_altered
    FROM {{ catalog }}.information_schema.tables
    WHERE LOWER(table_schema) = LOWER('{{ schema }}')
      AND table_type IN ('MANAGED', 'EXTERNAL')
),
trace_columns AS (
    SELECT LOWER(table_name) AS table_name,
           array_sort(collect_set(column_name)) AS trace_columns
    FROM {{ catalog }}.information_schema.columns
    WHERE LOWER(table_schema) = LOWER('{{ schema }}')
      AND REGEXP_LIKE(LOWER(column_name), '{{ trace_column_pattern }}')
    GROUP BY LOWER(table_name)
),
primary_keys AS (
    SELECT LOWER(k.table_name) AS table_name,
           array_sort(collect_set(k.column_name)) AS pk_columns
    FROM {{ catalog }}.information_schema.table_constraints c
    JOIN {{ catalog }}.information_schema.key_column_usage k
      ON c.constraint_name = k.constraint_name
     AND c.table_schema    = k.table_schema
     AND c.table_name      = k.table_name
    WHERE LOWER(c.table_schema) = LOWER('{{ schema }}')
      AND c.constraint_type = 'PRIMARY KEY'
    GROUP BY LOWER(k.table_name)
),
file_loaded AS (
    SELECT LOWER(target_table_name) AS table_name,
           array_sort(collect_set(source_path)) AS source_paths
    FROM system.access.table_lineage
    WHERE LOWER(target_catalog) = LOWER('{{ catalog }}')
      AND LOWER(target_schema)  = LOWER('{{ schema }}')
      AND target_table_full_name IS NOT NULL
      AND (source_path IS NOT NULL OR UPPER(source_type) = 'PATH')
      AND event_time >= current_timestamp() - INTERVAL {{ lookback_days }} DAYS
    GROUP BY LOWER(target_table_name)
)
SELECT
    t.table_name,
    t.table_owner,
    t.data_source_format,
    t.last_altered,
    tc.trace_columns,
    pk.pk_columns,
    fl.source_paths,
    fl.table_name IS NOT NULL                                   AS loaded_from_files,
    tc.table_name IS NOT NULL                                   AS has_trace_column,
    tc.table_name IS NULL AND t.data_source_format = 'DELTA'    AS needs_probe
FROM tables_in_scope t
LEFT JOIN trace_columns tc USING (table_name)
LEFT JOIN primary_keys  pk USING (table_name)
LEFT JOIN file_loaded   fl USING (table_name)
ORDER BY has_trace_column ASC, loaded_from_files DESC, t.table_name
```

### Per-table probe (for rows with `needs_probe = true`)

```sql
DESCRIBE DETAIL {{ catalog }}.{{ schema }}.`{{ asset }}`
```

Columns to derive from the probe output:

```sql
LOWER(properties['delta.enableRowTracking']) = 'true'
    OR array_contains(tableFeatures, 'rowTracking')             AS row_tracking_enabled,
properties['delta.enableRowTracking']                           AS row_tracking_property_raw,
properties['delta.enableChangeDataFeed']                        AS change_data_feed_raw,
numFiles                                                        AS num_files,
sizeInBytes                                                     AS size_bytes
```

`change_data_feed_raw` is included because row tracking plus Change Data Feed is what makes per-row history queryable (`table_changes(...)` with `_change_type`); a table with row tracking but no CDF can identify rows but not replay their changes.

### Final status (merge on `table_name`)

```sql
CASE
    WHEN NOT has_trace_column AND NOT COALESCE(row_tracking_enabled, false) THEN 'NOT_TRACEABLE'
    WHEN NOT has_trace_column                                               THEN 'ROW_TRACKING_ONLY'
    WHEN NOT COALESCE(row_tracking_enabled, false)                          THEN 'TRACE_COLUMN_ONLY'
    ELSE 'BOTH'
END AS status
```

Sort by `status` in the order above, then `loaded_from_files DESC`, then `size_bytes DESC`.
