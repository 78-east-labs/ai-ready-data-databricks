# Check: lineage_completeness

Fraction of base tables in the schema that have recorded upstream lineage at both the table level (`system.access.table_lineage`) and the column level (`system.access.column_lineage`) within the window.

## Context

Unity Catalog captures lineage at two granularities. `table_lineage` records that a write into `T` read from `S` (or from a path). `column_lineage` records that `T.c` was derived from `S.k`, one row per (source column, target column) pair. Table-level lineage is produced for essentially every observed write. Column-level lineage is produced only when the engine could resolve the expression that built each target column, which fails or is skipped for writes through non-DataFrame paths (pandas via `spark.createDataFrame` from local data, RDDs, `INSERT ... VALUES`), for some UDF-heavy transformations, and for external writers. A table with table-level edges but no column-level edges therefore has lineage you can follow to the source table but not to the source column, which is the level an agent needs to answer "where did this value come from".

The check counts a table as complete when, in the last `{{ lookback_days }}` days (default 30), it appears as `target_table_full_name` in both system tables. Upstream edges from external paths count at the table level (`source_path` is set), but such writes never carry column lineage, so pure landing tables loaded by Auto Loader or `COPY INTO` will pass the table half and fail the column half. That is a real limitation of the signal; the diagnostic separates those tables so they can be judged on their own.

The signal is native. It proves lineage was captured for at least one write, not that every writer is captured. Both tables lag by up to a few hours and keep one year of history. Reading them needs `SELECT` on `system.access.table_lineage` and `system.access.column_lineage`; rows are visible only for tables the caller can see. Returns NULL (N/A) when the schema has no base tables.

## SQL

### Table and column lineage (primary)

```sql
WITH tables_in_scope AS (
    SELECT LOWER(table_name) AS table_name
    FROM {{ catalog }}.information_schema.tables
    WHERE LOWER(table_schema) = LOWER('{{ schema }}')
      AND table_type IN ('MANAGED', 'EXTERNAL')
),
table_upstream AS (
    SELECT DISTINCT LOWER(target_table_name) AS table_name
    FROM system.access.table_lineage
    WHERE LOWER(target_catalog) = LOWER('{{ catalog }}')
      AND LOWER(target_schema)  = LOWER('{{ schema }}')
      AND target_table_full_name IS NOT NULL
      AND (source_table_full_name IS NOT NULL OR source_path IS NOT NULL)
      AND event_time >= current_timestamp() - INTERVAL {{ lookback_days }} DAYS
),
column_upstream AS (
    SELECT DISTINCT LOWER(target_table_name) AS table_name
    FROM system.access.column_lineage
    WHERE LOWER(target_catalog) = LOWER('{{ catalog }}')
      AND LOWER(target_schema)  = LOWER('{{ schema }}')
      AND target_table_full_name IS NOT NULL
      AND source_column_name IS NOT NULL
      AND event_time >= current_timestamp() - INTERVAL {{ lookback_days }} DAYS
)
SELECT
    COUNT_IF(tu.table_name IS NOT NULL AND cu.table_name IS NOT NULL)          AS tables_with_full_lineage,
    COUNT(*)                                                                   AS total_tables,
    COUNT_IF(tu.table_name IS NOT NULL AND cu.table_name IS NOT NULL)::DOUBLE
        / NULLIF(COUNT(*), 0)                                                  AS value
FROM tables_in_scope t
LEFT JOIN table_upstream  tu USING (table_name)
LEFT JOIN column_upstream cu USING (table_name)
```

### Table-level lineage only (variant)

Looser: any recorded upstream edge counts. Use as a first gate on a schema where the primary scores near zero, and to isolate whether the gap is "no lineage at all" or "no column lineage".

```sql
WITH tables_in_scope AS (
    SELECT LOWER(table_name) AS table_name
    FROM {{ catalog }}.information_schema.tables
    WHERE LOWER(table_schema) = LOWER('{{ schema }}')
      AND table_type IN ('MANAGED', 'EXTERNAL')
),
table_upstream AS (
    SELECT DISTINCT LOWER(target_table_name) AS table_name
    FROM system.access.table_lineage
    WHERE LOWER(target_catalog) = LOWER('{{ catalog }}')
      AND LOWER(target_schema)  = LOWER('{{ schema }}')
      AND target_table_full_name IS NOT NULL
      AND (source_table_full_name IS NOT NULL OR source_path IS NOT NULL)
      AND event_time >= current_timestamp() - INTERVAL {{ lookback_days }} DAYS
)
SELECT
    COUNT_IF(tu.table_name IS NOT NULL)           AS tables_with_lineage,
    COUNT(*)                                       AS total_tables,
    COUNT_IF(tu.table_name IS NOT NULL)::DOUBLE
        / NULLIF(COUNT(*), 0)                      AS value
FROM tables_in_scope t
LEFT JOIN table_upstream tu USING (table_name)
```

### Column coverage within derived tables (variant)

Stricter and more informative for tables that do have column lineage: over all columns of tables fed from other tables (landing tables from paths are excluded because they cannot have column lineage), the fraction that have at least one recorded source column. A schema whose pipelines record lineage for every column scores 1.0; one where only join keys are traced scores low.

```sql
WITH derived_tables AS (
    SELECT DISTINCT LOWER(target_table_name) AS table_name
    FROM system.access.table_lineage
    WHERE LOWER(target_catalog) = LOWER('{{ catalog }}')
      AND LOWER(target_schema)  = LOWER('{{ schema }}')
      AND target_table_full_name IS NOT NULL
      AND source_table_full_name IS NOT NULL
      AND event_time >= current_timestamp() - INTERVAL {{ lookback_days }} DAYS
),
table_columns AS (
    SELECT LOWER(c.table_name) AS table_name, LOWER(c.column_name) AS column_name
    FROM {{ catalog }}.information_schema.columns c
    JOIN {{ catalog }}.information_schema.tables t
      ON c.table_schema = t.table_schema AND c.table_name = t.table_name
    WHERE LOWER(c.table_schema) = LOWER('{{ schema }}')
      AND t.table_type IN ('MANAGED', 'EXTERNAL')
),
traced_columns AS (
    SELECT DISTINCT LOWER(target_table_name) AS table_name, LOWER(target_column_name) AS column_name
    FROM system.access.column_lineage
    WHERE LOWER(target_catalog) = LOWER('{{ catalog }}')
      AND LOWER(target_schema)  = LOWER('{{ schema }}')
      AND source_column_name IS NOT NULL
      AND event_time >= current_timestamp() - INTERVAL {{ lookback_days }} DAYS
),
per_column AS (
    SELECT c.table_name, c.column_name, tc.column_name IS NOT NULL AS traced
    FROM derived_tables d
    JOIN table_columns  c  USING (table_name)
    LEFT JOIN traced_columns tc USING (table_name, column_name)
)
SELECT
    COUNT_IF(traced)                                    AS traced_columns,
    COUNT(*)                                            AS total_columns,
    COUNT_IF(traced)::DOUBLE / NULLIF(COUNT(*), 0)      AS value
FROM per_column
```
