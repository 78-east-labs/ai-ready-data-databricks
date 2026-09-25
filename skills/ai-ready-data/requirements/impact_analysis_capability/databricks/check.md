# Check: impact_analysis_capability

Fraction of base tables in the schema whose downstream consumers can be enumerated from `system.access.table_lineage`: at least one row in the window where the table is the source.

## Context

Impact analysis asks "if I change this table, what breaks". On Databricks the answer comes from lineage: every observed read of a table produces a `system.access.table_lineage` row with the table as `source_table_full_name`, the consuming `entity_type` / `entity_id` (`JOB`, `PIPELINE`, `NOTEBOOK`, `DASHBOARD`, `QUERY`, `GENIE_SPACE`, `ALERT` and others, matched case-insensitively), and, if the read fed a write, the `target_table_full_name`. A table with at least one such row has enumerable consumers. A table with none either has no consumers on the platform or has consumers Unity Catalog cannot see (external readers of the storage path, clusters without UC access mode, exports).

The signal is native and it is a proxy in one respect: it lists observed consumers within the window, not all possible consumers. A quarterly report that has not run this month does not appear. Widen `{{ lookback_days }}` (default 30, retention one year) when the schema serves low-frequency workloads.

Views are excluded from the population: a view's consumers are recorded against the view and, through its definition, against the underlying tables, so counting views would double count. Streaming tables and materialized views are excluded because their consumers are usually within the same pipeline; use the variant to include them.

`table_lineage` lags by up to a few hours. Needs `SELECT` on `system.access.table_lineage`; rows are visible only for tables the caller can see. Returns NULL (N/A) when the schema has no base tables.

## SQL

### Any observed consumer (primary)

```sql
WITH tables_in_scope AS (
    SELECT LOWER(table_name) AS table_name
    FROM {{ catalog }}.information_schema.tables
    WHERE LOWER(table_schema) = LOWER('{{ schema }}')
      AND table_type IN ('MANAGED', 'EXTERNAL')
),
consumed AS (
    SELECT DISTINCT LOWER(source_table_name) AS table_name
    FROM system.access.table_lineage
    WHERE LOWER(source_catalog) = LOWER('{{ catalog }}')
      AND LOWER(source_schema)  = LOWER('{{ schema }}')
      AND source_table_full_name IS NOT NULL
      AND event_time >= current_timestamp() - INTERVAL {{ lookback_days }} DAYS
)
SELECT
    COUNT_IF(c.table_name IS NOT NULL)            AS tables_with_consumers,
    COUNT(*)                                       AS total_tables,
    COUNT_IF(c.table_name IS NOT NULL)::DOUBLE
        / NULLIF(COUNT(*), 0)                      AS value
FROM tables_in_scope t
LEFT JOIN consumed c USING (table_name)
```

### Materialized consumers only (variant)

Stricter: only reads that fed a write to another table count (`target_table_full_name IS NOT NULL`). This measures the transformation DAG, which is what a schema change actually propagates through. Dashboards and ad hoc reads are ignored, so the score is usually lower.

```sql
WITH tables_in_scope AS (
    SELECT LOWER(table_name) AS table_name
    FROM {{ catalog }}.information_schema.tables
    WHERE LOWER(table_schema) = LOWER('{{ schema }}')
      AND table_type IN ('MANAGED', 'EXTERNAL')
),
consumed AS (
    SELECT DISTINCT LOWER(source_table_name) AS table_name
    FROM system.access.table_lineage
    WHERE LOWER(source_catalog) = LOWER('{{ catalog }}')
      AND LOWER(source_schema)  = LOWER('{{ schema }}')
      AND source_table_full_name IS NOT NULL
      AND target_table_full_name IS NOT NULL
      AND LOWER(target_table_full_name) <> LOWER(source_table_full_name)
      AND event_time >= current_timestamp() - INTERVAL {{ lookback_days }} DAYS
)
SELECT
    COUNT_IF(c.table_name IS NOT NULL)            AS tables_with_consumers,
    COUNT(*)                                       AS total_tables,
    COUNT_IF(c.table_name IS NOT NULL)::DOUBLE
        / NULLIF(COUNT(*), 0)                      AS value
FROM tables_in_scope t
LEFT JOIN consumed c USING (table_name)
```

### Including pipeline-managed assets (variant)

Adds streaming tables and materialized views to the population. Use for a schema that is mostly a Lakeflow Declarative Pipeline output.

```sql
WITH tables_in_scope AS (
    SELECT LOWER(table_name) AS table_name
    FROM {{ catalog }}.information_schema.tables
    WHERE LOWER(table_schema) = LOWER('{{ schema }}')
      AND table_type IN ('MANAGED', 'EXTERNAL', 'STREAMING_TABLE', 'MATERIALIZED_VIEW')
),
consumed AS (
    SELECT DISTINCT LOWER(source_table_name) AS table_name
    FROM system.access.table_lineage
    WHERE LOWER(source_catalog) = LOWER('{{ catalog }}')
      AND LOWER(source_schema)  = LOWER('{{ schema }}')
      AND source_table_full_name IS NOT NULL
      AND event_time >= current_timestamp() - INTERVAL {{ lookback_days }} DAYS
)
SELECT
    COUNT_IF(c.table_name IS NOT NULL)            AS tables_with_consumers,
    COUNT(*)                                       AS total_tables,
    COUNT_IF(c.table_name IS NOT NULL)::DOUBLE
        / NULLIF(COUNT(*), 0)                      AS value
FROM tables_in_scope t
LEFT JOIN consumed c USING (table_name)
```
