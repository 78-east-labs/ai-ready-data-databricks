# Diagnostic: retention_policy

Per-table view of the declared retention, the Delta retention properties, VACUUM evidence, and the columns a deletion job could key on.

## Context

Two parts. The SQL part reuses the check's enumeration and adds, from metadata only:

- `retention_value` and `retention_days` (parsed; `-1` means `indefinite`), with `tag_status` `VALID`, `UNPARSEABLE` (a value like `2 years` or `per policy`) or `MISSING`.
- `has_pii_columns` and `legal_basis`, because retention matters most, and is usually legally required, where personal data is held.
- `timestamp_candidates`: columns of TIMESTAMP or DATE type, or named like `created_at|event_time|_ts|_date`, which is what a `DELETE WHERE ts < now - retention` job needs.
- `last_po_vacuum`: the most recent predictive-optimization VACUUM in `system.storage.predictive_optimization_operations_history` (lags hours; only populated when predictive optimization is enabled).

The probe part fills in what SQL cannot see: `delta.deletedFileRetentionDuration`, `delta.logRetentionDuration`, and the last `VACUUM END` commit from `DESCRIBE HISTORY`. Per table:

```sql
DESCRIBE DETAIL {{ catalog }}.{{ schema }}.`{table_name}`;
DESCRIBE HISTORY {{ catalog }}.{{ schema }}.`{table_name}` LIMIT 200;
```

From `DESCRIBE DETAIL` take `properties['delta.deletedFileRetentionDuration']` (NULL means the 7-day default) and `properties['delta.logRetentionDuration']` (NULL means 30 days). From `DESCRIBE HISTORY` take `MAX(timestamp) WHERE operation = 'VACUUM END'`; a table with rows deleted but no VACUUM in the last `deletedFileRetentionDuration + 7` days is not enforcing its retention regardless of the property. Combine with the SQL rows on `table_name` and apply the check's predicate to get the per-table `passes` flag.

Sorted worst-first: personal-data tables with no tag at the top.

## SQL

```sql
WITH tables_in_scope AS (
    SELECT LOWER(table_name) AS table_name, table_name AS table_name_cased,
           table_owner, data_source_format
    FROM {{ catalog }}.information_schema.tables
    WHERE LOWER(table_schema) = LOWER('{{ schema }}')
      AND table_type IN ('MANAGED', 'EXTERNAL')
),
tags AS (
    SELECT LOWER(table_name) AS table_name,
           MAX(CASE WHEN LOWER(tag_name) = 'retention_days' THEN tag_value END) AS retention_value,
           MAX(CASE WHEN LOWER(tag_name) = 'legal_basis'    THEN tag_value END) AS legal_basis,
           MAX(CASE WHEN LOWER(tag_name) = 'retention_ref'  THEN tag_value END) AS retention_ref
    FROM {{ catalog }}.information_schema.table_tags
    WHERE LOWER(schema_name) = LOWER('{{ schema }}')
    GROUP BY LOWER(table_name)
),
pii AS (
    SELECT DISTINCT LOWER(table_name) AS table_name
    FROM {{ catalog }}.information_schema.column_tags
    WHERE LOWER(schema_name) = LOWER('{{ schema }}')
      AND LOWER(tag_name) = LOWER('{{ pii_tag_key }}')
),
ts_cols AS (
    SELECT LOWER(table_name) AS table_name,
           array_sort(collect_set(column_name)) AS timestamp_candidates
    FROM {{ catalog }}.information_schema.columns
    WHERE LOWER(table_schema) = LOWER('{{ schema }}')
      AND (data_type IN ('TIMESTAMP', 'TIMESTAMP_NTZ', 'DATE')
           OR REGEXP_LIKE(LOWER(column_name), '(^|_)(created|updated|inserted|event|ingested|loaded)_(at|ts|time|date)$|_ts$|_date$'))
    GROUP BY LOWER(table_name)
),
po_vacuum AS (
    SELECT LOWER(table_name) AS table_name, MAX(end_time) AS last_po_vacuum
    FROM system.storage.predictive_optimization_operations_history
    WHERE LOWER(catalog_name) = LOWER('{{ catalog }}')
      AND LOWER(schema_name)  = LOWER('{{ schema }}')
      AND operation_type = 'VACUUM'
      AND operation_status = 'SUCCESSFUL'
    GROUP BY LOWER(table_name)
)
SELECT
    t.table_name_cased AS table_name,
    t.table_owner,
    t.data_source_format,
    g.retention_value,
    CASE
        WHEN LOWER(trim(g.retention_value)) = 'indefinite' THEN -1
        ELSE TRY_CAST(trim(g.retention_value) AS INT)
    END AS retention_days,
    CASE
        WHEN g.retention_value IS NULL THEN 'MISSING'
        WHEN LOWER(trim(g.retention_value)) = 'indefinite'
             OR TRY_CAST(trim(g.retention_value) AS INT) > 0 THEN 'VALID'
        ELSE 'UNPARSEABLE'
    END AS tag_status,
    g.retention_ref,
    p.table_name IS NOT NULL AS has_pii_columns,
    g.legal_basis,
    c.timestamp_candidates,
    v.last_po_vacuum
FROM tables_in_scope t
LEFT JOIN tags      g USING (table_name)
LEFT JOIN pii       p USING (table_name)
LEFT JOIN ts_cols   c USING (table_name)
LEFT JOIN po_vacuum v USING (table_name)
ORDER BY
    CASE WHEN g.retention_value IS NULL AND p.table_name IS NOT NULL THEN 0
         WHEN g.retention_value IS NULL                              THEN 1
         WHEN LOWER(trim(g.retention_value)) <> 'indefinite'
              AND TRY_CAST(trim(g.retention_value) AS INT) IS NULL   THEN 2
         ELSE 3 END,
    t.table_name
```

If `system.storage` is not enabled or granted, drop the `po_vacuum` CTE and column; the rest reads `information_schema` only. `operation_status` values are as documented for the operations history table (`SUCCESSFUL`, `FAILED`); if the literal does not match in your metastore, run `SELECT DISTINCT operation_status FROM system.storage.predictive_optimization_operations_history` and adjust.

### Property summary after probing

Once the probes have run, this is the shape to present (one row per table, produced by the orchestrator from the SQL rows plus `DESCRIBE DETAIL` / `DESCRIBE HISTORY`):

| column | source |
|---|---|
| `table_name`, `retention_days`, `tag_status`, `has_pii_columns` | SQL above |
| `deleted_file_retention` | `properties['delta.deletedFileRetentionDuration']`, `interval 7 days` if unset |
| `deleted_file_retention_days` | converted with the check's expression |
| `log_retention` | `properties['delta.logRetentionDuration']`, `interval 30 days` if unset |
| `last_vacuum_end` | `MAX(timestamp)` over `DESCRIBE HISTORY` rows with `operation = 'VACUUM END'` |
| `last_delete` | `MAX(timestamp)` over rows with `operation IN ('DELETE', 'MERGE', 'UPDATE')` |
| `status` | `CONSISTENT` (passes predicate), `PROPERTY_EXCEEDS_POLICY` (tag valid, property longer), `NO_VACUUM_SINCE_DELETE` (passes, but `last_delete` is more recent than `last_vacuum_end` by more than the retention), `NO_TAG`, `UNPARSEABLE_TAG` |
