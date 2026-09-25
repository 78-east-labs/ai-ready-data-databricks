# Fix: schema_evolution_tracking

Extend Delta log retention so schema history stays observable for the required window, and capture schema changes somewhere that outlives the log.

## Context

`delta.logRetentionDuration` is a table property; setting it is metadata-only (`ALTER TABLE ... SET TBLPROPERTIES`) and takes effect at the next checkpoint. It is prospective: log entries already deleted are gone, so a table whose history was cleaned yesterday gains nothing today and full coverage arrives after `{{ min_history_days }}` days. Cost is storage for JSON commit files and checkpoints, which is small relative to data files; a table with a commit every minute for 90 days holds about 130,000 small log files, which slows metadata operations slightly. Values are written as `interval N days`.

Do not confuse it with `delta.deletedFileRetentionDuration` (data files, governs time travel on row contents and `VACUUM`). Extending log retention alone does not let you query old row contents; extending both is `data_version_coverage`.

Setting the property creates a `SET TBLPROPERTIES` commit. Run the guard so repeated runs do not add commits. Needs `MODIFY` on the table or ownership.

## Fix: Set log retention on a single table

Guard:

```sql
SHOW TBLPROPERTIES {{ catalog }}.{{ schema }}.{{ asset }} ('delta.logRetentionDuration')
```

Skip if `value` already parses to at least `{{ min_history_days }}` days. Otherwise:

```sql
ALTER TABLE {{ catalog }}.{{ schema }}.{{ asset }}
SET TBLPROPERTIES ('delta.logRetentionDuration' = 'interval {{ min_history_days }} days')
```

Never set a value shorter than the current one on a table others depend on for time travel without telling them; a shorter log retention shortens how far back `VERSION AS OF` works even when data files are still present.

## Fix: Set log retention on every base Delta table below threshold

The property is not in `information_schema`, so this emits a statement for every base Delta table; remove the rows the diagnostic marked `RETAINED` before executing, or accept a harmless no-op commit on those.

```sql
SELECT concat(
    'ALTER TABLE `{{ catalog }}`.`{{ schema }}`.`', table_name,
    '` SET TBLPROPERTIES (''delta.logRetentionDuration'' = ''interval {{ min_history_days }} days'');'
) AS stmt
FROM {{ catalog }}.information_schema.tables
WHERE LOWER(table_schema) = LOWER('{{ schema }}')
  AND table_type IN ('MANAGED', 'EXTERNAL')
  AND UPPER(data_source_format) = 'DELTA'
ORDER BY table_name
```

Show the generated statements to the user before executing them.

## Fix: Snapshot the current schema into a durable record

Retention only delays deletion. To keep schema history for longer than any retention, record it. A daily job that appends `information_schema.columns` for the schema into a history table gives a diffable record with no dependence on the Delta log:

```sql
CREATE TABLE IF NOT EXISTS {{ catalog }}.{{ schema }}.__schema_history (
    snapshot_at      TIMESTAMP,
    table_name       STRING,
    column_name      STRING,
    ordinal_position INT,
    full_data_type   STRING,
    is_nullable      STRING,
    comment          STRING
)
```

```sql
INSERT INTO {{ catalog }}.{{ schema }}.__schema_history
SELECT current_timestamp(), table_name, column_name, ordinal_position, full_data_type, is_nullable, comment
FROM {{ catalog }}.information_schema.columns
WHERE LOWER(table_schema) = LOWER('{{ schema }}')
```

Diff two snapshots:

```sql
WITH a AS (SELECT * FROM {{ catalog }}.{{ schema }}.__schema_history WHERE snapshot_at = TIMESTAMP '{{ from_snapshot }}'),
     b AS (SELECT * FROM {{ catalog }}.{{ schema }}.__schema_history WHERE snapshot_at = TIMESTAMP '{{ to_snapshot }}')
SELECT COALESCE(a.table_name, b.table_name) AS table_name,
       COALESCE(a.column_name, b.column_name) AS column_name,
       CASE WHEN a.column_name IS NULL THEN 'ADDED'
            WHEN b.column_name IS NULL THEN 'DROPPED'
            WHEN a.full_data_type <> b.full_data_type THEN 'TYPE_CHANGED'
            ELSE 'UNCHANGED' END AS change,
       a.full_data_type AS old_type, b.full_data_type AS new_type
FROM a FULL OUTER JOIN b USING (table_name, column_name)
WHERE a.column_name IS NULL OR b.column_name IS NULL OR a.full_data_type <> b.full_data_type
```

Name the history table to match the schema's conventions; the `__` prefix is only a suggestion to keep it out of consumer-facing listings.

## Organizational guidance

Set the retention in the table-creation path so new tables arrive compliant: `TBLPROPERTIES ('delta.logRetentionDuration' = 'interval {{ min_history_days }} days')` in CTAS templates, `spark.databricks.delta.properties.defaults.logRetentionDuration` on the clusters and warehouses that create tables, `+tblproperties` in dbt, `table_properties` in Lakeflow pipeline table definitions, and the `properties` block of Terraform `databricks_sql_table`. Treat schema changes as code: require `ALTER TABLE` DDL to go through the same review as pipeline code, and let the schema-history snapshot job be the record of what actually happened, since DataFrame writes with `mergeSchema` bypass DDL review entirely.
