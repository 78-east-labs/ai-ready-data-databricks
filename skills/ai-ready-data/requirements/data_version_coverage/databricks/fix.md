# Fix: data_version_coverage

Raise the Delta retention properties so time travel covers the required window, and add an explicit version column where reproducibility matters more than rollback.

## Context

Setting `delta.logRetentionDuration` and `delta.deletedFileRetentionDuration` is a metadata-only `ALTER TABLE ... SET TBLPROPERTIES`. It creates one commit, rewrites no data, and takes effect for future `VACUUM` and log-cleanup runs. It cannot bring back files that an earlier `VACUUM` already deleted, so history before the fix stays as short as it was.

Cost is storage: every deleted or rewritten file is kept for `deletedFileRetentionDuration`. On a table that is fully rewritten daily, 30 days of retention means roughly 30 copies of the table on disk. Check `sizeInBytes` and the write pattern (see `incremental_update_coverage`) before applying a long retention to large overwrite-style tables. Predictive optimization runs `VACUUM` automatically using these properties, so no separate scheduling is needed once they are set.

Guard: `SHOW TBLPROPERTIES` on the table. Skip when both properties already equal the desired values.

Never `VACUUM` with a retention under 7 days (168 hours). It is not needed here and it destroys the history this requirement is about.

## Fix: Set retention on a single table

```sql
ALTER TABLE {{ catalog }}.{{ schema }}.{{ asset }}
SET TBLPROPERTIES (
    'delta.logRetentionDuration'         = 'interval {{ min_retention_days }} days',
    'delta.deletedFileRetentionDuration' = 'interval {{ min_retention_days }} days'
)
```

Setting `logRetentionDuration` equal to the deleted-file retention is the minimum that makes the window real. Many teams set the log longer (for example 90 days) than the files (30 days) so that `DESCRIBE HISTORY` keeps a longer audit trail of who changed what, even after the data for those versions is gone.

## Fix: Generate statements for every Delta table in the schema

Emits one statement per Delta base table. `SET TBLPROPERTIES` with an identical value is a harmless no-op commit, so the list can be re-run; filter it against the diagnostic's `status` column if you only want the `SHORT` and `LOG_ONLY` tables.

```sql
SELECT concat(
    'ALTER TABLE {{ catalog }}.{{ schema }}.`', table_name, '` SET TBLPROPERTIES (',
    '''delta.logRetentionDuration'' = ''interval {{ min_retention_days }} days'', ',
    '''delta.deletedFileRetentionDuration'' = ''interval {{ min_retention_days }} days'');'
) AS stmt
FROM {{ catalog }}.information_schema.tables
WHERE LOWER(table_schema) = LOWER('{{ schema }}')
  AND table_type IN ('MANAGED', 'EXTERNAL')
  AND UPPER(data_source_format) = 'DELTA'
ORDER BY table_name
```

Show the generated statements to the user before executing them.

## Fix: Make the retention the schema default for new tables

Tables created after this inherit the properties, so new tables do not start with a 7-day window. Existing tables are unaffected; run the bulk fix for those.

```sql
ALTER SCHEMA {{ catalog }}.{{ schema }}
SET DBPROPERTIES (
    'delta.logRetentionDuration'         = 'interval {{ min_retention_days }} days',
    'delta.deletedFileRetentionDuration' = 'interval {{ min_retention_days }} days'
)
```

Schema-level `delta.*` defaults are honored by Databricks Runtime for tables created in that schema; confirm with `DESCRIBE DETAIL` on a table created afterwards, because the behavior depends on the runtime version.

## Fix: Pin a training snapshot explicitly

Time travel is for rollback and audit. For a dataset that must stay reproducible for longer than any retention window, record the Delta version at read time and keep it with the model:

```sql
DESCRIBE HISTORY {{ catalog }}.{{ schema }}.{{ asset }} LIMIT 1
```

The first row is the current commit; take its `version` column as `pinned_version`. Then read with `SELECT * FROM {{ catalog }}.{{ schema }}.{{ asset }} VERSION AS OF {{ pinned_version }}` and store `pinned_version` alongside the run (MLflow `log_param`). If the window may lapse before the model is retired, clone that version into a frozen table instead of relying on retention: `CREATE TABLE {{ catalog }}.{{ schema }}.{{ asset }}_v{{ pinned_version }} SHALLOW CLONE {{ catalog }}.{{ schema }}.{{ asset }} VERSION AS OF {{ pinned_version }}` (fails if the target exists, so it is safe to re-run).

## Organizational guidance

Retention is a policy, not a per-table afterthought. Decide the reconstruction window per data tier (raw 7 days, curated 30, training sets 90 or a frozen clone), put the properties into the table-creation templates (Lakeflow `TBLPROPERTIES`, dbt `tblproperties` config, Terraform `databricks_sql_table.properties`), and make `retention_policy` and this check disagree loudly when a table's declared legal retention is shorter than its time-travel window.
