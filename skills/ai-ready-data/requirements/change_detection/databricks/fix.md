# Fix: change_detection

Enable Change Data Feed on Delta tables that lack it, and point consumers at `table_changes()`.

## Context

Enabling CDF is a metadata-only `ALTER TABLE ... SET TBLPROPERTIES`. It does not rewrite existing data, does not create a new table version of the data, and takes effect from the next commit. From that commit on, `UPDATE`, `DELETE` and `MERGE` write additional `_change_data` files (inserts and blind appends are derived from the main data files and cost nothing extra). Change files are cleaned up by `VACUUM` under `delta.deletedFileRetentionDuration` (default 7 days), so consumers that need to catch up after longer outages need a longer retention, which is the `data_version_coverage` requirement.

What it does not give you: history before the enabling version. `table_changes()` for versions older than that raises an error. If a consumer needs a full initial snapshot, read the table itself once and then start the feed from the current version.

Idempotency: re-running `SET TBLPROPERTIES` with the same value is harmless but does create a `SET TBLPROPERTIES` commit in history. Run the guard first.

Permissions: `MODIFY` on the table (or ownership). Do not use `CREATE OR REPLACE TABLE ... TBLPROPERTIES (...)` to enable it; that rewrites the table and breaks time travel, streaming checkpoints and Delta Sync indexes.

## Fix: Enable CDF on a single table

Guard:

```sql
SHOW TBLPROPERTIES {{ catalog }}.{{ schema }}.{{ asset }} ('delta.enableChangeDataFeed')
```

Skip if `value` is `true`. Otherwise:

```sql
ALTER TABLE {{ catalog }}.{{ schema }}.{{ asset }}
SET TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
```

Verify:

```sql
SELECT version, timestamp, operation, operationParameters
FROM (DESCRIBE HISTORY {{ catalog }}.{{ schema }}.{{ asset }})
ORDER BY version DESC
LIMIT 1
```

The top row should be `SET TBLPROPERTIES`; its `version` is the first version readable through `table_changes()`.

## Fix: Enable CDF on every base Delta table that lacks it

Generate one statement per table from the check's enumeration, filter to those whose probe failed, and run them in a batch. Because the property is not in `information_schema`, the SQL below emits a statement for every Delta base table; drop the ones the diagnostic reported as `ENABLED` before executing, or accept the harmless no-op commits.

```sql
SELECT concat(
    'ALTER TABLE `{{ catalog }}`.`{{ schema }}`.`', table_name,
    '` SET TBLPROPERTIES (''delta.enableChangeDataFeed'' = ''true'');'
) AS stmt
FROM {{ catalog }}.information_schema.tables
WHERE LOWER(table_schema) = LOWER('{{ schema }}')
  AND table_type IN ('MANAGED', 'EXTERNAL')
  AND UPPER(data_source_format) = 'DELTA'
ORDER BY table_name
```

Show the generated statements to the user before executing them. Skip tables the diagnostic marked `DISABLED_PREVIOUSLY_ENABLED` until the owner confirms it was not turned off on purpose.

## Fix: Make the retention long enough for consumers

If consumers poll less often than weekly, extend the change-file retention so a missed run does not lose the feed. Never set it below 7 days.

```sql
ALTER TABLE {{ catalog }}.{{ schema }}.{{ asset }}
SET TBLPROPERTIES ('delta.deletedFileRetentionDuration' = 'interval 30 days')
```

This increases storage for deleted and change files proportionally to the table's churn.

## Fix: Read the feed

Batch consumers use the `table_changes` table-valued function with a starting version or timestamp:

```sql
SELECT _change_type, _commit_version, _commit_timestamp, *
FROM table_changes('{{ catalog }}.{{ schema }}.{{ asset }}', {{ start_version }})
WHERE _change_type IN ('insert', 'update_postimage', 'delete')
```

Streaming consumers set `readChangeFeed`:

```python
(spark.readStream.format("delta")
   .option("readChangeFeed", "true")
   .option("startingVersion", {{ start_version }})
   .table("{{ catalog }}.{{ schema }}.{{ asset }}"))
```

Persist the last processed `_commit_version` per consumer (or rely on the streaming checkpoint) so each run resumes where it left off.

## Organizational guidance

Make CDF the default for new tables instead of back-filling it. On the pipelines and jobs that create tables, set `spark.databricks.delta.properties.defaults.enableChangeDataFeed = true` in the cluster or warehouse Spark configuration, add `TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')` to CTAS templates, set `+tblproperties: {delta.enableChangeDataFeed: "true"}` in dbt project configs, and put the property in Terraform `databricks_sql_table` resources. Lakeflow Declarative Pipelines tables that need CDF for downstream `APPLY CHANGES` or Delta Sync consumers should declare it in `table_properties`. Pair enabling with a retention decision so the feed is actually consumable for as long as the slowest consumer needs.
