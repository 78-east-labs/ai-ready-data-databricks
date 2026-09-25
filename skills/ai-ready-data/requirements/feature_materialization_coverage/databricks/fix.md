# Fix: feature_materialization_coverage

Create a Lakebase synced table (or, on older workspaces, an Online Table) for each feature table that a model serving endpoint needs at request time.

## Context

Materializing online is a cost decision, not a hygiene step. A synced table is a running Postgres copy that is billed while it exists, so create one only for feature tables a serving endpoint reads. For batch-only features, exclude the table from the profile (or drop the `feature_table` tag if it was applied loosely) instead of paying for an unused copy.

Prerequisites, in order:

1. **A primary key.** Both APIs require `primary_key_columns`. If the table has no PK constraint, add an informational one first (below). The key must be unique in practice; Databricks does not enforce it, and duplicate keys make the sync fail or overwrite rows.
2. **Change Data Feed** on the source for triggered and continuous sync (`delta.enableChangeDataFeed = true`). Snapshot mode does not need it. Enabling CDF does not rewrite data.
3. **A Lakebase database instance** (`w.database.create_database_instance`) for synced tables. Online Tables need none.

Sync policies: `SNAPSHOT` copies the whole table on each run (small tables, infrequent change), `TRIGGERED` applies CDF changes when asked (most feature tables, run after the feature job), `CONTINUOUS` streams changes (low-latency features; the source table cannot be overwritten while continuous sync runs).

Idempotency: both create calls fail if the name exists. Guard with `get` and skip; never delete and recreate, that drops the serving copy under a live endpoint.

Permissions: ownership or `MODIFY` on the source for `ALTER TABLE`; `CREATE TABLE` on the target schema; `CAN USE` on the Lakebase instance; `USE CATALOG` / `USE SCHEMA`.

## Fix: Add a primary key to a feature table

Guard: `SELECT 1 FROM {{ catalog }}.information_schema.table_constraints WHERE LOWER(table_schema) = LOWER('{{ schema }}') AND LOWER(table_name) = LOWER('{{ asset }}') AND constraint_type = 'PRIMARY KEY'`. Skip if it returns a row. Check uniqueness first:

```sql
SELECT COUNT(*) - COUNT(DISTINCT {{ key_columns }}) AS duplicate_keys,
       COUNT_IF({{ key_columns }} IS NULL)          AS null_keys
FROM {{ catalog }}.{{ schema }}.{{ asset }}
```

Both must be zero. Then:

```sql
ALTER TABLE {{ catalog }}.{{ schema }}.{{ asset }}
ALTER COLUMN {{ key_columns }} SET NOT NULL;

ALTER TABLE {{ catalog }}.{{ schema }}.{{ asset }}
ADD CONSTRAINT {{ asset }}_pk PRIMARY KEY ({{ key_columns }});
```

For a time-series feature table add `TIMESERIES` after the timestamp column: `PRIMARY KEY (entity_id, event_ts TIMESERIES)`.

## Fix: Enable Change Data Feed on the source

Guard: `SHOW TBLPROPERTIES {{ catalog }}.{{ schema }}.{{ asset }} ('delta.enableChangeDataFeed')`. Skip if `true`.

```sql
ALTER TABLE {{ catalog }}.{{ schema }}.{{ asset }}
SET TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
```

## Fix: Create a Lakebase synced table

Python, `databricks-sdk>=0.50`. Class and enum names are as of the 2025 SDK; if the import fails, inspect `databricks.sdk.service.database` for the current names (they have moved once already).

```python
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound
from databricks.sdk.service.database import (
    SyncedDatabaseTable, SyncedTableSpec, SyncedTableSchedulingPolicy)

w = WorkspaceClient()
name = "{{ catalog }}.{{ schema }}.{{ asset }}_online"
try:
    w.database.get_synced_database_table(name=name)
    print("exists, skipping")
except NotFound:
    w.database.create_synced_database_table(SyncedDatabaseTable(
        name=name,
        database_instance_name="{{ lakebase_instance }}",
        logical_database_name="{{ lakebase_database }}",
        spec=SyncedTableSpec(
            source_table_full_name="{{ catalog }}.{{ schema }}.{{ asset }}",
            primary_key_columns=["{{ key_columns }}"],
            scheduling_policy=SyncedTableSchedulingPolicy.TRIGGERED,
            timeseries_key="{{ timeseries_column }}",  # omit when there is none
        ),
    ))
```

CLI:

```bash
databricks database create-synced-database-table --json '{
  "name": "{{ catalog }}.{{ schema }}.{{ asset }}_online",
  "database_instance_name": "{{ lakebase_instance }}",
  "logical_database_name": "{{ lakebase_database }}",
  "spec": {
    "source_table_full_name": "{{ catalog }}.{{ schema }}.{{ asset }}",
    "primary_key_columns": ["{{ key_columns }}"],
    "scheduling_policy": "TRIGGERED"
  }
}'
```

Provisioning takes minutes. Poll `data_synchronization_status.detailed_state` until it starts with `ONLINE`.

## Fix: Create an Online Table (workspaces without Lakebase)

Deprecated path; use it only where `w.database` is unavailable. Same guard pattern.

```python
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound
from databricks.sdk.service.catalog import (
    OnlineTable, OnlineTableSpec, OnlineTableSpecTriggeredSchedulingPolicy)

w = WorkspaceClient()
name = "{{ catalog }}.{{ schema }}.{{ asset }}_online"
try:
    w.online_tables.get(name=name)
    print("exists, skipping")
except NotFound:
    w.online_tables.create(table=OnlineTable(
        name=name,
        spec=OnlineTableSpec(
            source_table_full_name="{{ catalog }}.{{ schema }}.{{ asset }}",
            primary_key_columns=["{{ key_columns }}"],
            run_triggered=OnlineTableSpecTriggeredSchedulingPolicy(),
        ),
    ))
```

CLI: `databricks online-tables create --json '{"name": "...", "spec": {"source_table_full_name": "...", "primary_key_columns": ["..."], "run_triggered": {}}}'`.

## Fix: Tag batch-only feature tables so they leave the population

If a PK table is not a feature table for serving, say so with the tag rather than leaving the check red. The tag records a decision; make it only after confirming with the owner.

```sql
ALTER TABLE {{ catalog }}.{{ schema }}.{{ asset }}
SET TAGS ('{{ feature_tag_key }}' = 'batch_only')
```

The check counts only `true`, `yes`, `1` as feature tables, so `batch_only` removes the table from the denominator while keeping the fact visible.

## Fix: Bulk-generate CDF and PK prerequisites for every NOT_ONLINE table

Feed the diagnostic's `NOT_ONLINE` rows in as a temp view `not_online(table_name)`. Synced-table creation itself is done through the SDK loop above with the same list.

```sql
SELECT concat(
    'ALTER TABLE {{ catalog }}.{{ schema }}.`', table_name,
    '` SET TBLPROPERTIES (''delta.enableChangeDataFeed'' = ''true'');'
) AS stmt
FROM not_online
ORDER BY table_name
```

Show the generated statements to the user before executing them.

## Organizational guidance

Decide per feature table, at design time, whether it is served online, and encode that in the feature job: the same Lakeflow job that writes the feature table triggers its synced table's refresh. Keep the source and online copy names paired (`t` and `t_online`) and in the same repo, tag `feature_table = 'true'` on serving tables and `'batch_only'` on the rest, and run this check after each new model deployment to catch a `FeatureLookup` pointing at a table with no online copy.
