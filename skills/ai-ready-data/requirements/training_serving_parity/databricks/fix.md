# Fix: training_serving_parity

Create an online copy of each feature table that is synced from the table itself, so serving reads what training read.

## Context

The fix is to create a Lakebase synced table (preferred) or, on workspaces without Lakebase, a legacy online table, with `source_table_full_name` set to the feature table. Both are created through the SDK, CLI or Catalog Explorer; there is no SQL DDL for them. Preconditions, all checked by the diagnostic:

- **Primary key** on the source (Unity Catalog constraint). Required by both mechanisms; `point_in_time_correctness` and `entity_identifier_declaration` cover how to add one.
- **Change Data Feed** on the source for `TRIGGERED` or `CONTINUOUS` sync (`change_detection/databricks/fix.md`). `SNAPSHOT` mode works without it but re-copies the whole table each run and is only appropriate for small, slowly changing tables.
- **A Lakebase database instance** to hold synced tables. Creating one provisions Postgres compute that bills while it runs; that is an infrastructure decision for the platform owner, not something to do silently inside an assessment fix.
- **Time-series key**: if the source is a time-series feature table, pass its `TIMESERIES` column as `timeseries_key` so the online copy keeps only the latest row per entity and lookups return the current value.

Idempotency: both `create` calls fail if the target name exists. The guard is the `get` call from the check; skip creation when it succeeds and the returned `spec.source_table_full_name` matches.

Do not build the online copy from a different query than the training table. If the serving path needs derived or filtered features, add them to the feature table first (that is the parity the requirement measures), then sync.

## Fix: Create a Lakebase synced table (SDK)

```python
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.database import (
    SyncedDatabaseTable, SyncedTableSpec, SyncedTableSchedulingPolicy, NewPipelineSpec)

w = WorkspaceClient()
source = "{{ catalog }}.{{ schema }}.{{ asset }}"
target = "{{ catalog }}.{{ schema }}.{{ asset }}_online"

try:                                             # guard
    existing = w.database.get_synced_database_table(name=target)
    assert existing.spec.source_table_full_name.lower() == source.lower(), existing.spec
    print("exists", target)
except Exception:
    w.database.create_synced_database_table(synced_table=SyncedDatabaseTable(
        name=target,
        database_instance_name="{{ database_instance }}",
        logical_database_name="{{ logical_database }}",
        spec=SyncedTableSpec(
            source_table_full_name=source,
            primary_key_columns=["{{ key_column }}"],
            timeseries_key="{{ timestamp_column }}",   # omit for non time-series tables
            scheduling_policy=SyncedTableSchedulingPolicy.TRIGGERED,
            new_pipeline_spec=NewPipelineSpec(storage_catalog="{{ catalog }}", storage_schema="{{ schema }}"),
        )))
    print("created", target)
```

`create_synced_database_table` starts a pipeline that performs the initial copy; poll `w.database.get_synced_database_table(name=target).data_synchronization_status` until it reports online. Class and argument names (`SyncedDatabaseTable`, `SyncedTableSpec`, `NewPipelineSpec`, the `synced_table=` keyword) are from the current SDK; earlier previews used `SyncedTable` and a positional argument, and the CLI command was `synced-database-tables create`. If an import fails, check `databricks.sdk.service.database` in the installed version for the current names.

CLI equivalent:

```bash
databricks database create-synced-database-table --json '{
  "name": "{{ catalog }}.{{ schema }}.{{ asset }}_online",
  "database_instance_name": "{{ database_instance }}",
  "logical_database_name": "{{ logical_database }}",
  "spec": {
    "source_table_full_name": "{{ catalog }}.{{ schema }}.{{ asset }}",
    "primary_key_columns": ["{{ key_column }}"],
    "timeseries_key": "{{ timestamp_column }}",
    "scheduling_policy": "TRIGGERED",
    "new_pipeline_spec": {"storage_catalog": "{{ catalog }}", "storage_schema": "{{ schema }}"}
  }
}'
```

Grant the serving principal `SELECT` on the synced table and `CAN USE` on the database instance afterwards.

## Fix: Create a legacy online table (SDK)

Only where Lakebase is unavailable. Online tables are deprecated and Databricks documents a migration path to synced tables.

```python
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import OnlineTable, OnlineTableSpec, OnlineTableSpecTriggeredSchedulingPolicy

w = WorkspaceClient()
source = "{{ catalog }}.{{ schema }}.{{ asset }}"
target = "{{ catalog }}.{{ schema }}.{{ asset }}_online"
try:                                             # guard
    assert w.online_tables.get(name=target).spec.source_table_full_name.lower() == source.lower()
    print("exists", target)
except Exception:
    w.online_tables.create(table=OnlineTable(name=target, spec=OnlineTableSpec(
        source_table_full_name=source,
        primary_key_columns=["{{ key_column }}"],
        timeseries_key="{{ timestamp_column }}",
        run_triggered=OnlineTableSpecTriggeredSchedulingPolicy(),
    )))
    print("created", target)
```

Older SDKs took `w.online_tables.create(name=..., spec=...)`; the `table=OnlineTable(...)` form is the current one.

```bash
databricks online-tables create --json '{
  "name": "{{ catalog }}.{{ schema }}.{{ asset }}_online",
  "spec": {
    "source_table_full_name": "{{ catalog }}.{{ schema }}.{{ asset }}",
    "primary_key_columns": ["{{ key_column }}"],
    "timeseries_key": "{{ timestamp_column }}",
    "run_triggered": {}
  }
}'
```

## Fix: Create with the Feature Engineering client

When the team already uses `databricks-feature-engineering`, creating the copy through it keeps the feature table's metadata (`online_stores`) in sync with reality. The method name has changed across releases (`create_online_table`, then `create_synced_table` / `publish_table` with a Lakebase store); check `dir(fe)` on the installed version.

```python
from databricks.feature_engineering import FeatureEngineeringClient
fe = FeatureEngineeringClient()
fe.create_online_table(   # or the synced-table equivalent in your release
    source_table_name="{{ catalog }}.{{ schema }}.{{ asset }}",
    name="{{ catalog }}.{{ schema }}.{{ asset }}_online",
)
```

## Fix: Repoint a copy that syncs from the wrong source

A synced or online table's `source_table_full_name` cannot be edited in place. Create a new copy from the correct source under a new name, switch the feature spec or serving endpoint to it, then remove the old copy with the owner's confirmation:

```bash
databricks database delete-synced-database-table {{ old_online_table_full_name }}
```

This deletes the online copy only; the source Delta table is untouched. It is listed here because it is the only way to retire a wrong-source copy, and it must be run by the operator after confirming nothing serves from it (`databricks serving-endpoints list` and the feature specs that reference it).

## Organizational guidance

Make "one feature table, one synced copy, same name plus `_online`" the convention and generate the copy in the same deployment that registers the feature table (Databricks Asset Bundles with a `synced_database_table` resource, or a post-deploy step in the feature pipeline). Serve features through Feature Serving endpoints or Model Serving with feature lookup, both of which resolve the online copy from the feature table's metadata; hand-written serving code that recomputes features is the parity gap no sync can close. Enable Change Data Feed and a primary key on every feature table at creation (`change_detection`, `entity_identifier_declaration`) so triggered sync is always an option, and monitor the synced tables' `data_synchronization_status` alongside `feature_refresh_compliance`.
