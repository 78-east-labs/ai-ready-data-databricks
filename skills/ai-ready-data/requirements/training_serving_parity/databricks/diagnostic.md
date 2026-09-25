# Diagnostic: training_serving_parity

One row per feature table with its online copies (if any), their kind, sync mode, pipeline status, and the reasons a copy could not be created yet.

## Context

Combines the check's SQL inventory with the SDK walk. For each feature table the report shows:

- `online_copies`: the full names of synced or online tables whose `spec.source_table_full_name` is this table, with kind (`synced` / `online`).
- `sync_mode`: `SNAPSHOT`, `TRIGGERED` or `CONTINUOUS` (synced) or `run_triggered` / `run_continuously` (online). Snapshot copies are re-created wholesale and can be stale; triggered and continuous copies need Change Data Feed on the source.
- `copy_status`: the synced table's `data_synchronization_status` (or the online table's `status`), which is where a broken sync shows up.
- `cdf_enabled`, `pk_columns`, `timeseries_declared`: the preconditions for creating a synced table. A synced table needs a primary key; a triggered or continuous one needs CDF; a time-series feature table should pass its `TIMESERIES` column as the synced table's `timeseries_key`.
- `orphan_online_copies` (separate list): online objects in the catalog whose source is not a feature table in this schema, or whose source no longer exists. These are the copies most likely to have drifted from any training set.

Status values: `SERVED` (at least one copy), `SERVED_SNAPSHOT_ONLY` (copies exist but all snapshot mode), `NOT_SERVED_READY` (no copy; PK present, CDF on), `NOT_SERVED_NEEDS_CDF` (no copy; PK present, CDF off), `NOT_SERVED_NO_PK` (tagged feature table without a key). Sorted worst-first.

## SQL

### Feature table preconditions (pure SQL)

```sql
WITH tables_in_scope AS (
    SELECT LOWER(table_name) AS table_name, table_owner
    FROM {{ catalog }}.information_schema.tables
    WHERE LOWER(table_schema) = LOWER('{{ schema }}')
      AND table_type IN ('MANAGED', 'EXTERNAL')
      AND UPPER(data_source_format) = 'DELTA'
),
pk AS (
    SELECT LOWER(tc.table_name) AS table_name, tc.constraint_name,
           array_join(transform(array_sort(collect_list(struct(k.ordinal_position, k.column_name))), x -> x.column_name), ', ')
               AS pk_columns
    FROM {{ catalog }}.information_schema.table_constraints tc
    JOIN {{ catalog }}.information_schema.key_column_usage k
      ON k.constraint_name = tc.constraint_name AND k.table_schema = tc.table_schema AND k.table_name = tc.table_name
    WHERE LOWER(tc.table_schema) = LOWER('{{ schema }}')
      AND tc.constraint_type = 'PRIMARY KEY'
    GROUP BY LOWER(tc.table_name), tc.constraint_name
),
tagged AS (
    SELECT DISTINCT LOWER(table_name) AS table_name
    FROM {{ catalog }}.information_schema.table_tags
    WHERE LOWER(schema_name) = LOWER('{{ schema }}')
      AND tag_name = 'feature_table' AND LOWER(tag_value) = 'true'
)
SELECT
    CONCAT(LOWER('{{ catalog }}'), '.', LOWER('{{ schema }}'), '.', t.table_name) AS full_name,
    t.table_owner,
    pk.constraint_name IS NOT NULL AS has_primary_key,
    pk.pk_columns,
    tg.table_name IS NOT NULL      AS tagged_feature_table
FROM tables_in_scope t
LEFT JOIN pk     USING (table_name)
LEFT JOIN tagged tg USING (table_name)
WHERE pk.constraint_name IS NOT NULL OR tg.table_name IS NOT NULL
ORDER BY full_name
```

`cdf_enabled` comes from `DESCRIBE DETAIL` (`properties['delta.enableChangeDataFeed']`, see `change_detection`) and `timeseries_declared` from `SHOW CREATE TABLE` (see `point_in_time_correctness`); run those two probes per feature table and merge on `full_name`.

### Online copies with status (SDK)

```python
from databricks.sdk import WorkspaceClient
w = WorkspaceClient()

def describe_online(full_name):
    try:
        t = w.database.get_synced_database_table(name=full_name)
        s = t.spec
        return dict(name=full_name, kind="synced", source=(s.source_table_full_name or "").lower(),
                    mode=str(getattr(s, "scheduling_policy", None)),
                    status=str(getattr(getattr(t, "data_synchronization_status", None), "detailed_state", None)),
                    instance=getattr(s, "database_instance_name", None),
                    timeseries_key=getattr(s, "timeseries_key", None))
    except Exception:
        pass
    try:
        t = w.online_tables.get(name=full_name)
        s = t.spec
        mode = "run_continuously" if getattr(s, "run_continuously", None) else "run_triggered"
        return dict(name=full_name, kind="online", source=(s.source_table_full_name or "").lower(),
                    mode=mode, status=str(getattr(getattr(t, "status", None), "detailed_state", None)),
                    instance=None, timeseries_key=getattr(s, "timeseries_key", None))
    except Exception:
        return None

copies = []
for schema in w.schemas.list(catalog_name="{{ catalog }}"):
    for t in w.tables.list(catalog_name="{{ catalog }}", schema_name=schema.name):
        if t.table_type and t.table_type.value in ("MANAGED", "EXTERNAL", "VIEW", "MATERIALIZED_VIEW", "STREAMING_TABLE"):
            continue
        d = describe_online(t.full_name)
        if d:
            copies.append(d)
```

Field names `data_synchronization_status.detailed_state`, `scheduling_policy`, `database_instance_name` and `timeseries_key` follow the current SDK; the `getattr` guards keep the script running on releases where one of them is missing, at the cost of a `None` in that column. Print `copies` alongside the SQL result and group by `source`.

### Assemble the report

For each feature table `f` from the SQL:

```
online_copies        = [c.name + " (" + c.kind + ")" for c in copies if c.source == f.full_name]
sync_modes           = {c.mode for c in matching copies}
copy_status          = {c.status for c in matching copies}
status = SERVED                 if copies and any mode not in (SNAPSHOT, None)
       | SERVED_SNAPSHOT_ONLY   if copies and all modes are SNAPSHOT
       | NOT_SERVED_NO_PK       if not has_primary_key
       | NOT_SERVED_NEEDS_CDF   if not cdf_enabled
       | NOT_SERVED_READY       otherwise
```

Orphans: `[c for c in copies if c.source not in feature_table_names]`, with `source` so the operator can see whether it points at a dropped table, a table in another schema, or a non-feature table.

Sort: `NOT_SERVED_NO_PK`, `NOT_SERVED_NEEDS_CDF`, `NOT_SERVED_READY`, `SERVED_SNAPSHOT_ONLY`, `SERVED`; within a status by `full_name`.

### CLI spot check for one copy

```bash
databricks database get-synced-database-table {{ online_table_full_name }} --output json \
  | jq '{source: .spec.source_table_full_name, mode: .spec.scheduling_policy, state: .data_synchronization_status.detailed_state}'
```
