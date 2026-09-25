# Check: training_serving_parity

Fraction of feature tables in the schema that have an online copy (Lakebase synced table or legacy online table) whose declared source is that same Unity Catalog table.

## Context

On Databricks the training path reads a feature table in Unity Catalog (a Delta table with a primary key) and the serving path reads an online copy of it. Parity is guaranteed by construction only when the online copy is *synced from* the training table rather than computed by a second pipeline. Two mechanisms do that sync, and both record the source table in their spec:

- **Lakebase synced tables** (current): a Postgres table in a Lakebase database instance kept in sync from a UC table. The object is registered in Unity Catalog under its own three-part name, and `SyncedDatabaseTable.spec.source_table_full_name` names the source. `spec.scheduling_policy` is `SNAPSHOT`, `TRIGGERED` or `CONTINUOUS`; the last two need Change Data Feed on the source (`change_detection`).
- **Online tables** (legacy, deprecated in favor of synced tables): `OnlineTable.spec.source_table_full_name`, with `run_triggered` or `run_continuously` modes.

Neither spec is visible in SQL. `information_schema.tables` lists the online object (its `table_type` is believed to be `FOREIGN`, since it is surfaced through the online store's connection; confirm with `SELECT table_name, table_type, data_source_format FROM {{ catalog }}.information_schema.tables WHERE table_type NOT IN ('MANAGED','EXTERNAL','VIEW')`) but not what it syncs from. So this is an **SDK mode** check: enumerate feature tables in SQL, enumerate online copies through the SDK, match on `source_table_full_name`.

A feature table passes when at least one synced or online table anywhere in the metastore has `LOWER(spec.source_table_full_name) = 'catalog.schema.table'`. It does not matter which schema or database instance the online copy lives in.

Strength is **proxy**. A matching source proves the serving copy is derived from the training table; it does not prove the copy is current (`spec.scheduling_policy = 'SNAPSHOT'` copies can be arbitrarily stale, and `feature_refresh_compliance` plus the synced table's own pipeline status cover that), nor that the model's feature lookup at inference reads this copy rather than recomputing features. Feature tables served through Feature Serving endpoints or Model Serving with automatic feature lookup do read the online copy, which is the case this check is designed for.

Denominator: base Delta tables in the schema with a `PRIMARY KEY` constraint or the table tag `feature_table = 'true'`. Tables that are not feature tables are out of scope. Returns NULL (N/A) when there are none.

Permissions: `USE CATALOG` / `USE SCHEMA` on the schemas holding the online objects, `BROWSE` or `SELECT` on each to `get` it, and for Lakebase, `CAN USE` on the database instance is not required just to read the spec. SDK and CLI names have changed across releases: `online_tables` (SDK 0.20+), `database.get_synced_database_table` (SDK 0.50+; earlier previews exposed `database_instances` and `synced_database_tables`). The snippet tries both surfaces and reports which one answered.

## SQL

### Feature tables in scope (denominator)

```sql
WITH tables_in_scope AS (
    SELECT LOWER(table_name) AS table_name
    FROM {{ catalog }}.information_schema.tables
    WHERE LOWER(table_schema) = LOWER('{{ schema }}')
      AND table_type IN ('MANAGED', 'EXTERNAL')
      AND UPPER(data_source_format) = 'DELTA'
),
with_pk AS (
    SELECT DISTINCT LOWER(table_name) AS table_name
    FROM {{ catalog }}.information_schema.table_constraints
    WHERE LOWER(table_schema) = LOWER('{{ schema }}')
      AND constraint_type = 'PRIMARY KEY'
),
tagged AS (
    SELECT DISTINCT LOWER(table_name) AS table_name
    FROM {{ catalog }}.information_schema.table_tags
    WHERE LOWER(schema_name) = LOWER('{{ schema }}')
      AND tag_name = 'feature_table'
      AND LOWER(tag_value) = 'true'
)
SELECT CONCAT(LOWER('{{ catalog }}'), '.', LOWER('{{ schema }}'), '.', t.table_name) AS full_name
FROM tables_in_scope t
LEFT JOIN with_pk pk USING (table_name)
LEFT JOIN tagged  tg USING (table_name)
WHERE pk.table_name IS NOT NULL OR tg.table_name IS NOT NULL
ORDER BY full_name
```

### Online copies and their sources (SDK, primary)

```python
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()
feature_tables = {r["full_name"] for r in sql(FEATURE_TABLES_SQL, WAREHOUSE_ID)}  # from the SQL above

def online_source(full_name):
    """Return (kind, source_table_full_name) if full_name is a synced or online table."""
    for kind, getter in (("synced", w.database.get_synced_database_table), ("online", w.online_tables.get)):
        try:
            return kind, (getter(name=full_name).spec.source_table_full_name or "").lower()
        except Exception:
            continue
    return None, None

sources = {}
for schema in w.schemas.list(catalog_name="{{ catalog }}"):
    for t in w.tables.list(catalog_name="{{ catalog }}", schema_name=schema.name):
        if t.table_type and t.table_type.value in ("MANAGED", "EXTERNAL", "VIEW", "MATERIALIZED_VIEW", "STREAMING_TABLE"):
            continue
        kind, src = online_source(t.full_name)
        if src:
            sources.setdefault(src, []).append((t.full_name, kind))

served = {ft for ft in feature_tables if ft in sources}
total = len(feature_tables)
print({"served_feature_tables": len(served), "total_feature_tables": total,
       "value": (len(served) / total) if total else None})
```

`sql(...)` is the helper from `platforms/DATABRICKS.md`. The loop walks the catalog the feature tables live in; online copies placed in another catalog need that catalog added to the outer loop. `t.table_type.value` skipping is an optimization; if online objects turn out to list under one of the skipped types in your workspace, remove the `continue`.

Aggregation: `value = served_feature_tables / total_feature_tables`, NULL when there are no feature tables.

### CLI equivalent

For each candidate online object:

```bash
databricks database get-synced-database-table {{ online_table_full_name }} --output json | jq -r '.spec.source_table_full_name'
databricks online-tables get {{ online_table_full_name }} --output json | jq -r '.spec.source_table_full_name'
```

Older CLI builds used `databricks synced-database-tables get` for the first; if the command is missing, upgrade the CLI or use the SDK. Candidate names come from `databricks tables list {{ catalog }} {{ schema }} --output json | jq -r '.[] | select(.table_type != "MANAGED" and .table_type != "EXTERNAL" and .table_type != "VIEW") | .full_name'`.

### Feature Engineering client view (variant)

The Feature Engineering client knows about online stores registered through it. For a feature table, `get_table` reports the online copies that were created with `fe.publish_table` or `fe.create_online_table` / `create_synced_table`:

```python
from databricks.feature_engineering import FeatureEngineeringClient
fe = FeatureEngineeringClient()
info = fe.get_table(name="{{ catalog }}.{{ schema }}.{{ asset }}")
print(getattr(info, "online_stores", None))
```

The attribute name and whether Lakebase synced tables appear there have changed across `databricks-feature-engineering` releases; treat an empty result as "unknown", not "no online copy", and fall back to the primary.
