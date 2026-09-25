# Check: feature_materialization_coverage

Fraction of feature tables in the schema that are also materialized for online serving as a Lakebase synced table or a (legacy) Databricks Online Table.

## Context

On Databricks a **feature table** is an ordinary Unity Catalog table with a `PRIMARY KEY` constraint; Feature Engineering in UC (`databricks-feature-engineering`) requires one and uses it for lookups and point-in-time joins. The check treats a base table as a feature table when it has a PK constraint in `information_schema.table_constraints`, or carries the tag `feature_table = 'true'` (from the tag conventions in `platforms/DATABRICKS.md`). Override the tag key with `{{ feature_tag_key }}`; default `feature_table`.

**Online materialization** means a copy that a model serving endpoint can read at request time. Two object types provide it:

- **Lakebase synced tables** (current): a UC table backed by a Lakebase Postgres instance, continuously or triggered-synced from a source Delta table. SDK: `w.database.get_synced_database_table(name)`, spec field `spec.source_table_full_name`.
- **Online Tables** (earlier release, still present in many workspaces, deprecated in favour of Lakebase): SDK `w.online_tables.get(name)`, spec field `spec.source_table_full_name`.

The API names changed across releases (`online_tables` in 2024, `database.synced_database_tables` from 2025), and the check reads both so a workspace mid-migration is scored correctly. If neither API exists in your SDK version, upgrade `databricks-sdk`; the check states which API found each match.

This is an **SDK-mode** check for the numerator. Feature tables come from SQL; online copies come from the SDK because neither API's objects are reliably distinguishable in `information_schema.tables` (see the SQL approximation for what can be seen there). Both synced tables and online tables are themselves UC tables, usually in the same schema or a dedicated `*_online` schema; the SDK snippet enumerates candidate tables in `{{ online_schemas }}` (default: the assessed schema) and asks both APIs about each.

What it proves: an online copy exists and its declared source is the feature table. It does not prove the copy is fresh or that the serving endpoint reads it; `training_serving_parity` and `feature_refresh_compliance` cover those. A feature table used only for batch scoring has no reason to be online and will score 0 here; if the schema is batch-only, say so in the report rather than fixing it.

Permissions: `USE CATALOG` / `USE SCHEMA` on the schemas listed, `SELECT` on the online or synced table object (or `BROWSE`) for the `get` calls, and `SELECT` on `information_schema`. `get` on an object the caller cannot see raises `PermissionDenied`, which the snippet treats as "not an online table".

`information_schema` is live; the SDK reads current state. No lag.

Returns NULL (N/A) when the schema contains no feature tables.

## SQL

### Feature tables in scope (denominator)

```sql
WITH pk_tables AS (
    SELECT DISTINCT LOWER(table_name) AS table_name
    FROM {{ catalog }}.information_schema.table_constraints
    WHERE LOWER(table_schema) = LOWER('{{ schema }}')
      AND constraint_type = 'PRIMARY KEY'
),
tagged AS (
    SELECT DISTINCT LOWER(table_name) AS table_name
    FROM {{ catalog }}.information_schema.table_tags
    WHERE LOWER(schema_name) = LOWER('{{ schema }}')
      AND LOWER(tag_name) = LOWER('{{ feature_tag_key }}')
      AND LOWER(tag_value) IN ('true', 'yes', '1')
)
SELECT concat(LOWER('{{ catalog }}'), '.', LOWER('{{ schema }}'), '.', t.table_name) AS feature_table
FROM (
    SELECT LOWER(table_name) AS table_name
    FROM {{ catalog }}.information_schema.tables
    WHERE LOWER(table_schema) = LOWER('{{ schema }}')
      AND table_type IN ('MANAGED', 'EXTERNAL')
) t
WHERE t.table_name IN (SELECT table_name FROM pk_tables)
   OR t.table_name IN (SELECT table_name FROM tagged)
ORDER BY feature_table
```

### Online copies (numerator, SDK)

Python, `databricks-sdk>=0.40`. Returns a map from source feature table to the online object(s) and which API found each.

```python
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound, PermissionDenied, BadRequest

w = WorkspaceClient()
CATALOG, SCHEMAS = "{{ catalog }}", ["{{ schema }}"]  # add "*_online" schemas here
online_by_source = {}

def record(source, name, api):
    online_by_source.setdefault(source.lower(), []).append((name, api))

for schema in SCHEMAS:
    for t in w.tables.list(catalog_name=CATALOG, schema_name=schema):
        for api, getter in (("synced_table", w.database.get_synced_database_table),
                            ("online_table", w.online_tables.get)):
            try:
                obj = getter(name=t.full_name)
                src = obj.spec.source_table_full_name if obj.spec else None
                if src:
                    record(src, t.full_name, api)
                break
            except (NotFound, PermissionDenied, BadRequest, AttributeError):
                continue

feature_tables = [...]  # rows from the SQL above
covered = [f for f in feature_tables if f.lower() in online_by_source]
value = len(covered) / len(feature_tables) if feature_tables else None
print(len(covered), len(feature_tables), value)
```

`AttributeError` is caught so the loop still runs on an SDK that lacks one of the two APIs. If `w.database` does not exist, the workspace SDK predates Lakebase; if `w.online_tables` does not exist, it postdates their removal.

CLI equivalent, one object at a time:

```bash
databricks tables list {{ catalog }} {{ schema }} --output json | jq -r '.[].full_name' \
  | while read t; do
      databricks database get-synced-database-table "$t" --output json 2>/dev/null \
        | jq -r --arg t "$t" '"\($t)\tsynced_table\t\(.spec.source_table_full_name)"' \
      || databricks online-tables get "$t" --output json 2>/dev/null \
        | jq -r --arg t "$t" '"\($t)\tonline_table\t\(.spec.source_table_full_name)"'
    done
```

Aggregate: `value = feature tables that appear as a source_table_full_name / feature tables`, NULL when there are no feature tables.

### Metadata approximation (variant, pure SQL)

Online and synced tables do appear in `information_schema.tables`, but the `table_type` they report has differed across releases (some show as `MANAGED` with a distinct `data_source_format`, others under their own type). Probe what your metastore reports:

```sql
SELECT table_type, data_source_format, COUNT(*)
FROM {{ catalog }}.information_schema.tables
GROUP BY table_type, data_source_format
ORDER BY 3 DESC
```

If a distinct type or format identifies them, credit a feature table when a table named `{table}_online` or `{table}_synced` with that type exists. This misses online copies with unrelated names or in another catalog, and cannot verify the source link; it is a lower bound.

```sql
WITH feature_tables AS (
    SELECT LOWER(t.table_name) AS table_name
    FROM {{ catalog }}.information_schema.tables t
    WHERE LOWER(t.table_schema) = LOWER('{{ schema }}')
      AND t.table_type IN ('MANAGED', 'EXTERNAL')
      AND (EXISTS (SELECT 1 FROM {{ catalog }}.information_schema.table_constraints c
                   WHERE c.table_schema = t.table_schema AND c.table_name = t.table_name
                     AND c.constraint_type = 'PRIMARY KEY')
        OR EXISTS (SELECT 1 FROM {{ catalog }}.information_schema.table_tags g
                   WHERE LOWER(g.schema_name) = LOWER(t.table_schema)
                     AND LOWER(g.table_name) = LOWER(t.table_name)
                     AND LOWER(g.tag_name) = LOWER('{{ feature_tag_key }}')
                     AND LOWER(g.tag_value) IN ('true', 'yes', '1')))
),
online_named AS (
    SELECT regexp_replace(LOWER(table_name), '_(online|synced)$', '') AS base_name
    FROM {{ catalog }}.information_schema.tables
    WHERE REGEXP_LIKE(LOWER(table_name), '_(online|synced)$')
)
SELECT
    COUNT_IF(o.base_name IS NOT NULL)             AS online_feature_tables,
    COUNT(*)                                       AS feature_tables,
    COUNT_IF(o.base_name IS NOT NULL)::DOUBLE
        / NULLIF(COUNT(*), 0)                      AS value
FROM feature_tables f
LEFT JOIN (SELECT DISTINCT base_name FROM online_named) o ON f.table_name = o.base_name
```
