# Diagnostic: feature_materialization_coverage

One row per feature table with its primary key columns, whether it has a `TIMESERIES` key, its tag, and the online or synced copy found for it (name, API, sync state, last sync time).

## Context

Two parts. The SQL lists every feature table in scope (same scoping as the check) with the columns a human needs to judge whether it should be online at all: the PK columns, whether the PK includes a `TIMESERIES` column (a time-series feature table is usually served through point-in-time lookups and is a strong online candidate), the `feature_table` tag, and when the table was last altered. The SDK part enriches each row with the online object's state.

Status:

- `ONLINE`: an online or synced table names this table as its source and reports a healthy sync state.
- `ONLINE_UNHEALTHY`: the copy exists but its state is failing, provisioning or offline. Fix the sync before counting on it.
- `NOT_ONLINE`: fix candidates, or batch-only tables that should be excluded by the profile.

Sync state fields: for synced tables, `data_synchronization_status.detailed_state` (values such as `PROVISIONING`, `ONLINE_CONTINUOUS_UPDATE`, `ONLINE_TRIGGERED_UPDATE`, `ONLINE_NO_PENDING_UPDATE`, `OFFLINE_FAILED`) and `.last_sync.timestamp`; for online tables, `status.detailed_state` with a similar vocabulary. The exact enum names have shifted across releases; treat anything starting with `ONLINE` as healthy and anything with `FAILED` or `OFFLINE` as unhealthy, and print the raw value.

Sorted `NOT_ONLINE` first, then unhealthy, then by name.

## SQL

### Feature tables with their keys and tags

```sql
WITH feature_tables AS (
    SELECT LOWER(t.table_name) AS table_name, t.table_owner, t.last_altered, t.comment
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
pk_columns AS (
    SELECT LOWER(k.table_name) AS table_name,
           array_sort(collect_list(struct(k.ordinal_position, k.column_name))).column_name AS pk_columns
    FROM {{ catalog }}.information_schema.key_column_usage k
    JOIN {{ catalog }}.information_schema.table_constraints c
      ON k.constraint_name = c.constraint_name
     AND k.table_schema = c.table_schema AND k.table_name = c.table_name
    WHERE LOWER(k.table_schema) = LOWER('{{ schema }}')
      AND c.constraint_type = 'PRIMARY KEY'
    GROUP BY LOWER(k.table_name)
),
tags AS (
    SELECT LOWER(table_name) AS table_name, tag_value AS feature_tag
    FROM {{ catalog }}.information_schema.table_tags
    WHERE LOWER(schema_name) = LOWER('{{ schema }}')
      AND LOWER(tag_name) = LOWER('{{ feature_tag_key }}')
)
SELECT
    f.table_name,
    f.table_owner,
    p.pk_columns,
    p.pk_columns IS NOT NULL                          AS has_primary_key,
    tg.feature_tag,
    f.last_altered,
    f.comment
FROM feature_tables f
LEFT JOIN pk_columns p  USING (table_name)
LEFT JOIN tags       tg USING (table_name)
ORDER BY f.table_name
```

Whether a PK is declared `TIMESERIES` is not in `information_schema`; probe per table with `SHOW CREATE TABLE {{ catalog }}.{{ schema }}.{{ asset }}` and look for `TIMESERIES` in the `PRIMARY KEY (...)` clause (that is what `point_in_time_correctness` measures).

### Online copies with state (SDK)

```python
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound, PermissionDenied, BadRequest

w = WorkspaceClient()
CATALOG, SCHEMAS = "{{ catalog }}", ["{{ schema }}"]
rows = []
for schema in SCHEMAS:
    for t in w.tables.list(catalog_name=CATALOG, schema_name=schema):
        try:
            o = w.database.get_synced_database_table(name=t.full_name)
            st = o.data_synchronization_status
            rows.append((o.spec.source_table_full_name, t.full_name, "synced_table",
                         str(getattr(st, "detailed_state", None)),
                         getattr(getattr(st, "last_sync", None), "timestamp", None)))
            continue
        except (NotFound, PermissionDenied, BadRequest, AttributeError):
            pass
        try:
            o = w.online_tables.get(name=t.full_name)
            rows.append((o.spec.source_table_full_name, t.full_name, "online_table",
                         str(getattr(o.status, "detailed_state", None)), None))
        except (NotFound, PermissionDenied, BadRequest, AttributeError):
            pass
for r in sorted(rows):
    print("\t".join(str(x) for x in r))  # source, online_name, api, state, last_sync
```

Join the output to the SQL rows on `source = catalog.schema.table_name` in the orchestrator and assign the status labels above. CLI equivalents are in the check file.

### Lookups against one feature table from serving

Whether anything actually reads the table at request time is visible in `system.query.history` only for SQL warehouse reads; model serving lookups against Lakebase do not appear there. The cheapest evidence is the serving endpoint's config: `w.serving_endpoints.get(name).config.served_entities[*].entity_name` names the UC model, and the model's feature spec (`FeatureLookup` in the logged model) names the feature table. That is a manual read, not a query.
