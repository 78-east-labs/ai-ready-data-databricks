# Check: access_audit_coverage

Fraction of base tables in the schema that were read in the lookback window (per Unity Catalog lineage) and whose reads also appear in the Unity Catalog audit log.

## Context

Two metastore-wide system tables are joined. `system.access.table_lineage` says which tables were read: any row whose `source_table_full_name` is a table in the schema is a read event. `system.access.audit` says which of those reads were recorded by the audit log: rows with `service_name = 'unityCatalog'` and `action_name IN ('getTable', 'generateTemporaryTableCredential')`, where `request_params['full_name_arg']` holds the `catalog.schema.table` that was resolved. `getTable` fires when a query resolves the table's metadata; `generateTemporaryTableCredential` fires when compute obtains storage credentials to read the files. Either one is proof the access was audited.

The signal is native. Unity Catalog auditing is always on and cannot be disabled per table, so for UC tables the honest expectation is 1.0. A lower score almost always means one of three things: the table lives in `hive_metastore` (no UC audit, no lineage; it will not even be in the denominator here because `information_schema` does not list it, but check with the diagnostic if the user pointed at the wrong catalog), the audit table lags lineage or vice versa (`system.access.audit` lands within minutes to a few hours; `table_lineage` can take a few hours), or the `system.access` schema is not enabled or not granted to the caller. It does not mean auditing is broken for that table.

The denominator is tables that were read, not all tables. A dormant table that nobody queried has nothing to audit and is excluded. The variant below uses all base tables as the denominator, which matches the upstream framework's behaviour but conflates "not read" with "not audited"; use it only when the user wants a usage-style view.

`{{ lookback_days }}` defaults to 30 (lineage-based). Both scans are bounded by `event_date` for partition pruning and by `event_time` for the exact window. Reading `system.access.*` requires `USE SCHEMA` on `system.access` and `SELECT` on the two tables; the schema must be enabled by a metastore admin.

If you are not sure which `request_params` key carries the table name for `generateTemporaryTableCredential` in your workspace (it is `full_name_arg` for `getTable`; some releases also expose `table_full_name`), confirm with:

```sql
SELECT action_name, map_keys(request_params) AS keys
FROM system.access.audit
WHERE service_name = 'unityCatalog'
  AND action_name IN ('getTable', 'generateTemporaryTableCredential')
  AND event_date >= date_sub(current_date(), 1)
ORDER BY event_time DESC
LIMIT 5
```

The SQL below reads both keys with `COALESCE`, so it works either way.

Returns NULL (N/A) when no table in the schema was read in the window (primary) or the schema has no base tables (variant).

## SQL

### Tables read in the window that appear in the audit log (primary)

```sql
WITH tables_in_scope AS (
    SELECT LOWER(table_name) AS table_name
    FROM {{ catalog }}.information_schema.tables
    WHERE LOWER(table_schema) = LOWER('{{ schema }}')
      AND table_type IN ('MANAGED', 'EXTERNAL')
),
read_tables AS (
    SELECT DISTINCT LOWER(source_table_name) AS table_name
    FROM system.access.table_lineage
    WHERE LOWER(source_catalog) = LOWER('{{ catalog }}')
      AND LOWER(source_schema)  = LOWER('{{ schema }}')
      AND source_table_name IS NOT NULL
      AND event_date >= date_sub(current_date(), {{ lookback_days }})
      AND event_time >= current_timestamp() - INTERVAL {{ lookback_days }} DAYS
),
audited_tables AS (
    SELECT DISTINCT
        LOWER(element_at(split(full_name, '\\.'), 3)) AS table_name
    FROM (
        SELECT COALESCE(request_params['full_name_arg'],
                        request_params['table_full_name']) AS full_name
        FROM system.access.audit
        WHERE service_name = 'unityCatalog'
          AND action_name IN ('getTable', 'generateTemporaryTableCredential')
          AND event_date >= date_sub(current_date(), {{ lookback_days }})
          AND event_time >= current_timestamp() - INTERVAL {{ lookback_days }} DAYS
    )
    WHERE full_name IS NOT NULL
      AND LOWER(element_at(split(full_name, '\\.'), 1)) = LOWER('{{ catalog }}')
      AND LOWER(element_at(split(full_name, '\\.'), 2)) = LOWER('{{ schema }}')
)
SELECT
    COUNT_IF(a.table_name IS NOT NULL)            AS audited_read_tables,
    COUNT(*)                                       AS read_tables,
    COUNT_IF(a.table_name IS NOT NULL)::DOUBLE
        / NULLIF(COUNT(*), 0)                      AS value
FROM tables_in_scope t
JOIN read_tables    r USING (table_name)
LEFT JOIN audited_tables a USING (table_name)
```

### All base tables that appear in the audit log (variant)

Denominator is every base table in the schema, so tables nobody read score as unaudited. Equivalent to the upstream framework's definition.

```sql
WITH tables_in_scope AS (
    SELECT LOWER(table_name) AS table_name
    FROM {{ catalog }}.information_schema.tables
    WHERE LOWER(table_schema) = LOWER('{{ schema }}')
      AND table_type IN ('MANAGED', 'EXTERNAL')
),
audited_tables AS (
    SELECT DISTINCT
        LOWER(element_at(split(full_name, '\\.'), 3)) AS table_name
    FROM (
        SELECT COALESCE(request_params['full_name_arg'],
                        request_params['table_full_name']) AS full_name
        FROM system.access.audit
        WHERE service_name = 'unityCatalog'
          AND action_name IN ('getTable', 'generateTemporaryTableCredential')
          AND event_date >= date_sub(current_date(), {{ lookback_days }})
          AND event_time >= current_timestamp() - INTERVAL {{ lookback_days }} DAYS
    )
    WHERE full_name IS NOT NULL
      AND LOWER(element_at(split(full_name, '\\.'), 1)) = LOWER('{{ catalog }}')
      AND LOWER(element_at(split(full_name, '\\.'), 2)) = LOWER('{{ schema }}')
)
SELECT
    COUNT_IF(a.table_name IS NOT NULL)            AS audited_tables,
    COUNT(*)                                       AS total_tables,
    COUNT_IF(a.table_name IS NOT NULL)::DOUBLE
        / NULLIF(COUNT(*), 0)                      AS value
FROM tables_in_scope t
LEFT JOIN audited_tables a USING (table_name)
```
