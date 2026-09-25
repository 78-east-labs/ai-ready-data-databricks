# Check: dependency_graph_completeness

Fraction of base tables in the schema that have both an upstream edge (something wrote into them) and a downstream edge (something read them) in `system.access.table_lineage` within the window.

## Context

Unity Catalog lineage is observed, not declared: `system.access.table_lineage` gets a row every time a query, job, pipeline, notebook or dashboard reads or writes a table. For a table `T`:

- an **upstream edge** is a row with `target_table_full_name = T`. The source may be another table (`source_table_full_name`) or an external path (`source_path`), and a plain `INSERT VALUES` produces a row with neither. All count: the table was written by something Unity Catalog saw.
- a **downstream edge** is a row with `source_table_full_name = T`. The target may be another table (a derived write) or NULL (a read by a dashboard, a Genie space, an ad hoc query). Both count as consumers.

A table with both edges sits inside the graph: you can walk back to where its data came from and forward to what depends on it. A table with neither is invisible to impact analysis, which usually means it is loaded and read outside Unity Catalog compute (external Delta writers, files copied out of the lake) or has simply been idle. The check cannot tell those apart; the diagnostic shows `last_altered` to help.

The signal is native. It proves that at least one observed operation touched each side within `{{ lookback_days }}` days (default 30; lineage retention is one year, so the window can be widened). It does not prove the graph is complete: a table can pass while a second, unobserved writer also feeds it.

`table_lineage` lags by up to a few hours, so edges from very recent runs may be missing. Reading it needs `USE SCHEMA` on `system.access` and `SELECT` on the table, and lineage for a table is only visible to callers who can see that table. Returns NULL (N/A) when the schema has no base tables.

## SQL

### Both directions on base tables (primary)

```sql
WITH tables_in_scope AS (
    SELECT LOWER(table_name) AS table_name
    FROM {{ catalog }}.information_schema.tables
    WHERE LOWER(table_schema) = LOWER('{{ schema }}')
      AND table_type IN ('MANAGED', 'EXTERNAL')
),
upstream AS (
    SELECT DISTINCT LOWER(target_table_name) AS table_name
    FROM system.access.table_lineage
    WHERE LOWER(target_catalog) = LOWER('{{ catalog }}')
      AND LOWER(target_schema)  = LOWER('{{ schema }}')
      AND target_table_full_name IS NOT NULL
      AND event_time >= current_timestamp() - INTERVAL {{ lookback_days }} DAYS
),
downstream AS (
    SELECT DISTINCT LOWER(source_table_name) AS table_name
    FROM system.access.table_lineage
    WHERE LOWER(source_catalog) = LOWER('{{ catalog }}')
      AND LOWER(source_schema)  = LOWER('{{ schema }}')
      AND source_table_full_name IS NOT NULL
      AND event_time >= current_timestamp() - INTERVAL {{ lookback_days }} DAYS
)
SELECT
    COUNT_IF(u.table_name IS NOT NULL AND d.table_name IS NOT NULL)          AS tables_with_both_edges,
    COUNT(*)                                                                 AS total_tables,
    COUNT_IF(u.table_name IS NOT NULL AND d.table_name IS NOT NULL)::DOUBLE
        / NULLIF(COUNT(*), 0)                                                AS value
FROM tables_in_scope t
LEFT JOIN upstream   u USING (table_name)
LEFT JOIN downstream d USING (table_name)
```

### Either direction, all assets (variant)

Matches the upstream framework's looser definition: an object participates in the graph if it has an edge in either direction, and views, materialized views and streaming tables are included. Views appear in lineage as `source_type = 'VIEW'` when read, and their definitions create edges from base tables to the view when queried. Use this for a first pass on a schema where the primary scores near zero; the gap between the two numbers is the count of dead-end tables.

```sql
WITH assets_in_scope AS (
    SELECT LOWER(table_name) AS table_name
    FROM {{ catalog }}.information_schema.tables
    WHERE LOWER(table_schema) = LOWER('{{ schema }}')
      AND table_type IN ('MANAGED', 'EXTERNAL', 'VIEW', 'MATERIALIZED_VIEW', 'STREAMING_TABLE')
),
any_edge AS (
    SELECT DISTINCT LOWER(target_table_name) AS table_name
    FROM system.access.table_lineage
    WHERE LOWER(target_catalog) = LOWER('{{ catalog }}')
      AND LOWER(target_schema)  = LOWER('{{ schema }}')
      AND target_table_full_name IS NOT NULL
      AND event_time >= current_timestamp() - INTERVAL {{ lookback_days }} DAYS
    UNION
    SELECT DISTINCT LOWER(source_table_name) AS table_name
    FROM system.access.table_lineage
    WHERE LOWER(source_catalog) = LOWER('{{ catalog }}')
      AND LOWER(source_schema)  = LOWER('{{ schema }}')
      AND source_table_full_name IS NOT NULL
      AND event_time >= current_timestamp() - INTERVAL {{ lookback_days }} DAYS
)
SELECT
    COUNT_IF(e.table_name IS NOT NULL)            AS assets_with_any_edge,
    COUNT(*)                                       AS total_assets,
    COUNT_IF(e.table_name IS NOT NULL)::DOUBLE
        / NULLIF(COUNT(*), 0)                      AS value
FROM assets_in_scope a
LEFT JOIN any_edge e USING (table_name)
```

### Table-to-table edges only (variant)

Stricter: only edges whose other end is a Unity Catalog table count, so a table read solely by dashboards or loaded solely from files fails. This is the right definition when the question is "can I rebuild the transformation DAG from lineage alone".

```sql
WITH tables_in_scope AS (
    SELECT LOWER(table_name) AS table_name
    FROM {{ catalog }}.information_schema.tables
    WHERE LOWER(table_schema) = LOWER('{{ schema }}')
      AND table_type IN ('MANAGED', 'EXTERNAL')
),
upstream AS (
    SELECT DISTINCT LOWER(target_table_name) AS table_name
    FROM system.access.table_lineage
    WHERE LOWER(target_catalog) = LOWER('{{ catalog }}')
      AND LOWER(target_schema)  = LOWER('{{ schema }}')
      AND target_table_full_name IS NOT NULL
      AND source_table_full_name IS NOT NULL
      AND event_time >= current_timestamp() - INTERVAL {{ lookback_days }} DAYS
),
downstream AS (
    SELECT DISTINCT LOWER(source_table_name) AS table_name
    FROM system.access.table_lineage
    WHERE LOWER(source_catalog) = LOWER('{{ catalog }}')
      AND LOWER(source_schema)  = LOWER('{{ schema }}')
      AND source_table_full_name IS NOT NULL
      AND target_table_full_name IS NOT NULL
      AND event_time >= current_timestamp() - INTERVAL {{ lookback_days }} DAYS
)
SELECT
    COUNT_IF(u.table_name IS NOT NULL AND d.table_name IS NOT NULL)          AS tables_with_both_edges,
    COUNT(*)                                                                 AS total_tables,
    COUNT_IF(u.table_name IS NOT NULL AND d.table_name IS NOT NULL)::DOUBLE
        / NULLIF(COUNT(*), 0)                                                AS value
FROM tables_in_scope t
LEFT JOIN upstream   u USING (table_name)
LEFT JOIN downstream d USING (table_name)
```
