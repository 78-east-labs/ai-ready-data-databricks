# Diagnostic: dependency_graph_completeness

Per-table counts of upstream and downstream lineage edges, the distinct neighbors on each side, and a status, isolated tables first.

## Context

Same population and window as the check (base tables, `{{ lookback_days }}` default 30). For each table it shows how many distinct upstream sources wrote into it (tables and external paths separately), how many distinct downstream targets were written from it, how many read-only consumers touched it, and the entity types on each side. `last_altered` from `information_schema` is included so an isolated table that was modified recently (a writer Unity Catalog cannot see) can be told apart from one that is simply idle.

`status` values, worst first: `ISOLATED` (no edges), `NO_DOWNSTREAM` (written but never read: a dead end or a table consumed outside the platform), `NO_UPSTREAM` (read but never written in the window: static reference data, or an external writer), `CONNECTED`.

Lineage lags by up to a few hours.

## SQL

```sql
WITH tables_in_scope AS (
    SELECT LOWER(table_name) AS table_name, table_owner, last_altered
    FROM {{ catalog }}.information_schema.tables
    WHERE LOWER(table_schema) = LOWER('{{ schema }}')
      AND table_type IN ('MANAGED', 'EXTERNAL')
),
upstream AS (
    SELECT LOWER(target_table_name)                                        AS table_name,
           COUNT(DISTINCT source_table_full_name)                          AS upstream_tables,
           COUNT(DISTINCT source_path)                                     AS upstream_paths,
           array_sort(collect_set(source_table_full_name))                 AS upstream_table_names,
           array_sort(collect_set(UPPER(entity_type)))                     AS writer_entity_types,
           MAX(event_time)                                                 AS last_write
    FROM system.access.table_lineage
    WHERE LOWER(target_catalog) = LOWER('{{ catalog }}')
      AND LOWER(target_schema)  = LOWER('{{ schema }}')
      AND target_table_full_name IS NOT NULL
      AND event_time >= current_timestamp() - INTERVAL {{ lookback_days }} DAYS
    GROUP BY LOWER(target_table_name)
),
downstream AS (
    SELECT LOWER(source_table_name)                                        AS table_name,
           COUNT(DISTINCT target_table_full_name)                          AS downstream_tables,
           COUNT_IF(target_table_full_name IS NULL)                        AS read_only_events,
           array_sort(collect_set(target_table_full_name))                 AS downstream_table_names,
           array_sort(collect_set(UPPER(entity_type)))                     AS reader_entity_types,
           MAX(event_time)                                                 AS last_read
    FROM system.access.table_lineage
    WHERE LOWER(source_catalog) = LOWER('{{ catalog }}')
      AND LOWER(source_schema)  = LOWER('{{ schema }}')
      AND source_table_full_name IS NOT NULL
      AND event_time >= current_timestamp() - INTERVAL {{ lookback_days }} DAYS
    GROUP BY LOWER(source_table_name)
)
SELECT
    t.table_name,
    t.table_owner,
    t.last_altered,
    COALESCE(u.upstream_tables, 0)      AS upstream_tables,
    COALESCE(u.upstream_paths, 0)       AS upstream_paths,
    u.upstream_table_names,
    u.writer_entity_types,
    u.last_write,
    COALESCE(d.downstream_tables, 0)    AS downstream_tables,
    COALESCE(d.read_only_events, 0)     AS read_only_events,
    d.downstream_table_names,
    d.reader_entity_types,
    d.last_read,
    CASE
        WHEN u.table_name IS NULL AND d.table_name IS NULL THEN 'ISOLATED'
        WHEN d.table_name IS NULL                          THEN 'NO_DOWNSTREAM'
        WHEN u.table_name IS NULL                          THEN 'NO_UPSTREAM'
        ELSE 'CONNECTED'
    END                                 AS status
FROM tables_in_scope t
LEFT JOIN upstream   u USING (table_name)
LEFT JOIN downstream d USING (table_name)
ORDER BY
    CASE
        WHEN u.table_name IS NULL AND d.table_name IS NULL THEN 0
        WHEN d.table_name IS NULL                          THEN 1
        WHEN u.table_name IS NULL                          THEN 2
        ELSE 3
    END ASC,
    t.last_altered DESC,
    t.table_name
```
