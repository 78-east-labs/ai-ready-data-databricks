# Diagnostic: impact_analysis_capability

Lists every base table with the number and kind of downstream consumers lineage has recorded, the derived tables they write, and when the table was last read, tables with no consumers first.

## Context

This is the impact report itself, one row per table. `consumer_entities` is the count of distinct (entity_type, entity_id) pairs that read the table, `consumer_entity_types` the kinds involved, `derived_tables` the distinct targets written from it, and `read_only_events` the reads that did not feed a write (dashboards, Genie, ad hoc SQL). `top_consumers` lists up to ten `entity_type:entity_id` strings so the operator can open them directly.

`status` is `NO_CONSUMERS` when nothing read the table in the window, `READ_ONLY` when it has readers but no derived tables, and `HAS_DERIVED` when other tables depend on it. `last_altered` helps separate an actively written table nobody reads from a stale one.

Same population and window as the check (`{{ lookback_days }}` default 30). Lineage lags by up to a few hours.

## SQL

```sql
WITH tables_in_scope AS (
    SELECT LOWER(table_name) AS table_name, table_owner, last_altered
    FROM {{ catalog }}.information_schema.tables
    WHERE LOWER(table_schema) = LOWER('{{ schema }}')
      AND table_type IN ('MANAGED', 'EXTERNAL')
),
reads AS (
    SELECT
        LOWER(source_table_name)                                              AS table_name,
        UPPER(entity_type)                                                    AS entity_type,
        entity_id,
        target_table_full_name,
        event_time
    FROM system.access.table_lineage
    WHERE LOWER(source_catalog) = LOWER('{{ catalog }}')
      AND LOWER(source_schema)  = LOWER('{{ schema }}')
      AND source_table_full_name IS NOT NULL
      AND event_time >= current_timestamp() - INTERVAL {{ lookback_days }} DAYS
),
per_table AS (
    SELECT
        table_name,
        COUNT(DISTINCT concat_ws(':', entity_type, entity_id))               AS consumer_entities,
        array_sort(collect_set(entity_type))                                  AS consumer_entity_types,
        COUNT(DISTINCT target_table_full_name)                                AS derived_tables,
        array_sort(collect_set(target_table_full_name))                       AS derived_table_names,
        COUNT_IF(target_table_full_name IS NULL)                              AS read_only_events,
        slice(array_sort(collect_set(concat_ws(':', entity_type, entity_id))), 1, 10)
                                                                              AS top_consumers,
        MAX(event_time)                                                       AS last_read
    FROM reads
    GROUP BY table_name
)
SELECT
    t.table_name,
    t.table_owner,
    t.last_altered,
    COALESCE(p.consumer_entities, 0)  AS consumer_entities,
    p.consumer_entity_types,
    COALESCE(p.derived_tables, 0)     AS derived_tables,
    p.derived_table_names,
    COALESCE(p.read_only_events, 0)   AS read_only_events,
    p.top_consumers,
    p.last_read,
    CASE
        WHEN p.table_name IS NULL      THEN 'NO_CONSUMERS'
        WHEN p.derived_tables = 0      THEN 'READ_ONLY'
        ELSE 'HAS_DERIVED'
    END                               AS status
FROM tables_in_scope t
LEFT JOIN per_table p USING (table_name)
ORDER BY
    CASE WHEN p.table_name IS NULL THEN 0 WHEN p.derived_tables = 0 THEN 1 ELSE 2 END ASC,
    consumer_entities ASC,
    t.table_name
```
