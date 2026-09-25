# Diagnostic: data_provenance

Lists every base table with its provenance tags, the external paths and upstream tables lineage has seen feeding it, and a status, undocumented tables first.

## Context

Use this to decide which tables need tags and what the tag values should be. `external_sources` collects the distinct `source_path` values that wrote into the table in the window (an S3 or ADLS prefix is usually enough to name the source system), and `upstream_tables` collects Unity Catalog tables that fed it, which tells you whether the table is a landing table (needs tags) or a derived one (provenance belongs upstream). `writer_entity_types` shows how the writes arrived.

`provenance_status` values:

- `TAGGED`: both tags present with non-empty values
- `PARTIAL_TAGS`: only one of the two tags present
- `LINEAGE_ONLY`: no tags, but an external file source is recorded
- `NONE`: nothing

Tag keys default to `source_system` / `collection_method` and follow `{{ source_system_tag }}` / `{{ collection_method_tag }}`. Lineage lags by up to a few hours; `{{ lookback_days }}` defaults to 30.

## SQL

```sql
WITH tables_in_scope AS (
    SELECT LOWER(table_name) AS table_name, table_owner, comment, created
    FROM {{ catalog }}.information_schema.tables
    WHERE LOWER(table_schema) = LOWER('{{ schema }}')
      AND table_type IN ('MANAGED', 'EXTERNAL')
),
tags AS (
    SELECT LOWER(table_name) AS table_name,
           MAX(CASE WHEN LOWER(tag_name) = LOWER('{{ source_system_tag }}')
                     AND trim(tag_value) <> '' THEN tag_value END)     AS source_system,
           MAX(CASE WHEN LOWER(tag_name) = LOWER('{{ collection_method_tag }}')
                     AND trim(tag_value) <> '' THEN tag_value END)     AS collection_method
    FROM {{ catalog }}.information_schema.table_tags
    WHERE LOWER(schema_name) = LOWER('{{ schema }}')
    GROUP BY LOWER(table_name)
),
lineage AS (
    SELECT LOWER(target_table_name) AS table_name,
           array_sort(collect_set(CASE WHEN source_path IS NOT NULL OR UPPER(source_type) = 'PATH'
                                       THEN source_path END))              AS external_sources,
           array_sort(collect_set(source_table_full_name))                 AS upstream_tables,
           array_sort(collect_set(UPPER(entity_type)))                     AS writer_entity_types,
           MAX(event_time)                                                 AS last_write
    FROM system.access.table_lineage
    WHERE LOWER(target_catalog) = LOWER('{{ catalog }}')
      AND LOWER(target_schema)  = LOWER('{{ schema }}')
      AND target_table_full_name IS NOT NULL
      AND event_time >= current_timestamp() - INTERVAL {{ lookback_days }} DAYS
    GROUP BY LOWER(target_table_name)
)
SELECT
    t.table_name,
    t.table_owner,
    tg.source_system,
    tg.collection_method,
    l.external_sources,
    l.upstream_tables,
    l.writer_entity_types,
    l.last_write,
    t.comment IS NOT NULL AND t.comment <> ''                       AS has_comment,
    CASE
        WHEN tg.source_system IS NOT NULL AND tg.collection_method IS NOT NULL THEN 'TAGGED'
        WHEN tg.source_system IS NOT NULL OR  tg.collection_method IS NOT NULL THEN 'PARTIAL_TAGS'
        WHEN size(l.external_sources) > 0                                      THEN 'LINEAGE_ONLY'
        ELSE 'NONE'
    END                                                              AS provenance_status
FROM tables_in_scope t
LEFT JOIN tags    tg USING (table_name)
LEFT JOIN lineage l  USING (table_name)
ORDER BY
    CASE
        WHEN tg.source_system IS NOT NULL AND tg.collection_method IS NOT NULL THEN 3
        WHEN tg.source_system IS NOT NULL OR  tg.collection_method IS NOT NULL THEN 2
        WHEN size(l.external_sources) > 0                                      THEN 1
        ELSE 0
    END ASC,
    t.table_name
```
