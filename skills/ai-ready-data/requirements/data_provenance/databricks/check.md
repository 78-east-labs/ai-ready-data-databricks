# Check: data_provenance

Fraction of base tables in the schema whose origin is documented, either by the `source_system` and `collection_method` tags or by at least one upstream lineage edge from an external file source.

## Context

Two signals, either of which counts:

- **Tags** (declared provenance). `{{ catalog }}.information_schema.table_tags` rows with `tag_name = 'source_system'` and `tag_name = 'collection_method'` on the same table, both with a non-empty value. This is the tag convention from `platforms/DATABRICKS.md`; the keys can be renamed through `{{ source_system_tag }}` (default `source_system`) and `{{ collection_method_tag }}` (default `collection_method`). Tags are a human statement of origin, which is what provenance actually means, so they are the primary evidence.
- **External-source lineage** (observed provenance). `system.access.table_lineage` rows where the table is the target and the source is a file path rather than another table: `source_path IS NOT NULL` or `source_type = 'PATH'`. This is what Auto Loader, `COPY INTO` and `spark.read.format(...).load(path)` writes leave behind. It proves where the bytes came from but not which system produced them, so it is a proxy. The `source_type` value for path sources is expected to be `PATH`; confirm with `SELECT DISTINCT source_type FROM system.access.table_lineage WHERE source_path IS NOT NULL LIMIT 10` if the lineage half returns nothing.

Tables fed only from other Unity Catalog tables do not get lineage credit here, on purpose: their provenance is the upstream table's problem, and `lineage_completeness` measures that edge. If they are the landing point for a system (a JDBC extract written by a job), they need the tags.

`information_schema` is current; `table_lineage` lags by up to a few hours and needs `SELECT` on `system.access.table_lineage`. `{{ lookback_days }}` defaults to 30 for the lineage half.

Returns NULL (N/A) when the schema contains no base tables.

## SQL

### Tags or external-source lineage (primary)

```sql
WITH tables_in_scope AS (
    SELECT LOWER(table_name) AS table_name
    FROM {{ catalog }}.information_schema.tables
    WHERE LOWER(table_schema) = LOWER('{{ schema }}')
      AND table_type IN ('MANAGED', 'EXTERNAL')
),
tagged AS (
    SELECT LOWER(table_name) AS table_name
    FROM {{ catalog }}.information_schema.table_tags
    WHERE LOWER(schema_name) = LOWER('{{ schema }}')
      AND tag_value IS NOT NULL AND trim(tag_value) <> ''
    GROUP BY LOWER(table_name)
    HAVING COUNT_IF(LOWER(tag_name) = LOWER('{{ source_system_tag }}'))     > 0
       AND COUNT_IF(LOWER(tag_name) = LOWER('{{ collection_method_tag }}')) > 0
),
external_sourced AS (
    SELECT DISTINCT LOWER(target_table_name) AS table_name
    FROM system.access.table_lineage
    WHERE LOWER(target_catalog) = LOWER('{{ catalog }}')
      AND LOWER(target_schema)  = LOWER('{{ schema }}')
      AND (source_path IS NOT NULL OR UPPER(source_type) = 'PATH')
      AND event_time >= current_timestamp() - INTERVAL {{ lookback_days }} DAYS
)
SELECT
    COUNT_IF(tg.table_name IS NOT NULL OR ex.table_name IS NOT NULL)           AS tables_with_provenance,
    COUNT(*)                                                                   AS total_tables,
    COUNT_IF(tg.table_name IS NOT NULL OR ex.table_name IS NOT NULL)::DOUBLE
        / NULLIF(COUNT(*), 0)                                                  AS value
FROM tables_in_scope t
LEFT JOIN tagged           tg USING (table_name)
LEFT JOIN external_sourced ex USING (table_name)
```

### Tags only (variant)

Strict form for organizations that require declared provenance regardless of lineage. Does not touch system tables, so it has no lag and no `system.access` permission requirement.

```sql
WITH tables_in_scope AS (
    SELECT LOWER(table_name) AS table_name
    FROM {{ catalog }}.information_schema.tables
    WHERE LOWER(table_schema) = LOWER('{{ schema }}')
      AND table_type IN ('MANAGED', 'EXTERNAL')
),
tagged AS (
    SELECT LOWER(table_name) AS table_name
    FROM {{ catalog }}.information_schema.table_tags
    WHERE LOWER(schema_name) = LOWER('{{ schema }}')
      AND tag_value IS NOT NULL AND trim(tag_value) <> ''
    GROUP BY LOWER(table_name)
    HAVING COUNT_IF(LOWER(tag_name) = LOWER('{{ source_system_tag }}'))     > 0
       AND COUNT_IF(LOWER(tag_name) = LOWER('{{ collection_method_tag }}')) > 0
)
SELECT
    COUNT_IF(tg.table_name IS NOT NULL)            AS tables_with_provenance,
    COUNT(*)                                        AS total_tables,
    COUNT_IF(tg.table_name IS NOT NULL)::DOUBLE
        / NULLIF(COUNT(*), 0)                       AS value
FROM tables_in_scope t
LEFT JOIN tagged tg USING (table_name)
```

### Comment keywords (variant)

Weakest form, kept for parity with the upstream framework: a table comment longer than 20 characters that mentions a source, origin or upstream system. Use only to credit legacy documentation before tags exist; the regex cannot tell "loaded from Salesforce nightly" from "do not load from this table".

```sql
WITH tables_in_scope AS (
    SELECT LOWER(table_name) AS table_name, comment
    FROM {{ catalog }}.information_schema.tables
    WHERE LOWER(table_schema) = LOWER('{{ schema }}')
      AND table_type IN ('MANAGED', 'EXTERNAL')
)
SELECT
    COUNT_IF(comment IS NOT NULL AND length(comment) > 20
             AND REGEXP_LIKE(LOWER(comment),
                 '(^|\\W)(source|origin|from|upstream|loaded|extracted|ingested)(\\W|$)'))   AS tables_with_provenance,
    COUNT(*)                                                                                  AS total_tables,
    COUNT_IF(comment IS NOT NULL AND length(comment) > 20
             AND REGEXP_LIKE(LOWER(comment),
                 '(^|\\W)(source|origin|from|upstream|loaded|extracted|ingested)(\\W|$)'))::DOUBLE
        / NULLIF(COUNT(*), 0)                                                                 AS value
FROM tables_in_scope
```
