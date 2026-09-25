# Check: classification

Fraction of base tables in the schema that have at least one Unity Catalog tag applied at the table level.

## Context

Reads `{{ catalog }}.information_schema.table_tags`, which lists every table-level tag in the catalog visible to the caller. A table counts as classified if it carries any tag. The check measures presence, not quality: a table tagged `team = 'growth'` scores the same as one tagged `sensitivity = 'high'`. Use the column-level variant, or `column_masking`, for a stricter view.

`information_schema` reflects tags within seconds of `ALTER TABLE ... SET TAGS`, so there is no lag to warn about. Rows are filtered to objects the caller can see; if the assessment runs as a user with partial `USE SCHEMA` grants, both numerator and denominator shrink together.

If Databricks Data Classification (automatic PII tagging) is enabled on the catalog, its results show up here as ordinary column tags and count in the column-level variant.

Returns NULL (N/A) when the schema contains no base tables (primary) or no columns (variant).

## SQL

### Table-level classification (primary)

```sql
WITH tables_in_scope AS (
    SELECT LOWER(table_name) AS table_name
    FROM {{ catalog }}.information_schema.tables
    WHERE LOWER(table_schema) = LOWER('{{ schema }}')
      AND table_type IN ('MANAGED', 'EXTERNAL')
),
tagged AS (
    SELECT DISTINCT LOWER(table_name) AS table_name
    FROM {{ catalog }}.information_schema.table_tags
    WHERE LOWER(schema_name) = LOWER('{{ schema }}')
)
SELECT
    COUNT_IF(tg.table_name IS NOT NULL)           AS tagged_tables,
    COUNT(*)                                       AS total_tables,
    COUNT_IF(tg.table_name IS NOT NULL)::DOUBLE
        / NULLIF(COUNT(*), 0)                      AS value
FROM tables_in_scope t
LEFT JOIN tagged tg USING (table_name)
```

### Column-level classification (variant)

Stricter: fraction of columns on base tables that carry at least one tag.

```sql
WITH columns_in_scope AS (
    SELECT LOWER(c.table_name) AS table_name, LOWER(c.column_name) AS column_name
    FROM {{ catalog }}.information_schema.columns c
    JOIN {{ catalog }}.information_schema.tables t
      ON c.table_schema = t.table_schema AND c.table_name = t.table_name
    WHERE LOWER(c.table_schema) = LOWER('{{ schema }}')
      AND t.table_type IN ('MANAGED', 'EXTERNAL')
),
tagged AS (
    SELECT DISTINCT LOWER(table_name) AS table_name, LOWER(column_name) AS column_name
    FROM {{ catalog }}.information_schema.column_tags
    WHERE LOWER(schema_name) = LOWER('{{ schema }}')
)
SELECT
    COUNT_IF(tg.column_name IS NOT NULL)          AS tagged_columns,
    COUNT(*)                                       AS total_columns,
    COUNT_IF(tg.column_name IS NOT NULL)::DOUBLE
        / NULLIF(COUNT(*), 0)                      AS value
FROM columns_in_scope c
LEFT JOIN tagged tg USING (table_name, column_name)
```
