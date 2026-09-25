# Check: schema_type_coverage

Fraction of columns on base tables in the schema that carry a semantic-role signal beyond their physical type: a non-empty comment, at least one column tag, or a name that follows a recognizable role convention.

## Context

Every Unity Catalog column has a physical type in `information_schema.columns.data_type`, so "is the type declared" is always 1.0 and says nothing. What an AI consumer needs is the column's role: is this `BIGINT` an identifier, a count, or cents? This check measures whether that role can be read from metadata without scanning rows. It mixes a **native** signal with a **proxy**:

1. **Comment** (`columns.comment` non-empty). Native. Any text counts; `semantic_documentation` measures the same field but weighted with table comments, and `business_glossary_linkage` requires a controlled term.
2. **Column tag** (any row in `information_schema.column_tags`). Native. A `pii`, `unit`, `glossary_term` or `semantic_role` tag all count; each states something about the role.
3. **Name pattern** (proxy). The column name matches one of these families, matched with `REGEXP_LIKE` so `_id` does not match `userxid`:
   - identifier: `^id$`, `_id$`, `_key$`, `_sk$`, `_uuid$`, `_code$`
   - temporal: `_at$`, `_ts$`, `_date$`, `_time$`, `_timestamp$`, `^date_`, `_dt$`
   - measure: `amount`, `price`, `cost`, `total`, `revenue`, `qty`, `quantity`, `count`, `_num$`, `rate`, `pct`, `percent`, `score`
   - flag: `^is_`, `^has_`, `_flag$`, `^flag_`, `enabled$`, `active$`
   - descriptive: `name$`, `description$`, `status$`, `type$`, `category$`, `label$`, `title$`, `text$`, `body$`, `message$`
   A name match is a weak signal: `total` could be a count or a currency amount. It is included so that a schema with disciplined naming and no comments is not scored the same as one with `col1 ... col40`. The strict variant drops it.

Comments and tags appear in `information_schema` immediately. Rows are filtered to objects the caller can see. AI-generated comments accepted in Catalog Explorer land in `columns.comment` and count like any other.

Returns NULL (N/A) when the schema contains no columns on base tables.

## SQL

### Comment, tag or name pattern (primary)

```sql
WITH columns_in_scope AS (
    SELECT LOWER(c.table_name)  AS table_name,
           LOWER(c.column_name) AS column_name,
           c.comment
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
),
classified AS (
    SELECT c.table_name, c.column_name,
           (   (c.comment IS NOT NULL AND trim(c.comment) <> '')
            OR tg.column_name IS NOT NULL
            OR REGEXP_LIKE(c.column_name,
                 '(^id$|_id$|_key$|_sk$|_uuid$|_code$'
              || '|_at$|_ts$|_date$|_time$|_timestamp$|^date_|_dt$'
              || '|amount|price|cost|total|revenue|qty|quantity|count|_num$|rate|pct|percent|score'
              || '|^is_|^has_|_flag$|^flag_|enabled$|active$'
              || '|name$|description$|status$|type$|category$|label$|title$|text$|body$|message$)')
           ) AS has_role_signal
    FROM columns_in_scope c
    LEFT JOIN tagged tg USING (table_name, column_name)
)
SELECT
    COUNT_IF(has_role_signal)                              AS columns_with_role,
    COUNT(*)                                               AS total_columns,
    COUNT_IF(has_role_signal)::DOUBLE / NULLIF(COUNT(*), 0) AS value
FROM classified
```

### Explicit metadata only (variant)

Stricter: only a comment or a tag counts. Use this when the team wants a score that reflects deliberate documentation rather than naming habits, or when the schema's naming is already clean and the primary is saturated near 1.0.

```sql
WITH columns_in_scope AS (
    SELECT LOWER(c.table_name)  AS table_name,
           LOWER(c.column_name) AS column_name,
           c.comment
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
    COUNT_IF((c.comment IS NOT NULL AND trim(c.comment) <> '') OR tg.column_name IS NOT NULL)          AS columns_with_role,
    COUNT(*)                                                                                             AS total_columns,
    COUNT_IF((c.comment IS NOT NULL AND trim(c.comment) <> '') OR tg.column_name IS NOT NULL)::DOUBLE
        / NULLIF(COUNT(*), 0)                                                                            AS value
FROM columns_in_scope c
LEFT JOIN tagged tg USING (table_name, column_name)
```
