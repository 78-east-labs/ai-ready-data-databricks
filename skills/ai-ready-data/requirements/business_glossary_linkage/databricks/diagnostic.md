# Diagnostic: business_glossary_linkage

Lists every column on base tables in the schema with its glossary tag value, the metric views that reference it, and a linkage status.

## Context

Use this to see which columns are unlinked, which glossary terms are already in use (so a fix extends the existing vocabulary), and where metric views already act as the semantic layer. Status values:

- `TAGGED`: carries the `{{ glossary_tag_key }}` tag (default `glossary_term`) with a non-empty value.
- `METRIC_VIEW`: no tag, but referenced from at least one metric view whose source is this table (best-effort text match, see check.md).
- `UNLINKED`: neither.

`metric_views` lists the referencing metric views. `existing_terms_on_table` shows the distinct glossary terms already applied elsewhere on the same table, which is usually the fastest hint for what vocabulary the team uses. `has_comment` is included because a commented but unlinked column is the cheapest one to tag: the term is usually already written in the comment.

Sorted so unlinked columns come first, then by table and ordinal position.

## SQL

```sql
WITH columns_in_scope AS (
    SELECT LOWER(c.table_name)  AS table_name,
           c.column_name        AS column_name_cased,
           LOWER(c.column_name) AS column_name,
           c.data_type,
           c.ordinal_position,
           c.comment
    FROM {{ catalog }}.information_schema.columns c
    JOIN {{ catalog }}.information_schema.tables t
      ON c.table_schema = t.table_schema AND c.table_name = t.table_name
    WHERE LOWER(c.table_schema) = LOWER('{{ schema }}')
      AND t.table_type IN ('MANAGED', 'EXTERNAL')
),
tagged AS (
    SELECT LOWER(table_name) AS table_name, LOWER(column_name) AS column_name,
           max(tag_value)    AS glossary_term
    FROM {{ catalog }}.information_schema.column_tags
    WHERE LOWER(schema_name) = LOWER('{{ schema }}')
      AND LOWER(tag_name) = LOWER('{{ glossary_tag_key }}')
      AND tag_value IS NOT NULL AND tag_value <> ''
    GROUP BY 1, 2
),
terms_per_table AS (
    SELECT table_name, array_sort(collect_set(glossary_term)) AS existing_terms_on_table
    FROM tagged GROUP BY table_name
),
metric_view_defs AS (
    SELECT t.table_schema || '.' || t.table_name AS metric_view,
           LOWER(v.view_definition)              AS definition
    FROM {{ catalog }}.information_schema.tables t
    JOIN {{ catalog }}.information_schema.views v
      ON v.table_schema = t.table_schema AND v.table_name = t.table_name
    WHERE t.table_type = 'METRIC_VIEW'
      AND v.view_definition IS NOT NULL
),
metric_linked AS (
    SELECT c.table_name, c.column_name,
           array_sort(collect_set(m.metric_view)) AS metric_views
    FROM columns_in_scope c
    JOIN metric_view_defs m
      ON m.definition RLIKE concat('source: *`?', LOWER('{{ catalog }}'), '`?\\.`?', LOWER('{{ schema }}'), '`?\\.`?', c.table_name, '`?')
     AND m.definition RLIKE concat('(^|[^a-z0-9_])', c.column_name, '([^a-z0-9_]|$)')
    GROUP BY c.table_name, c.column_name
)
SELECT
    c.table_name,
    c.column_name_cased                      AS column_name,
    c.data_type,
    CASE
        WHEN tg.glossary_term IS NOT NULL THEN 'TAGGED'
        WHEN ml.metric_views  IS NOT NULL THEN 'METRIC_VIEW'
        ELSE 'UNLINKED'
    END                                      AS linkage_status,
    tg.glossary_term,
    ml.metric_views,
    tp.existing_terms_on_table,
    c.comment IS NOT NULL AND c.comment <> '' AS has_comment,
    left(c.comment, 120)                     AS comment_preview
FROM columns_in_scope c
LEFT JOIN tagged          tg USING (table_name, column_name)
LEFT JOIN metric_linked   ml USING (table_name, column_name)
LEFT JOIN terms_per_table tp USING (table_name)
ORDER BY
    CASE WHEN tg.glossary_term IS NULL AND ml.metric_views IS NULL THEN 0
         WHEN tg.glossary_term IS NULL THEN 1 ELSE 2 END,
    c.table_name, c.ordinal_position
```
