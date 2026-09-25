# Check: business_glossary_linkage

Fraction of columns on base tables in the schema that are linked to a business term, either through a `glossary_term` column tag or by being referenced from a Unity Catalog metric view.

## Context

Databricks has no first-class business glossary object that `information_schema` exposes, so this is a **tag / proxy** check. A column counts as linked when either signal is present:

1. **Column tag** `{{ glossary_tag_key }}` in `{{ catalog }}.information_schema.column_tags`. Default key is `glossary_term`; the tag value is the term (for example `net_revenue`). Any non-empty value passes. This is the deterministic signal and the one the fix applies.
2. **Metric view reference.** Metric views (`information_schema.tables.table_type = 'METRIC_VIEW'`) are the closest Databricks primitive to a governed semantic layer: each measure and dimension in the view's YAML has a name and an expression over a source table. A base-table column that appears as a token in a metric view definition whose `source:` is that table is treated as glossary-linked. Detection is **best-effort**: it reads `information_schema.views.view_definition` for metric views and does a word-boundary text match on the column name. Two caveats. First, on some workspace versions `view_definition` is NULL for metric views; run the probe below and, if it comes back empty, fall back to `SHOW CREATE TABLE {{ catalog }}.{{ schema }}.<metric_view>` per metric view and match the YAML by hand. Second, a text match cannot tell a measure expression from a filter clause, so a column used only in a `filter:` counts as linked. Treat metric-view linkage as an indicator, not proof.

Probe to confirm metric view definitions are readable in your workspace:

```sql
SELECT t.table_name, v.view_definition IS NOT NULL AS has_definition
FROM {{ catalog }}.information_schema.tables t
LEFT JOIN {{ catalog }}.information_schema.views v
  ON v.table_schema = t.table_schema AND v.table_name = t.table_name
WHERE LOWER(t.table_schema) = LOWER('{{ schema }}') AND t.table_type = 'METRIC_VIEW'
```

Comments are deliberately not counted here; `semantic_documentation` measures them. A column with a 200-character comment but no term still scores 0 on this check, because a comment does not link the column to a controlled vocabulary.

Tags appear in `information_schema` within seconds of `ALTER TABLE ... ALTER COLUMN ... SET TAGS`. Rows are filtered to objects the caller can see. Metric views located in a different schema than their source tables are included by the primary query as long as they are in the same catalog.

Placeholders beyond the standard set: `{{ glossary_tag_key }}` (default `glossary_term`).

Returns NULL (N/A) when the schema contains no columns on base tables.

## SQL

### Tag or metric view reference (primary)

```sql
WITH columns_in_scope AS (
    SELECT LOWER(c.table_name)  AS table_name,
           LOWER(c.column_name) AS column_name
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
      AND LOWER(tag_name) = LOWER('{{ glossary_tag_key }}')
      AND tag_value IS NOT NULL AND tag_value <> ''
),
metric_view_defs AS (
    SELECT LOWER(v.view_definition) AS definition
    FROM {{ catalog }}.information_schema.tables t
    JOIN {{ catalog }}.information_schema.views v
      ON v.table_schema = t.table_schema AND v.table_name = t.table_name
    WHERE t.table_type = 'METRIC_VIEW'
      AND v.view_definition IS NOT NULL
),
metric_linked AS (
    SELECT DISTINCT c.table_name, c.column_name
    FROM columns_in_scope c
    JOIN metric_view_defs m
      ON m.definition RLIKE concat('source: *`?', LOWER('{{ catalog }}'), '`?\\.`?', LOWER('{{ schema }}'), '`?\\.`?', c.table_name, '`?')
     AND m.definition RLIKE concat('(^|[^a-z0-9_])', c.column_name, '([^a-z0-9_]|$)')
),
classified AS (
    SELECT c.table_name, c.column_name,
           (tg.column_name IS NOT NULL OR ml.column_name IS NOT NULL) AS is_linked
    FROM columns_in_scope c
    LEFT JOIN tagged        tg USING (table_name, column_name)
    LEFT JOIN metric_linked ml USING (table_name, column_name)
)
SELECT
    COUNT_IF(is_linked)                              AS linked_columns,
    COUNT(*)                                         AS total_columns,
    COUNT_IF(is_linked)::DOUBLE / NULLIF(COUNT(*), 0) AS value
FROM classified
```

### Tag only (variant)

Strict form: only the glossary tag counts. Use this when the workspace has no metric views, when `view_definition` is NULL for metric views, or when the team wants a score that a tagging fix can move deterministically.

```sql
WITH columns_in_scope AS (
    SELECT LOWER(c.table_name)  AS table_name,
           LOWER(c.column_name) AS column_name
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
      AND LOWER(tag_name) = LOWER('{{ glossary_tag_key }}')
      AND tag_value IS NOT NULL AND tag_value <> ''
)
SELECT
    COUNT_IF(tg.column_name IS NOT NULL)                              AS linked_columns,
    COUNT(*)                                                          AS total_columns,
    COUNT_IF(tg.column_name IS NOT NULL)::DOUBLE / NULLIF(COUNT(*), 0) AS value
FROM columns_in_scope c
LEFT JOIN tagged tg USING (table_name, column_name)
```
