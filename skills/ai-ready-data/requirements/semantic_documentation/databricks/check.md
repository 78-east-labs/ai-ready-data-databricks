# Check: semantic_documentation

Weighted fraction of base tables and their columns in the schema that carry a non-empty comment.

## Context

This is a **native** check. Unity Catalog stores object descriptions in `information_schema.tables.comment` and `information_schema.columns.comment`, and those two fields are exactly what Catalog Explorer, Genie, the Databricks Assistant and the `ai_*` functions read as the semantic description of a dataset. Both levels matter: a table comment says what one row is and where the data comes from; column comments say what each field means. A schema with commented tables and bare columns (or the reverse) is half documented, so the score blends the two.

Formula:

```
value = {{ table_weight }} * (commented_tables / total_tables)
      + (1 - {{ table_weight }}) * (commented_columns / total_columns)
```

`{{ table_weight }}` defaults to `0.3`. Column comments get the larger share because there are more of them, they are where units, allowed values and grain are stated, and they are what text-to-SQL uses to pick columns. Set `table_weight` to `0.5` if the team's convention is a rich table comment that documents every column in one place. `commented_objects` and `total_objects` are also returned (tables plus columns, unweighted) so the report can show counts.

A comment counts if it is non-empty after `trim()`. Content is not judged; a one-word comment passes. AI-generated comments accepted in Catalog Explorer are stored the same way and count. `business_glossary_linkage` and `schema_type_coverage` measure stronger forms of the same intent.

Comments appear in `information_schema` immediately after `COMMENT ON`. Rows are filtered to objects the caller can see. Views, streaming tables and materialized views are excluded from the primary; the variant includes them because Lakeflow objects carry their own `COMMENT` clauses and are often the consumer-facing layer.

Placeholders beyond the standard set: `{{ table_weight }}` (default `0.3`).

Returns NULL (N/A) when the schema contains no base tables.

## SQL

### Weighted table and column comments (primary)

```sql
WITH tables_in_scope AS (
    SELECT LOWER(table_name) AS table_name,
           comment IS NOT NULL AND trim(comment) <> '' AS has_comment
    FROM {{ catalog }}.information_schema.tables
    WHERE LOWER(table_schema) = LOWER('{{ schema }}')
      AND table_type IN ('MANAGED', 'EXTERNAL')
),
columns_in_scope AS (
    SELECT LOWER(c.table_name) AS table_name,
           c.comment IS NOT NULL AND trim(c.comment) <> '' AS has_comment
    FROM {{ catalog }}.information_schema.columns c
    JOIN tables_in_scope t ON LOWER(c.table_name) = t.table_name
    WHERE LOWER(c.table_schema) = LOWER('{{ schema }}')
),
t AS (SELECT COUNT_IF(has_comment) AS commented_tables,  COUNT(*) AS total_tables  FROM tables_in_scope),
c AS (SELECT COUNT_IF(has_comment) AS commented_columns, COUNT(*) AS total_columns FROM columns_in_scope)
SELECT
    t.commented_tables,
    t.total_tables,
    c.commented_columns,
    c.total_columns,
    t.commented_tables + c.commented_columns                          AS commented_objects,
    t.total_tables + c.total_columns                                  AS total_objects,
    {{ table_weight }}::DOUBLE       * (t.commented_tables::DOUBLE  / NULLIF(t.total_tables, 0))
  + (1 - {{ table_weight }})::DOUBLE * (c.commented_columns::DOUBLE / NULLIF(c.total_columns, 0)) AS value
FROM t CROSS JOIN c
```

### Including views, streaming tables and materialized views (variant)

Same formula over every relation type in the schema. Use when the schema is a serving layer made of views or Lakeflow objects.

```sql
WITH tables_in_scope AS (
    SELECT LOWER(table_name) AS table_name,
           comment IS NOT NULL AND trim(comment) <> '' AS has_comment
    FROM {{ catalog }}.information_schema.tables
    WHERE LOWER(table_schema) = LOWER('{{ schema }}')
      AND table_type IN ('MANAGED', 'EXTERNAL', 'VIEW', 'STREAMING_TABLE', 'MATERIALIZED_VIEW')
),
columns_in_scope AS (
    SELECT LOWER(c.table_name) AS table_name,
           c.comment IS NOT NULL AND trim(c.comment) <> '' AS has_comment
    FROM {{ catalog }}.information_schema.columns c
    JOIN tables_in_scope t ON LOWER(c.table_name) = t.table_name
    WHERE LOWER(c.table_schema) = LOWER('{{ schema }}')
),
t AS (SELECT COUNT_IF(has_comment) AS commented_tables,  COUNT(*) AS total_tables  FROM tables_in_scope),
c AS (SELECT COUNT_IF(has_comment) AS commented_columns, COUNT(*) AS total_columns FROM columns_in_scope)
SELECT
    t.commented_tables,
    t.total_tables,
    c.commented_columns,
    c.total_columns,
    t.commented_tables + c.commented_columns                          AS commented_objects,
    t.total_tables + c.total_columns                                  AS total_objects,
    {{ table_weight }}::DOUBLE       * (t.commented_tables::DOUBLE  / NULLIF(t.total_tables, 0))
  + (1 - {{ table_weight }})::DOUBLE * (c.commented_columns::DOUBLE / NULLIF(c.total_columns, 0)) AS value
FROM t CROSS JOIN c
```

### Fully documented tables (variant)

Table-granular and strict: a table passes only when it has a table comment and every one of its columns has a comment. Use this to report "how many tables are done" rather than a blended fraction.

```sql
WITH tables_in_scope AS (
    SELECT LOWER(table_name) AS table_name,
           comment IS NOT NULL AND trim(comment) <> '' AS table_commented
    FROM {{ catalog }}.information_schema.tables
    WHERE LOWER(table_schema) = LOWER('{{ schema }}')
      AND table_type IN ('MANAGED', 'EXTERNAL')
),
column_cov AS (
    SELECT LOWER(c.table_name) AS table_name,
           COUNT(*) AS n_cols,
           COUNT_IF(c.comment IS NOT NULL AND trim(c.comment) <> '') AS n_commented
    FROM {{ catalog }}.information_schema.columns c
    JOIN tables_in_scope t ON LOWER(c.table_name) = t.table_name
    WHERE LOWER(c.table_schema) = LOWER('{{ schema }}')
    GROUP BY LOWER(c.table_name)
)
SELECT
    COUNT_IF(t.table_commented AND cc.n_commented = cc.n_cols)                              AS fully_documented_tables,
    COUNT(*)                                                                                 AS total_tables,
    COUNT_IF(t.table_commented AND cc.n_commented = cc.n_cols)::DOUBLE / NULLIF(COUNT(*), 0) AS value
FROM tables_in_scope t
LEFT JOIN column_cov cc USING (table_name)
```
