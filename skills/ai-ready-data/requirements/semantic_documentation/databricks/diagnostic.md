# Diagnostic: semantic_documentation

Lists every base table in the schema with its table-comment status, column-comment coverage, the uncommented column names, and a status that says what kind of fix it needs.

## Context

One row per table. `column_coverage` is `commented_columns / total_columns` for that table. `uncommented_columns` lists up to 25 column names without a comment so the fix can target them; `uncommented_column_count` gives the full number. `table_comment_preview` shows the first 120 characters so a placeholder comment ("tbd", "table") is visible.

`documentation_status`:

- `COMPLETE`: table comment present and every column commented.
- `TABLE_ONLY`: table comment present, some or all columns bare. Usual case for tables created by a pipeline template.
- `COLUMNS_ONLY`: columns documented, no table comment. One `COMMENT ON TABLE` fixes it.
- `PARTIAL`: table comment missing and some columns commented.
- `NONE`: nothing. Candidates for AI-generated comments as a first pass.

`table_owner` is included because comments are written by whoever owns the model, and `last_altered` because a recently changed table is more likely to have a live owner who can write them. Sorted worst-first: `NONE`, then `PARTIAL`, then by lowest column coverage.

## SQL

```sql
WITH tables_in_scope AS (
    SELECT LOWER(t.table_name) AS table_name,
           t.table_owner,
           t.last_altered,
           t.comment           AS table_comment,
           t.comment IS NOT NULL AND trim(t.comment) <> '' AS table_commented
    FROM {{ catalog }}.information_schema.tables t
    WHERE LOWER(t.table_schema) = LOWER('{{ schema }}')
      AND t.table_type IN ('MANAGED', 'EXTERNAL')
),
column_cov AS (
    SELECT LOWER(c.table_name) AS table_name,
           COUNT(*)                                                    AS total_columns,
           COUNT_IF(c.comment IS NOT NULL AND trim(c.comment) <> '')  AS commented_columns,
           slice(array_sort(collect_list(
               CASE WHEN c.comment IS NULL OR trim(c.comment) = '' THEN c.column_name END)), 1, 25)
                                                                       AS uncommented_columns
    FROM {{ catalog }}.information_schema.columns c
    JOIN tables_in_scope t ON LOWER(c.table_name) = t.table_name
    WHERE LOWER(c.table_schema) = LOWER('{{ schema }}')
    GROUP BY LOWER(c.table_name)
)
SELECT
    t.table_name,
    t.table_owner,
    t.last_altered,
    t.table_commented,
    left(t.table_comment, 120)                                          AS table_comment_preview,
    COALESCE(cc.commented_columns, 0)                                   AS commented_columns,
    COALESCE(cc.total_columns, 0)                                       AS total_columns,
    round(cc.commented_columns::DOUBLE / NULLIF(cc.total_columns, 0), 3) AS column_coverage,
    COALESCE(cc.total_columns, 0) - COALESCE(cc.commented_columns, 0)   AS uncommented_column_count,
    cc.uncommented_columns,
    CASE
        WHEN t.table_commented AND cc.commented_columns = cc.total_columns THEN 'COMPLETE'
        WHEN t.table_commented                                              THEN 'TABLE_ONLY'
        WHEN cc.commented_columns = cc.total_columns                        THEN 'COLUMNS_ONLY'
        WHEN COALESCE(cc.commented_columns, 0) > 0                          THEN 'PARTIAL'
        ELSE 'NONE'
    END                                                                 AS documentation_status
FROM tables_in_scope t
LEFT JOIN column_cov cc USING (table_name)
ORDER BY
    CASE
        WHEN NOT t.table_commented AND COALESCE(cc.commented_columns, 0) = 0 THEN 0
        WHEN NOT t.table_commented                                          THEN 1
        WHEN cc.commented_columns < cc.total_columns                        THEN 2
        ELSE 3
    END,
    column_coverage ASC NULLS FIRST,
    t.table_name
```
