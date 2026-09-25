# Diagnostic: classification

Lists every base table in the schema with its tag count, the tag keys present, and whether any column on it is tagged.

## Context

Use this to see which tables are untagged and which tag vocabulary is already in use, so a fix can extend the existing convention rather than start a new one. The `tag_keys` column is the distinct set of keys on the table; `tagged_columns` counts columns with at least one tag.

Sorted so untagged tables come first.

## SQL

```sql
WITH tables_in_scope AS (
    SELECT LOWER(table_name) AS table_name, table_owner, comment
    FROM {{ catalog }}.information_schema.tables
    WHERE LOWER(table_schema) = LOWER('{{ schema }}')
      AND table_type IN ('MANAGED', 'EXTERNAL')
),
table_tags AS (
    SELECT LOWER(table_name) AS table_name,
           COUNT(*)                          AS tag_count,
           array_sort(collect_set(tag_name)) AS tag_keys
    FROM {{ catalog }}.information_schema.table_tags
    WHERE LOWER(schema_name) = LOWER('{{ schema }}')
    GROUP BY LOWER(table_name)
),
column_tags AS (
    SELECT LOWER(table_name) AS table_name,
           COUNT(DISTINCT column_name) AS tagged_columns
    FROM {{ catalog }}.information_schema.column_tags
    WHERE LOWER(schema_name) = LOWER('{{ schema }}')
    GROUP BY LOWER(table_name)
)
SELECT
    t.table_name,
    t.table_owner,
    COALESCE(tt.tag_count, 0)       AS tag_count,
    tt.tag_keys,
    COALESCE(ct.tagged_columns, 0)  AS tagged_columns,
    t.comment IS NOT NULL AND t.comment <> '' AS has_comment
FROM tables_in_scope t
LEFT JOIN table_tags  tt USING (table_name)
LEFT JOIN column_tags ct USING (table_name)
ORDER BY tag_count ASC, t.table_name
```
