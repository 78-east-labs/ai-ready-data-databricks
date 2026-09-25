# Diagnostic: schema_type_coverage

Lists every column on base tables in the schema with its physical type, the semantic role inferred from its name and type, which signals it carries, and whether it needs attention.

## Context

One row per column. `inferred_role` is the framework's best guess from name and type (`IDENTIFIER`, `TEMPORAL`, `MEASURE`, `FLAG`, `CATEGORICAL`, `ATTRIBUTE`, `TEXT_CONTENT`, `EMBEDDING`, `STRUCTURED`, `UNKNOWN`). It is a guess: use it to prioritise and to seed comments, not as truth. `signals` lists which of `COMMENT`, `TAG`, `NAME` the column has; an empty array is a column the check scores 0.

`coverage_status`:

- `EXPLICIT`: has a comment or a tag.
- `NAME_ONLY`: passes the check on naming alone; a comment would make the role explicit.
- `NONE`: no signal at all. These are the rows to fix first, and `UNKNOWN` role among them are the ones no consumer can interpret.

`tag_keys` shows which tags are already on the column so a fix extends the existing vocabulary. Sorted with `NONE` first, then `NAME_ONLY`, then by table and ordinal position.

## SQL

```sql
WITH columns_in_scope AS (
    SELECT LOWER(c.table_name)  AS table_name,
           c.column_name        AS column_name_cased,
           LOWER(c.column_name) AS column_name,
           c.data_type,
           c.full_data_type,
           c.ordinal_position,
           c.is_nullable,
           c.comment
    FROM {{ catalog }}.information_schema.columns c
    JOIN {{ catalog }}.information_schema.tables t
      ON c.table_schema = t.table_schema AND c.table_name = t.table_name
    WHERE LOWER(c.table_schema) = LOWER('{{ schema }}')
      AND t.table_type IN ('MANAGED', 'EXTERNAL')
),
tags AS (
    SELECT LOWER(table_name) AS table_name, LOWER(column_name) AS column_name,
           array_sort(collect_set(tag_name)) AS tag_keys
    FROM {{ catalog }}.information_schema.column_tags
    WHERE LOWER(schema_name) = LOWER('{{ schema }}')
    GROUP BY 1, 2
),
classified AS (
    SELECT c.*, tg.tag_keys,
           c.comment IS NOT NULL AND trim(c.comment) <> '' AS has_comment,
           tg.tag_keys IS NOT NULL                          AS has_tag,
           REGEXP_LIKE(c.column_name,
                 '(^id$|_id$|_key$|_sk$|_uuid$|_code$'
              || '|_at$|_ts$|_date$|_time$|_timestamp$|^date_|_dt$'
              || '|amount|price|cost|total|revenue|qty|quantity|count|_num$|rate|pct|percent|score'
              || '|^is_|^has_|_flag$|^flag_|enabled$|active$'
              || '|name$|description$|status$|type$|category$|label$|title$|text$|body$|message$)') AS has_name_pattern,
           CASE
               WHEN LOWER(c.full_data_type) RLIKE '^array<(float|double)>'                THEN 'EMBEDDING'
               WHEN c.data_type IN ('STRUCT', 'MAP', 'ARRAY', 'VARIANT')                  THEN 'STRUCTURED'
               WHEN REGEXP_LIKE(c.column_name, '(^id$|_id$|_key$|_sk$|_uuid$|_guid$)')    THEN 'IDENTIFIER'
               WHEN c.data_type IN ('DATE', 'TIMESTAMP', 'TIMESTAMP_NTZ')
                 OR REGEXP_LIKE(c.column_name, '(_at$|_ts$|_date$|_time$|_timestamp$|_dt$)') THEN 'TEMPORAL'
               WHEN c.data_type = 'BOOLEAN'
                 OR REGEXP_LIKE(c.column_name, '(^is_|^has_|_flag$|^flag_|enabled$|active$)') THEN 'FLAG'
               WHEN REGEXP_LIKE(c.column_name, '(amount|price|cost|total|revenue|qty|quantity|count|_num$|rate|pct|percent|score|balance|weight|duration)')
                                                                                            THEN 'MEASURE'
               WHEN REGEXP_LIKE(c.column_name, '(status$|type$|category$|code$|tier$|segment$|state$|region$|country$)')
                                                                                            THEN 'CATEGORICAL'
               WHEN c.data_type = 'STRING'
                 AND REGEXP_LIKE(c.column_name, '(text$|body$|message$|content$|description$|notes?$|comment$)')
                                                                                            THEN 'TEXT_CONTENT'
               WHEN REGEXP_LIKE(c.column_name, '(name$|label$|title$|email|phone|address|url$)') THEN 'ATTRIBUTE'
               WHEN c.data_type IN ('INT', 'BIGINT', 'SMALLINT', 'TINYINT', 'DECIMAL', 'FLOAT', 'DOUBLE')
                                                                                            THEN 'MEASURE'
               ELSE 'UNKNOWN'
           END AS inferred_role
    FROM columns_in_scope c
    LEFT JOIN tags tg USING (table_name, column_name)
)
SELECT
    table_name,
    column_name_cased                                      AS column_name,
    full_data_type,
    inferred_role,
    filter(array(
        CASE WHEN has_comment      THEN 'COMMENT' END,
        CASE WHEN has_tag          THEN 'TAG' END,
        CASE WHEN has_name_pattern THEN 'NAME' END
    ), x -> x IS NOT NULL)                                 AS signals,
    CASE
        WHEN has_comment OR has_tag THEN 'EXPLICIT'
        WHEN has_name_pattern       THEN 'NAME_ONLY'
        ELSE 'NONE'
    END                                                    AS coverage_status,
    tag_keys,
    left(comment, 120)                                     AS comment_preview
FROM classified
ORDER BY
    CASE WHEN has_comment OR has_tag THEN 2 WHEN has_name_pattern THEN 1 ELSE 0 END,
    inferred_role = 'UNKNOWN' DESC,
    table_name, ordinal_position
```
