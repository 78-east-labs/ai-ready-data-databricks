# Diagnostic: constraint_declaration

Lists every column on base tables in the schema with its nullability, key membership, CHECK references, whether its comment states a range, and a recommendation.

## Context

One row per column. `constraint_kinds` is the set of signals present (`NOT_NULL`, `PK`, `FK`, `CHECK`, `RANGE_COMMENT`); an empty array means the column is unconstrained. `check_constraints` lists the names of CHECK constraints referencing the column so you can read their expressions with `SELECT sql FROM {{ catalog }}.information_schema.check_constraints WHERE constraint_name = ...`.

`null_hint` and `suggested_action` come from name and type only. They do not scan data. Before running any suggested `SET NOT NULL`, run the blast-radius count in fix.md; the ALTER fails when NULLs exist.

Sorted so unconstrained columns come first, then columns whose only signal is a comment, then by table and ordinal position.

## SQL

```sql
WITH columns_in_scope AS (
    SELECT LOWER(c.table_name)  AS table_name,
           c.column_name        AS column_name_cased,
           LOWER(c.column_name) AS column_name,
           c.data_type,
           c.ordinal_position,
           c.is_nullable,
           c.comment
    FROM {{ catalog }}.information_schema.columns c
    JOIN {{ catalog }}.information_schema.tables t
      ON c.table_schema = t.table_schema AND c.table_name = t.table_name
    WHERE LOWER(c.table_schema) = LOWER('{{ schema }}')
      AND t.table_type IN ('MANAGED', 'EXTERNAL')
),
key_columns AS (
    SELECT LOWER(k.table_name) AS table_name, LOWER(k.column_name) AS column_name,
           bool_or(tc.constraint_type = 'PRIMARY KEY') AS in_pk,
           bool_or(tc.constraint_type = 'FOREIGN KEY') AS in_fk
    FROM {{ catalog }}.information_schema.table_constraints tc
    JOIN {{ catalog }}.information_schema.key_column_usage k
      ON k.constraint_schema = tc.constraint_schema
     AND k.constraint_name   = tc.constraint_name
     AND k.table_schema      = tc.table_schema
     AND k.table_name        = tc.table_name
    WHERE LOWER(tc.table_schema) = LOWER('{{ schema }}')
      AND tc.constraint_type IN ('PRIMARY KEY', 'FOREIGN KEY')
    GROUP BY 1, 2
),
check_columns AS (
    SELECT LOWER(ccu.table_name) AS table_name, LOWER(ccu.column_name) AS column_name,
           array_sort(collect_set(tc.constraint_name)) AS check_constraints
    FROM {{ catalog }}.information_schema.table_constraints tc
    JOIN {{ catalog }}.information_schema.constraint_column_usage ccu
      ON ccu.constraint_schema = tc.constraint_schema
     AND ccu.constraint_name   = tc.constraint_name
     AND ccu.table_schema      = tc.table_schema
     AND ccu.table_name        = tc.table_name
    WHERE LOWER(tc.table_schema) = LOWER('{{ schema }}')
      AND tc.constraint_type = 'CHECK'
    GROUP BY 1, 2
),
classified AS (
    SELECT c.*,
           kc.in_pk, kc.in_fk, ck.check_constraints,
           c.comment IS NOT NULL AND REGEXP_LIKE(
               LOWER(c.comment),
               '(range|\\bmin\\b|\\bmax\\b|between|allowed|one of|enum|[0-9]+ *(to|-) *[0-9]+)') AS has_range_comment
    FROM columns_in_scope c
    LEFT JOIN key_columns   kc USING (table_name, column_name)
    LEFT JOIN check_columns ck USING (table_name, column_name)
)
SELECT
    table_name,
    column_name_cased                                   AS column_name,
    data_type,
    is_nullable,
    filter(array(
        CASE WHEN is_nullable = 'NO'                THEN 'NOT_NULL' END,
        CASE WHEN in_pk                              THEN 'PK' END,
        CASE WHEN in_fk                              THEN 'FK' END,
        CASE WHEN check_constraints IS NOT NULL      THEN 'CHECK' END,
        CASE WHEN has_range_comment                  THEN 'RANGE_COMMENT' END
    ), x -> x IS NOT NULL)                              AS constraint_kinds,
    check_constraints,
    left(comment, 120)                                  AS comment_preview,
    CASE
        WHEN REGEXP_LIKE(column_name, '(^id$|_id$|_key$|_sk$)') AND is_nullable = 'YES'
            THEN 'Identifier-like and nullable: SET NOT NULL, then declare PK or FK'
        WHEN REGEXP_LIKE(column_name, '(created|inserted|loaded)_(at|ts|time|date)$') AND is_nullable = 'YES'
            THEN 'Load timestamp: usually safe to SET NOT NULL'
        WHEN REGEXP_LIKE(column_name, '(status|type|category|code|tier|state)$') AND check_constraints IS NULL
            THEN 'Categorical: add CHECK (col IN (...)) from distinct values'
        WHEN REGEXP_LIKE(column_name, '(pct|percent|ratio|rate|score)') AND check_constraints IS NULL
            THEN 'Bounded measure: add CHECK (col BETWEEN lo AND hi)'
        WHEN data_type IN ('BOOLEAN') AND is_nullable = 'YES'
            THEN 'Boolean: SET NOT NULL with an explicit default'
        ELSE NULL
    END                                                 AS suggested_action
FROM classified
ORDER BY
    CASE WHEN is_nullable = 'YES' AND NOT COALESCE(in_pk, false) AND NOT COALESCE(in_fk, false)
              AND check_constraints IS NULL AND NOT has_range_comment THEN 0
         WHEN is_nullable = 'YES' AND NOT COALESCE(in_pk, false) AND NOT COALESCE(in_fk, false)
              AND check_constraints IS NULL THEN 1
         ELSE 2 END,
    table_name, ordinal_position
```
