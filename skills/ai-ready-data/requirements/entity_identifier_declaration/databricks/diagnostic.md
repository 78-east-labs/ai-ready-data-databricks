# Diagnostic: entity_identifier_declaration

Lists every base table in the schema with its PRIMARY KEY (name and columns) or, when none is declared, the candidate identifier columns a PK could be built from.

## Context

One row per table. For tables with a PK, `pk_columns` is the ordered key. For tables without one, `candidate_columns` lists columns whose name looks like an identifier (`id`, `{table}_id`, `*_id`, `*_key`, `*_sk`, `*_uuid`), each annotated with its nullability, because a PK column must be NOT NULL before the constraint can be added. `candidate_status` summarises what stands in the way:

- `HAS_PK`: nothing to do.
- `CANDIDATE_READY`: exactly one candidate column and it is already NOT NULL. The bulk fix can declare it after a uniqueness count.
- `CANDIDATE_NULLABLE`: exactly one candidate but it allows NULL. Set NOT NULL first.
- `MULTIPLE_CANDIDATES`: more than one identifier-like column. A human picks the grain (it may be composite).
- `NO_CANDIDATE`: no identifier-like column. Usually an event or log table; consider a composite key over (entity id, event time) or a surrogate.

`is_feature_table` flags tables tagged `feature_table = 'true'`; those must have a PK to be usable by Feature Engineering. Sorted so tables without a PK come first, feature tables first among them.

## SQL

```sql
WITH tables_in_scope AS (
    SELECT LOWER(t.table_name) AS table_name, t.table_owner
    FROM {{ catalog }}.information_schema.tables t
    WHERE LOWER(t.table_schema) = LOWER('{{ schema }}')
      AND t.table_type IN ('MANAGED', 'EXTERNAL')
),
pk AS (
    SELECT LOWER(tc.table_name) AS table_name,
           max(tc.constraint_name) AS pk_name,
           array_join(transform(
               array_sort(collect_list(struct(k.ordinal_position, k.column_name))),
               s -> s.column_name), ', ') AS pk_columns
    FROM {{ catalog }}.information_schema.table_constraints tc
    JOIN {{ catalog }}.information_schema.key_column_usage k
      ON k.constraint_schema = tc.constraint_schema
     AND k.constraint_name   = tc.constraint_name
     AND k.table_schema      = tc.table_schema
     AND k.table_name        = tc.table_name
    WHERE LOWER(tc.table_schema) = LOWER('{{ schema }}')
      AND tc.constraint_type = 'PRIMARY KEY'
    GROUP BY LOWER(tc.table_name)
),
candidates AS (
    SELECT LOWER(c.table_name) AS table_name,
           collect_list(concat(c.column_name, CASE WHEN c.is_nullable = 'YES' THEN ' (nullable)' ELSE '' END)) AS candidate_columns,
           COUNT(*)                                     AS n_candidates,
           COUNT_IF(c.is_nullable = 'NO')               AS n_not_null
    FROM {{ catalog }}.information_schema.columns c
    JOIN tables_in_scope t ON LOWER(c.table_name) = t.table_name
    WHERE LOWER(c.table_schema) = LOWER('{{ schema }}')
      AND (   LOWER(c.column_name) IN ('id', 'pk', 'key')
           OR LOWER(c.column_name) = concat(regexp_replace(t.table_name, 's$', ''), '_id')
           OR LOWER(c.column_name) = concat(t.table_name, '_id')
           OR REGEXP_LIKE(LOWER(c.column_name), '(_id|_key|_sk|_uuid|_guid)$'))
    GROUP BY LOWER(c.table_name)
),
feature_tables AS (
    SELECT DISTINCT LOWER(table_name) AS table_name
    FROM {{ catalog }}.information_schema.table_tags
    WHERE LOWER(schema_name) = LOWER('{{ schema }}')
      AND LOWER(tag_name) = 'feature_table'
)
SELECT
    t.table_name,
    t.table_owner,
    pk.pk_name,
    pk.pk_columns,
    cd.candidate_columns,
    CASE
        WHEN pk.pk_name IS NOT NULL                          THEN 'HAS_PK'
        WHEN cd.n_candidates = 1 AND cd.n_not_null = 1       THEN 'CANDIDATE_READY'
        WHEN cd.n_candidates = 1                             THEN 'CANDIDATE_NULLABLE'
        WHEN cd.n_candidates > 1                             THEN 'MULTIPLE_CANDIDATES'
        ELSE 'NO_CANDIDATE'
    END                                                       AS candidate_status,
    ft.table_name IS NOT NULL                                 AS is_feature_table
FROM tables_in_scope t
LEFT JOIN pk             USING (table_name)
LEFT JOIN candidates cd  USING (table_name)
LEFT JOIN feature_tables ft USING (table_name)
ORDER BY
    pk.pk_name IS NOT NULL,
    ft.table_name IS NULL,
    t.table_name
```
