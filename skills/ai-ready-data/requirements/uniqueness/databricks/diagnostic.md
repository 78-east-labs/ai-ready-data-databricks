# Diagnostic: uniqueness

Lists the key combinations with the most duplicates, plus how many of them are null-key rows, and shows which tables in the schema declare a primary key at all.

## Context

Two queries. The first is per table and returns the top duplicate groups on `{{ key_columns }}` with the count, whether the key is NULL, and the range of `{{ tiebreaker_column }}` (default: no tiebreaker, omit the two `MIN`/`MAX` lines) so you can decide whether "keep latest" is a safe dedup rule. The second is per schema and pairs every base table with its declared primary key columns so you can see which tables can use the discovery variant of the check and which need a hand-supplied key.

Primary keys are informational in Unity Catalog; a declared key with duplicates underneath it is exactly the case this diagnostic is for.

Sorted worst-first: largest duplicate groups, then tables without any key.

## SQL

### Duplicate groups for one table

```sql
WITH groups AS (
    SELECT
        {{ key_columns }},
        COUNT(*)                                  AS rows_in_group,
        MIN({{ tiebreaker_column }})              AS earliest_tiebreaker,
        MAX({{ tiebreaker_column }})              AS latest_tiebreaker,
        COUNT(DISTINCT {{ tiebreaker_column }})   AS distinct_tiebreakers
    FROM {{ catalog }}.{{ schema }}.{{ asset }}
    GROUP BY {{ key_columns }}
    HAVING COUNT(*) > 1
)
SELECT
    {{ key_columns }},
    rows_in_group,
    rows_in_group - 1                          AS surplus_rows,
    earliest_tiebreaker,
    latest_tiebreaker,
    distinct_tiebreakers = rows_in_group       AS tiebreaker_resolves_group,
    to_json(struct({{ key_columns }})) = '{}'  AS key_is_null
FROM groups
ORDER BY rows_in_group DESC
LIMIT 100
```

`key_is_null` relies on `to_json` dropping NULL fields (its default), so an all-NULL key serializes to `{}`. It works for any mix of key column types.

`tiebreaker_resolves_group = FALSE` means at least two rows in the group share the same tiebreaker value; a "keep first / keep last" DELETE keyed on `(key, tiebreaker)` would remove both or neither. Use the whole-row dedup option in the fix for those.

### Declared primary keys across the schema

```sql
WITH tables_in_scope AS (
    SELECT LOWER(table_name) AS table_name, table_owner
    FROM {{ catalog }}.information_schema.tables
    WHERE LOWER(table_schema) = LOWER('{{ schema }}')
      AND table_type IN ('MANAGED', 'EXTERNAL')
),
pk_cols AS (
    SELECT
        LOWER(tc.table_name) AS table_name,
        tc.constraint_name,
        array_join(
            transform(
                array_sort(collect_list(struct(k.ordinal_position, k.column_name))),
                s -> s.column_name
            ), ', ') AS pk_columns
    FROM {{ catalog }}.information_schema.table_constraints tc
    JOIN {{ catalog }}.information_schema.key_column_usage k
      ON k.constraint_name = tc.constraint_name
     AND k.table_schema = tc.table_schema
     AND k.table_name   = tc.table_name
    WHERE LOWER(tc.table_schema) = LOWER('{{ schema }}')
      AND tc.constraint_type = 'PRIMARY KEY'
    GROUP BY LOWER(tc.table_name), tc.constraint_name
),
id_like AS (
    SELECT LOWER(table_name) AS table_name,
           array_sort(collect_set(column_name)) AS id_like_columns
    FROM {{ catalog }}.information_schema.columns
    WHERE LOWER(table_schema) = LOWER('{{ schema }}')
      AND REGEXP_LIKE(LOWER(column_name), '(^id$|_id$|_key$|_uuid$|_pk$)')
    GROUP BY LOWER(table_name)
)
SELECT
    t.table_name,
    t.table_owner,
    p.constraint_name                     AS pk_constraint,
    p.pk_columns,
    i.id_like_columns                     AS candidate_key_columns,
    p.pk_columns IS NOT NULL              AS has_primary_key
FROM tables_in_scope t
LEFT JOIN pk_cols p USING (table_name)
LEFT JOIN id_like i USING (table_name)
ORDER BY has_primary_key ASC, t.table_name
```

`candidate_key_columns` is a name-pattern hint for tables with no declared key. Confirm with the table owner before using it as `{{ key_columns }}`.
