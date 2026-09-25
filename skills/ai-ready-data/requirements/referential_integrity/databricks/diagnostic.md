# Diagnostic: referential_integrity

Lists the declared foreign keys in the schema with their parent tables, then the most frequent orphan values for one FK so you can tell a systemic gap (one missing parent row hit by thousands of children) from scattered bad keys.

## Context

Three queries:

1. **Declared FKs across the schema.** One row per constraint with child and parent columns and whether the constraint carries `RELY`. Use it to build the list of pairs to test and to spot tables whose `_id` columns have no declared relationship at all.
2. **Orphan values for one FK.** Distinct orphan values with their row counts, worst-first. A single value with a huge count usually means a deleted or late-arriving parent; many values with count 1 usually means a key-format mismatch (see `referential_accuracy`).
3. **Orphan rows for one FK.** Up to 100 raw rows for inspection. `{{ key_columns }}` is the child table's own identifier (default `*`).

Probe for `position_in_unique_constraint` if you have not used it before on this workspace:

```sql
SELECT position_in_unique_constraint FROM {{ catalog }}.information_schema.key_column_usage LIMIT 1
```

## SQL

### Declared foreign keys in the schema

```sql
WITH fk AS (
    SELECT
        tc.constraint_name,
        LOWER(tc.table_name)            AS child_table,
        tc.enforced,
        rc.unique_constraint_catalog,
        rc.unique_constraint_schema,
        rc.unique_constraint_name
    FROM {{ catalog }}.information_schema.table_constraints tc
    JOIN {{ catalog }}.information_schema.referential_constraints rc
      ON rc.constraint_name = tc.constraint_name
    WHERE LOWER(tc.table_schema) = LOWER('{{ schema }}')
      AND tc.constraint_type = 'FOREIGN KEY'
),
child_cols AS (
    SELECT k.constraint_name,
           array_join(transform(array_sort(collect_list(struct(k.ordinal_position, k.column_name))),
                                s -> s.column_name), ', ') AS child_columns
    FROM {{ catalog }}.information_schema.key_column_usage k
    JOIN fk ON fk.constraint_name = k.constraint_name
    WHERE LOWER(k.table_schema) = LOWER('{{ schema }}')
      AND LOWER(k.table_name)   = fk.child_table
    GROUP BY k.constraint_name
),
parent_cols AS (
    SELECT fk.constraint_name,
           MAX(k.table_name) AS parent_table,
           array_join(transform(array_sort(collect_list(struct(k.ordinal_position, k.column_name))),
                                s -> s.column_name), ', ') AS parent_columns
    FROM fk
    JOIN system.information_schema.key_column_usage k
      ON  k.constraint_name = fk.unique_constraint_name
      AND LOWER(k.table_schema)  = LOWER(fk.unique_constraint_schema)
      AND LOWER(k.table_catalog) = LOWER(fk.unique_constraint_catalog)
    GROUP BY fk.constraint_name
)
SELECT
    fk.child_table,
    c.child_columns,
    concat(fk.unique_constraint_catalog, '.', fk.unique_constraint_schema, '.', p.parent_table) AS parent_table,
    p.parent_columns,
    fk.constraint_name,
    fk.enforced
FROM fk
LEFT JOIN child_cols  c USING (constraint_name)
LEFT JOIN parent_cols p USING (constraint_name)
ORDER BY fk.child_table, fk.constraint_name
```

`enforced` is always `NO` for Unity Catalog foreign keys; it is included so the report states that plainly.

### Orphan values for one foreign key

```sql
SELECT
    s.{{ fk_column }}                       AS orphan_value,
    COUNT(*)                                AS child_rows,
    COUNT(*)::DOUBLE
        / SUM(COUNT(*)) OVER ()             AS share_of_orphans
FROM {{ catalog }}.{{ schema }}.{{ asset }} s
LEFT JOIN {{ ref_table }} r
  ON s.{{ fk_column }} = r.{{ ref_key }}
WHERE s.{{ fk_column }} IS NOT NULL
  AND r.{{ ref_key }} IS NULL
GROUP BY s.{{ fk_column }}
ORDER BY child_rows DESC
LIMIT 100
```

### Orphan rows for one foreign key

```sql
SELECT
    s.{{ key_columns }},
    s.{{ fk_column }}                       AS fk_value,
    'ORPHAN'                                AS integrity_status,
    '{{ ref_table }}.{{ ref_key }}'         AS missing_in
FROM {{ catalog }}.{{ schema }}.{{ asset }} s
WHERE s.{{ fk_column }} IS NOT NULL
  AND NOT EXISTS (
      SELECT 1 FROM {{ ref_table }} r WHERE r.{{ ref_key }} = s.{{ fk_column }}
  )
ORDER BY s.{{ fk_column }}
LIMIT 100
```

If the parent is a Delta table with history, `SELECT ... FROM {{ ref_table }} VERSION AS OF <n>` (or `TIMESTAMP AS OF`) in the anti-join tells you whether the parent rows used to exist, which separates "deleted parent" from "never loaded".
