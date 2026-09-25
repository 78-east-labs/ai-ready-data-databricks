# Check: referential_integrity

Fraction of non-null foreign-key values in a table that resolve to a row in the referenced table.

## Context

Anti-joins the child table to the parent along the foreign key and counts non-null child keys with no match (orphans). `value = 1 - orphan_rows / fk_rows`.

Strength is **native + data**. Unity Catalog records `FOREIGN KEY` constraints in `information_schema.table_constraints`, `referential_constraints` and `key_column_usage`, but does not enforce them (they are informational; `RELY` only affects the optimizer). So the declared constraints tell you which pairs to test, and only the data scan tells you whether they hold. Tables with no declared FK can still be tested with the manual variant; `relationship_declaration` is the requirement that scores the missing declaration.

Placeholders for the manual variant (none of these are in the manifest; they are needed when no FK is declared):

- `{{ fk_column }}`: the child column. For a composite key, pass a comma-separated list and extend the join by hand.
- `{{ ref_table }}`: fully qualified parent table, for example `prod.dim.customers`. Default: `{{ catalog }}.{{ schema }}.<parent>` from the declared constraint.
- `{{ ref_key }}`: the parent key column.

NULL child keys are excluded from numerator and denominator (a NULL FK is a completeness question, measured by `data_completeness`). For composite keys a row is in scope only when every FK column is non-null.

The discovery variant emits one statement per declared foreign key on the table; the orchestrator runs each and averages, or you can wrap them in `UNION ALL` and aggregate. It returns no rows when the table declares no FK. `key_column_usage.position_in_unique_constraint` links each child column to the parent column at that ordinal in the referenced primary or unique key; if the column comes back NULL on your workspace (run the one-line probe in the diagnostic), match by `ordinal_position` instead, which is correct whenever the FK lists its columns in the same order as the parent key.

Parents in other catalogs are covered by `referential_constraints.unique_constraint_catalog`; the discovery SQL reads `{{ catalog }}.information_schema`, which only sees constraints whose child table is in this catalog, and builds the parent's three-part name from the constraint's catalog and schema.

Returns NULL when the table has no non-null FK values (or, for discovery, no declared FK).

## SQL

### Manual foreign key (primary)

```sql
WITH fk_check AS (
    SELECT
        COUNT(*)                                AS fk_rows,
        COUNT_IF(r.{{ ref_key }} IS NULL)       AS orphan_rows
    FROM {{ catalog }}.{{ schema }}.{{ asset }} s
    LEFT JOIN {{ ref_table }} r
      ON s.{{ fk_column }} = r.{{ ref_key }}
    WHERE s.{{ fk_column }} IS NOT NULL
)
SELECT
    '{{ asset }}'                               AS table_name,
    '{{ fk_column }}'                           AS fk_column,
    '{{ ref_table }}.{{ ref_key }}'             AS references,
    orphan_rows,
    fk_rows,
    1.0 - orphan_rows::DOUBLE / NULLIF(fk_rows, 0) AS value
FROM fk_check
```

If `{{ ref_key }}` is nullable in the parent, the `LEFT JOIN ... IS NULL` test still works because a NULL parent key never matches an equality join.

### Discover declared foreign keys (variant)

Emits one ready-to-run statement per FOREIGN KEY constraint on the table, including composite keys.

```sql
WITH fk AS (
    SELECT
        tc.constraint_name,
        tc.table_name                    AS child_table,
        rc.unique_constraint_catalog,
        rc.unique_constraint_schema,
        rc.unique_constraint_name
    FROM {{ catalog }}.information_schema.table_constraints tc
    JOIN {{ catalog }}.information_schema.referential_constraints rc
      ON rc.constraint_name = tc.constraint_name
    WHERE LOWER(tc.table_schema) = LOWER('{{ schema }}')
      AND LOWER(tc.table_name)   = LOWER('{{ asset }}')
      AND tc.constraint_type = 'FOREIGN KEY'
),
child_cols AS (
    SELECT k.constraint_name, k.column_name, k.ordinal_position, k.position_in_unique_constraint
    FROM {{ catalog }}.information_schema.key_column_usage k
    JOIN fk ON fk.constraint_name = k.constraint_name
    WHERE LOWER(k.table_schema) = LOWER('{{ schema }}')
      AND LOWER(k.table_name)   = LOWER(fk.child_table)
),
parent_cols AS (
    SELECT fk.constraint_name, k.table_name AS parent_table, k.column_name, k.ordinal_position
    FROM fk
    JOIN system.information_schema.key_column_usage k
      ON  k.constraint_name = fk.unique_constraint_name
      AND LOWER(k.table_schema) = LOWER(fk.unique_constraint_schema)
      AND LOWER(k.table_catalog) = LOWER(fk.unique_constraint_catalog)
),
pairs AS (
    SELECT
        c.constraint_name,
        p.parent_table,
        c.ordinal_position,
        c.column_name AS child_col,
        p.column_name AS parent_col
    FROM child_cols c
    JOIN parent_cols p
      ON p.constraint_name = c.constraint_name
     AND p.ordinal_position = COALESCE(c.position_in_unique_constraint, c.ordinal_position)
),
built AS (
    SELECT
        fk.constraint_name,
        concat(fk.unique_constraint_catalog, '.', fk.unique_constraint_schema, '.`', MAX(p.parent_table), '`') AS parent_fqn,
        array_join(transform(array_sort(collect_list(struct(p.ordinal_position, p.child_col, p.parent_col))),
                   s -> concat('s.`', s.child_col, '` = r.`', s.parent_col, '`')), ' AND ')      AS join_pred,
        array_join(transform(array_sort(collect_list(struct(p.ordinal_position, p.child_col, p.parent_col))),
                   s -> concat('s.`', s.child_col, '` IS NOT NULL')), ' AND ')                  AS notnull_pred,
        array_join(transform(array_sort(collect_list(struct(p.ordinal_position, p.child_col, p.parent_col))),
                   s -> s.child_col), ', ')                                                     AS fk_columns,
        MIN(p.parent_col)                                                                       AS any_parent_col
    FROM fk
    JOIN pairs p ON p.constraint_name = fk.constraint_name
    GROUP BY fk.constraint_name, fk.unique_constraint_catalog, fk.unique_constraint_schema
)
SELECT
    constraint_name,
    fk_columns,
    parent_fqn,
    concat(
        'WITH fk_check AS (SELECT COUNT(*) AS fk_rows, COUNT_IF(r.`', any_parent_col, '` IS NULL) AS orphan_rows ',
        'FROM {{ catalog }}.{{ schema }}.`{{ asset }}` s LEFT JOIN ', parent_fqn, ' r ON ', join_pred,
        ' WHERE ', notnull_pred, ') ',
        'SELECT ''{{ asset }}'' AS table_name, ''', fk_columns, ''' AS fk_column, ''', parent_fqn, ''' AS references, ',
        'orphan_rows, fk_rows, 1.0 - orphan_rows::DOUBLE / NULLIF(fk_rows, 0) AS value FROM fk_check'
    ) AS stmt
FROM built
ORDER BY constraint_name
```

`parent_cols` reads `system.information_schema.key_column_usage` because the parent may live in another catalog. If your account has not enabled the `system` catalog for the assessment principal, replace it with `{{ catalog }}.information_schema.key_column_usage` and accept that cross-catalog parents are skipped.

### Sampled (variant)

For very large child tables. Samples the child only; the parent must be read in full or orphans are over-counted.

```sql
WITH fk_check AS (
    SELECT
        COUNT(*)                                AS fk_rows,
        COUNT_IF(r.{{ ref_key }} IS NULL)       AS orphan_rows
    FROM {{ catalog }}.{{ schema }}.{{ asset }} TABLESAMPLE ({{ sample_rows }} ROWS) s
    LEFT JOIN {{ ref_table }} r
      ON s.{{ fk_column }} = r.{{ ref_key }}
    WHERE s.{{ fk_column }} IS NOT NULL
)
SELECT
    '{{ asset }}'                               AS table_name,
    '{{ fk_column }}'                           AS fk_column,
    orphan_rows,
    fk_rows,
    1.0 - orphan_rows::DOUBLE / NULLIF(fk_rows, 0) AS value
FROM fk_check
```

`{{ sample_rows }}` defaults to 1,000,000. `TABLESAMPLE (n ROWS)` is a prefix sample, not random; it is fine for triage.
