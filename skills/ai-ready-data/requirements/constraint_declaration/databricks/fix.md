# Fix: constraint_declaration

Declare NOT NULL, CHECK, PRIMARY KEY or FOREIGN KEY constraints on unconstrained columns, or document a range in the column comment.

## Context

Four options, from strongest to weakest:

- **NOT NULL** (enforced). `ALTER TABLE ... ALTER COLUMN ... SET NOT NULL` scans the table once and fails if any NULL exists. Run the blast-radius count first and back-fill or agree on a default before applying. Idempotent: re-running on a NOT NULL column is a no-op. The guard is `information_schema.columns.is_nullable = 'NO'`.
- **CHECK** (enforced). `ALTER TABLE ... ADD CONSTRAINT name CHECK (expr)` validates existing rows and fails if any violate, then rejects future violating writes. Requires Delta writer version 3 or higher (every current table). Not idempotent: a second `ADD CONSTRAINT` with the same name fails, so run the guard. Constraint names are unique per table for CHECK; pick names that include the column (`chk_orders_status`).
- **PRIMARY KEY / FOREIGN KEY** (informational). Not enforced; they document grain and join paths and, with `RELY`, let the optimizer eliminate joins. Only add `RELY` when you have verified uniqueness or referential integrity, because the optimizer will trust it. Covered in detail by `entity_identifier_declaration` and `relationship_declaration`.
- **Range in the comment** (documentation only). Cheapest and weakest. Prefer it only when the range is advisory and cannot be enforced (for example a column that is legitimately NULL for one row class).

All options need ownership of the table or `MODIFY` plus the relevant privilege. None rewrites data files; NOT NULL and CHECK read the table once to validate.

## Fix: SET NOT NULL on one column

Blast radius (must return 0 before the ALTER):

```sql
SELECT COUNT(*) AS null_rows
FROM {{ catalog }}.{{ schema }}.{{ asset }}
WHERE {{ column }} IS NULL
```

If it returns rows and a default is agreed, back-fill first (show the count and get confirmation; this is a data mutation):

```sql
UPDATE {{ catalog }}.{{ schema }}.{{ asset }}
SET {{ column }} = {{ default_value }}
WHERE {{ column }} IS NULL
```

Then:

```sql
ALTER TABLE {{ catalog }}.{{ schema }}.{{ asset }}
ALTER COLUMN {{ column }} SET NOT NULL
```

## Fix: Add a CHECK constraint on one column

Blast radius (must return 0; `{{ check_expr }}` is the constraint body, for example `status IN ('open','closed')` or `pct BETWEEN 0 AND 100`):

```sql
SELECT COUNT(*) AS violating_rows
FROM {{ catalog }}.{{ schema }}.{{ asset }}
WHERE NOT ({{ check_expr }})
```

Guard (skip the ALTER if a row comes back):

```sql
SELECT 1
FROM {{ catalog }}.information_schema.table_constraints
WHERE LOWER(table_schema)    = LOWER('{{ schema }}')
  AND LOWER(table_name)      = LOWER('{{ asset }}')
  AND LOWER(constraint_name) = LOWER('{{ constraint_name }}')
```

Apply:

```sql
ALTER TABLE {{ catalog }}.{{ schema }}.{{ asset }}
ADD CONSTRAINT {{ constraint_name }} CHECK ({{ check_expr }})
```

Note that a CHECK does not reject NULL (the expression evaluates to unknown, which passes). Combine with NOT NULL when both matter.

## Fix: Bulk NOT NULL for columns that have no NULLs today

Emits one `SET NOT NULL` per nullable, unconstrained column, but only after you have confirmed the column has no NULLs. Step 1 generates the counting queries; step 2 generates the ALTERs for the columns you keep.

Step 1, generate the null counts (run the output as one statement):

```sql
SELECT concat_ws(' UNION ALL ',
    collect_list(concat(
        'SELECT ''', table_name, ''' AS table_name, ''', column_name, ''' AS column_name, ',
        'COUNT_IF(`', column_name, '` IS NULL) AS null_rows, COUNT(*) AS total_rows ',
        'FROM {{ catalog }}.{{ schema }}.`', table_name, '`'
    ))) AS stmt
FROM (
    SELECT c.table_name, c.column_name
    FROM {{ catalog }}.information_schema.columns c
    JOIN {{ catalog }}.information_schema.tables t
      ON c.table_schema = t.table_schema AND c.table_name = t.table_name
    WHERE LOWER(c.table_schema) = LOWER('{{ schema }}')
      AND t.table_type IN ('MANAGED', 'EXTERNAL')
      AND c.is_nullable = 'YES'
      AND REGEXP_LIKE(LOWER(c.column_name), '(^id$|_id$|_key$|_sk$|(created|inserted|loaded)_(at|ts|time|date)$)')
)
```

The filter limits step 1 to identifier and load-timestamp columns, which are the ones that are almost always non-null by construction. Widen it if you want. This scans every listed table once; add `TABLESAMPLE ({{ sample_rows }} ROWS)` after the table name for a first pass on very large tables (default `sample_rows` 1,000,000), then confirm with a full count before altering.

Step 2, for each row of the step 1 result with `null_rows = 0`, emit:

```sql
SELECT concat(
    'ALTER TABLE {{ catalog }}.{{ schema }}.`', c.table_name,
    '` ALTER COLUMN `', c.column_name, '` SET NOT NULL;'
) AS stmt
FROM {{ catalog }}.information_schema.columns c
JOIN {{ catalog }}.information_schema.tables t
  ON c.table_schema = t.table_schema AND c.table_name = t.table_name
WHERE LOWER(c.table_schema) = LOWER('{{ schema }}')
  AND t.table_type IN ('MANAGED', 'EXTERNAL')
  AND c.is_nullable = 'YES'
  AND (LOWER(c.table_name), LOWER(c.column_name)) IN ( {{ verified_non_null_pairs }} )
ORDER BY c.table_name, c.ordinal_position
```

`{{ verified_non_null_pairs }}` is the list from step 1, written as `('orders','order_id'), ('orders','created_at')`. Show the statements before executing. Each ALTER re-validates, so a NULL written between step 1 and step 2 fails loudly rather than silently.

## Fix: Bulk CHECK for low-cardinality categorical columns

For string columns whose name ends in `status`, `type`, `category`, `code`, `tier` or `state` and that have no CHECK yet, generate a `CHECK (col IN (...))` from the current distinct values. Step 1 emits the distinct-value queries; run them, review the value lists (a typo in the data becomes a permanent allowed value if you do not), then fill in step 2.

```sql
SELECT concat(
    'SELECT ''', c.table_name, ''' AS table_name, ''', c.column_name, ''' AS column_name, ',
    'array_sort(collect_set(`', c.column_name, '`)) AS values, COUNT(DISTINCT `', c.column_name, '`) AS n ',
    'FROM {{ catalog }}.{{ schema }}.`', c.table_name, '`;'
) AS stmt
FROM {{ catalog }}.information_schema.columns c
JOIN {{ catalog }}.information_schema.tables t
  ON c.table_schema = t.table_schema AND c.table_name = t.table_name
LEFT JOIN (
    SELECT DISTINCT LOWER(ccu.table_name) AS table_name, LOWER(ccu.column_name) AS column_name
    FROM {{ catalog }}.information_schema.table_constraints tc
    JOIN {{ catalog }}.information_schema.constraint_column_usage ccu
      ON ccu.constraint_schema = tc.constraint_schema AND ccu.constraint_name = tc.constraint_name
     AND ccu.table_schema = tc.table_schema AND ccu.table_name = tc.table_name
    WHERE LOWER(tc.table_schema) = LOWER('{{ schema }}') AND tc.constraint_type = 'CHECK'
) ck ON ck.table_name = LOWER(c.table_name) AND ck.column_name = LOWER(c.column_name)
WHERE LOWER(c.table_schema) = LOWER('{{ schema }}')
  AND t.table_type IN ('MANAGED', 'EXTERNAL')
  AND c.data_type = 'STRING'
  AND REGEXP_LIKE(LOWER(c.column_name), '(status|type|category|code|tier|state)$')
  AND ck.column_name IS NULL
ORDER BY c.table_name, c.ordinal_position
```

Step 2, per reviewed column:

```sql
ALTER TABLE {{ catalog }}.{{ schema }}.{{ asset }}
ADD CONSTRAINT chk_{{ asset }}_{{ column }} CHECK ({{ column }} IN ({{ allowed_values }}))
```

Only generate a CHECK where `n` is small (under about 50) and the list is stable. A column with 400 distinct product codes wants a FOREIGN KEY to a reference table, not a CHECK.

## Fix: Document a range in the comment

When a bound is advisory and cannot be enforced, state it in the comment so the check and downstream consumers can read it:

```sql
COMMENT ON COLUMN {{ catalog }}.{{ schema }}.{{ asset }}.{{ column }}
IS '{{ existing_comment }} Valid range: {{ min_value }} to {{ max_value }}.'
```

`COMMENT ON COLUMN` replaces the whole comment, so include the existing text (guard: read `information_schema.columns.comment` first and prompt before overwriting). On runtimes where `COMMENT ON COLUMN` is unavailable use `ALTER TABLE t ALTER COLUMN c COMMENT '...'`.

## Organizational guidance

Constraints belong in the table definition, not in a back-fill. Put `NOT NULL` and `CHECK` clauses into the `CREATE TABLE` in dbt models (`contracts: enforced: true` with `constraints:` in the model YAML renders them on Databricks), Lakeflow declarative pipelines (`CONSTRAINT ... EXPECT ... ON VIOLATION FAIL UPDATE` for streaming tables, which is the pipeline equivalent of CHECK), or Terraform `databricks_sql_table` column specs. Treat a nullable identifier column in a gold table as a review comment, not a style preference.
