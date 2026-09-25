# Check: constraint_declaration

Fraction of columns on base tables in the schema that carry at least one declared constraint: NOT NULL, membership in a PRIMARY KEY or FOREIGN KEY, reference from a CHECK constraint, or a comment that states a valid range.

## Context

This is a **native** check. Unity Catalog exposes all four signals through `information_schema`, and each is read directly:

1. `columns.is_nullable = 'NO'`. NOT NULL is enforced by Delta at write time. PRIMARY KEY columns must be NOT NULL, so they are captured here too.
2. Column appears in `key_column_usage` for a constraint of type `PRIMARY KEY` or `FOREIGN KEY` (joined through `table_constraints`). These are **informational** on Databricks: they are not enforced, but they state intent, drive join elimination when `RELY` is set, and are what Genie, the Assistant and Text-to-SQL tools read to plan joins. They count.
3. Column is referenced by a `CHECK` constraint. CHECK constraints are enforced. The primary variant reads `constraint_column_usage`, which lists the columns each constraint references, so a multi-column CHECK counts for every column it touches. Some workspace versions populate `constraint_column_usage` only for key constraints; run the probe below once. If it returns zero rows while `check_constraints` has rows, use the text-match variant instead.
4. Comment matches a range pattern: the words `range`, `min`, `max`, `between`, `allowed`, `one of`, `enum`, or an explicit `N-M` / `N to M` bound. This is the weakest signal, a documented range that nothing enforces, and is included so that a well-commented column is not scored the same as an undocumented one.

Probe for CHECK coverage in `constraint_column_usage`:

```sql
SELECT COUNT(*) AS check_columns
FROM {{ catalog }}.information_schema.constraint_column_usage ccu
JOIN {{ catalog }}.information_schema.table_constraints tc
  ON ccu.constraint_schema = tc.constraint_schema AND ccu.constraint_name = tc.constraint_name
 AND ccu.table_schema = tc.table_schema AND ccu.table_name = tc.table_name
WHERE LOWER(tc.table_schema) = LOWER('{{ schema }}') AND tc.constraint_type = 'CHECK'
```

Physical type width (`DECIMAL(18,2)`, `VARCHAR(50)`) is deliberately not counted. Delta stores `STRING` without a length in almost every table, so width would either be trivially present or trivially absent and would say nothing about intent.

`information_schema` reflects constraint changes immediately. Rows are filtered to objects the caller can see.

Returns NULL (N/A) when the schema contains no columns on base tables.

## SQL

### Constraint coverage (primary)

```sql
WITH columns_in_scope AS (
    SELECT LOWER(c.table_name)  AS table_name,
           LOWER(c.column_name) AS column_name,
           c.is_nullable,
           c.comment
    FROM {{ catalog }}.information_schema.columns c
    JOIN {{ catalog }}.information_schema.tables t
      ON c.table_schema = t.table_schema AND c.table_name = t.table_name
    WHERE LOWER(c.table_schema) = LOWER('{{ schema }}')
      AND t.table_type IN ('MANAGED', 'EXTERNAL')
),
key_columns AS (
    SELECT DISTINCT LOWER(k.table_name) AS table_name, LOWER(k.column_name) AS column_name
    FROM {{ catalog }}.information_schema.table_constraints tc
    JOIN {{ catalog }}.information_schema.key_column_usage k
      ON k.constraint_schema = tc.constraint_schema
     AND k.constraint_name   = tc.constraint_name
     AND k.table_schema      = tc.table_schema
     AND k.table_name        = tc.table_name
    WHERE LOWER(tc.table_schema) = LOWER('{{ schema }}')
      AND tc.constraint_type IN ('PRIMARY KEY', 'FOREIGN KEY')
),
check_columns AS (
    SELECT DISTINCT LOWER(ccu.table_name) AS table_name, LOWER(ccu.column_name) AS column_name
    FROM {{ catalog }}.information_schema.table_constraints tc
    JOIN {{ catalog }}.information_schema.constraint_column_usage ccu
      ON ccu.constraint_schema = tc.constraint_schema
     AND ccu.constraint_name   = tc.constraint_name
     AND ccu.table_schema      = tc.table_schema
     AND ccu.table_name        = tc.table_name
    WHERE LOWER(tc.table_schema) = LOWER('{{ schema }}')
      AND tc.constraint_type = 'CHECK'
),
classified AS (
    SELECT c.table_name, c.column_name,
           (   c.is_nullable = 'NO'
            OR kc.column_name IS NOT NULL
            OR ck.column_name IS NOT NULL
            OR (c.comment IS NOT NULL AND REGEXP_LIKE(
                    LOWER(c.comment),
                    '(range|\\bmin\\b|\\bmax\\b|between|allowed|one of|enum|[0-9]+ *(to|-) *[0-9]+)'))
           ) AS is_constrained
    FROM columns_in_scope c
    LEFT JOIN key_columns   kc USING (table_name, column_name)
    LEFT JOIN check_columns ck USING (table_name, column_name)
)
SELECT
    COUNT_IF(is_constrained)                              AS constrained_columns,
    COUNT(*)                                              AS total_columns,
    COUNT_IF(is_constrained)::DOUBLE / NULLIF(COUNT(*), 0) AS value
FROM classified
```

### CHECK detection by expression text (variant)

Same score, but discovers CHECK-referenced columns by matching the column name as a whole word inside `check_constraints.sql`. Use when the probe above shows `constraint_column_usage` does not list CHECK columns. Caveat: `check_constraints` has no table column, so the join back to `table_constraints` is by constraint name within the schema; if two tables use the same CHECK constraint name, both tables receive both expressions and a column can be over-counted.

```sql
WITH columns_in_scope AS (
    SELECT LOWER(c.table_name)  AS table_name,
           LOWER(c.column_name) AS column_name,
           c.is_nullable,
           c.comment
    FROM {{ catalog }}.information_schema.columns c
    JOIN {{ catalog }}.information_schema.tables t
      ON c.table_schema = t.table_schema AND c.table_name = t.table_name
    WHERE LOWER(c.table_schema) = LOWER('{{ schema }}')
      AND t.table_type IN ('MANAGED', 'EXTERNAL')
),
key_columns AS (
    SELECT DISTINCT LOWER(k.table_name) AS table_name, LOWER(k.column_name) AS column_name
    FROM {{ catalog }}.information_schema.table_constraints tc
    JOIN {{ catalog }}.information_schema.key_column_usage k
      ON k.constraint_schema = tc.constraint_schema
     AND k.constraint_name   = tc.constraint_name
     AND k.table_schema      = tc.table_schema
     AND k.table_name        = tc.table_name
    WHERE LOWER(tc.table_schema) = LOWER('{{ schema }}')
      AND tc.constraint_type IN ('PRIMARY KEY', 'FOREIGN KEY')
),
check_exprs AS (
    SELECT LOWER(tc.table_name) AS table_name, LOWER(cc.sql) AS expr
    FROM {{ catalog }}.information_schema.table_constraints tc
    JOIN {{ catalog }}.information_schema.check_constraints cc
      ON cc.constraint_schema = tc.constraint_schema
     AND cc.constraint_name   = tc.constraint_name
    WHERE LOWER(tc.table_schema) = LOWER('{{ schema }}')
      AND tc.constraint_type = 'CHECK'
),
check_columns AS (
    SELECT DISTINCT c.table_name, c.column_name
    FROM columns_in_scope c
    JOIN check_exprs e
      ON e.table_name = c.table_name
     AND e.expr RLIKE concat('(^|[^a-z0-9_])`?', c.column_name, '`?([^a-z0-9_]|$)')
),
classified AS (
    SELECT c.table_name, c.column_name,
           (   c.is_nullable = 'NO'
            OR kc.column_name IS NOT NULL
            OR ck.column_name IS NOT NULL
            OR (c.comment IS NOT NULL AND REGEXP_LIKE(
                    LOWER(c.comment),
                    '(range|\\bmin\\b|\\bmax\\b|between|allowed|one of|enum|[0-9]+ *(to|-) *[0-9]+)'))
           ) AS is_constrained
    FROM columns_in_scope c
    LEFT JOIN key_columns   kc USING (table_name, column_name)
    LEFT JOIN check_columns ck USING (table_name, column_name)
)
SELECT
    COUNT_IF(is_constrained)                              AS constrained_columns,
    COUNT(*)                                              AS total_columns,
    COUNT_IF(is_constrained)::DOUBLE / NULLIF(COUNT(*), 0) AS value
FROM classified
```

### Enforced constraints only (variant)

Stricter: a column passes only when Delta will reject a bad write, that is NOT NULL or CHECK. Use this when the question is "does the platform guarantee anything about this column" rather than "did someone declare intent".

```sql
WITH columns_in_scope AS (
    SELECT LOWER(c.table_name)  AS table_name,
           LOWER(c.column_name) AS column_name,
           c.is_nullable
    FROM {{ catalog }}.information_schema.columns c
    JOIN {{ catalog }}.information_schema.tables t
      ON c.table_schema = t.table_schema AND c.table_name = t.table_name
    WHERE LOWER(c.table_schema) = LOWER('{{ schema }}')
      AND t.table_type IN ('MANAGED', 'EXTERNAL')
),
check_columns AS (
    SELECT DISTINCT LOWER(ccu.table_name) AS table_name, LOWER(ccu.column_name) AS column_name
    FROM {{ catalog }}.information_schema.table_constraints tc
    JOIN {{ catalog }}.information_schema.constraint_column_usage ccu
      ON ccu.constraint_schema = tc.constraint_schema
     AND ccu.constraint_name   = tc.constraint_name
     AND ccu.table_schema      = tc.table_schema
     AND ccu.table_name        = tc.table_name
    WHERE LOWER(tc.table_schema) = LOWER('{{ schema }}')
      AND tc.constraint_type = 'CHECK'
      AND tc.enforced = 'YES'
)
SELECT
    COUNT_IF(c.is_nullable = 'NO' OR ck.column_name IS NOT NULL)           AS enforced_columns,
    COUNT(*)                                                                AS total_columns,
    COUNT_IF(c.is_nullable = 'NO' OR ck.column_name IS NOT NULL)::DOUBLE
        / NULLIF(COUNT(*), 0)                                               AS value
FROM columns_in_scope c
LEFT JOIN check_columns ck USING (table_name, column_name)
```
