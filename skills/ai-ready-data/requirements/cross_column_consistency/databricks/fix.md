# Fix: cross_column_consistency

Repair rows that break a cross-column rule, then make the rule enforced.

## Context

There is no generic data fix: which column is wrong depends on the rule and on which source is authoritative. The options below are templates for the four patterns that cover almost every case. Run the diagnostic first; the cluster query usually tells you whether you are fixing data or fixing one upstream job.

1. **Recompute a derived column** (`total = qty * price`, `age = datediff(...)`). Safe when the inputs are trusted.
2. **NULL the untrusted column** (`end_date` earlier than `start_date` where `start_date` comes from the system of record). Keeps the row; the column becomes a completeness gap.
3. **Apply a business default** (`status = 'SHIPPED'` with no `shipped_at`: set it to the last modification time). Only with the owner's agreement, because it fabricates a value.
4. **Flag for review** by writing violating rows to a quarantine table, or by setting a `needs_review` column, when nobody can say which side is right.

Then, once the check returns 1.0, **add the rule as a CHECK constraint**. Databricks enforces CHECK on Delta tables, so the rule cannot be broken again by any writer. This is the only durable fix.

Every mutating statement is preceded by the blast-radius query and restricted to rows that currently violate, so re-running is a no-op. `{{ null_filter }}` defaults to `TRUE`.

## Fix: Blast radius

```sql
SELECT
    COUNT_IF(NOT COALESCE({{ consistency_predicate }}, FALSE))  AS violating_rows,
    COUNT(*)                                                    AS rows_in_scope
FROM {{ catalog }}.{{ schema }}.{{ asset }}
WHERE {{ null_filter }}
```

## Fix: Recompute a derived column

`{{ derived_column }}` is the column defined by the others; `{{ derivation }}` is its formula, for example `round(quantity * unit_price, 2)`.

```sql
UPDATE {{ catalog }}.{{ schema }}.{{ asset }}
SET {{ derived_column }} = {{ derivation }}
WHERE {{ null_filter }}
  AND NOT COALESCE({{ consistency_predicate }}, FALSE)
```

If the formula's inputs can be NULL, the update writes NULL into the derived column for those rows; decide first whether that is acceptable.

## Fix: NULL the untrusted column

```sql
UPDATE {{ catalog }}.{{ schema }}.{{ asset }}
SET {{ untrusted_column }} = NULL
WHERE {{ null_filter }}
  AND NOT COALESCE({{ consistency_predicate }}, FALSE)
```

## Fix: Apply a business default

`{{ default_expression }}` is agreed with the owner, for example `COALESCE(updated_at, created_at)` or `DATE '9999-12-31'` for an open-ended `end_date`.

```sql
UPDATE {{ catalog }}.{{ schema }}.{{ asset }}
SET {{ target_column }} = {{ default_expression }}
WHERE {{ null_filter }}
  AND NOT COALESCE({{ consistency_predicate }}, FALSE)
  AND {{ target_column }} IS NULL
```

## Fix: Quarantine violating rows for review

Leaves the source table untouched and gives stewards a worklist. Idempotent by rule name plus a stable key: rows already quarantined for this rule are skipped.

```sql
CREATE TABLE IF NOT EXISTS {{ catalog }}.{{ schema }}.{{ asset }}_rule_violations
AS SELECT *, '' AS rule_name, current_timestamp() AS flagged_at
   FROM {{ catalog }}.{{ schema }}.{{ asset }} WHERE 1 = 0;

INSERT INTO {{ catalog }}.{{ schema }}.{{ asset }}_rule_violations
SELECT s.*, '{{ rule_name }}', current_timestamp()
FROM {{ catalog }}.{{ schema }}.{{ asset }} s
WHERE {{ null_filter }}
  AND NOT COALESCE({{ consistency_predicate }}, FALSE)
  AND NOT EXISTS (
      SELECT 1 FROM {{ catalog }}.{{ schema }}.{{ asset }}_rule_violations q
      WHERE q.rule_name = '{{ rule_name }}' AND q.{{ key_column }} = s.{{ key_column }}
  );
```

## Fix: Enforce the rule as a CHECK constraint

Guard:

```sql
SELECT 1
FROM {{ catalog }}.information_schema.table_constraints
WHERE LOWER(table_schema) = LOWER('{{ schema }}')
  AND LOWER(table_name)   = LOWER('{{ asset }}')
  AND constraint_name = '{{ rule_name }}'
```

If no row, and the check returns 1.0:

```sql
ALTER TABLE {{ catalog }}.{{ schema }}.{{ asset }}
ADD CONSTRAINT {{ rule_name }} CHECK ({{ consistency_predicate }})
```

CHECK constraints treat a NULL result as satisfied, so write the predicate so NULL means "not applicable" (`end_date IS NULL OR end_date >= start_date`), or add `SET NOT NULL` on the operands first. `ADD CONSTRAINT` scans the table and fails on any violating row. The constraint's expression can only reference columns of the same table; no subqueries, no UDFs.

## Fix: Bulk generation of CHECK constraints for common column pairs

Proposes `end >= start` style constraints for date and timestamp column pairs found by name in the schema's base tables. Proposals only; each one needs the check to return 1.0 before it is applied.

```sql
WITH ts_cols AS (
    SELECT LOWER(c.table_name) AS table_name, c.column_name, LOWER(c.column_name) AS lc
    FROM {{ catalog }}.information_schema.columns c
    JOIN {{ catalog }}.information_schema.tables t
      ON t.table_schema = c.table_schema AND t.table_name = c.table_name
    WHERE LOWER(c.table_schema) = LOWER('{{ schema }}')
      AND t.table_type IN ('MANAGED', 'EXTERNAL')
      AND c.data_type IN ('DATE', 'TIMESTAMP', 'TIMESTAMP_NTZ')
),
pairs AS (
    SELECT s.table_name, s.column_name AS start_col, e.column_name AS end_col
    FROM ts_cols s
    JOIN ts_cols e
      ON e.table_name = s.table_name
     AND e.lc <> s.lc
     AND (
          (s.lc RLIKE '(^|_)(start|begin|from)(_|$)' AND e.lc = regexp_replace(regexp_replace(regexp_replace(s.lc, 'start', 'end'), 'begin', 'end'), 'from', 'to'))
       OR (s.lc RLIKE '(^|_)created(_|$)' AND e.lc = replace(s.lc, 'created', 'updated'))
       OR (s.lc RLIKE '(^|_)opened(_|$)'  AND e.lc = replace(s.lc, 'opened', 'closed'))
     )
),
existing AS (
    SELECT LOWER(tc.table_name) AS table_name, LOWER(c.sql) AS s
    FROM {{ catalog }}.information_schema.table_constraints tc
    JOIN {{ catalog }}.information_schema.check_constraints c
      ON c.constraint_name = tc.constraint_name AND c.constraint_schema = tc.constraint_schema
    WHERE LOWER(tc.table_schema) = LOWER('{{ schema }}') AND tc.constraint_type = 'CHECK'
)
SELECT concat(
    'ALTER TABLE {{ catalog }}.{{ schema }}.`', p.table_name, '` ADD CONSTRAINT `',
    p.end_col, '_after_', p.start_col, '` CHECK (`', p.end_col, '` IS NULL OR `', p.start_col,
    '` IS NULL OR `', p.end_col, '` >= `', p.start_col, '`);'
) AS stmt
FROM pairs p
LEFT JOIN existing x
  ON x.table_name = p.table_name
 AND x.s RLIKE concat('`?', LOWER(p.end_col), '`?\\s*>=\\s*`?', LOWER(p.start_col), '`?')
WHERE x.table_name IS NULL
ORDER BY p.table_name, p.start_col
```

## Organizational guidance

Cross-column rules are business logic and should live where the business logic is written: as `CHECK` constraints in the silver and gold table DDL, as Lakeflow expectations (`CONSTRAINT dates_ordered EXPECT (end_date >= start_date) ON VIOLATION DROP ROW`) on the flows that build them, or as dbt `expression_is_true` tests. Record each rule's owner and rationale in the constraint name and the table comment so the next person who hits a violation knows who decides. When a rule depends on another table (a status must match a lookup), it belongs in `referential_integrity`, not here.
