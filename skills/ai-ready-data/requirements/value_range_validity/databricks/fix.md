# Fix: value_range_validity

Bring out-of-range values back inside the declared bounds, then declare the range as a CHECK constraint so Delta rejects future violations.

## Context

Options, from least to most destructive:

1. **Recode sentinels.** Most range violations are placeholder values (`-1`, `0`, `9999`, `999999`) written by a source system for "unknown". The honest repair is `NULL`, not a clamped number. Run the diagnostic; if the top out-of-range values are a few repeated sentinels, use this.
2. **Clamp** to the nearest bound. Keeps the row and keeps the value near the truth when the excess is measurement noise (a 101% completion rate, a negative zero). Silently wrong for sentinels, so do step 1 first.
3. **Quarantine and delete** the rows when an out-of-range value means the whole record is untrustworthy.
4. **Declare the CHECK constraint.** Databricks enforces CHECK on Delta tables: `ADD CONSTRAINT` scans the table and fails if any row violates, and every later write that violates is rejected. This is the durable fix and the one that makes the discovery variant of the check work. It requires ownership or `MODIFY` on the table and does not apply to views or foreign tables.

Every mutating option changes data; the deleted-file retention window (7 days by default) is the only undo. Run the blast-radius query first and record the counts.

Placeholders: `{{ min_value }}`, `{{ max_value }}` as in the check; `{{ sentinel_values }}` is a comma-separated numeric list such as `-1, 9999`.

## Fix: Blast radius

```sql
SELECT
    COUNT_IF({{ column }} < {{ min_value }})                                 AS below_min,
    COUNT_IF({{ column }} > {{ max_value }})                                 AS above_max,
    COUNT_IF({{ column }} < {{ min_value }} OR {{ column }} > {{ max_value }}) AS rows_affected,
    COUNT(*)                                                                 AS total_rows
FROM {{ catalog }}.{{ schema }}.{{ asset }}
```

## Fix: Recode sentinel values to NULL

```sql
UPDATE {{ catalog }}.{{ schema }}.{{ asset }}
SET {{ column }} = NULL
WHERE {{ column }} IN ({{ sentinel_values }})
```

Idempotent. Afterwards the column's `data_completeness` drops by the same count; that is the true picture.

## Fix: Clamp to the declared bounds

```sql
UPDATE {{ catalog }}.{{ schema }}.{{ asset }}
SET {{ column }} = CASE
    WHEN {{ column }} < {{ min_value }} THEN {{ min_value }}
    WHEN {{ column }} > {{ max_value }} THEN {{ max_value }}
    ELSE {{ column }}
END
WHERE {{ column }} < {{ min_value }} OR {{ column }} > {{ max_value }}
```

The `WHERE` restricts the rewrite to affected files. Idempotent. If the column is a DECIMAL with a scale, cast the bounds to the same type to avoid an implicit widening error.

## Fix: Quarantine, then delete out-of-range rows

```sql
CREATE TABLE IF NOT EXISTS {{ catalog }}.{{ schema }}.{{ asset }}_range_violations
AS SELECT *, '' AS violated_column, current_timestamp() AS quarantined_at
   FROM {{ catalog }}.{{ schema }}.{{ asset }} WHERE 1 = 0;

INSERT INTO {{ catalog }}.{{ schema }}.{{ asset }}_range_violations
SELECT *, '{{ column }}', current_timestamp()
FROM {{ catalog }}.{{ schema }}.{{ asset }}
WHERE {{ column }} < {{ min_value }} OR {{ column }} > {{ max_value }};

DELETE FROM {{ catalog }}.{{ schema }}.{{ asset }}
WHERE {{ column }} < {{ min_value }} OR {{ column }} > {{ max_value }};
```

## Fix: Declare the range as a CHECK constraint

Guard (constraint names are unique per table):

```sql
SELECT 1
FROM {{ catalog }}.information_schema.table_constraints
WHERE LOWER(table_schema) = LOWER('{{ schema }}')
  AND LOWER(table_name)   = LOWER('{{ asset }}')
  AND constraint_name = '{{ column }}_range'
```

If no row, and the check now returns 1.0:

```sql
ALTER TABLE {{ catalog }}.{{ schema }}.{{ asset }}
ADD CONSTRAINT {{ column }}_range CHECK ({{ column }} IS NULL OR {{ column }} BETWEEN {{ min_value }} AND {{ max_value }})
```

`IS NULL OR` keeps the constraint about range, not completeness; drop it if the column is also `NOT NULL`. `ADD CONSTRAINT` fails with `DELTA_NEW_CHECK_CONSTRAINT_VIOLATION` if any existing row violates, which is the intended safety net. Streaming writers and Lakeflow flows into the table will fail on a violating row, so coordinate with the pipeline owner before adding it to a table that receives raw data.

## Fix: Bulk generation of CHECK constraints from observed percentiles

Emits a proposal per unconstrained numeric column, using p0.1 and p99.9 of the current data rounded outward. These are proposals for review, not facts; a human confirms each range against the business meaning before running the statement.

```sql
WITH numeric_cols AS (
    SELECT col.table_name, col.column_name
    FROM {{ catalog }}.information_schema.columns col
    JOIN {{ catalog }}.information_schema.tables t
      ON t.table_schema = col.table_schema AND t.table_name = col.table_name
    WHERE LOWER(col.table_schema) = LOWER('{{ schema }}')
      AND LOWER(col.table_name)   = LOWER('{{ asset }}')
      AND t.table_type IN ('MANAGED', 'EXTERNAL')
      AND col.data_type IN ('TINYINT', 'SMALLINT', 'INT', 'BIGINT', 'FLOAT', 'DOUBLE', 'DECIMAL')
      AND NOT REGEXP_LIKE(LOWER(col.column_name), '(^id$|_id$|_key$|_flag$|^is_)')
),
constrained AS (
    SELECT DISTINCT LOWER(tc.table_name) AS table_name, LOWER(c.sql) AS s
    FROM {{ catalog }}.information_schema.table_constraints tc
    JOIN {{ catalog }}.information_schema.check_constraints c
      ON c.constraint_name = tc.constraint_name AND c.constraint_schema = tc.constraint_schema
    WHERE LOWER(tc.table_schema) = LOWER('{{ schema }}') AND tc.constraint_type = 'CHECK'
)
SELECT concat(
    'SELECT concat(''ALTER TABLE {{ catalog }}.{{ schema }}.`', n.table_name, '` ADD CONSTRAINT `',
    n.column_name, '_range` CHECK (`', n.column_name, '` IS NULL OR `', n.column_name, '` BETWEEN '', ',
    'floor(APPROX_PERCENTILE(`', n.column_name, '`, 0.001)), '' AND '', ',
    'ceil(APPROX_PERCENTILE(`', n.column_name, '`, 0.999)), ''));'') AS stmt ',
    'FROM {{ catalog }}.{{ schema }}.`', n.table_name, '`;'
) AS proposal_query
FROM numeric_cols n
LEFT JOIN constrained c
  ON c.table_name = LOWER(n.table_name)
 AND REGEXP_LIKE(c.s, concat('(^|[^a-z0-9_])`?', LOWER(n.column_name), '`?([^a-z0-9_]|$)'))
WHERE c.table_name IS NULL
ORDER BY n.column_name
```

This emits one query per column; each of those, when run, prints the `ALTER TABLE` proposal with real numbers. Two hops because the percentiles need a data scan and `information_schema` does not.

## Organizational guidance

Ranges belong in the contract, not in a post-hoc assessment. Put `CHECK` constraints in the table DDL template, or in Lakeflow use `CONSTRAINT valid_range EXPECT (col BETWEEN a AND b) ON VIOLATION DROP ROW` (or `FAIL UPDATE` for gold tables) so violations are counted in the pipeline event log instead of landing in the table. In dbt, `accepted_range` from `dbt_utils` gives the same guarantee at test time. Keep sentinels out of numeric columns entirely: source systems that emit `-1` for unknown should be mapped to `NULL` at bronze-to-silver.
