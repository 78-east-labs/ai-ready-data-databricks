# Diagnostic: value_range_validity

Distribution profile of the column with counts on each side of the declared range, plus every CHECK constraint in the schema so you can see which numeric columns already carry a range.

## Context

Two queries:

1. **Column profile.** Min, max, mean, median, stddev, p1 / p5 / p95 / p99, how many non-null rows fall below `{{ min_value }}` and above `{{ max_value }}`, and the most common out-of-range values. Use it to set a range for an unconstrained column (p1 / p99 are a reasonable starting proposal, min / max tell you how far the tails go) or to see whether violations are a handful of sentinels (`-1`, `9999`) or a real shift.
2. **CHECK constraints in the schema.** One row per constraint with its raw expression, so you can read ranges that the parser in the check could not extract, and see which numeric columns have no constraint at all (candidates for the fix).

Defaults: `{{ min_value }}` and `{{ max_value }}` as in the check (`double('-Infinity')` / `double('Infinity')` when unknown).

## SQL

### Column profile with range violations

```sql
WITH vals AS (
    SELECT {{ column }} AS v
    FROM {{ catalog }}.{{ schema }}.{{ asset }}
    WHERE {{ column }} IS NOT NULL
),
profile AS (
    SELECT
        COUNT(*)                                        AS non_null_rows,
        MIN(v)                                          AS min_observed,
        MAX(v)                                          AS max_observed,
        AVG(v)                                          AS mean,
        MEDIAN(v)                                       AS median,
        STDDEV(v)                                       AS stddev,
        APPROX_PERCENTILE(v, 0.01)                      AS p1,
        APPROX_PERCENTILE(v, 0.05)                      AS p5,
        APPROX_PERCENTILE(v, 0.95)                      AS p95,
        APPROX_PERCENTILE(v, 0.99)                      AS p99,
        COUNT_IF(v < {{ min_value }})                   AS below_min,
        COUNT_IF(v > {{ max_value }})                   AS above_max
    FROM vals
),
top_violations AS (
    SELECT array_sort(collect_list(struct(-cnt AS neg_cnt, v))) AS top
    FROM (
        SELECT v, COUNT(*) AS cnt
        FROM vals
        WHERE v < {{ min_value }} OR v > {{ max_value }}
        GROUP BY v
        ORDER BY cnt DESC
        LIMIT 10
    )
)
SELECT
    '{{ asset }}'                                   AS table_name,
    '{{ column }}'                                  AS column_name,
    {{ min_value }}                                 AS declared_min,
    {{ max_value }}                                 AS declared_max,
    p.non_null_rows,
    p.below_min,
    p.above_max,
    (p.below_min + p.above_max)::DOUBLE / NULLIF(p.non_null_rows, 0) AS violation_rate,
    p.min_observed, p.p1, p.p5, p.median, p.mean, p.p95, p.p99, p.max_observed, p.stddev,
    transform(t.top, s -> named_struct('value', s.v, 'rows', -s.neg_cnt)) AS top_out_of_range_values
FROM profile p CROSS JOIN top_violations t
```

### CHECK constraints in the schema and unconstrained numeric columns

```sql
WITH checks AS (
    SELECT
        LOWER(tc.table_name)     AS table_name,
        tc.constraint_name,
        c.sql                    AS constraint_sql
    FROM {{ catalog }}.information_schema.table_constraints tc
    JOIN {{ catalog }}.information_schema.check_constraints c
      ON c.constraint_name = tc.constraint_name
     AND c.constraint_schema = tc.constraint_schema
    WHERE LOWER(tc.table_schema) = LOWER('{{ schema }}')
      AND tc.constraint_type = 'CHECK'
),
numeric_cols AS (
    SELECT LOWER(col.table_name) AS table_name, col.column_name, col.full_data_type
    FROM {{ catalog }}.information_schema.columns col
    JOIN {{ catalog }}.information_schema.tables t
      ON t.table_schema = col.table_schema AND t.table_name = col.table_name
    WHERE LOWER(col.table_schema) = LOWER('{{ schema }}')
      AND t.table_type IN ('MANAGED', 'EXTERNAL')
      AND col.data_type IN ('TINYINT', 'SMALLINT', 'INT', 'BIGINT', 'FLOAT', 'DOUBLE', 'DECIMAL')
)
SELECT
    n.table_name,
    n.column_name,
    n.full_data_type,
    ch.constraint_name,
    ch.constraint_sql,
    ch.constraint_name IS NOT NULL AS has_range_constraint
FROM numeric_cols n
LEFT JOIN checks ch
  ON ch.table_name = n.table_name
 AND REGEXP_LIKE(LOWER(ch.constraint_sql), concat('(^|[^a-z0-9_])`?', LOWER(n.column_name), '`?([^a-z0-9_]|$)'))
ORDER BY has_range_constraint ASC, n.table_name, n.column_name
```

A numeric column that is a surrogate key or a flag does not need a range; ignore those rows. Columns named like `amount`, `price`, `qty`, `age`, `pct`, `rate`, `score`, `lat`, `lon` almost always do.
