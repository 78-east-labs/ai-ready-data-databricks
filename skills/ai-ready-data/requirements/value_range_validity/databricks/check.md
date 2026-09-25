# Check: value_range_validity

Fraction of non-null values in a numeric column that fall inside the declared inclusive range `[min_value, max_value]`.

## Context

Column-scoped data scan. `value = in_range_rows / non_null_rows`. NULLs are excluded from both counts; `data_completeness` measures them.

Strength is **data**, with a **native** source for the bounds. Databricks enforces `CHECK` constraints on Delta tables, so a column that already has `CHECK (age BETWEEN 0 AND 150)` scores 1.0 by construction (the constraint rejected any violating write, and `ADD CONSTRAINT` validated existing rows). The discovery variant reads `information_schema.check_constraints` for that reason: it tells you which columns already carry a range, and it lets you reuse a curated table's range against a raw upstream table with the same column name, which is where violations actually live. Views, `FOREIGN` tables and non-Delta external tables have no CHECK constraints.

Placeholders:

- `{{ min_value }}`, `{{ max_value }}`: numeric literals. Default: parsed from a CHECK constraint on the column when one exists (discovery variant). With no constraint and no caller value, use `double('-Infinity')` and `double('Infinity')` respectively, which makes the corresponding side open; a check with both sides open returns 1.0 and is meaningless, so the orchestrator should skip the column instead.
- `{{ sample_rows }}`: default 1,000,000.

The parser in the discovery variant handles the common shapes: `col >= 0`, `col > 0`, `0 <= col`, `col <= 100`, `col BETWEEN 0 AND 100`, with optional parentheses and backticks and integer or decimal literals. Strict inequalities are reported as-is in `strict_lower` / `strict_upper` and the emitted statement uses `>` / `<` for them. Bounds that are expressions or other columns are not parsed; the diagnostic prints the raw constraint text so you can set the placeholders by hand.

Works on any type that orders numerically (integers, DECIMAL, FLOAT, DOUBLE). For DATE or TIMESTAMP ranges pass typed literals (`DATE '2020-01-01'`).

Returns NULL when the column has no non-null values.

## SQL

### Declared range (primary)

```sql
WITH col_check AS (
    SELECT
        COUNT(*)                                                                   AS non_null_rows,
        COUNT_IF({{ column }} BETWEEN {{ min_value }} AND {{ max_value }})          AS in_range_rows
    FROM {{ catalog }}.{{ schema }}.{{ asset }}
    WHERE {{ column }} IS NOT NULL
)
SELECT
    '{{ asset }}'                                           AS table_name,
    '{{ column }}'                                          AS column_name,
    in_range_rows,
    non_null_rows,
    in_range_rows::DOUBLE / NULLIF(non_null_rows, 0)        AS value
FROM col_check
```

`BETWEEN` is inclusive on both ends. Use `{{ column }} > {{ min_value }} AND {{ column }} < {{ max_value }}` when the declared range is open.

### Discover the range from a CHECK constraint (variant)

Finds CHECK constraints on the table that mention the column, parses literal bounds, and emits the primary statement with them filled in. Point `{{ source_asset }}` at the table to measure (default: `{{ asset }}` itself) when reusing a curated table's constraint on an upstream table.

```sql
WITH cc AS (
    SELECT
        tc.constraint_name,
        c.sql                                                        AS constraint_sql,
        LOWER(c.sql)                                                 AS s
    FROM {{ catalog }}.information_schema.table_constraints tc
    JOIN {{ catalog }}.information_schema.check_constraints c
      ON c.constraint_name = tc.constraint_name
     AND c.constraint_schema = tc.constraint_schema
    WHERE LOWER(tc.table_schema) = LOWER('{{ schema }}')
      AND LOWER(tc.table_name)   = LOWER('{{ asset }}')
      AND tc.constraint_type = 'CHECK'
      AND REGEXP_LIKE(LOWER(c.sql), concat('(^|[^a-z0-9_])`?', LOWER('{{ column }}'), '`?([^a-z0-9_]|$)'))
),
parsed AS (
    SELECT
        constraint_name,
        constraint_sql,
        -- BETWEEN a AND b
        NULLIF(regexp_extract(s, concat('`?', LOWER('{{ column }}'), '`?\\s+between\\s+(-?[0-9]+(?:\\.[0-9]+)?)\\s+and\\s+(-?[0-9]+(?:\\.[0-9]+)?)'), 1), '') AS between_lo,
        NULLIF(regexp_extract(s, concat('`?', LOWER('{{ column }}'), '`?\\s+between\\s+(-?[0-9]+(?:\\.[0-9]+)?)\\s+and\\s+(-?[0-9]+(?:\\.[0-9]+)?)'), 2), '') AS between_hi,
        -- col >= a  /  col > a
        NULLIF(regexp_extract(s, concat('`?', LOWER('{{ column }}'), '`?\\s*>(=?)\\s*(-?[0-9]+(?:\\.[0-9]+)?)'), 2), '') AS lo_right,
        regexp_extract(s, concat('`?', LOWER('{{ column }}'), '`?\\s*>(=?)\\s*(-?[0-9]+(?:\\.[0-9]+)?)'), 1)             AS lo_right_eq,
        -- a <= col  /  a < col
        NULLIF(regexp_extract(s, concat('(-?[0-9]+(?:\\.[0-9]+)?)\\s*<(=?)\\s*`?', LOWER('{{ column }}'), '`?'), 1), '') AS lo_left,
        regexp_extract(s, concat('(-?[0-9]+(?:\\.[0-9]+)?)\\s*<(=?)\\s*`?', LOWER('{{ column }}'), '`?'), 2)             AS lo_left_eq,
        -- col <= b  /  col < b
        NULLIF(regexp_extract(s, concat('`?', LOWER('{{ column }}'), '`?\\s*<(=?)\\s*(-?[0-9]+(?:\\.[0-9]+)?)'), 2), '') AS hi_right,
        regexp_extract(s, concat('`?', LOWER('{{ column }}'), '`?\\s*<(=?)\\s*(-?[0-9]+(?:\\.[0-9]+)?)'), 1)             AS hi_right_eq,
        -- b >= col  /  b > col
        NULLIF(regexp_extract(s, concat('(-?[0-9]+(?:\\.[0-9]+)?)\\s*>(=?)\\s*`?', LOWER('{{ column }}'), '`?'), 1), '') AS hi_left,
        regexp_extract(s, concat('(-?[0-9]+(?:\\.[0-9]+)?)\\s*>(=?)\\s*`?', LOWER('{{ column }}'), '`?'), 2)             AS hi_left_eq
    FROM cc
),
bounds AS (
    SELECT
        constraint_name,
        constraint_sql,
        COALESCE(between_lo, lo_right, lo_left)                          AS min_value,
        COALESCE(between_hi, hi_right, hi_left)                          AS max_value,
        CASE WHEN between_lo IS NOT NULL THEN FALSE
             WHEN lo_right IS NOT NULL THEN lo_right_eq = ''
             WHEN lo_left  IS NOT NULL THEN lo_left_eq  = ''
             ELSE NULL END                                               AS strict_lower,
        CASE WHEN between_hi IS NOT NULL THEN FALSE
             WHEN hi_right IS NOT NULL THEN hi_right_eq = ''
             WHEN hi_left  IS NOT NULL THEN hi_left_eq  = ''
             ELSE NULL END                                               AS strict_upper
    FROM parsed
)
SELECT
    constraint_name,
    constraint_sql,
    min_value,
    max_value,
    strict_lower,
    strict_upper,
    concat(
        'WITH col_check AS (SELECT COUNT(*) AS non_null_rows, COUNT_IF(',
        '`{{ column }}` ', CASE WHEN strict_lower THEN '> ' ELSE '>= ' END, COALESCE(min_value, 'double(''-Infinity'')'),
        ' AND `{{ column }}` ', CASE WHEN strict_upper THEN '< ' ELSE '<= ' END, COALESCE(max_value, 'double(''Infinity'')'),
        ') AS in_range_rows FROM {{ catalog }}.{{ schema }}.`{{ source_asset }}` WHERE `{{ column }}` IS NOT NULL) ',
        'SELECT ''{{ source_asset }}'' AS table_name, ''{{ column }}'' AS column_name, in_range_rows, non_null_rows, ',
        'in_range_rows::DOUBLE / NULLIF(non_null_rows, 0) AS value FROM col_check'
    ) AS stmt
FROM bounds
WHERE min_value IS NOT NULL OR max_value IS NOT NULL
ORDER BY constraint_name
```

No rows means no parseable range constraint exists for the column; fall back to caller-supplied bounds or to the diagnostic's percentiles to propose some.

### Sampled (variant)

```sql
WITH col_check AS (
    SELECT
        COUNT(*)                                                                   AS non_null_rows,
        COUNT_IF({{ column }} BETWEEN {{ min_value }} AND {{ max_value }})          AS in_range_rows
    FROM {{ catalog }}.{{ schema }}.{{ asset }} TABLESAMPLE ({{ sample_rows }} ROWS)
    WHERE {{ column }} IS NOT NULL
)
SELECT
    '{{ asset }}'                                           AS table_name,
    '{{ column }}'                                          AS column_name,
    in_range_rows,
    non_null_rows,
    in_range_rows::DOUBLE / NULLIF(non_null_rows, 0)        AS value
FROM col_check
```

`TABLESAMPLE (n ROWS)` returns the first `n` rows scanned, not a random sample. For a random sample use `TABLESAMPLE (1 PERCENT)` instead; it is slower because it still reads every file.
