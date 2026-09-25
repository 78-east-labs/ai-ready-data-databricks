# Diagnostic: data_completeness

Null rate for every column of a table in one scan, with sentinel and empty-string counts for string columns, and the declared nullability from `information_schema`.

## Context

Two queries:

1. **Column inventory.** Every column of the table with type, `is_nullable`, ordinal position and whether it is part of a primary key. Cheap; shows which columns are already enforced `NOT NULL` and which would be candidates.
2. **Generated one-pass profile.** `information_schema` cannot count nulls, and Databricks has no `RESULT_SCAN`, so this query emits a single `SELECT` that computes, per column, the null count, and for STRING columns the count of empty or whitespace-only values and of common sentinels (`'N/A'`, `'NULL'`, `'none'`, `'-'`, `'unknown'`). Run the emitted statement; it scans the table once and returns one row per column, worst-first.

`ANALYZE TABLE {{ catalog }}.{{ schema }}.{{ asset }} COMPUTE STATISTICS FOR ALL COLUMNS` followed by `DESCRIBE EXTENDED {{ catalog }}.{{ schema }}.{{ asset }} {{ column }}` also shows `num_nulls`, but the output cannot be joined in SQL and the statistics are as old as the last ANALYZE. Predictive optimization keeps them fresh on managed tables where it is enabled.

## SQL

### Column inventory with declared nullability

```sql
WITH pk_cols AS (
    SELECT DISTINCT k.column_name
    FROM {{ catalog }}.information_schema.table_constraints tc
    JOIN {{ catalog }}.information_schema.key_column_usage k
      ON k.constraint_name = tc.constraint_name
     AND k.table_schema = tc.table_schema
     AND k.table_name   = tc.table_name
    WHERE LOWER(tc.table_schema) = LOWER('{{ schema }}')
      AND LOWER(tc.table_name)   = LOWER('{{ asset }}')
      AND tc.constraint_type = 'PRIMARY KEY'
)
SELECT
    c.column_name,
    c.ordinal_position,
    c.full_data_type,
    c.is_nullable,
    p.column_name IS NOT NULL               AS in_primary_key,
    c.comment IS NOT NULL AND c.comment <> '' AS has_comment
FROM {{ catalog }}.information_schema.columns c
LEFT JOIN pk_cols p ON p.column_name = c.column_name
WHERE LOWER(c.table_schema) = LOWER('{{ schema }}')
  AND LOWER(c.table_name)   = LOWER('{{ asset }}')
ORDER BY c.is_nullable DESC, c.ordinal_position
```

### Generate the one-pass null profile

```sql
WITH cols AS (
    SELECT column_name, data_type, ordinal_position
    FROM {{ catalog }}.information_schema.columns
    WHERE LOWER(table_schema) = LOWER('{{ schema }}')
      AND LOWER(table_name)   = LOWER('{{ asset }}')
),
per_col AS (
    SELECT
        ordinal_position,
        concat(
            'named_struct(',
            '''column_name'', ''', column_name, ''', ',
            '''null_rows'', COUNT_IF(`', column_name, '` IS NULL), ',
            '''blank_rows'', ',
                CASE WHEN data_type = 'STRING'
                     THEN concat('COUNT_IF(TRIM(`', column_name, '`) = '''')')
                     ELSE 'CAST(NULL AS BIGINT)' END, ', ',
            '''sentinel_rows'', ',
                CASE WHEN data_type = 'STRING'
                     THEN concat('COUNT_IF(LOWER(TRIM(`', column_name, '`)) IN (''n/a'', ''na'', ''null'', ''none'', ''-'', ''unknown'', ''?''))')
                     ELSE 'CAST(NULL AS BIGINT)' END,
            ')'
        ) AS expr
    FROM cols
)
SELECT concat(
    'WITH agg AS (SELECT COUNT(*) AS total_rows, array(',
    array_join(transform(array_sort(collect_list(struct(ordinal_position, expr))), s -> s.expr), ', '),
    ') AS cols FROM {{ catalog }}.{{ schema }}.`{{ asset }}`) ',
    'SELECT c.column_name, c.null_rows, c.blank_rows, c.sentinel_rows, total_rows, ',
    '1.0 - c.null_rows::DOUBLE / NULLIF(total_rows, 0) AS completeness, ',
    '1.0 - (c.null_rows + COALESCE(c.blank_rows, 0) + COALESCE(c.sentinel_rows, 0))::DOUBLE / NULLIF(total_rows, 0) AS strict_completeness ',
    'FROM agg LATERAL VIEW explode(cols) AS c ',
    'ORDER BY strict_completeness ASC'
) AS stmt
FROM per_col
```

The emitted statement returns one row per column. `completeness` matches the check; `strict_completeness` also treats blanks and sentinels as missing. A large gap between the two on a column means the pipeline is hiding missing values behind placeholders, and the honest fix is to write NULL upstream.

On very wide tables (hundreds of columns) split the generated statement into batches of about 100 columns; the single `array(...)` of structs is fine for Spark but the query text gets long.

### Lakehouse Monitoring profile (variant)

If the table is monitored, the latest window's per-column null counts are already computed.

```sql
WITH latest AS (
    SELECT MAX(window.end) AS window_end
    FROM {{ monitor_schema }}.{{ asset }}_profile_metrics
    WHERE log_type = 'INPUT' AND slice_key IS NULL
)
SELECT
    p.column_name,
    p.num_nulls                                     AS null_rows,
    p.count                                         AS total_rows,
    1.0 - p.num_nulls::DOUBLE / NULLIF(p.count, 0)  AS completeness,
    p.window.end                                    AS as_of
FROM {{ monitor_schema }}.{{ asset }}_profile_metrics p
JOIN latest l ON p.window.end = l.window_end
WHERE p.log_type = 'INPUT'
  AND p.slice_key IS NULL
  AND p.column_name <> ':table'
ORDER BY completeness ASC
```
