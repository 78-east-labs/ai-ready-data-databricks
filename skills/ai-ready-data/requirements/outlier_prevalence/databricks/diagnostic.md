# Diagnostic: outlier_prevalence

Per-column outlier counts under both z-score and IQR rules, the most extreme rows for one column with their z-scores, and the repeated values that indicate sentinels rather than genuine extremes.

## Context

Three queries:

1. **Per-column summary (generated).** Because columns cannot be iterated in SQL, the first query emits a statement that, in one scan, returns for every numeric column: non-null count, mean, stddev, min, max, p1, p99, the count beyond `{{ z_threshold }}` sigma, and the count beyond 3 IQR from the quartiles. A column where the z-count is small but the IQR count is large has a heavy tail that inflates its own stddev; a column where both are large has real extremes; a column where `max` equals a round number (`9999`, `999999`, `-1`) has sentinels.
2. **Extreme rows for one column.** Up to 100 rows past the threshold, with z-score, direction, and the source file. Sorted by |z|.
3. **Repeated extreme values.** Distinct outlier values with counts. A value that repeats hundreds of times is a placeholder or a cap, not a measurement, and the fix is to NULL it, not to clamp it.

`{{ key_columns }}` names the table's identifier for query 2 (no default).

## SQL

### Generate the per-column summary

```sql
WITH numeric_cols AS (
    SELECT column_name, ordinal_position
    FROM {{ catalog }}.information_schema.columns
    WHERE LOWER(table_schema) = LOWER('{{ schema }}')
      AND LOWER(table_name)   = LOWER('{{ asset }}')
      AND data_type IN ('TINYINT', 'SMALLINT', 'INT', 'BIGINT', 'FLOAT', 'DOUBLE', 'DECIMAL')
      AND NOT REGEXP_LIKE(LOWER(column_name), '(^id$|_id$|_key$|_pk$|_fk$)')
),
parts AS (
    SELECT
        array_join(transform(array_sort(collect_list(struct(ordinal_position, column_name))),
            s -> concat(
                'AVG(`', s.column_name, '`)::DOUBLE AS mean_', s.ordinal_position,
                ', STDDEV(`', s.column_name, '`)::DOUBLE AS sd_', s.ordinal_position,
                ', APPROX_PERCENTILE(`', s.column_name, '`, 0.25)::DOUBLE AS q1_', s.ordinal_position,
                ', APPROX_PERCENTILE(`', s.column_name, '`, 0.75)::DOUBLE AS q3_', s.ordinal_position)), ', ') AS stats_exprs,
        array_join(transform(array_sort(collect_list(struct(ordinal_position, column_name))),
            s -> concat(
                'named_struct(''column_name'', ''', s.column_name, ''', ',
                '''non_null_rows'', COUNT(t.`', s.column_name, '`), ',
                '''mean'', MAX(s.mean_', s.ordinal_position, '), ''stddev'', MAX(s.sd_', s.ordinal_position, '), ',
                '''min_value'', MIN(t.`', s.column_name, '`)::DOUBLE, ''max_value'', MAX(t.`', s.column_name, '`)::DOUBLE, ',
                '''p1'', APPROX_PERCENTILE(t.`', s.column_name, '`, 0.01)::DOUBLE, ''p99'', APPROX_PERCENTILE(t.`', s.column_name, '`, 0.99)::DOUBLE, ',
                '''z_outliers'', COUNT_IF(s.sd_', s.ordinal_position, ' > 0 AND ABS(t.`', s.column_name, '` - s.mean_', s.ordinal_position, ') > {{ z_threshold }} * s.sd_', s.ordinal_position, '), ',
                '''iqr_outliers'', COUNT_IF(t.`', s.column_name, '` < s.q1_', s.ordinal_position, ' - 3 * (s.q3_', s.ordinal_position, ' - s.q1_', s.ordinal_position,
                ') OR t.`', s.column_name, '` > s.q3_', s.ordinal_position, ' + 3 * (s.q3_', s.ordinal_position, ' - s.q1_', s.ordinal_position, ')))'
            )), ', ') AS score_structs
    FROM numeric_cols
)
SELECT concat(
    'WITH stats AS (SELECT ', stats_exprs, ' FROM {{ catalog }}.{{ schema }}.`{{ asset }}`), ',
    'scored AS (SELECT array(', score_structs, ') AS arr ',
    'FROM {{ catalog }}.{{ schema }}.`{{ asset }}` t CROSS JOIN stats s) ',
    'SELECT r.*, ROUND(r.z_outliers::DOUBLE / NULLIF(r.non_null_rows, 0), 5) AS z_outlier_rate ',
    'FROM scored LATERAL VIEW explode(arr) AS r ORDER BY z_outlier_rate DESC'
) AS stmt
FROM parts
```

The emitted statement scans the table twice (stats, then scoring) and returns one row per numeric column. On a table with hundreds of numeric columns, split the column list into batches.

### Single-column summary (fallback, no generation)

```sql
WITH vals AS (
    SELECT {{ column }}::DOUBLE AS v
    FROM {{ catalog }}.{{ schema }}.{{ asset }}
    WHERE {{ column }} IS NOT NULL
),
stats AS (
    SELECT
        COUNT(*)                                AS non_null_rows,
        AVG(v)                                  AS mean,
        STDDEV(v)                               AS stddev,
        MIN(v)                                  AS min_value,
        MAX(v)                                  AS max_value,
        APPROX_PERCENTILE(v, 0.01)              AS p1,
        APPROX_PERCENTILE(v, 0.25)              AS q1,
        MEDIAN(v)                               AS median,
        APPROX_PERCENTILE(v, 0.75)              AS q3,
        APPROX_PERCENTILE(v, 0.99)              AS p99
    FROM vals
)
SELECT
    '{{ column }}'                                                                  AS column_name,
    s.non_null_rows, s.mean, s.stddev, s.min_value, s.p1, s.q1, s.median, s.q3, s.p99, s.max_value,
    COUNT_IF(s.stddev > 0 AND ABS(v.v - s.mean) > {{ z_threshold }} * s.stddev)      AS z_outliers,
    COUNT_IF(v.v < s.q1 - 3 * (s.q3 - s.q1) OR v.v > s.q3 + 3 * (s.q3 - s.q1))        AS iqr_outliers,
    (s.max_value - s.mean) / NULLIF(s.stddev, 0)                                     AS max_z,
    (s.min_value - s.mean) / NULLIF(s.stddev, 0)                                     AS min_z
FROM vals v CROSS JOIN stats s
GROUP BY s.non_null_rows, s.mean, s.stddev, s.min_value, s.p1, s.q1, s.median, s.q3, s.p99, s.max_value
```

### Extreme rows for one column

```sql
WITH stats AS (
    SELECT AVG({{ column }})::DOUBLE AS mean_val, STDDEV({{ column }})::DOUBLE AS sd_val
    FROM {{ catalog }}.{{ schema }}.{{ asset }}
    WHERE {{ column }} IS NOT NULL
)
SELECT
    {{ key_columns }},
    t.{{ column }}                                                          AS value,
    ROUND((t.{{ column }} - s.mean_val) / NULLIF(s.sd_val, 0), 2)           AS z_score,
    CASE WHEN t.{{ column }} > s.mean_val THEN 'HIGH' ELSE 'LOW' END        AS direction,
    t._metadata.file_path                                                   AS source_file
FROM {{ catalog }}.{{ schema }}.{{ asset }} t
CROSS JOIN stats s
WHERE t.{{ column }} IS NOT NULL
  AND s.sd_val > 0
  AND ABS(t.{{ column }} - s.mean_val) > {{ z_threshold }} * s.sd_val
ORDER BY ABS((t.{{ column }} - s.mean_val) / NULLIF(s.sd_val, 0)) DESC
LIMIT 100
```

### Repeated extreme values (sentinel detection)

```sql
WITH stats AS (
    SELECT AVG({{ column }})::DOUBLE AS mean_val, STDDEV({{ column }})::DOUBLE AS sd_val
    FROM {{ catalog }}.{{ schema }}.{{ asset }}
    WHERE {{ column }} IS NOT NULL
)
SELECT
    t.{{ column }}                                                          AS value,
    COUNT(*)                                                                AS occurrences,
    ROUND((t.{{ column }} - MAX(s.mean_val)) / NULLIF(MAX(s.sd_val), 0), 2) AS z_score,
    CASE WHEN COUNT(*) >= 10 THEN 'LIKELY_SENTINEL_OR_CAP' ELSE 'DISTINCT_EXTREME' END AS interpretation
FROM {{ catalog }}.{{ schema }}.{{ asset }} t
CROSS JOIN stats s
WHERE t.{{ column }} IS NOT NULL
  AND s.sd_val > 0
  AND ABS(t.{{ column }} - s.mean_val) > {{ z_threshold }} * s.sd_val
GROUP BY t.{{ column }}
ORDER BY occurrences DESC
LIMIT 50
```
