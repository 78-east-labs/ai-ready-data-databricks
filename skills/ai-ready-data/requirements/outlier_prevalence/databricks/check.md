# Check: outlier_prevalence

Fraction of rows in a table with no numeric value further than `{{ z_threshold }}` standard deviations from its column mean.

## Context

Table-scoped data scan. For every numeric column the check computes mean and stddev over non-null values, then counts rows where any column's |z| exceeds the threshold. `value = 1 - outlier_rows / total_rows`. A row with three extreme columns counts once; a column with zero or NULL stddev (constant, all-NULL) contributes no outliers.

Strength is **data**. Databricks has no native outlier signal, but if a Lakehouse Monitoring monitor exists on the table its `_profile_metrics` output holds `avg`, `stddev`, `min` and `max` per column, which gives a free column-level approximation (does the column's extreme value exceed the threshold?) without a scan. That variant measures columns, not rows, so its `value` is a different quantity; it is offered for triage on large tables and labelled as such.

Placeholders:

- `{{ z_threshold }}`: default 4. Three is the textbook value and flags roughly 0.3% of a normal column; four is the framework default because real business measures are heavy-tailed and three produces noise.
- `{{ sample_rows }}`: default 1,000,000.
- `{{ monitor_schema }}`: default `{{ catalog }}.{{ schema }}`.

Numeric columns come from `information_schema.columns` with `data_type IN ('TINYINT','SMALLINT','INT','BIGINT','FLOAT','DOUBLE','DECIMAL')`, excluding identifier-like names (`id`, `*_id`, `*_key`), because z-scores on surrogate keys are meaningless. Add columns to the exclusion in the generator if the table has numeric codes (postal codes, SKUs stored as INT).

Z-scores assume a roughly symmetric distribution. On a heavy right tail (revenue, latency) the mean and stddev are themselves inflated by the tail and the check under-reports; the diagnostic includes an IQR-based count for comparison. Z-scores also say nothing about *why* a value is extreme; a legitimate whale customer and a unit error look the same here.

Because SQL cannot iterate over columns, the table-level check is two hops: the generator emits one statement that scans the table once (a stats pass in a CTE, then a scoring pass) for all numeric columns. The single-column variant is for the orchestrator's column-scoped mode or for a hand-picked measure.

Returns NULL when the table is empty or has no numeric columns with non-zero stddev.

## SQL

### Generate the all-numeric-columns statement (primary)

Run this, then run the `stmt` it returns.

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
            s -> concat('AVG(`', s.column_name, '`)::DOUBLE AS mean_', s.ordinal_position,
                        ', STDDEV(`', s.column_name, '`)::DOUBLE AS sd_', s.ordinal_position)), ', ') AS stats_exprs,
        array_join(transform(array_sort(collect_list(struct(ordinal_position, column_name))),
            s -> concat('(s.sd_', s.ordinal_position, ' > 0 AND ABS(t.`', s.column_name,
                        '` - s.mean_', s.ordinal_position, ') > {{ z_threshold }} * s.sd_', s.ordinal_position, ')')), ' OR ') AS outlier_pred,
        COUNT(*) AS numeric_columns
    FROM numeric_cols
)
SELECT
    numeric_columns,
    concat(
        'WITH stats AS (SELECT ', stats_exprs, ' FROM {{ catalog }}.{{ schema }}.`{{ asset }}`), ',
        'scored AS (SELECT COUNT(*) AS total_rows, COUNT_IF(', outlier_pred, ') AS outlier_rows ',
        'FROM {{ catalog }}.{{ schema }}.`{{ asset }}` t CROSS JOIN stats s) ',
        'SELECT ''{{ asset }}'' AS table_name, ', numeric_columns, ' AS numeric_columns, {{ z_threshold }} AS z_threshold, ',
        'outlier_rows, total_rows, 1.0 - outlier_rows::DOUBLE / NULLIF(total_rows, 0) AS value FROM scored'
    ) AS stmt
FROM parts
WHERE numeric_columns > 0
```

`COUNT_IF` of an `OR` chain where some terms are NULL (a NULL cell) still counts the row when any other term is TRUE, and skips it when every term is NULL or FALSE, which is the intended semantics.

### Single column (variant)

Column-scoped. `total_rows` here is the non-null count for the column, matching the upstream definition.

```sql
WITH stats AS (
    SELECT AVG({{ column }})::DOUBLE AS mean_val, STDDEV({{ column }})::DOUBLE AS sd_val
    FROM {{ catalog }}.{{ schema }}.{{ asset }}
    WHERE {{ column }} IS NOT NULL
),
scored AS (
    SELECT
        COUNT(*)                                                                    AS total_rows,
        COUNT_IF(s.sd_val > 0 AND ABS(t.{{ column }} - s.mean_val) > {{ z_threshold }} * s.sd_val) AS outlier_rows
    FROM {{ catalog }}.{{ schema }}.{{ asset }} t
    CROSS JOIN stats s
    WHERE t.{{ column }} IS NOT NULL
)
SELECT
    '{{ asset }}'                                           AS table_name,
    '{{ column }}'                                          AS column_name,
    outlier_rows,
    total_rows,
    1.0 - outlier_rows::DOUBLE / NULLIF(total_rows, 0)      AS value
FROM scored
```

### Sampled single column (variant)

Statistics and scoring both on the sample; a prefix sample is biased toward older data, so a recent unit change is missed. Triage only.

```sql
WITH sample AS (
    SELECT {{ column }} AS v
    FROM {{ catalog }}.{{ schema }}.{{ asset }} TABLESAMPLE ({{ sample_rows }} ROWS)
    WHERE {{ column }} IS NOT NULL
),
stats AS (
    SELECT AVG(v)::DOUBLE AS mean_val, STDDEV(v)::DOUBLE AS sd_val FROM sample
)
SELECT
    '{{ asset }}'                                           AS table_name,
    '{{ column }}'                                          AS column_name,
    COUNT_IF(s.sd_val > 0 AND ABS(v - s.mean_val) > {{ z_threshold }} * s.sd_val) AS outlier_rows,
    COUNT(*)                                                AS total_rows,
    1.0 - COUNT_IF(s.sd_val > 0 AND ABS(v - s.mean_val) > {{ z_threshold }} * s.sd_val)::DOUBLE
        / NULLIF(COUNT(*), 0)                               AS value
FROM sample CROSS JOIN stats s
```

### Lakehouse Monitoring profile (variant, column-level)

No scan. Uses the latest profile window's `avg`, `stddev`, `min`, `max`. A column "has outliers" when its min or max lies beyond the threshold. `value` is the fraction of numeric columns without outliers, which is coarser than the row fraction above and is reported with a different denominator name so the two are not confused.

```sql
WITH latest AS (
    SELECT MAX(window.end) AS window_end
    FROM {{ monitor_schema }}.{{ asset }}_profile_metrics
    WHERE log_type = 'INPUT' AND slice_key IS NULL
),
numeric_cols AS (
    SELECT column_name
    FROM {{ catalog }}.information_schema.columns
    WHERE LOWER(table_schema) = LOWER('{{ schema }}')
      AND LOWER(table_name)   = LOWER('{{ asset }}')
      AND data_type IN ('TINYINT', 'SMALLINT', 'INT', 'BIGINT', 'FLOAT', 'DOUBLE', 'DECIMAL')
      AND NOT REGEXP_LIKE(LOWER(column_name), '(^id$|_id$|_key$)')
),
scored AS (
    SELECT
        p.column_name,
        p.stddev > 0 AND (
            (p.max - p.avg) > {{ z_threshold }} * p.stddev OR
            (p.avg - p.min) > {{ z_threshold }} * p.stddev
        ) AS has_outliers
    FROM {{ monitor_schema }}.{{ asset }}_profile_metrics p
    JOIN latest l ON p.window.end = l.window_end
    JOIN numeric_cols n ON LOWER(n.column_name) = LOWER(p.column_name)
    WHERE p.log_type = 'INPUT' AND p.slice_key IS NULL
      AND p.stddev IS NOT NULL
)
SELECT
    '{{ asset }}'                                           AS table_name,
    (SELECT window_end FROM latest)                         AS as_of,
    COUNT_IF(NOT has_outliers)                              AS columns_without_outliers,
    COUNT(*)                                                AS numeric_columns_measured,
    COUNT_IF(NOT has_outliers)::DOUBLE / NULLIF(COUNT(*), 0) AS value
FROM scored
```

`avg`, `stddev`, `min`, `max` are documented profile-metric columns; `DESCRIBE {{ monitor_schema }}.{{ asset }}_profile_metrics` confirms their names on your workspace if the monitor is old.
