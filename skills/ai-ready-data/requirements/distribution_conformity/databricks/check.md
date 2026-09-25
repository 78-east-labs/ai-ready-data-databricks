# Check: distribution_conformity

Fraction of a table's monitored numeric columns whose current distribution is within the declared drift tolerance of their baseline.

## Context

Table-scoped. `value = columns_within_tolerance / numeric_columns_measured`.

Strength is **native** when a Lakehouse Monitoring monitor exists on the table, **data** otherwise.

**Primary signal: Lakehouse Monitoring drift metrics.** A monitor writes `{{ monitor_schema }}.{{ asset }}_drift_metrics` with one row per column per window comparison. Numeric columns carry `ks_test` (struct with `statistic` and `pvalue`) and `wasserstein_distance`; categorical columns carry `js_distance` and `chi_squared_test`. The check uses `COALESCE(ks_test.statistic, js_distance)` as the drift statistic because both live in `[0, 1]` and both measure distance between the two distributions, so one tolerance applies to either. `drift_type = 'BASELINE'` compares against the baseline table configured on the monitor; `'CONSECUTIVE'` compares against the previous window and is what you get when no baseline was configured. The number is as fresh as the monitor's last refresh; `window.end` in the output says when.

**Fallback: compare against a Delta version.** With no monitor, the table's own history is the baseline. The variant computes mean and stddev per numeric column now and at `VERSION AS OF {{ baseline_version }}` (or `TIMESTAMP AS OF`), and passes a column when the standardized mean shift and the relative stddev change are both within `{{ mean_shift_tolerance }}`. That is a weaker test than KS (it misses shape changes with the same moments) and it says nothing when the baseline version has been vacuumed away. `DESCRIBE HISTORY` lists the versions still available; `delta.logRetentionDuration` (30 days by default) bounds how far back you can go.

Placeholders:

- `{{ drift_tolerance }}`: maximum drift statistic. Default 0.1.
- `{{ monitor_schema }}`: schema of the monitor output tables. Default `{{ catalog }}.{{ schema }}`.
- `{{ drift_type }}`: `BASELINE` (default) or `CONSECUTIVE`. The primary query falls back to `CONSECUTIVE` automatically when no `BASELINE` rows exist.
- `{{ baseline_version }}`: Delta version for the fallback. No default; pick from `DESCRIBE HISTORY`.
- `{{ mean_shift_tolerance }}`: fallback threshold in stddev units. Default 0.25.

Columns are restricted to numeric types from `information_schema.columns` so that identifier-like BIGINT columns still count (drift in an ID column is noise; exclude them by name in `column_filter` if it matters). Boolean and string columns are out of scope for this requirement.

Returns NULL when the table has no monitor output (primary) or no numeric columns (fallback).

## SQL

### Lakehouse Monitoring drift metrics (primary)

```sql
WITH numeric_cols AS (
    SELECT column_name
    FROM {{ catalog }}.information_schema.columns
    WHERE LOWER(table_schema) = LOWER('{{ schema }}')
      AND LOWER(table_name)   = LOWER('{{ asset }}')
      AND data_type IN ('TINYINT', 'SMALLINT', 'INT', 'BIGINT', 'FLOAT', 'DOUBLE', 'DECIMAL')
),
drift AS (
    SELECT
        d.column_name,
        d.drift_type,
        d.window.end                                        AS window_end,
        COALESCE(d.ks_test.statistic, d.js_distance)        AS drift_stat
    FROM {{ monitor_schema }}.{{ asset }}_drift_metrics d
    WHERE d.slice_key IS NULL
      AND d.column_name <> ':table'
),
chosen_type AS (
    SELECT CASE WHEN (SELECT COUNT(*) FROM drift WHERE drift_type = '{{ drift_type }}') > 0
                THEN '{{ drift_type }}' ELSE 'CONSECUTIVE' END AS drift_type
),
latest AS (
    SELECT MAX(window_end) AS window_end
    FROM drift JOIN chosen_type USING (drift_type)
),
scored AS (
    SELECT
        n.column_name,
        d.drift_stat,
        d.drift_stat <= {{ drift_tolerance }}                AS within_tolerance
    FROM numeric_cols n
    JOIN drift d ON LOWER(d.column_name) = LOWER(n.column_name)
    JOIN chosen_type ct ON d.drift_type = ct.drift_type
    JOIN latest l ON d.window_end = l.window_end
    WHERE d.drift_stat IS NOT NULL
)
SELECT
    '{{ asset }}'                                           AS table_name,
    (SELECT drift_type FROM chosen_type)                    AS drift_type,
    (SELECT window_end FROM latest)                         AS as_of,
    COUNT_IF(within_tolerance)                              AS columns_within_tolerance,
    COUNT(*)                                                AS numeric_columns_measured,
    COUNT_IF(within_tolerance)::DOUBLE / NULLIF(COUNT(*), 0) AS value
FROM scored
```

If the query fails with `TABLE_OR_VIEW_NOT_FOUND`, the table has no monitor; use the fallback. If your monitor output also has `population_stability_index` or `tv_distance` (added to newer monitors), confirm with `DESCRIBE {{ monitor_schema }}.{{ asset }}_drift_metrics` before adding them to the `COALESCE`.

### Compare against a Delta version (variant)

Two hops: the first query emits a statement that computes both snapshots' moments for every numeric column in one pass each; running the emitted statement returns the score.

```sql
WITH numeric_cols AS (
    SELECT column_name, ordinal_position
    FROM {{ catalog }}.information_schema.columns
    WHERE LOWER(table_schema) = LOWER('{{ schema }}')
      AND LOWER(table_name)   = LOWER('{{ asset }}')
      AND data_type IN ('TINYINT', 'SMALLINT', 'INT', 'BIGINT', 'FLOAT', 'DOUBLE', 'DECIMAL')
      AND NOT REGEXP_LIKE(LOWER(column_name), '(^id$|_id$|_key$)')
),
exprs AS (
    SELECT
        array_join(transform(array_sort(collect_list(struct(ordinal_position, column_name))),
            s -> concat('named_struct(''column_name'', ''', s.column_name,
                        ''', ''mean'', AVG(`', s.column_name, '`)::DOUBLE, ''stddev'', STDDEV(`', s.column_name, '`)::DOUBLE)')),
            ', ') AS struct_list
    FROM numeric_cols
)
SELECT concat(
    'WITH cur AS (SELECT explode(array(', struct_list, ')) AS s FROM {{ catalog }}.{{ schema }}.`{{ asset }}`), ',
    'base AS (SELECT explode(array(', struct_list, ')) AS s FROM {{ catalog }}.{{ schema }}.`{{ asset }}` VERSION AS OF {{ baseline_version }}), ',
    'scored AS (SELECT c.s.column_name, ',
    'ABS(c.s.mean - b.s.mean) / NULLIF(b.s.stddev, 0) AS mean_shift, ',
    'ABS(c.s.stddev - b.s.stddev) / NULLIF(b.s.stddev, 0) AS stddev_change ',
    'FROM cur c JOIN base b ON c.s.column_name = b.s.column_name WHERE b.s.stddev IS NOT NULL) ',
    'SELECT ''{{ asset }}'' AS table_name, ''VERSION {{ baseline_version }}'' AS baseline, ',
    'COUNT_IF(mean_shift <= {{ mean_shift_tolerance }} AND stddev_change <= {{ mean_shift_tolerance }}) AS columns_within_tolerance, ',
    'COUNT(*) AS numeric_columns_measured, ',
    'COUNT_IF(mean_shift <= {{ mean_shift_tolerance }} AND stddev_change <= {{ mean_shift_tolerance }})::DOUBLE / NULLIF(COUNT(*), 0) AS value ',
    'FROM scored'
) AS stmt
FROM exprs
```

Replace `VERSION AS OF {{ baseline_version }}` with `TIMESTAMP AS OF '{{ baseline_timestamp }}'`, or with a separate `{{ baseline_table }}` (a saved snapshot, a training set), as needed. Columns whose baseline stddev is zero or NULL (constants, all-NULL) are excluded from the denominator because a standardized shift is undefined for them.

### Single column against explicit baseline moments (variant)

For one column with a documented baseline (from a model card, a data sheet). Column-scoped; the orchestrator aggregates. `value` here is 1.0 or 0.0 for the column.

```sql
WITH cur AS (
    SELECT AVG({{ column }})::DOUBLE AS mean, STDDEV({{ column }})::DOUBLE AS stddev
    FROM {{ catalog }}.{{ schema }}.{{ asset }}
    WHERE {{ column }} IS NOT NULL
)
SELECT
    '{{ asset }}'                                                       AS table_name,
    '{{ column }}'                                                      AS column_name,
    ABS(mean - {{ baseline_mean }}) / NULLIF({{ baseline_stddev }}, 0)  AS mean_shift,
    ABS(stddev - {{ baseline_stddev }}) / NULLIF({{ baseline_stddev }}, 0) AS stddev_change,
    COUNT_IF(ABS(mean - {{ baseline_mean }}) / NULLIF({{ baseline_stddev }}, 0) <= {{ mean_shift_tolerance }}
         AND ABS(stddev - {{ baseline_stddev }}) / NULLIF({{ baseline_stddev }}, 0) <= {{ mean_shift_tolerance }}) AS columns_within_tolerance,
    COUNT(*)                                                            AS numeric_columns_measured,
    COUNT_IF(ABS(mean - {{ baseline_mean }}) / NULLIF({{ baseline_stddev }}, 0) <= {{ mean_shift_tolerance }}
         AND ABS(stddev - {{ baseline_stddev }}) / NULLIF({{ baseline_stddev }}, 0) <= {{ mean_shift_tolerance }})::DOUBLE
        / NULLIF(COUNT(*), 0)                                           AS value
FROM cur
GROUP BY mean, stddev
```
