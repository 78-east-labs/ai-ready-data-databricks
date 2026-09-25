# Diagnostic: distribution_conformity

Per-column drift statistics from the monitor (or from a Delta version comparison), a drift trend over recent windows, and a full profile of one column now versus the baseline.

## Context

Three queries:

1. **Per-column drift, latest window.** Every column in the monitor's latest drift window with its KS statistic and p-value, JS distance, Wasserstein distance and a `status` label. Worst-first. This is the check's population with the aggregation removed.
2. **Drift trend.** The drift statistic per column per window for the last `{{ windows }}` (default 12) windows, so you can tell a one-off spike (a bad load) from a steady shift (the population moved). A single window at the tolerance boundary is not a problem; five in a row is.
3. **Column profile now vs baseline.** For one `{{ column }}`: count, nulls, mean, stddev, min, max, p5 / p25 / median / p75 / p95 now and at `VERSION AS OF {{ baseline_version }}`. Works with or without a monitor and is the query to run before deciding whether the drift is real, an outlier problem or a unit change.

Placeholders as in the check, plus `{{ windows }}`.

## SQL

### Per-column drift, latest window

```sql
WITH drift AS (
    SELECT
        d.column_name,
        d.drift_type,
        d.window.start                                      AS window_start,
        d.window.end                                        AS window_end,
        d.ks_test.statistic                                 AS ks_statistic,
        d.ks_test.pvalue                                    AS ks_pvalue,
        d.js_distance,
        d.wasserstein_distance,
        d.chi_squared_test.pvalue                           AS chi2_pvalue,
        COALESCE(d.ks_test.statistic, d.js_distance)        AS drift_stat
    FROM {{ monitor_schema }}.{{ asset }}_drift_metrics d
    WHERE d.slice_key IS NULL
      AND d.column_name <> ':table'
      AND d.drift_type = '{{ drift_type }}'
),
latest AS (SELECT MAX(window_end) AS window_end FROM drift),
col_types AS (
    SELECT column_name, data_type
    FROM {{ catalog }}.information_schema.columns
    WHERE LOWER(table_schema) = LOWER('{{ schema }}')
      AND LOWER(table_name)   = LOWER('{{ asset }}')
)
SELECT
    d.column_name,
    t.data_type,
    d.window_start,
    d.window_end,
    ROUND(d.drift_stat, 4)                                  AS drift_stat,
    ROUND(d.ks_pvalue, 4)                                   AS ks_pvalue,
    ROUND(d.wasserstein_distance, 4)                        AS wasserstein_distance,
    ROUND(d.chi2_pvalue, 4)                                 AS chi2_pvalue,
    CASE
        WHEN d.drift_stat IS NULL                           THEN 'NOT_MEASURED'
        WHEN d.drift_stat > 2 * {{ drift_tolerance }}       THEN 'SEVERE'
        WHEN d.drift_stat > {{ drift_tolerance }}           THEN 'DRIFTED'
        ELSE 'STABLE'
    END                                                     AS status
FROM drift d
JOIN latest l ON d.window_end = l.window_end
LEFT JOIN col_types t ON LOWER(t.column_name) = LOWER(d.column_name)
ORDER BY d.drift_stat DESC NULLS LAST, d.column_name
```

### Drift trend over recent windows

```sql
WITH drift AS (
    SELECT
        d.column_name,
        d.window.end                                        AS window_end,
        COALESCE(d.ks_test.statistic, d.js_distance)        AS drift_stat
    FROM {{ monitor_schema }}.{{ asset }}_drift_metrics d
    WHERE d.slice_key IS NULL
      AND d.column_name <> ':table'
      AND d.drift_type = '{{ drift_type }}'
),
recent_windows AS (
    SELECT DISTINCT window_end
    FROM drift
    ORDER BY window_end DESC
    LIMIT {{ windows }}
)
SELECT
    d.column_name,
    COUNT(*)                                                AS windows_measured,
    COUNT_IF(d.drift_stat > {{ drift_tolerance }})          AS windows_drifted,
    ROUND(AVG(d.drift_stat), 4)                             AS mean_drift_stat,
    ROUND(MAX(d.drift_stat), 4)                             AS max_drift_stat,
    array_sort(collect_list(struct(d.window_end, ROUND(d.drift_stat, 3) AS drift_stat))) AS series
FROM drift d
JOIN recent_windows w ON d.window_end = w.window_end
WHERE d.drift_stat IS NOT NULL
GROUP BY d.column_name
ORDER BY windows_drifted DESC, max_drift_stat DESC
```

### One column now vs a Delta version

```sql
WITH cur AS (
    SELECT
        'current'                                   AS snapshot,
        COUNT(*)                                    AS row_count,
        COUNT_IF({{ column }} IS NULL)              AS null_count,
        AVG({{ column }})                           AS mean,
        STDDEV({{ column }})                        AS stddev,
        MIN({{ column }})                           AS min_value,
        APPROX_PERCENTILE({{ column }}, 0.05)       AS p5,
        APPROX_PERCENTILE({{ column }}, 0.25)       AS p25,
        MEDIAN({{ column }})                        AS median,
        APPROX_PERCENTILE({{ column }}, 0.75)       AS p75,
        APPROX_PERCENTILE({{ column }}, 0.95)       AS p95,
        MAX({{ column }})                           AS max_value
    FROM {{ catalog }}.{{ schema }}.{{ asset }}
),
base AS (
    SELECT
        'version {{ baseline_version }}'            AS snapshot,
        COUNT(*)                                    AS row_count,
        COUNT_IF({{ column }} IS NULL)              AS null_count,
        AVG({{ column }})                           AS mean,
        STDDEV({{ column }})                        AS stddev,
        MIN({{ column }})                           AS min_value,
        APPROX_PERCENTILE({{ column }}, 0.05)       AS p5,
        APPROX_PERCENTILE({{ column }}, 0.25)       AS p25,
        MEDIAN({{ column }})                        AS median,
        APPROX_PERCENTILE({{ column }}, 0.75)       AS p75,
        APPROX_PERCENTILE({{ column }}, 0.95)       AS p95,
        MAX({{ column }})                           AS max_value
    FROM {{ catalog }}.{{ schema }}.{{ asset }} VERSION AS OF {{ baseline_version }}
)
SELECT * FROM cur
UNION ALL
SELECT * FROM base
UNION ALL
SELECT
    'standardized shift',
    c.row_count - b.row_count,
    c.null_count - b.null_count,
    (c.mean - b.mean) / NULLIF(b.stddev, 0),
    (c.stddev - b.stddev) / NULLIF(b.stddev, 0),
    (c.min_value - b.min_value) / NULLIF(b.stddev, 0),
    (c.p5 - b.p5) / NULLIF(b.stddev, 0),
    (c.p25 - b.p25) / NULLIF(b.stddev, 0),
    (c.median - b.median) / NULLIF(b.stddev, 0),
    (c.p75 - b.p75) / NULLIF(b.stddev, 0),
    (c.p95 - b.p95) / NULLIF(b.stddev, 0),
    (c.max_value - b.max_value) / NULLIF(b.stddev, 0)
FROM cur c CROSS JOIN base b
```

Reading the third row: a shift in `mean` with `p25`..`p75` unchanged and `max` moved means outliers (see `outlier_prevalence`); every quantile moved by the same amount means a level shift (a unit or currency change, a new source segment); `median` unchanged but `stddev` up means a widened tail. If the baseline version is gone, `DESCRIBE HISTORY {{ catalog }}.{{ schema }}.{{ asset }}` lists what remains.
