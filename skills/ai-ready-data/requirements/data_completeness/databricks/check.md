# Check: data_completeness

Fraction of non-null values in a column (1.0 = no NULLs).

## Context

Column-scoped data scan. `value = 1 - null_rows / total_rows`. This is the plain SQL `IS NULL` test; empty strings, `'N/A'`, `-1` and other sentinels count as present. The diagnostic reports those separately so you can decide whether to treat them as missing.

Strength is **data**. Two Databricks signals shortcut the scan when they exist, and both are offered as variants:

- **Column-level `NOT NULL`.** `information_schema.columns.is_nullable = 'NO'` is enforced by Delta, so the column is complete by construction and the scan can be skipped (value 1.0 with `total_rows` from the table if you want the counts).
- **Lakehouse Monitoring profile metrics.** If a monitor exists on the table, `{{ monitor_schema }}.{{ asset }}_profile_metrics` already holds `count` and `num_nulls` per column per window, refreshed on the monitor's schedule. No scan, but the number is as old as the last refresh.

Placeholders:

- `{{ sample_rows }}`: default 1,000,000.
- `{{ monitor_schema }}`: schema holding the monitor's output tables. Default `{{ catalog }}.{{ schema }}` (the monitor default when created from Catalog Explorer).

For nested columns pass the path (`address.postal_code`); `IS NULL` on a struct field works. For MAP or ARRAY columns `IS NULL` tests the container only; use `size(col) = 0` in the predicate if an empty collection should count as missing. VARIANT columns: `col IS NULL` is SQL NULL; a JSON `null` inside the variant is `is_variant_null(col)` (DBR 15.3+).

`TABLESAMPLE (n ROWS)` returns the first `n` rows the scan produces, not a random sample. Fine for triage, not for gating a `SET NOT NULL`.

Returns NULL when the table is empty.

## SQL

### Full scan (primary)

```sql
SELECT
    '{{ asset }}'                                   AS table_name,
    '{{ column }}'                                  AS column_name,
    COUNT_IF({{ column }} IS NULL)                  AS null_rows,
    COUNT(*)                                        AS total_rows,
    1.0 - COUNT_IF({{ column }} IS NULL)::DOUBLE
        / NULLIF(COUNT(*), 0)                       AS value
FROM {{ catalog }}.{{ schema }}.{{ asset }}
```

### Sampled (variant)

```sql
SELECT
    '{{ asset }}'                                   AS table_name,
    '{{ column }}'                                  AS column_name,
    COUNT_IF({{ column }} IS NULL)                  AS null_rows,
    COUNT(*)                                        AS total_rows,
    1.0 - COUNT_IF({{ column }} IS NULL)::DOUBLE
        / NULLIF(COUNT(*), 0)                       AS value
FROM {{ catalog }}.{{ schema }}.{{ asset }} TABLESAMPLE ({{ sample_rows }} ROWS)
```

### Skip columns declared NOT NULL (variant)

Run before the scan. A row back means the column is enforced non-null and the check can record 1.0 without reading data.

```sql
SELECT
    '{{ asset }}'                                   AS table_name,
    column_name,
    is_nullable,
    CASE WHEN is_nullable = 'NO' THEN 1.0 END       AS value
FROM {{ catalog }}.information_schema.columns
WHERE LOWER(table_schema) = LOWER('{{ schema }}')
  AND LOWER(table_name)   = LOWER('{{ asset }}')
  AND LOWER(column_name)  = LOWER('{{ column }}')
  AND is_nullable = 'NO'
```

### Lakehouse Monitoring profile metrics (variant)

Reads the latest window of the monitor's profile table, whole-table slice only. `log_type = 'INPUT'` excludes the baseline table's row when one is configured. The profile table has `count` and `num_nulls` per column; `count` is the number of rows in the window, including nulls.

```sql
WITH latest AS (
    SELECT MAX(window.end) AS window_end
    FROM {{ monitor_schema }}.{{ asset }}_profile_metrics
    WHERE log_type = 'INPUT' AND slice_key IS NULL
)
SELECT
    '{{ asset }}'                                   AS table_name,
    p.column_name,
    p.num_nulls                                     AS null_rows,
    p.count                                         AS total_rows,
    1.0 - p.num_nulls::DOUBLE / NULLIF(p.count, 0)  AS value,
    p.window.end                                    AS as_of
FROM {{ monitor_schema }}.{{ asset }}_profile_metrics p
JOIN latest l ON p.window.end = l.window_end
WHERE p.log_type = 'INPUT'
  AND p.slice_key IS NULL
  AND LOWER(p.column_name) = LOWER('{{ column }}')
```

If the table is not monitored the query fails with `TABLE_OR_VIEW_NOT_FOUND`; fall back to the full scan. Confirm the column set with `DESCRIBE {{ monitor_schema }}.{{ asset }}_profile_metrics` if your monitor was created before 2024; older output tables may lack `log_type`.
