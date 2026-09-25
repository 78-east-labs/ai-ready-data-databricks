# Check: temporal_referential_integrity

Fraction of rows in `{{ asset }}` whose `{{ timestamp_column }}` is non-null, not in the future, and not before `{{ min_valid_timestamp }}`.

## Context

Validates that the event timestamps a table carries are plausible. Three conditions, all required for a row to count as valid: the value is not NULL, it is at or before `current_timestamp()`, and it is at or after `{{ min_valid_timestamp }}` (default `1900-01-01`, supplied as an ISO date or timestamp literal). Rows failing any condition are invalid. A score of 1.0 means every row has a usable event time, which is what point-in-time joins, freshness windows and time-travel training splits all assume.

This is a **data** check (strength: data, mode: sql). It scans the table's rows, so it costs a full read of one column; the sampled variant caps that. `{{ timestamp_column }}` is required. If it is not provided, pick it from `information_schema.columns` with the discovery query below: prefer the `TIMESERIES` key column if the table has one (`point_in_time_correctness`), then a `TIMESTAMP`, `TIMESTAMP_NTZ` or `DATE` column whose name matches `event|_at$|_ts$|_time$|timestamp`.

The column is coerced with `TRY_CAST(... AS TIMESTAMP)` so the check also works on `DATE`, `TIMESTAMP_NTZ` and on `STRING` columns holding ISO timestamps; a string that does not parse becomes NULL and fails, which is the right outcome. Epoch sentinels (`1970-01-01 00:00:00`) pass the range test because they are after 1900; the diagnostic flags them separately, and a stricter run can set `{{ min_valid_timestamp }}` to `1971-01-01`. Timezone: `current_timestamp()` is in the session time zone; `TIMESTAMP_NTZ` columns written in a different zone can look up to a day in the future or past. For those, allow a grace window by comparing against `current_timestamp() + INTERVAL 1 DAY`, and say so in the report.

Returns NULL (N/A) when the table has no rows.

## SQL

### Full scan (primary)

```sql
WITH scored AS (
    SELECT
        TRY_CAST({{ timestamp_column }} AS TIMESTAMP) AS ts
    FROM {{ catalog }}.{{ schema }}.{{ asset }}
),
counts AS (
    SELECT
        COUNT(*) AS total_rows,
        COUNT_IF(
            ts IS NOT NULL
            AND ts <= current_timestamp()
            AND ts >= TIMESTAMP '{{ min_valid_timestamp }}'
        ) AS valid_rows
    FROM scored
)
SELECT
    valid_rows,
    total_rows,
    valid_rows::DOUBLE / NULLIF(total_rows, 0) AS value
FROM counts
```

### Sampled (variant)

Same predicate over `TABLESAMPLE ({{ sample_rows }} ROWS)` (default 1,000,000). The sample is a prefix of the table's file order, not a uniform random sample, so a table loaded chronologically will over-represent old rows. Use `TABLESAMPLE (10 PERCENT)` when a spread across files matters more than a bounded row count.

```sql
WITH scored AS (
    SELECT
        TRY_CAST({{ timestamp_column }} AS TIMESTAMP) AS ts
    FROM {{ catalog }}.{{ schema }}.{{ asset }} TABLESAMPLE ({{ sample_rows }} ROWS)
),
counts AS (
    SELECT
        COUNT(*) AS total_rows,
        COUNT_IF(
            ts IS NOT NULL
            AND ts <= current_timestamp()
            AND ts >= TIMESTAMP '{{ min_valid_timestamp }}'
        ) AS valid_rows
    FROM scored
)
SELECT
    valid_rows,
    total_rows,
    valid_rows::DOUBLE / NULLIF(total_rows, 0) AS value
FROM counts
```

### Every candidate timestamp column at once (variant)

When the table has several time columns and no single one has been designated, score them all in one scan and let the orchestrator pick the designated column's row or report the minimum. Generate the projection from `information_schema.columns`:

```sql
SELECT concat(
    'SELECT ''', column_name, ''' AS column_name, ',
    'COUNT(*) AS total_rows, ',
    'COUNT_IF(TRY_CAST(`', column_name, '` AS TIMESTAMP) IS NOT NULL ',
    'AND TRY_CAST(`', column_name, '` AS TIMESTAMP) <= current_timestamp() ',
    'AND TRY_CAST(`', column_name, '` AS TIMESTAMP) >= TIMESTAMP ''{{ min_valid_timestamp }}'') AS valid_rows, ',
    'COUNT_IF(TRY_CAST(`', column_name, '` AS TIMESTAMP) IS NOT NULL ',
    'AND TRY_CAST(`', column_name, '` AS TIMESTAMP) <= current_timestamp() ',
    'AND TRY_CAST(`', column_name, '` AS TIMESTAMP) >= TIMESTAMP ''{{ min_valid_timestamp }}'')::DOUBLE ',
    '/ NULLIF(COUNT(*), 0) AS value ',
    'FROM {{ catalog }}.{{ schema }}.{{ asset }}'
) AS stmt
FROM {{ catalog }}.information_schema.columns
WHERE LOWER(table_schema) = LOWER('{{ schema }}')
  AND LOWER(table_name)   = LOWER('{{ asset }}')
  AND UPPER(data_type) IN ('TIMESTAMP', 'TIMESTAMP_NTZ', 'DATE')
ORDER BY ordinal_position
```

Join the emitted statements with `UNION ALL` and run once.

### Discover the timestamp column (helper)

```sql
WITH ts_key AS (
    SELECT k.column_name
    FROM {{ catalog }}.information_schema.table_constraints tc
    JOIN {{ catalog }}.information_schema.key_column_usage k
      ON k.constraint_name = tc.constraint_name AND k.table_schema = tc.table_schema AND k.table_name = tc.table_name
    JOIN {{ catalog }}.information_schema.columns c
      ON c.table_schema = k.table_schema AND c.table_name = k.table_name AND c.column_name = k.column_name
    WHERE LOWER(tc.table_schema) = LOWER('{{ schema }}')
      AND LOWER(tc.table_name)   = LOWER('{{ asset }}')
      AND tc.constraint_type = 'PRIMARY KEY'
      AND UPPER(c.data_type) IN ('TIMESTAMP', 'TIMESTAMP_NTZ', 'DATE')
)
SELECT column_name, data_type, is_nullable,
       CASE
           WHEN column_name IN (SELECT column_name FROM ts_key)                                   THEN 1
           WHEN REGEXP_LIKE(LOWER(column_name), 'event|_at$|_ts$|_time$|timestamp')               THEN 2
           WHEN REGEXP_LIKE(LOWER(column_name), 'created|updated|modified|effective|valid_from')  THEN 3
           ELSE 4
       END AS preference
FROM {{ catalog }}.information_schema.columns
WHERE LOWER(table_schema) = LOWER('{{ schema }}')
  AND LOWER(table_name)   = LOWER('{{ asset }}')
  AND UPPER(data_type) IN ('TIMESTAMP', 'TIMESTAMP_NTZ', 'DATE')
ORDER BY preference, ordinal_position
LIMIT 1
```

The primary-key join cannot see the `TIMESERIES` flag (see `point_in_time_correctness`), so a time-typed key column is preferred whether or not it carries the flag.
