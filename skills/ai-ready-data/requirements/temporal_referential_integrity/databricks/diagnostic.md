# Diagnostic: temporal_referential_integrity

Breakdown of invalid timestamps in `{{ asset }}.{{ timestamp_column }}` by failure class, with sample rows and, where lineage allows, the writer that produced them.

## Context

Three queries. The first aggregates rows into classes so the operator knows the shape of the problem before touching data: `NULL_TIMESTAMP`, `FUTURE_TIMESTAMP`, `ANCIENT_TIMESTAMP` (before `{{ min_valid_timestamp }}`), `EPOCH_TIMESTAMP` (exactly `1970-01-01 00:00:00`, a common uninitialized value that passes the range test), `UNPARSEABLE` (string that does not cast) and `VALID`. It also reports the min and max per class, which tells apart "one row in year 9999" from "everything is shifted by a timezone".

The second returns up to 100 offending rows with `{{ key_columns }}` for follow-up. `{{ key_columns }}` is the table's primary key column list; take it from `information_schema.key_column_usage` (query included) or pass `*` to see whole rows.

The third correlates the invalid rows with the commits that added them, when the table has Change Data Feed enabled (`change_detection`). It reads `table_changes()` over the last `{{ lookback_days }}` days and groups invalid inserts by `_commit_version`, then looks up the writer for those versions in `DESCRIBE HISTORY`. This is the fastest route to the pipeline that emits bad timestamps.

Sorted worst-first: classes by row count, sample rows with NULLs first.

## SQL

### Failure classes

```sql
WITH scored AS (
    SELECT
        {{ timestamp_column }}                          AS raw_value,
        TRY_CAST({{ timestamp_column }} AS TIMESTAMP)   AS ts
    FROM {{ catalog }}.{{ schema }}.{{ asset }}
),
classified AS (
    SELECT
        ts,
        CASE
            WHEN raw_value IS NULL                                 THEN 'NULL_TIMESTAMP'
            WHEN ts IS NULL                                        THEN 'UNPARSEABLE'
            WHEN ts > current_timestamp()                          THEN 'FUTURE_TIMESTAMP'
            WHEN ts < TIMESTAMP '{{ min_valid_timestamp }}'        THEN 'ANCIENT_TIMESTAMP'
            WHEN ts = TIMESTAMP '1970-01-01 00:00:00'              THEN 'EPOCH_TIMESTAMP'
            ELSE 'VALID'
        END AS timestamp_status
    FROM scored
)
SELECT
    timestamp_status,
    COUNT(*)                                                       AS row_count,
    ROUND(COUNT(*)::DOUBLE / SUM(COUNT(*)) OVER (), 4)             AS share_of_rows,
    MIN(ts)                                                        AS min_value,
    MAX(ts)                                                        AS max_value,
    CASE timestamp_status
        WHEN 'NULL_TIMESTAMP'    THEN 'Missing event time; backfill from source or ingestion metadata'
        WHEN 'UNPARSEABLE'       THEN 'String does not parse as a timestamp; fix the format at the source'
        WHEN 'FUTURE_TIMESTAMP'  THEN 'Ahead of now; timezone conversion or unit error (ms vs s) likely'
        WHEN 'ANCIENT_TIMESTAMP' THEN 'Before the declared epoch; placeholder or default value'
        WHEN 'EPOCH_TIMESTAMP'   THEN 'Unix epoch zero; uninitialized value, counted as valid by the check'
        ELSE 'Valid'
    END                                                            AS recommendation
FROM classified
GROUP BY timestamp_status
ORDER BY CASE timestamp_status WHEN 'VALID' THEN 1 ELSE 0 END, row_count DESC
```

### Sample offending rows

Primary key columns for `{{ key_columns }}`:

```sql
SELECT array_join(collect_list(k.column_name), ', ') AS key_columns
FROM {{ catalog }}.information_schema.table_constraints tc
JOIN {{ catalog }}.information_schema.key_column_usage k
  ON k.constraint_name = tc.constraint_name AND k.table_schema = tc.table_schema AND k.table_name = tc.table_name
WHERE LOWER(tc.table_schema) = LOWER('{{ schema }}')
  AND LOWER(tc.table_name)   = LOWER('{{ asset }}')
  AND tc.constraint_type = 'PRIMARY KEY'
```

Then:

```sql
SELECT
    {{ key_columns }},
    {{ timestamp_column }}                          AS raw_value,
    TRY_CAST({{ timestamp_column }} AS TIMESTAMP)   AS parsed_value,
    CASE
        WHEN {{ timestamp_column }} IS NULL                                                THEN 'NULL_TIMESTAMP'
        WHEN TRY_CAST({{ timestamp_column }} AS TIMESTAMP) IS NULL                         THEN 'UNPARSEABLE'
        WHEN TRY_CAST({{ timestamp_column }} AS TIMESTAMP) > current_timestamp()           THEN 'FUTURE_TIMESTAMP'
        WHEN TRY_CAST({{ timestamp_column }} AS TIMESTAMP) < TIMESTAMP '{{ min_valid_timestamp }}' THEN 'ANCIENT_TIMESTAMP'
        WHEN TRY_CAST({{ timestamp_column }} AS TIMESTAMP) = TIMESTAMP '1970-01-01 00:00:00' THEN 'EPOCH_TIMESTAMP'
    END                                             AS timestamp_status
FROM {{ catalog }}.{{ schema }}.{{ asset }}
WHERE {{ timestamp_column }} IS NULL
   OR TRY_CAST({{ timestamp_column }} AS TIMESTAMP) IS NULL
   OR TRY_CAST({{ timestamp_column }} AS TIMESTAMP) > current_timestamp()
   OR TRY_CAST({{ timestamp_column }} AS TIMESTAMP) < TIMESTAMP '{{ min_valid_timestamp }}'
   OR TRY_CAST({{ timestamp_column }} AS TIMESTAMP) = TIMESTAMP '1970-01-01 00:00:00'
ORDER BY parsed_value NULLS FIRST
LIMIT 100
```

### Which commits introduced the invalid rows (requires Change Data Feed)

```sql
WITH bad_inserts AS (
    SELECT _commit_version, _commit_timestamp, COUNT(*) AS invalid_rows
    FROM table_changes('{{ catalog }}.{{ schema }}.{{ asset }}',
                       current_timestamp() - INTERVAL {{ lookback_days }} DAYS)
    WHERE _change_type IN ('insert', 'update_postimage')
      AND (
            {{ timestamp_column }} IS NULL
         OR TRY_CAST({{ timestamp_column }} AS TIMESTAMP) IS NULL
         OR TRY_CAST({{ timestamp_column }} AS TIMESTAMP) > current_timestamp()
         OR TRY_CAST({{ timestamp_column }} AS TIMESTAMP) < TIMESTAMP '{{ min_valid_timestamp }}'
      )
    GROUP BY _commit_version, _commit_timestamp
)
SELECT *
FROM bad_inserts
ORDER BY invalid_rows DESC, _commit_version DESC
LIMIT 50
```

`table_changes()` accepts a starting timestamp as its second argument; it errors if that timestamp is before CDF was enabled, in which case pass the enabling version instead (from `change_detection/databricks/diagnostic.md`). Then map versions to writers:

```sql
SELECT version, timestamp, operation, userName, job.jobId AS job_id, notebook.notebookId AS notebook_id
FROM (DESCRIBE HISTORY {{ catalog }}.{{ schema }}.{{ asset }})
WHERE version IN ({{ commit_versions }})
ORDER BY version DESC
```

`{{ lookback_days }}` defaults to 7. Without CDF, the second query's sample rows plus the table's `_metadata.file_modification_time` (select `_metadata.file_path, _metadata.file_modification_time` alongside the key columns) give a coarser pointer to when the bad rows landed.
