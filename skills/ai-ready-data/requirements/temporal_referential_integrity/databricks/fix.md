# Fix: temporal_referential_integrity

Repair invalid event timestamps by class, and stop new ones at the write path.

## Context

There is no single fix because each failure class has a different cause. The diagnostic's class breakdown decides which of the sections below apply:

- **NULL_TIMESTAMP**: the writer did not populate the column. Backfill from a source audit column if one exists, from ingestion metadata (`_metadata.file_modification_time` on file-loaded tables) if not, and only as a last resort from the commit time (`DESCRIBE HISTORY` timestamp via Change Data Feed).
- **FUTURE_TIMESTAMP**: almost always a unit or timezone error. Milliseconds cast as seconds land in the year 50000+; a `TIMESTAMP_NTZ` written in UTC+14 and compared in UTC is up to 14 hours ahead. Fix the conversion, not the rows, unless the rows are the only copy.
- **ANCIENT_TIMESTAMP** and **EPOCH_TIMESTAMP**: sentinel or default values (`0001-01-01`, `1900-01-01`, `1970-01-01`). Set them to NULL so they fail loudly, then treat as NULL_TIMESTAMP.
- **UNPARSEABLE**: strings in a non-ISO format. Fix the parse in the pipeline; for existing rows, `try_to_timestamp(col, '<format>')` with the real format.

Every data-mutating statement below is preceded by the blast-radius count on the same predicate. Updates on a Delta table create a new version and, with Change Data Feed on, `update_preimage` / `update_postimage` rows; that is how the change stays auditable. Do not fix timestamps with `CREATE OR REPLACE TABLE ... AS SELECT`.

After any backfill, put a `CHECK` constraint on the column so the class cannot come back; that section is at the end.

## Fix: Replace sentinel and epoch values with NULL

Blast radius:

```sql
SELECT COUNT(*) AS rows_affected
FROM {{ catalog }}.{{ schema }}.{{ asset }}
WHERE TRY_CAST({{ timestamp_column }} AS TIMESTAMP) < TIMESTAMP '{{ min_valid_timestamp }}'
   OR TRY_CAST({{ timestamp_column }} AS TIMESTAMP) = TIMESTAMP '1970-01-01 00:00:00'
```

Then:

```sql
UPDATE {{ catalog }}.{{ schema }}.{{ asset }}
SET {{ timestamp_column }} = NULL
WHERE TRY_CAST({{ timestamp_column }} AS TIMESTAMP) < TIMESTAMP '{{ min_valid_timestamp }}'
   OR TRY_CAST({{ timestamp_column }} AS TIMESTAMP) = TIMESTAMP '1970-01-01 00:00:00'
```

This lowers the score for `EPOCH_TIMESTAMP` rows (they were counted valid) and is still the right move: a NULL is honest, an epoch is a lie that point-in-time joins will believe.

## Fix: Backfill NULLs from another column on the same table

Use when a `created_at`, `loaded_at`, `updated_at` or source audit column holds a usable value. `{{ source_timestamp_expression }}` is that column or an expression over it.

```sql
SELECT COUNT(*) AS rows_affected
FROM {{ catalog }}.{{ schema }}.{{ asset }}
WHERE {{ timestamp_column }} IS NULL
  AND TRY_CAST({{ source_timestamp_expression }} AS TIMESTAMP) IS NOT NULL
```

```sql
UPDATE {{ catalog }}.{{ schema }}.{{ asset }}
SET {{ timestamp_column }} = TRY_CAST({{ source_timestamp_expression }} AS TIMESTAMP)
WHERE {{ timestamp_column }} IS NULL
  AND TRY_CAST({{ source_timestamp_expression }} AS TIMESTAMP) IS NOT NULL
```

## Fix: Backfill NULLs from an upstream table

Use when the source system still has the row. `{{ key_column }}` is the shared key.

```sql
SELECT COUNT(*) AS rows_affected
FROM {{ catalog }}.{{ schema }}.{{ asset }} t
JOIN {{ source_catalog }}.{{ source_schema }}.{{ source_asset }} s
  ON s.{{ key_column }} = t.{{ key_column }}
WHERE t.{{ timestamp_column }} IS NULL
  AND s.{{ source_timestamp_column }} IS NOT NULL
```

```sql
MERGE INTO {{ catalog }}.{{ schema }}.{{ asset }} AS t
USING (
    SELECT {{ key_column }}, MAX({{ source_timestamp_column }}) AS src_ts
    FROM {{ source_catalog }}.{{ source_schema }}.{{ source_asset }}
    WHERE {{ source_timestamp_column }} IS NOT NULL
    GROUP BY {{ key_column }}
) AS s
  ON t.{{ key_column }} = s.{{ key_column }} AND t.{{ timestamp_column }} IS NULL
WHEN MATCHED THEN UPDATE SET t.{{ timestamp_column }} = s.src_ts
```

## Fix: Backfill NULLs from file or commit metadata (last resort)

For tables loaded from files, each row's source file modification time is the best available proxy. It is a load time, not an event time; record that in the column comment.

```sql
MERGE INTO {{ catalog }}.{{ schema }}.{{ asset }} AS t
USING (
    SELECT {{ key_column }}, _metadata.file_modification_time AS file_ts
    FROM {{ catalog }}.{{ schema }}.{{ asset }}
    WHERE {{ timestamp_column }} IS NULL
) AS s
  ON t.{{ key_column }} = s.{{ key_column }} AND t.{{ timestamp_column }} IS NULL
WHEN MATCHED THEN UPDATE SET t.{{ timestamp_column }} = s.file_ts
```

```sql
COMMENT ON COLUMN {{ catalog }}.{{ schema }}.{{ asset }}.{{ timestamp_column }}
IS 'Event time. Rows backfilled on {{ today }} from file modification time where the source value was missing; treat those as load time.'
```

## Fix: Correct a unit error on future timestamps

When the diagnostic's `FUTURE_TIMESTAMP` max is thousands of years out, the source was in milliseconds and was cast as seconds. Blast radius:

```sql
SELECT COUNT(*) AS rows_affected
FROM {{ catalog }}.{{ schema }}.{{ asset }}
WHERE TRY_CAST({{ timestamp_column }} AS TIMESTAMP) > TIMESTAMP '2100-01-01'
```

```sql
UPDATE {{ catalog }}.{{ schema }}.{{ asset }}
SET {{ timestamp_column }} = timestamp_millis(CAST(unix_timestamp({{ timestamp_column }}) AS BIGINT))
WHERE TRY_CAST({{ timestamp_column }} AS TIMESTAMP) > TIMESTAMP '2100-01-01'
```

This assumes the column holds `TIMESTAMP` values that were built from a millisecond count interpreted as seconds; `unix_timestamp()` recovers the original number and `timestamp_millis()` reinterprets it. Verify on the sample rows from the diagnostic before running it. For timezone shifts of a few hours, fix the writer's session zone and leave the rows; a blanket shift risks double-correcting rows that were right.

## Fix: Prevent recurrence with a CHECK constraint

Delta enforces `CHECK` constraints on write, so a future write with a NULL, ancient or future timestamp fails instead of landing. Adding the constraint validates existing rows first and fails if any violate, so run it only after the backfill.

Guard:

```sql
SELECT constraint_name
FROM {{ catalog }}.information_schema.table_constraints
WHERE LOWER(table_schema) = LOWER('{{ schema }}')
  AND LOWER(table_name)   = LOWER('{{ asset }}')
  AND constraint_name = '{{ asset }}_{{ timestamp_column }}_valid'
```

Skip if a row exists. Otherwise:

```sql
ALTER TABLE {{ catalog }}.{{ schema }}.{{ asset }}
ALTER COLUMN {{ timestamp_column }} SET NOT NULL;

ALTER TABLE {{ catalog }}.{{ schema }}.{{ asset }}
ADD CONSTRAINT {{ asset }}_{{ timestamp_column }}_valid
CHECK ({{ timestamp_column }} >= TIMESTAMP '{{ min_valid_timestamp }}'
       AND {{ timestamp_column }} <= current_timestamp() + INTERVAL 1 DAY);
```

The one-day grace on the upper bound tolerates clock skew and `TIMESTAMP_NTZ` written ahead of UTC. `current_timestamp()` inside a `CHECK` is evaluated at write time, which is the intent: it rejects future-dated writes as of when they happen. If the warehouse rejects a non-deterministic function in a `CHECK` constraint, drop the upper bound and keep the lower one; the check will still catch future values on the next run.

## Organizational guidance

Bad timestamps are made at ingestion. Put the validation there: Lakeflow expectations (`CONSTRAINT valid_event_time EXPECT (event_time IS NOT NULL AND event_time <= current_timestamp()) ON VIOLATION DROP ROW` or `FAIL UPDATE`), an explicit parse with a format string instead of implicit casts, and a single documented time zone (UTC, `TIMESTAMP` not `TIMESTAMP_NTZ`) for every event-time column. Tag the designated event-time column (`temporal_scope` on the table, a `unit`-style column tag such as `time_role = 'event_time'`) so downstream checks and feature tables agree on which column is the event time.
