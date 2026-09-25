# Check: record_level_traceability

Fraction of base tables in the schema whose rows can be traced individually: Delta row tracking is enabled (`delta.enableRowTracking = true`), or the table carries an explicit source-record correlation column.

## Context

Two mechanisms, either of which counts:

- **Delta row tracking** (native). With `delta.enableRowTracking = true`, every row gets a stable `_metadata.row_id` that survives `UPDATE` and `MERGE`, and a `_metadata.row_commit_version` that says which commit last touched it. That is row identity inside Delta: you can follow one record across versions and through Change Data Feed. It does not say which source record the row came from. The property is only visible through `DESCRIBE DETAIL` (`properties` map, or `rowTracking` in `tableFeatures`), so this half is a **probe**.
- **Explicit correlation column** (proxy). A column whose name matches `{{ trace_column_pattern }}`, default `^(source_record_id|source_id|_source_id|correlation_id|trace_id|request_id|event_id|origin_id|record_id|lineage_id|ingest_id|_ingest_id|source_file|source_file_name|source_file_path|_metadata_file_path|_file_path|_file_name)$`. This is what Auto Loader pipelines produce when they persist `_metadata.file_path` / `_metadata.file_name`, and what well-behaved CDC feeds carry from the source system. The column proves the intent to trace; it does not prove the values are populated. The sampled variant measures that for one table.

The check runs in two steps. Step 1 is pure SQL over `information_schema.columns` and resolves every table that has a trace column. Step 2 probes only the remaining tables with `DESCRIBE DETAIL`, which keeps the probe count small. Non-Delta tables cannot have row tracking and are decided by step 1 alone.

`information_schema` is current; `DESCRIBE DETAIL` needs `SELECT` on the table. Returns NULL (N/A) when the schema has no base tables.

## SQL

### Row tracking or trace column (primary)

**(a) Enumerate tables and resolve the column half in SQL**

```sql
WITH tables_in_scope AS (
    SELECT LOWER(table_name) AS table_name, UPPER(data_source_format) AS data_source_format
    FROM {{ catalog }}.information_schema.tables
    WHERE LOWER(table_schema) = LOWER('{{ schema }}')
      AND table_type IN ('MANAGED', 'EXTERNAL')
),
trace_columns AS (
    SELECT LOWER(table_name) AS table_name,
           array_sort(collect_set(column_name)) AS trace_columns
    FROM {{ catalog }}.information_schema.columns
    WHERE LOWER(table_schema) = LOWER('{{ schema }}')
      AND REGEXP_LIKE(LOWER(column_name), '{{ trace_column_pattern }}')
    GROUP BY LOWER(table_name)
)
SELECT
    t.table_name,
    t.data_source_format,
    tc.trace_columns,
    tc.table_name IS NOT NULL                                   AS has_trace_column,
    tc.table_name IS NULL AND t.data_source_format = 'DELTA'    AS needs_probe
FROM tables_in_scope t
LEFT JOIN trace_columns tc USING (table_name)
ORDER BY t.table_name
```

**(b) Probe each table where `needs_probe` is true**

```sql
DESCRIBE DETAIL {{ catalog }}.{{ schema }}.`{{ asset }}`
```

**(c) Per-table predicate**

In words: the table passes if it has a trace column (from step a), or if its probe shows row tracking enabled, either as the property `delta.enableRowTracking` set to `true` or as `rowTracking` present in `tableFeatures`. Tables that did not need a probe and have no trace column fail.

As a SQL expression over the probe's output columns:

```sql
LOWER(properties['delta.enableRowTracking']) = 'true'
OR array_contains(tableFeatures, 'rowTracking')
```

`tableFeatures` is the more reliable of the two on tables where row tracking was enabled by a runtime default rather than an explicit property. Row tracking that is enabled but not yet backfilled (`delta.rowTrackingSuspended` or a `rowTracking` feature without the property) still counts; the backfill completes on its own.

**(d) Aggregation**

```
tables_passing = tables with has_trace_column
               + probed tables where the predicate is TRUE
tables_total   = all tables from (a)
value          = tables_passing / tables_total, NULL when tables_total = 0
```

Report `tables_passing` and `tables_total`. Tables whose probe fails with a permission error count as not passing and are listed separately in the report.

### Trace column only (variant, pure SQL)

No probe. Misses every table that relies on row tracking alone, so it under-reports on schemas built with Lakeflow pipelines or Delta Live Tables where row tracking is often on by default.

```sql
WITH tables_in_scope AS (
    SELECT LOWER(table_name) AS table_name
    FROM {{ catalog }}.information_schema.tables
    WHERE LOWER(table_schema) = LOWER('{{ schema }}')
      AND table_type IN ('MANAGED', 'EXTERNAL')
),
trace_columns AS (
    SELECT DISTINCT LOWER(table_name) AS table_name
    FROM {{ catalog }}.information_schema.columns
    WHERE LOWER(table_schema) = LOWER('{{ schema }}')
      AND REGEXP_LIKE(LOWER(column_name), '{{ trace_column_pattern }}')
)
SELECT
    COUNT_IF(tc.table_name IS NOT NULL)           AS tables_with_trace_column,
    COUNT(*)                                       AS total_tables,
    COUNT_IF(tc.table_name IS NOT NULL)::DOUBLE
        / NULLIF(COUNT(*), 0)                      AS value
FROM tables_in_scope t
LEFT JOIN trace_columns tc USING (table_name)
```

### Trace column population for one table (variant, table-scoped)

For a table that has a trace column, the fraction of rows where it is actually populated. Run per `{{ asset }}` with `{{ column }}` set to the trace column from step (a). Sampled by default because it scans rows; drop the `TABLESAMPLE` clause for an exact count.

```sql
SELECT
    COUNT_IF(`{{ column }}` IS NOT NULL AND CAST(`{{ column }}` AS STRING) <> '')          AS rows_with_trace_id,
    COUNT(*)                                                                                AS rows_sampled,
    COUNT_IF(`{{ column }}` IS NOT NULL AND CAST(`{{ column }}` AS STRING) <> '')::DOUBLE
        / NULLIF(COUNT(*), 0)                                                               AS value
FROM {{ catalog }}.{{ schema }}.{{ asset }} TABLESAMPLE ({{ sample_rows }} ROWS)
```

`{{ sample_rows }}` defaults to 1,000,000.

### Row ids readable for one table (variant, table-scoped)

Confirms row tracking is live, not merely configured, by reading the hidden metadata columns. Fails with an analysis error on a table without row tracking, which is itself the answer.

```sql
SELECT
    COUNT_IF(_metadata.row_id IS NOT NULL)              AS rows_with_row_id,
    COUNT(*)                                            AS rows_sampled,
    COUNT_IF(_metadata.row_id IS NOT NULL)::DOUBLE
        / NULLIF(COUNT(*), 0)                           AS value,
    MIN(_metadata.row_commit_version)                   AS oldest_row_commit_version,
    MAX(_metadata.row_commit_version)                   AS newest_row_commit_version
FROM {{ catalog }}.{{ schema }}.{{ asset }} TABLESAMPLE ({{ sample_rows }} ROWS)
```
