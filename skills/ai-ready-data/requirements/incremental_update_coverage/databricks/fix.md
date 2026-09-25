# Fix: incremental_update_coverage

Move full-reload pipelines onto an incremental write path.

## Context

There is no table property that makes a table "incremental". The score moves when the pipeline that writes the table stops overwriting it. Which rewrite is right depends on what the source offers:

- **Source has a stable key and a change indicator** (updated_at, version, CDC feed): replace the overwrite with `MERGE INTO` keyed on the primary key. This is the general answer for dimension and entity tables.
- **Source is append-only** (events, logs, files landing in a path): replace the overwrite with `INSERT INTO` or `COPY INTO`, or with Auto Loader into a streaming table. `COPY INTO` and Auto Loader track which files have been loaded, so re-runs are idempotent.
- **Source is a Delta table with Change Data Feed**: read `table_changes()` since the last processed version and `MERGE` the result (see `change_detection/databricks/fix.md`).
- **Table is a derived aggregate**: declare it as a materialized view. Databricks refreshes it incrementally when the query shape allows and falls back to full recompute otherwise, which still counts as pipeline-managed refresh under `feature_refresh_compliance`.
- **Partitioned table reloaded by period**: if the pipeline must rewrite a day or a month, use `INSERT INTO ... REPLACE WHERE <partition predicate>` (recorded as `WRITE` / `Overwrite` with a predicate, which the check accepts) rather than a whole-table `INSERT OVERWRITE`.

Everything below is a pipeline change. Test it on a copy first (`CREATE TABLE {{ catalog }}.{{ schema }}.{{ asset }}_incr_test SHALLOW CLONE {{ catalog }}.{{ schema }}.{{ asset }}`), then switch the job. Do not use `CREATE OR REPLACE TABLE` anywhere in the new path; that is the pattern being removed.

## Fix: Replace the overwrite with a MERGE

Blast-radius check first. Count how many rows the merge would touch on the current source batch:

```sql
SELECT
    COUNT_IF(t.{{ key_column }} IS NOT NULL) AS rows_to_update,
    COUNT_IF(t.{{ key_column }} IS NULL)     AS rows_to_insert
FROM {{ source_catalog }}.{{ source_schema }}.{{ source_asset }} s
LEFT JOIN {{ catalog }}.{{ schema }}.{{ asset }} t
  ON t.{{ key_column }} = s.{{ key_column }}
```

Then the merge, keyed on the table's primary key (from `information_schema.key_column_usage`):

```sql
MERGE INTO {{ catalog }}.{{ schema }}.{{ asset }} AS t
USING {{ source_catalog }}.{{ source_schema }}.{{ source_asset }} AS s
  ON t.{{ key_column }} = s.{{ key_column }}
WHEN MATCHED AND s.{{ timestamp_column }} > t.{{ timestamp_column }} THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
```

Add `WHEN NOT MATCHED BY SOURCE THEN DELETE` only when the source batch is a full snapshot and hard deletes are wanted; otherwise leave it out. Enable deletion vectors (`delta.enableDeletionVectors = 'true'`) and liquid clustering on the key so repeated merges do not rewrite whole files.

## Fix: Replace the overwrite with an idempotent append

For file-based sources, `COPY INTO` loads each file once:

```sql
COPY INTO {{ catalog }}.{{ schema }}.{{ asset }}
FROM '{{ source_path }}'
FILEFORMAT = PARQUET
COPY_OPTIONS ('mergeSchema' = 'true')
```

For table sources, append with a watermark and record the watermark in a control table so re-runs do not duplicate:

```sql
INSERT INTO {{ catalog }}.{{ schema }}.{{ asset }}
SELECT *
FROM {{ source_catalog }}.{{ source_schema }}.{{ source_asset }}
WHERE {{ timestamp_column }} > (
    SELECT COALESCE(MAX({{ timestamp_column }}), TIMESTAMP '1900-01-01')
    FROM {{ catalog }}.{{ schema }}.{{ asset }}
)
```

## Fix: Keep a per-period reload but scope it

When the business logic needs a whole day recomputed, replace `INSERT OVERWRITE` with a predicate-scoped replace. Delta records it as an `Overwrite` with a `predicate`, and only the matching files are rewritten:

```sql
INSERT INTO {{ catalog }}.{{ schema }}.{{ asset }}
REPLACE WHERE {{ partition_column }} = DATE '{{ period }}'
SELECT * FROM {{ source_catalog }}.{{ source_schema }}.{{ source_asset }}
WHERE {{ partition_column }} = DATE '{{ period }}'
```

## Fix: Declare the table as a materialized view or streaming table

Only when the table is produced entirely by a SQL query over other UC tables and can be renamed. Create the new object under a new name, validate it against the old table, then swap consumers. The old table is left in place for the owner to retire; this file does not drop it.

```sql
CREATE MATERIALIZED VIEW {{ catalog }}.{{ schema }}.{{ asset }}_mv
SCHEDULE EVERY 1 HOUR
AS
{{ source_query }}
```

For an append-only source path, a streaming table with Auto Loader:

```sql
CREATE STREAMING TABLE {{ catalog }}.{{ schema }}.{{ asset }}_st
SCHEDULE EVERY 1 HOUR
AS SELECT * FROM STREAM read_files('{{ source_path }}', format => 'parquet')
```

## Organizational guidance

Make "no `CREATE OR REPLACE TABLE` and no unscoped `INSERT OVERWRITE` in production jobs" a review rule, and enforce it with a Databricks SQL alert on the diagnostic's query-history variant (any `REPLACE_TABLE_AS_SELECT` targeting a governed schema pages the owner). Give every table a primary key constraint so `MERGE` has something to key on; the `entity_identifier_declaration` requirement measures that. New pipelines should start from a Lakeflow template (Auto Loader plus `APPLY CHANGES INTO` or `MERGE`) rather than a notebook that rebuilds the table, and dbt models should use `incremental` materialization with `incremental_strategy: merge` instead of `table`.
