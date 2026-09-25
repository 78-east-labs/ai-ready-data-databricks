# Fix: record_level_traceability

Enable Delta row tracking for row identity, and add a source-record column, populated from the ingestion path, for trace-back to the originating record.

## Context

The two fixes solve different halves of the problem and most tables want both.

**Row tracking** gives each row a stable `_metadata.row_id` and a `_metadata.row_commit_version`. It is a table property (`delta.enableRowTracking = true`) that adds the `rowTracking` writer feature to the protocol. Consequences to state before applying: it raises the minimum writer version, so any external writer or old runtime that cannot handle the feature will fail to write; on an existing table Databricks backfills row ids, which is a metadata operation over every file and can take a while on large tables (it does not rewrite data); and it cannot be cleanly removed afterwards (dropping a table feature is a restricted operation). Enable it together with Change Data Feed when the goal is replaying per-row history. Guard: `SHOW TBLPROPERTIES` shows `delta.enableRowTracking = true`, or `DESCRIBE DETAIL` lists `rowTracking` in `tableFeatures`.

**A source-record column** is a schema change plus a pipeline change. Adding the column is metadata-only (`ALTER TABLE ... ADD COLUMN`, existing rows read as NULL). Populating it for existing rows is a data mutation and must be preceded by the blast-radius count. Populating it going forward is the real fix and belongs in the ingestion code: Auto Loader exposes `_metadata.file_path` and `_metadata.file_name` on every row it reads, CDC sources usually carry their own primary key or LSN, and API extracts have a record id in the payload. Generating a fresh `uuid()` for the backfill gives row identity but not source traceability; say so in the column comment so nobody mistakes it for a source key.

Pick the column name from the check's pattern (`source_record_id` for a source-system key, `source_file` plus `source_file_row` for file loads, `correlation_id` for event streams) so the check recognizes it without a pattern override.

## Fix: Enable row tracking on a single table

```sql
ALTER TABLE {{ catalog }}.{{ schema }}.{{ asset }}
SET TBLPROPERTIES ('delta.enableRowTracking' = 'true')
```

With Change Data Feed, so row history can be replayed:

```sql
ALTER TABLE {{ catalog }}.{{ schema }}.{{ asset }}
SET TBLPROPERTIES ('delta.enableRowTracking' = 'true',
                   'delta.enableChangeDataFeed' = 'true')
```

Re-running with the same values is a no-op commit.

## Fix: Generate row-tracking statements for Delta tables without a trace column

Emits one statement per Delta base table that has no trace column (those tables are the ones the probe decides). Filter further against the diagnostic's `row_tracking_enabled` to skip tables already done; re-running on them is harmless.

```sql
WITH trace_columns AS (
    SELECT DISTINCT LOWER(table_name) AS table_name
    FROM {{ catalog }}.information_schema.columns
    WHERE LOWER(table_schema) = LOWER('{{ schema }}')
      AND REGEXP_LIKE(LOWER(column_name), '{{ trace_column_pattern }}')
)
SELECT concat(
    'ALTER TABLE {{ catalog }}.{{ schema }}.`', t.table_name,
    '` SET TBLPROPERTIES (''delta.enableRowTracking'' = ''true'');'
) AS stmt
FROM {{ catalog }}.information_schema.tables t
LEFT JOIN trace_columns tc ON LOWER(t.table_name) = tc.table_name
WHERE LOWER(t.table_schema) = LOWER('{{ schema }}')
  AND t.table_type IN ('MANAGED', 'EXTERNAL')
  AND UPPER(t.data_source_format) = 'DELTA'
  AND tc.table_name IS NULL
ORDER BY t.table_name
```

Show the statements and the protocol-upgrade caveat to the user before executing.

## Fix: Add a source-record column and populate it going forward

Metadata-only; skip if the column exists (`information_schema.columns`).

```sql
ALTER TABLE {{ catalog }}.{{ schema }}.{{ asset }}
ADD COLUMNS (source_record_id STRING COMMENT 'Primary identifier of the originating record in {{ source_system }}')
```

Then change the writer. For an Auto Loader stream, select the file metadata into named columns:

```python
from pyspark.sql import functions as F

(spark.readStream.format("cloudFiles")
      .option("cloudFiles.format", "json")
      .option("cloudFiles.schemaLocation", "{{ schema_location }}")
      .load("{{ source_location }}")
      .withColumn("source_file", F.col("_metadata.file_path"))
      .withColumn("source_file_name", F.col("_metadata.file_name"))
      .withColumn("source_record_id", F.col("{{ source_key_field }}"))
      .writeStream.option("checkpointLocation", "{{ checkpoint_location }}")
      .toTable("{{ catalog }}.{{ schema }}.{{ asset }}"))
```

In a Lakeflow Declarative Pipeline the same works in SQL:

```sql
CREATE OR REFRESH STREAMING TABLE {{ asset }}
COMMENT 'Landed from {{ source_location }}; source_file traces each row to its input file'
AS SELECT *,
       _metadata.file_path AS source_file,
       _metadata.file_name AS source_file_name
FROM STREAM read_files('{{ source_location }}', format => 'json')
```

For `COPY INTO`, the metadata column is available in the select list: `COPY INTO t FROM (SELECT *, _metadata.file_path AS source_file FROM '{{ source_location }}') FILEFORMAT = JSON`.

## Fix: Backfill the new column on existing rows

Only when the source identifier can be recovered from existing columns (for example a natural key already present). Count first:

```sql
SELECT COUNT(*) AS rows_to_update
FROM {{ catalog }}.{{ schema }}.{{ asset }}
WHERE source_record_id IS NULL
```

Then, if the count is acceptable and the expression really is the source key:

```sql
UPDATE {{ catalog }}.{{ schema }}.{{ asset }}
SET source_record_id = {{ source_key_expression }}
WHERE source_record_id IS NULL
```

If no source key exists, do not fabricate one with `uuid()` under a source-record name. Either leave the column NULL for legacy rows and document the cutover date in the table comment, or rely on row tracking for identity of those rows.

## Organizational guidance

Traceability is set at ingestion or not at all. Put `source_file` / `source_record_id` into the bronze-layer template and require every pipeline to carry the identifier through silver and gold (a `MERGE` that drops it silently breaks the chain). Enable row tracking and Change Data Feed in the table-creation defaults for governed schemas (Lakeflow `TBLPROPERTIES`, dbt `tblproperties`, Terraform), so new tables never start without them. Where a source system has no stable record id, raise it with that system's owners; a hash of the payload is a workable stand-in only if the payload is immutable.
