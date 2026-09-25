# Fix: native_format_availability

Convert Parquet tables to Delta in place, load other file formats into Delta tables, and materialize federated sources that are read often.

## Context

The right fix depends on the diagnostic's `format_class`:

- **`FILE` / `PARQUET`, external.** `CONVERT TO DELTA` writes a transaction log next to the existing Parquet files. No data is copied, the location does not change, and the table becomes a full Delta table. It is the cheapest conversion there is. After conversion the old Parquet readers (outside Databricks) still see the files but not the log, and any new writes go through Delta.
- **`FILE`, other formats (CSV, JSON, Avro, ORC, text).** No in-place path; data has to be read and written as Delta. `COPY INTO` for a one-off load, Auto Loader for continuous arrival. The new table gets a new name (`{{ asset }}_delta`); switching consumers over is a separate step.
- **`FEDERATED`.** The source lives in another system. A materialized view over the foreign table gives a Delta copy with scheduled refresh and no pipeline code. For large or fast-changing sources, a Lakeflow Connect ingestion pipeline is the durable version.
- **`SHARED`.** Leave it unless layout control matters; then a materialized view as for federated.

Never `CREATE OR REPLACE TABLE` over the existing name; create the Delta table beside it, verify counts, then move consumers. Dropping the old table is out of scope for this fix.

Permissions: ownership of the external table for `CONVERT TO DELTA`; `CREATE TABLE` on the schema; `READ FILES` on the external location for `COPY INTO` / Auto Loader; `SELECT` on the foreign table for a materialized view.

## Fix: Convert an external Parquet table to Delta in place

Guard: `SELECT data_source_format FROM {{ catalog }}.information_schema.tables WHERE LOWER(table_schema) = LOWER('{{ schema }}') AND LOWER(table_name) = LOWER('{{ asset }}')` returns `PARQUET`. `CONVERT TO DELTA` on a table that is already Delta is a no-op with a notice.

For an unpartitioned table:

```sql
CONVERT TO DELTA {{ catalog }}.{{ schema }}.{{ asset }};
```

For a Hive-partitioned directory the partition schema must be declared or the log will not know about the partition columns:

```sql
CONVERT TO DELTA {{ catalog }}.{{ schema }}.{{ asset }}
PARTITIONED BY ({{ partition_column }} {{ partition_type }});
```

`CONVERT TO DELTA` lists every file once to collect statistics; on a directory with millions of files add `NO STATISTICS` and run `ANALYZE TABLE ... COMPUTE DELTA STATISTICS` later. Then apply the same defaults a new Delta table gets:

```sql
ALTER TABLE {{ catalog }}.{{ schema }}.{{ asset }}
SET TBLPROPERTIES (
    'delta.enableDeletionVectors' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true'
);
```

Verify: `SELECT format FROM (DESCRIBE DETAIL {{ catalog }}.{{ schema }}.{{ asset }})` returns `delta`.

## Fix: Load a CSV / JSON / Avro / ORC table into a Delta table

Blast radius: `SELECT COUNT(*) FROM {{ catalog }}.{{ schema }}.{{ asset }}` on the file table, so the load can be verified against it.

```sql
CREATE TABLE IF NOT EXISTS {{ catalog }}.{{ schema }}.{{ asset }}_delta
COMMENT 'Delta copy of {{ asset }} (source format {{ source_format }} at {{ source_path }})'
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
CLUSTER BY AUTO
AS SELECT * FROM {{ catalog }}.{{ schema }}.{{ asset }} WHERE 1 = 0;

COPY INTO {{ catalog }}.{{ schema }}.{{ asset }}_delta
FROM '{{ source_path }}'
FILEFORMAT = {{ source_format }}
FORMAT_OPTIONS ('header' = 'true', 'inferSchema' = 'true', 'mergeSchema' = 'true')
COPY_OPTIONS  ('mergeSchema' = 'true');
```

`COPY INTO` is idempotent per file; re-running loads only files it has not seen. Drop `header` / `inferSchema` for non-CSV formats. Compare `SELECT COUNT(*) FROM {{ catalog }}.{{ schema }}.{{ asset }}_delta` with the blast-radius count before pointing consumers at it.

For files that keep arriving, replace the `COPY INTO` with an Auto Loader stream (`cloudFiles` source) in a Lakeflow pipeline writing to the same target.

## Fix: Materialize a federated or shared table

A materialized view is Delta storage with a refresh schedule and needs a serverless SQL warehouse or a pipeline to refresh. `CREATE MATERIALIZED VIEW IF NOT EXISTS` is idempotent.

```sql
CREATE MATERIALIZED VIEW IF NOT EXISTS {{ catalog }}.{{ schema }}.{{ asset }}_mv
SCHEDULE EVERY {{ refresh_hours }} HOURS
COMMENT 'Delta materialization of federated table {{ asset }}'
AS SELECT * FROM {{ catalog }}.{{ schema }}.{{ asset }};
```

The MV is excluded from this check's denominator (it is not a base table), so the score moves only when consumers stop reading the foreign table; the point of the fix is the consumers' latency, not the score. For very large sources, a Lakeflow Connect managed ingestion pipeline (SQL Server, Salesforce, Workday connectors) is the durable answer and produces a real Delta table.

## Fix: Bulk-generate CONVERT TO DELTA for every external Parquet table

```sql
SELECT concat(
    'CONVERT TO DELTA {{ catalog }}.{{ schema }}.`', table_name, '`;'
) AS stmt
FROM {{ catalog }}.information_schema.tables
WHERE LOWER(table_schema) = LOWER('{{ schema }}')
  AND table_type = 'EXTERNAL'
  AND UPPER(data_source_format) = 'PARQUET'
ORDER BY table_name
```

Partitioned Parquet tables need the `PARTITIONED BY` clause; check `partition_index` in `information_schema.columns` for each and add it by hand. Show the generated statements to the user before executing them.

## Organizational guidance

Non-native tables appear when teams register raw landing zones directly in Unity Catalog. Keep the landing zone as a volume or external location, not as a table, and let Auto Loader or `COPY INTO` produce the Delta table that gets registered. For federated sources, treat the foreign catalog as a discovery surface and require a materialized view or ingestion pipeline before any AI workload reads it. Put `data_source_format = 'DELTA'` in the definition of done for schema promotion.
