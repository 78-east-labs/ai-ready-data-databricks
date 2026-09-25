# Diagnostic: native_format_availability

One row per table with its type, storage format, format class, read activity in the lookback window, and a recommendation.

## Context

Same scoping as the check. Adds two things a conversion decision needs:

- **Read activity** from `system.access.table_lineage` (`source_table_full_name` = the table, last `{{ lookback_days }}` days, default 30): how many distinct reads and readers. A non-native table nobody reads is not worth converting; one read hourly by a serving job is.
- **Downstream targets** from the same lineage: how many tables are built from it. Converting a source that feeds ten Delta tables removes ten runtime conversions.

Format classes:

- `NATIVE`: `DELTA`, `ICEBERG`.
- `SHARED`: `DELTASHARING`. Read-only for the recipient; convert by materializing if layout control is needed.
- `FILE`: `PARQUET`, `CSV`, `JSON`, `AVRO`, `ORC`, `TEXT`, `BINARYFILE`. Convertible in place (Parquet) or by load (others).
- `FEDERATED`: any `*_FORMAT` connector. Convertible only by copying (materialized view or ingest pipeline).
- `UNKNOWN`: `data_source_format` is NULL. Probe with `DESCRIBE DETAIL` (its `format` column) to find out.

Size is not in `information_schema`; for Delta tables `DESCRIBE DETAIL` gives `sizeInBytes`, for file tables it is the directory size in cloud storage. The per-table probe at the end returns it where available.

Sorted so non-native tables with the most reads come first.

Lag: lineage up to a few hours; recent reads may be missing.

## SQL

### Format inventory with read and downstream activity

```sql
WITH tables_in_scope AS (
    SELECT LOWER(table_name) AS table_name,
           table_type,
           table_owner,
           UPPER(COALESCE(data_source_format, 'UNKNOWN')) AS data_source_format,
           created,
           last_altered
    FROM {{ catalog }}.information_schema.tables
    WHERE LOWER(table_schema) = LOWER('{{ schema }}')
      AND table_type IN ('MANAGED', 'EXTERNAL', 'FOREIGN')
),
reads AS (
    SELECT LOWER(source_table_name)          AS table_name,
           COUNT(*)                          AS read_events,
           COUNT(DISTINCT user_identity.email) AS distinct_readers,
           MAX(event_time)                   AS last_read_at
    FROM system.access.table_lineage
    WHERE LOWER(source_catalog) = LOWER('{{ catalog }}')
      AND LOWER(source_schema)  = LOWER('{{ schema }}')
      AND event_time >= current_timestamp() - INTERVAL {{ lookback_days }} DAYS
    GROUP BY LOWER(source_table_name)
),
downstream AS (
    SELECT LOWER(source_table_name) AS table_name,
           COUNT(DISTINCT target_table_full_name) AS downstream_tables
    FROM system.access.table_lineage
    WHERE LOWER(source_catalog) = LOWER('{{ catalog }}')
      AND LOWER(source_schema)  = LOWER('{{ schema }}')
      AND target_table_full_name IS NOT NULL
      AND event_time >= current_timestamp() - INTERVAL {{ lookback_days }} DAYS
    GROUP BY LOWER(source_table_name)
)
SELECT
    t.table_name,
    t.table_type,
    t.table_owner,
    t.data_source_format,
    CASE
        WHEN t.data_source_format IN ('DELTA', 'ICEBERG')                              THEN 'NATIVE'
        WHEN t.data_source_format = 'DELTASHARING'                                     THEN 'SHARED'
        WHEN t.data_source_format IN ('PARQUET','CSV','JSON','AVRO','ORC','TEXT','BINARYFILE') THEN 'FILE'
        WHEN t.data_source_format LIKE '%\\_FORMAT'                                    THEN 'FEDERATED'
        ELSE 'UNKNOWN'
    END AS format_class,
    COALESCE(r.read_events, 0)        AS read_events,
    COALESCE(r.distinct_readers, 0)   AS distinct_readers,
    r.last_read_at,
    COALESCE(d.downstream_tables, 0)  AS downstream_tables,
    t.last_altered,
    CASE
        WHEN t.data_source_format IN ('DELTA', 'ICEBERG') THEN 'Native, no action'
        WHEN t.data_source_format = 'PARQUET' AND t.table_type = 'EXTERNAL'
             THEN 'CONVERT TO DELTA in place (no data copy)'
        WHEN t.data_source_format IN ('CSV','JSON','AVRO','ORC','TEXT')
             THEN 'Load into a Delta table with COPY INTO or Auto Loader'
        WHEN t.data_source_format LIKE '%\\_FORMAT'
             THEN 'Materialize with a materialized view or an ingestion pipeline if read often'
        WHEN t.data_source_format = 'DELTASHARING'
             THEN 'Read-only share; materialize locally only if layout control is needed'
        ELSE 'Run DESCRIBE DETAIL to identify the format'
    END AS recommendation
FROM tables_in_scope t
LEFT JOIN reads      r USING (table_name)
LEFT JOIN downstream d USING (table_name)
ORDER BY
    CASE format_class WHEN 'NATIVE' THEN 2 ELSE 1 END,
    read_events DESC,
    t.table_name
```

### Per-table storage detail

```sql
SELECT format, location, numFiles, sizeInBytes, partitionColumns, lastModified
FROM (DESCRIBE DETAIL {{ catalog }}.{{ schema }}.{{ asset }})
```

For a Parquet external table `format` is `parquet` and `numFiles` / `sizeInBytes` are populated from a listing; for a federated table `DESCRIBE DETAIL` is not supported and `DESCRIBE EXTENDED` shows the connection instead.
