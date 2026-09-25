# Check: native_format_availability

Fraction of tables in the schema stored as Delta or Iceberg rather than in a format that needs runtime conversion (Parquet, CSV, JSON, Avro, ORC, text) or a federated connection.

## Context

Reads `information_schema.tables.data_source_format`. Managed tables are always Delta (or Iceberg on newer metastores). External tables can be any format Unity Catalog supports: an external `PARQUET` or `CSV` table is a plain directory listing with no transaction log, no time travel, no data skipping statistics, no `MERGE`, no Change Data Feed and no Vector Search source eligibility. Foreign tables (`table_type = 'FOREIGN'`) come from Lakehouse Federation or Delta Sharing and are read through a connector at query time.

Scoring:

- `DELTA` and `ICEBERG` are native. UniForm Delta tables (Delta with Iceberg reads enabled) report `DELTA`.
- `DELTASHARING` (a Delta Sharing recipient table) is scored native in the variant: the bytes arrive as Delta or Parquet from the provider and support most reads, but the recipient has no write path and no layout control. The primary variant scores it non-native because the check is about what this schema controls.
- Any other format (`PARQUET`, `CSV`, `JSON`, `AVRO`, `ORC`, `TEXT`, `BINARYFILE`) and any federation format (`MYSQL_FORMAT`, `POSTGRESQL_FORMAT`, `SNOWFLAKE_FORMAT`, `BIGQUERY_FORMAT`, `SQLSERVER_FORMAT`, `REDSHIFT_FORMAT`, `SALESFORCE_DATA_CLOUD_FORMAT`, `DATABRICKS_FORMAT`, ...) is non-native.

Views and materialized views are excluded (they have no storage format of their own; an MV is Delta-backed by construction). Streaming tables are Delta by construction and excluded so they do not pad the score.

The signal is **native** and exact for the format. It says nothing about whether the format is a problem: a 50 TB Parquet archive read once a quarter is fine as Parquet. Use the diagnostic to see size and last-read next to the format before converting anything.

`information_schema` is live. If `data_source_format` is NULL for a table, the metastore did not record it (older external tables); the check treats NULL as non-native and the diagnostic surfaces it as `UNKNOWN`. Confirm the list of formats present with `SELECT data_source_format, COUNT(*) FROM {{ catalog }}.information_schema.tables GROUP BY 1`.

Returns NULL (N/A) when the schema contains no base or foreign tables.

## SQL

### Delta or Iceberg, strict (primary)

```sql
WITH tables_in_scope AS (
    SELECT LOWER(table_name) AS table_name,
           table_type,
           UPPER(COALESCE(data_source_format, 'UNKNOWN')) AS data_source_format
    FROM {{ catalog }}.information_schema.tables
    WHERE LOWER(table_schema) = LOWER('{{ schema }}')
      AND table_type IN ('MANAGED', 'EXTERNAL', 'FOREIGN')
)
SELECT
    COUNT_IF(data_source_format IN ('DELTA', 'ICEBERG'))            AS native_tables,
    COUNT(*)                                                         AS total_tables,
    COUNT_IF(data_source_format IN ('DELTA', 'ICEBERG'))::DOUBLE
        / NULLIF(COUNT(*), 0)                                        AS value
FROM tables_in_scope
```

### Delta Sharing counted as native (variant)

For consumers of shared data who cannot change the provider's format.

```sql
WITH tables_in_scope AS (
    SELECT LOWER(table_name) AS table_name,
           table_type,
           UPPER(COALESCE(data_source_format, 'UNKNOWN')) AS data_source_format
    FROM {{ catalog }}.information_schema.tables
    WHERE LOWER(table_schema) = LOWER('{{ schema }}')
      AND table_type IN ('MANAGED', 'EXTERNAL', 'FOREIGN')
)
SELECT
    COUNT_IF(data_source_format IN ('DELTA', 'ICEBERG', 'DELTASHARING'))          AS native_tables,
    COUNT(*)                                                                       AS total_tables,
    COUNT_IF(data_source_format IN ('DELTA', 'ICEBERG', 'DELTASHARING'))::DOUBLE
        / NULLIF(COUNT(*), 0)                                                      AS value
FROM tables_in_scope
```

### Base tables only, federation excluded (variant)

When the schema deliberately mixes federated catalogs for exploration and only the tables this team stores should be judged.

```sql
WITH tables_in_scope AS (
    SELECT LOWER(table_name) AS table_name,
           UPPER(COALESCE(data_source_format, 'UNKNOWN')) AS data_source_format
    FROM {{ catalog }}.information_schema.tables
    WHERE LOWER(table_schema) = LOWER('{{ schema }}')
      AND table_type IN ('MANAGED', 'EXTERNAL')
)
SELECT
    COUNT_IF(data_source_format IN ('DELTA', 'ICEBERG'))            AS native_tables,
    COUNT(*)                                                         AS total_tables,
    COUNT_IF(data_source_format IN ('DELTA', 'ICEBERG'))::DOUBLE
        / NULLIF(COUNT(*), 0)                                        AS value
FROM tables_in_scope
```
