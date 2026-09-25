# Check: change_detection

Fraction of base Delta tables in the schema that have Change Data Feed (CDF) enabled.

## Context

Change Data Feed is the Databricks primitive for row-level change detection. When `delta.enableChangeDataFeed = 'true'` is set on a table, every commit from that point on writes `_change_data` files recording inserts, updates (pre- and post-image) and deletes, and consumers read them with `table_changes('catalog.schema.table', start_version)` or `readChangeFeed` in Structured Streaming. Without CDF, downstream consumers have to diff snapshots or re-read the full table to find what changed. Delta Sync vector indexes and Lakebase synced tables in triggered or continuous mode also require it.

The property is a Delta table property. It is not exposed in `information_schema.tables` or `information_schema.table_tags`, so this is a **probe mode** check: enumerate tables in SQL, run one metadata statement per table, evaluate the predicate, aggregate. `DESCRIBE DETAIL` returns the property inside the `properties` map; `SHOW TBLPROPERTIES` returns it as a row. Neither scans data. Both need `SELECT` on the table.

Strength is **native**: the property is the exact thing the requirement asks about. It proves the table emits change records from the enabling version onward. It does not prove anyone consumes them; the consumption variant below is a weaker, query-history based approximation of that.

Scope is base tables (`MANAGED`, `EXTERNAL`) whose `data_source_format` is `DELTA`. Streaming tables and materialized views are excluded because their pipelines manage change tracking internally. Non-Delta external tables (Parquet, CSV, Iceberg) cannot carry the property and are excluded from the denominator rather than counted as failures.

Returns NULL (N/A) when the schema contains no base Delta tables.

## SQL

### Change Data Feed property (primary, probe mode)

**(a) Enumerate tables in scope**

```sql
SELECT LOWER(table_name) AS table_name,
       CONCAT('`{{ catalog }}`.`{{ schema }}`.`', table_name, '`') AS qualified_name
FROM {{ catalog }}.information_schema.tables
WHERE LOWER(table_schema) = LOWER('{{ schema }}')
  AND table_type IN ('MANAGED', 'EXTERNAL')
  AND UPPER(data_source_format) = 'DELTA'
ORDER BY table_name
```

**(b) Per-table probe statement**

```sql
DESCRIBE DETAIL {{ catalog }}.{{ schema }}.{{ asset }}
```

Relevant output columns: `name`, `format`, `properties` (MAP<STRING,STRING>), `tableFeatures` (ARRAY<STRING>), `lastModified`.

**(c) Per-table predicate**

In words: the table passes when the `properties` map contains `delta.enableChangeDataFeed` with the value `true` (case-insensitive). A missing key means CDF is off; there is no metastore-level default that turns it on.

As a SQL expression over the probe's output columns:

```sql
LOWER(COALESCE(properties['delta.enableChangeDataFeed'], 'false')) = 'true'
```

**(d) Aggregation rule**

```
tables_with_cdf = count of probed tables where the predicate is true
total_tables    = count of probed tables (tables that errored on DESCRIBE DETAIL are excluded and reported)
value           = tables_with_cdf / total_tables, NULL when total_tables = 0
```

Report `tables_with_cdf`, `total_tables` and `value`.

### SHOW TBLPROPERTIES probe (variant, probe mode)

Same enumeration and aggregation. Use when `DESCRIBE DETAIL` is slow on very large tables (it computes `numFiles` and `sizeInBytes`) or when the caller only has metadata access.

```sql
SHOW TBLPROPERTIES {{ catalog }}.{{ schema }}.{{ asset }} ('delta.enableChangeDataFeed')
```

Output columns: `key`, `value`. The predicate is `LOWER(value) = 'true'`. When the property is unset the statement returns a row whose `value` is the literal text `Table ... does not have property: delta.enableChangeDataFeed`; treat any value other than `true` as failing.

### Change feed consumption (variant, pure SQL approximation)

Approximates the upstream framework's "stream coverage" variant: which tables have had their change feed actually read recently. Counts tables that appear inside a `table_changes(...)` call in `system.query.history` within the window. This is evidence of consumption, not of the property; a table can have CDF enabled and score 0 here, and a table that had CDF disabled after a consumer stopped can still show up. It misses consumers running on classic all-purpose clusters (not in `query.history`) and streaming readers using `readChangeFeed`, which never appear as SQL text.

```sql
WITH tables_in_scope AS (
    SELECT LOWER(table_name) AS table_name
    FROM {{ catalog }}.information_schema.tables
    WHERE LOWER(table_schema) = LOWER('{{ schema }}')
      AND table_type IN ('MANAGED', 'EXTERNAL')
      AND UPPER(data_source_format) = 'DELTA'
),
cdf_reads AS (
    SELECT DISTINCT LOWER(regexp_extract(statement_text,
               '(?i)table_changes\\s*\\(\\s*[\'"]([^\'"]+)[\'"]', 1)) AS full_name
    FROM system.query.history
    WHERE start_time >= current_timestamp() - INTERVAL {{ lookback_days }} DAYS
      AND execution_status = 'FINISHED'
      AND REGEXP_LIKE(LOWER(statement_text), 'table_changes\\s*\\(')
),
consumed AS (
    SELECT DISTINCT element_at(split(full_name, '\\.'), -1) AS table_name
    FROM cdf_reads
    WHERE full_name LIKE CONCAT(LOWER('{{ catalog }}'), '.', LOWER('{{ schema }}'), '.%')
       OR full_name LIKE CONCAT(LOWER('{{ schema }}'), '.%')
)
SELECT
    COUNT_IF(c.table_name IS NOT NULL)            AS tables_with_cdf_readers,
    COUNT(*)                                       AS total_tables,
    COUNT_IF(c.table_name IS NOT NULL)::DOUBLE
        / NULLIF(COUNT(*), 0)                      AS value
FROM tables_in_scope t
LEFT JOIN consumed c USING (table_name)
```

`{{ lookback_days }}` defaults to 7. Two-part names (`schema.table`) are matched by suffix, so a same-named table in another catalog can produce a false positive; the primary variant is the one to score.
