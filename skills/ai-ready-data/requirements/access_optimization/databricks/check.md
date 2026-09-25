# Check: access_optimization

Fraction of large Delta tables in the schema that have a physical layout strategy: liquid clustering, partitioning, or predictive optimization actively maintaining them.

## Context

Databricks does not expose clustering or partitioning in `information_schema.tables`, so this is a **probe-mode** check. The orchestrator enumerates base tables, runs `DESCRIBE DETAIL` on each, and evaluates a per-table predicate. Cost is one metadata call per table; no rows are scanned.

A table is "large" when `DESCRIBE DETAIL.sizeInBytes >= {{ large_table_bytes }}` (default `10737418240`, 10 GB). Small tables are excluded from both numerator and denominator: a 200 MB table fits in a single scan and gains nothing from layout work, and counting it would inflate or deflate the score for no reason.

A large table passes when any of these holds:

- `clusteringColumns` is non-empty (liquid clustering with explicit keys), or `CLUSTER BY AUTO` is on (`properties['clusterByAuto'] = 'true'`; automatic liquid clustering picks keys from query history and the `clusteringColumns` array can be empty until it does).
- `partitionColumns` is non-empty (Hive-style partitioning). Partitioning is a weaker signal than liquid clustering because a bad partition key hurts more than it helps, but it is still a deliberate layout decision.
- Predictive optimization ran `COMPACTION` on the table within the last 30 days, read from `system.storage.predictive_optimization_operations_history`. Predictive optimization keeps file sizes healthy even without clustering keys, so an actively maintained large table counts.

The signal is **native** and proves that a layout decision exists. It does not prove the decision is good: liquid clustering on a column nobody filters on passes this check and still delivers full scans. Use the diagnostic to see keys next to size and file count, and `search_optimization` to check whether the layout is being maintained.

Lag: `DESCRIBE DETAIL` is live. `system.storage.predictive_optimization_operations_history` lags by hours and only has rows if predictive optimization is enabled somewhere in the metastore; if the schema is not enabled, that branch simply never fires.

Permissions: `SELECT` on each table for `DESCRIBE DETAIL`; `SELECT` on `system.storage.predictive_optimization_operations_history` for the third branch. If the system table is not granted, evaluate the first two branches only and say so in the report.

If you are not sure your runtime exposes `clusterByAuto`, confirm with `SELECT properties['clusterByAuto'] FROM (DESCRIBE DETAIL {{ catalog }}.{{ schema }}.{{ asset }})`. Some workspaces also surface it as a top-level `clusterByAuto` column in `DESCRIBE DETAIL`; read whichever is present.

Returns NULL (N/A) when no base table in the schema meets the size threshold.

## SQL

### Probe-and-aggregate (primary)

**(a) Enumerate tables in scope**

```sql
SELECT table_name
FROM {{ catalog }}.information_schema.tables
WHERE LOWER(table_schema) = LOWER('{{ schema }}')
  AND table_type IN ('MANAGED', 'EXTERNAL')
  AND data_source_format = 'DELTA'
ORDER BY table_name
```

**(b) Per-table probe** (run once per enumerated table)

```sql
DESCRIBE DETAIL {{ catalog }}.{{ schema }}.`{{ asset }}`
```

Read `sizeInBytes`, `numFiles`, `clusteringColumns`, `partitionColumns`, `properties`.

**(b2) Predictive optimization activity** (run once for the schema, join by table name in the orchestrator)

```sql
SELECT LOWER(table_name) AS table_name,
       MAX(end_time)     AS last_po_compaction
FROM system.storage.predictive_optimization_operations_history
WHERE LOWER(catalog_name) = LOWER('{{ catalog }}')
  AND LOWER(schema_name)  = LOWER('{{ schema }}')
  AND operation_type = 'COMPACTION'
  AND operation_status = 'SUCCESSFUL'
  AND start_time >= current_timestamp() - INTERVAL 30 DAYS
GROUP BY LOWER(table_name)
```

If `operation_status` values in your workspace differ (some releases use `SUCCESS`), confirm with `SELECT DISTINCT operation_status FROM system.storage.predictive_optimization_operations_history`.

**(c) Per-table predicate**

In words: the table is large, and it has liquid clustering keys, or automatic liquid clustering, or partition columns, or a successful predictive optimization compaction in the last 30 days.

As a SQL expression over the probe output (treat `last_po_compaction` as NULL when the table has no row in b2):

```sql
sizeInBytes >= {{ large_table_bytes }}
AND (
       size(clusteringColumns) > 0
    OR LOWER(COALESCE(properties['clusterByAuto'], 'false')) = 'true'
    OR size(partitionColumns) > 0
    OR last_po_compaction IS NOT NULL
)
```

Tables with `sizeInBytes < {{ large_table_bytes }}` are skipped, not failed.

**(d) Aggregation**

```
large_tables      = count of probed tables with sizeInBytes >= {{ large_table_bytes }}
optimized_tables  = count of those where the predicate is true
value             = optimized_tables / large_tables    (NULL when large_tables = 0)
```

Report `optimized_tables`, `large_tables`, `value`.

### Partition-only approximation (variant, pure SQL)

`information_schema.columns.partition_index` is populated for Hive-style partition columns, so partitioning can be detected without probes. This variant **misses liquid clustering and predictive optimization entirely** and cannot apply the size threshold (size is not in `information_schema`), so every base table is in the denominator. Use it only to get a quick lower bound when probes are not possible.

```sql
WITH tables_in_scope AS (
    SELECT LOWER(table_name) AS table_name
    FROM {{ catalog }}.information_schema.tables
    WHERE LOWER(table_schema) = LOWER('{{ schema }}')
      AND table_type IN ('MANAGED', 'EXTERNAL')
      AND data_source_format = 'DELTA'
),
partitioned AS (
    SELECT DISTINCT LOWER(table_name) AS table_name
    FROM {{ catalog }}.information_schema.columns
    WHERE LOWER(table_schema) = LOWER('{{ schema }}')
      AND partition_index IS NOT NULL
)
SELECT
    COUNT_IF(p.table_name IS NOT NULL)            AS optimized_tables,
    COUNT(*)                                       AS large_tables,
    COUNT_IF(p.table_name IS NOT NULL)::DOUBLE
        / NULLIF(COUNT(*), 0)                      AS value
FROM tables_in_scope t
LEFT JOIN partitioned p USING (table_name)
```
