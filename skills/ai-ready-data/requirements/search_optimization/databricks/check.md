# Check: search_optimization

Fraction of base Delta tables in the schema whose file layout is being actively maintained: a predictive optimization operation ran on the table within `{{ lookback_days }}` days, or the table has liquid clustering keys and an `OPTIMIZE` commit within the same window.

## Context

On Databricks there is no "search optimization" switch to flip. Fast selective reads come from data skipping statistics and clustering, and both only help while files are compacted and ordered; every write erodes them. So this check measures **maintenance**, not configuration. `access_optimization` asks "is there a layout"; this check asks "is anyone keeping it in shape".

Two signals, combined with OR:

- **Predictive optimization ran.** `system.storage.predictive_optimization_operations_history` has a `COMPACTION` (or `VACUUM` / `ANALYZE`) row for the table with `start_time` in the window. This is the **native** signal and is one SQL statement for the schema. It only ever fires for tables under predictive optimization; the table is otherwise absent from the system table entirely.
- **Manual maintenance on a clustered table.** `DESCRIBE DETAIL.clusteringColumns` is non-empty (or `CLUSTER BY AUTO` is on) and `DESCRIBE HISTORY` has an `OPTIMIZE` commit in the window. This is the **probe** part, one `DESCRIBE DETAIL` plus one `DESCRIBE HISTORY ... LIMIT n` per table. A scheduled `OPTIMIZE` job shows up here; so do predictive optimization's own compactions (they commit as `OPTIMIZE` with a Databricks service principal as `userName`), so a table can pass through either leg.

Placeholder default: `{{ lookback_days }}` = `7`. Predictive optimization runs only when it judges a table worth compacting, and a table with no writes in a week gets nothing; for schemas that load weekly or less, set `{{ lookback_days }}` to 30. The active-tables variant below limits the denominator to tables that were written in the window, which is the fairer question ("of the tables that changed, which were re-optimized?").

What it proves: maintenance happened recently. Not that it was sufficient (a single `OPTIMIZE` on a table receiving 10,000 small commits a day is still behind), and not that the clustering keys match the queries. Read with the diagnostic's `avg_file_mb` and `commits_since_optimize`.

Lag: `system.storage.predictive_optimization_operations_history` lags by hours; `DESCRIBE HISTORY` is live. Permissions: `SELECT` on the system table (metastore admin enables `system.storage`); `SELECT` on each table for the probes. If the system table is not granted, run the probe leg only and say so.

Returns NULL (N/A) when the schema has no base Delta tables (or, in the variant, none written in the window).

## SQL

### Predictive optimization activity (SQL leg, once per schema)

```sql
SELECT LOWER(table_name)                        AS table_name,
       MAX(end_time)                            AS last_po_operation,
       COUNT_IF(operation_type = 'COMPACTION')  AS po_compactions,
       COUNT_IF(operation_type = 'VACUUM')      AS po_vacuums,
       COUNT_IF(operation_type = 'ANALYZE')     AS po_analyzes
FROM system.storage.predictive_optimization_operations_history
WHERE LOWER(catalog_name) = LOWER('{{ catalog }}')
  AND LOWER(schema_name)  = LOWER('{{ schema }}')
  AND operation_status = 'SUCCESSFUL'
  AND start_time >= current_timestamp() - INTERVAL {{ lookback_days }} DAYS
GROUP BY LOWER(table_name)
```

Confirm the status vocabulary with `SELECT DISTINCT operation_status FROM system.storage.predictive_optimization_operations_history` if this returns nothing on a schema known to be under predictive optimization.

### Probe-and-aggregate (probe leg)

**(a) Enumerate tables in scope**

```sql
SELECT table_name
FROM {{ catalog }}.information_schema.tables
WHERE LOWER(table_schema) = LOWER('{{ schema }}')
  AND table_type IN ('MANAGED', 'EXTERNAL')
  AND data_source_format = 'DELTA'
ORDER BY table_name
```

**(b) Per-table probes** (two statements per table)

```sql
DESCRIBE DETAIL {{ catalog }}.{{ schema }}.`{{ asset }}`
```

Read `clusteringColumns`, `properties['clusterByAuto']`, `numFiles`, `sizeInBytes`.

```sql
SELECT MAX(timestamp)                                          AS last_optimize,
       COUNT(*)                                                AS optimize_commits
FROM (DESCRIBE HISTORY {{ catalog }}.{{ schema }}.`{{ asset }}` LIMIT 200)
WHERE operation = 'OPTIMIZE'
  AND timestamp >= current_timestamp() - INTERVAL {{ lookback_days }} DAYS
```

`LIMIT 200` bounds the history read on very active tables; raise it if a table receives more than 200 commits in the window (the diagnostic reports commit counts).

**(c) Per-table predicate**

In words: the table had a successful predictive optimization operation in the window, or it has liquid clustering (explicit or automatic) and an `OPTIMIZE` commit in the window.

As a SQL expression over the combined outputs (`last_po_operation` NULL when the table has no row in the SQL leg):

```sql
   last_po_operation IS NOT NULL
OR (
     (size(clusteringColumns) > 0 OR LOWER(COALESCE(properties['clusterByAuto'], 'false')) = 'true')
     AND last_optimize IS NOT NULL
   )
```

**(d) Aggregation**

```
total_tables       = count of probed tables
maintained_tables  = count where the predicate is true
value              = maintained_tables / total_tables    (NULL when total_tables = 0)
```

Report `maintained_tables`, `total_tables`, `value`.

### Active tables only (variant)

Restrict the denominator to tables with at least one write commit in the window (`operation IN ('WRITE','MERGE','UPDATE','DELETE','STREAMING UPDATE','COPY INTO')` in the same `DESCRIBE HISTORY` read). Tables that did not change needed no maintenance and are skipped, not failed. Add to the per-table probe:

```sql
SELECT MAX(CASE WHEN operation = 'OPTIMIZE' THEN timestamp END)       AS last_optimize,
       COUNT_IF(operation = 'OPTIMIZE')                                AS optimize_commits,
       COUNT_IF(operation IN ('WRITE','MERGE','UPDATE','DELETE','STREAMING UPDATE','COPY INTO'))
                                                                       AS write_commits
FROM (DESCRIBE HISTORY {{ catalog }}.{{ schema }}.`{{ asset }}` LIMIT 200)
WHERE timestamp >= current_timestamp() - INTERVAL {{ lookback_days }} DAYS
```

and skip tables with `write_commits = 0` in step (d).

### Predictive optimization only (variant, pure SQL)

No probes. Misses tables maintained by scheduled `OPTIMIZE` jobs, so it under-counts on schemas that predate predictive optimization; exact for schemas fully under it.

```sql
WITH tables_in_scope AS (
    SELECT LOWER(table_name) AS table_name
    FROM {{ catalog }}.information_schema.tables
    WHERE LOWER(table_schema) = LOWER('{{ schema }}')
      AND table_type IN ('MANAGED', 'EXTERNAL')
      AND data_source_format = 'DELTA'
),
po AS (
    SELECT DISTINCT LOWER(table_name) AS table_name
    FROM system.storage.predictive_optimization_operations_history
    WHERE LOWER(catalog_name) = LOWER('{{ catalog }}')
      AND LOWER(schema_name)  = LOWER('{{ schema }}')
      AND operation_status = 'SUCCESSFUL'
      AND start_time >= current_timestamp() - INTERVAL {{ lookback_days }} DAYS
)
SELECT
    COUNT_IF(po.table_name IS NOT NULL)           AS maintained_tables,
    COUNT(*)                                       AS total_tables,
    COUNT_IF(po.table_name IS NOT NULL)::DOUBLE
        / NULLIF(COUNT(*), 0)                      AS value
FROM tables_in_scope t
LEFT JOIN po USING (table_name)
```
