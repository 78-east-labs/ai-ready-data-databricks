# Check: point_lookup_availability

Fraction of base Delta tables in the schema with a key-friendly physical layout: liquid clustering, partitioning, or a Bloom filter index, so a single-key lookup prunes files instead of scanning the table.

## Context

Databricks has no secondary indexes. A `WHERE id = ?` lookup is fast only when Delta can skip files, and Delta skips files using one of three things: liquid clustering keys (files are laid out by key, with per-file min/max statistics), Hive partitions (the key is a directory), or a Bloom filter index on the column (a per-file probabilistic index for equality predicates). None of these appear in `information_schema`, so this is a **probe-mode** check.

Per table the orchestrator runs `DESCRIBE DETAIL` and evaluates: `clusteringColumns` non-empty, or `CLUSTER BY AUTO` on, or `partitionColumns` non-empty, or a Bloom filter index present. Unlike `access_optimization` there is no size threshold: a point lookup on a 500 MB table with 2,000 small files still reads 2,000 footers, and the requirement is about lookup readiness, not about large-table hygiene.

**Bloom filter detection.** Bloom filter index metadata is stored in the **column** metadata of the Delta schema (`delta.bloomFilter.enabled`, `delta.bloomFilter.fpp`, `delta.bloomFilter.numItems`, `delta.bloomFilter.maxExpectedFpp`), not in table properties, so `DESCRIBE DETAIL.properties` and `SHOW TBLPROPERTIES` do not show it. The reliable probe is the schema's field metadata, read from PySpark (`spark.table(t).schema`) or from the SDK's `w.tables.get(full_name).columns[*].type_json`, which carries the same metadata. Both are given below. When only SQL is available, the check drops the Bloom branch and reports a lower bound; say so in the report. Bloom filter indexes cannot coexist with liquid clustering and Databricks now recommends liquid clustering instead, so on most current schemas the Bloom branch adds few tables.

What passes proves a layout exists, not that it matches the lookup key. A table clustered on `event_date` still scans for `WHERE customer_id = ?`. The diagnostic puts the layout columns next to the declared primary key so a mismatch is visible; use `{{ lookup_columns }}` in the strict predicate below when the key is known.

Lag: none, `DESCRIBE DETAIL` is live. Permissions: `SELECT` on each table.

Returns NULL (N/A) when the schema has no base Delta tables.

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

Read `clusteringColumns`, `partitionColumns`, `properties`, `numFiles`, `sizeInBytes`.

**(b2) Bloom filter probe** (per table, PySpark in a notebook; skip when unavailable)

```python
def bloom_columns(full_name):
    return [f.name for f in spark.table(full_name).schema.fields
            if str(f.metadata.get("delta.bloomFilter.enabled", "")).lower() == "true"]
```

SDK equivalent without a Spark session (`type_json` holds the field metadata as a JSON string):

```python
import json
from databricks.sdk import WorkspaceClient
w = WorkspaceClient()
def bloom_columns(full_name):
    cols = w.tables.get(full_name=full_name).columns or []
    return [c.name for c in cols
            if str(json.loads(c.type_json or "{}").get("metadata", {})
                   .get("delta.bloomFilter.enabled", "")).lower() == "true"]
```

If `type_json` does not carry the metadata on your release, confirm with one table that is known to have a Bloom filter index (`CREATE BLOOMFILTER INDEX` history in `DESCRIBE HISTORY`, operation `CREATE BLOOMFILTER INDEX`) and fall back to the PySpark probe.

**(c) Per-table predicate**

In words: the table has liquid clustering keys, or automatic liquid clustering, or partition columns, or at least one Bloom-filtered column.

As a SQL expression over the probe output (with `bloom_columns` supplied by b2 as an array, empty when b2 was skipped):

```sql
   size(clusteringColumns) > 0
OR LOWER(COALESCE(properties['clusterByAuto'], 'false')) = 'true'
OR size(partitionColumns) > 0
OR size(bloom_columns) > 0
```

Strict form, when the lookup key is known (`{{ lookup_columns }}`, comma-separated, defaults to the table's primary key columns from `key_column_usage`): the layout must cover the key.

```sql
   arrays_overlap(clusteringColumns, split('{{ lookup_columns }}', ','))
OR arrays_overlap(partitionColumns,  split('{{ lookup_columns }}', ','))
OR arrays_overlap(bloom_columns,     split('{{ lookup_columns }}', ','))
```

**(d) Aggregation**

```
total_tables         = count of probed tables
lookup_ready_tables  = count where the predicate is true
value                = lookup_ready_tables / total_tables    (NULL when total_tables = 0)
```

Report `lookup_ready_tables`, `total_tables`, `value`, and whether the Bloom branch was evaluated.

### Partition-only approximation (variant, pure SQL)

`information_schema.columns.partition_index` reveals Hive partitioning only. Misses liquid clustering (the common case on current schemas) and Bloom filters, so treat the result as a floor.

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
    COUNT_IF(p.table_name IS NOT NULL)            AS lookup_ready_tables,
    COUNT(*)                                       AS total_tables,
    COUNT_IF(p.table_name IS NOT NULL)::DOUBLE
        / NULLIF(COUNT(*), 0)                      AS value
FROM tables_in_scope t
LEFT JOIN partitioned p USING (table_name)
```

### Primary key declared (variant, pure SQL, proxy)

A declared `PRIMARY KEY` says which column lookups use but does nothing physically. Useful as a companion number: tables with a PK and no layout on it are the highest-value fixes.

```sql
WITH tables_in_scope AS (
    SELECT LOWER(table_name) AS table_name
    FROM {{ catalog }}.information_schema.tables
    WHERE LOWER(table_schema) = LOWER('{{ schema }}')
      AND table_type IN ('MANAGED', 'EXTERNAL')
      AND data_source_format = 'DELTA'
),
pk AS (
    SELECT DISTINCT LOWER(table_name) AS table_name
    FROM {{ catalog }}.information_schema.table_constraints
    WHERE LOWER(table_schema) = LOWER('{{ schema }}')
      AND constraint_type = 'PRIMARY KEY'
)
SELECT
    COUNT_IF(pk.table_name IS NOT NULL)           AS tables_with_pk,
    COUNT(*)                                       AS total_tables,
    COUNT_IF(pk.table_name IS NOT NULL)::DOUBLE
        / NULLIF(COUNT(*), 0)                      AS value
FROM tables_in_scope t
LEFT JOIN pk USING (table_name)
```
