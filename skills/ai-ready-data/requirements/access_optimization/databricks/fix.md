# Fix: access_optimization

Give large tables a physical layout: enable predictive optimization, apply liquid clustering, then run `OPTIMIZE` once so the layout takes effect.

## Context

Three options, from least to most decision-making:

- **Enable predictive optimization** on the schema (or catalog). Databricks then runs `OPTIMIZE`, `VACUUM` and `ANALYZE` on its own schedule, sized to how the table is written and read. No keys to choose, no jobs to maintain. On its own it fixes small files but does not add data skipping on filter columns.
- **`CLUSTER BY AUTO`** turns on liquid clustering and lets Databricks pick keys from observed query patterns. Requires predictive optimization to be enabled for the table. Good default when you do not know the access pattern yet.
- **`CLUSTER BY (cols)`** sets explicit liquid clustering keys. Pick 1 to 4 columns that appear most often in filters and joins, most selective first. Best when the serving queries are known (a customer id, a date, a tenant).

All three are metadata operations: `ALTER TABLE ... CLUSTER BY` does not rewrite data. Existing files stay unclustered until `OPTIMIZE` runs (manually or by predictive optimization). New writes are clustered as they land. Liquid clustering is incompatible with Hive-style partitioning and with Bloom filter indexes on the same table; a partitioned table has to be converted first (see below).

Guard: `ALTER TABLE ... CLUSTER BY` replaces existing keys. Read `clusteringColumns` from `DESCRIBE DETAIL` first. If it already equals the intended keys the fix is a no-op; if it holds different keys, show them to the owner and get approval before replacing.

```sql
SELECT clusteringColumns, partitionColumns, properties['clusterByAuto'] AS cluster_by_auto
FROM (DESCRIBE DETAIL {{ catalog }}.{{ schema }}.{{ asset }})
```

Permissions: table ownership or `MODIFY` for `ALTER TABLE` and `OPTIMIZE`; schema or catalog ownership (or `MANAGE`) for `ALTER SCHEMA ... ENABLE PREDICTIVE OPTIMIZATION`. Predictive optimization must be enabled at the account level by an account admin before schema-level enablement has any effect.

## Fix: Enable predictive optimization on the schema

Idempotent; re-running is a no-op. Check first with `DESCRIBE SCHEMA EXTENDED {{ catalog }}.{{ schema }}` (row `Predictive Optimization`).

```sql
ALTER SCHEMA {{ catalog }}.{{ schema }} ENABLE PREDICTIVE OPTIMIZATION
```

Per-table form, for schemas where only some tables should be maintained:

```sql
ALTER TABLE {{ catalog }}.{{ schema }}.{{ asset }} ENABLE PREDICTIVE OPTIMIZATION
```

The first `COMPACTION` row appears in `system.storage.predictive_optimization_operations_history` hours to a day later, depending on write activity. Until then the check's third branch does not fire.

## Fix: Automatic liquid clustering on a single table

Requires predictive optimization enabled on the table (above). Databricks selects keys and revises them as query patterns change.

```sql
ALTER TABLE {{ catalog }}.{{ schema }}.{{ asset }} CLUSTER BY AUTO
```

## Fix: Explicit liquid clustering keys on a single table

Replace `{{ clustering_columns }}` with 1 to 4 comma-separated columns, most selective filter column first. Then run `OPTIMIZE` so existing files are rewritten in clustered order; on a 10 GB+ table budget for a full rewrite the first time.

```sql
ALTER TABLE {{ catalog }}.{{ schema }}.{{ asset }} CLUSTER BY ({{ clustering_columns }});

OPTIMIZE {{ catalog }}.{{ schema }}.{{ asset }};
```

`OPTIMIZE` is safe to re-run; it only touches files that need it.

## Fix: Convert a partitioned table to liquid clustering

Liquid clustering cannot be enabled while `partitionColumns` is non-empty. `ALTER TABLE ... CLUSTER BY` on a partitioned table removes the partitioning as part of the same operation on current runtimes (DBR 15.2+ / current SQL warehouses); older runtimes reject it. Confirm on a copy or with the owner before running on production. Do not use `CREATE OR REPLACE TABLE` for this: it breaks time travel, streaming readers and Delta Sync indexes.

```sql
ALTER TABLE {{ catalog }}.{{ schema }}.{{ asset }} CLUSTER BY ({{ clustering_columns }});

OPTIMIZE {{ catalog }}.{{ schema }}.{{ asset }} FULL;
```

`OPTIMIZE FULL` rewrites every file so the old partition layout is fully replaced. Run it once, off-peak.

## Fix: Bulk-generate CLUSTER BY AUTO for every NEEDS LAYOUT table

Feed the diagnostic's `NEEDS LAYOUT` table names in as a temp view `needs_layout(table_name)` and generate one statement per table. Tables with `partition_columns` set are excluded here so partition conversion is handled deliberately.

```sql
SELECT concat(
    'ALTER TABLE {{ catalog }}.{{ schema }}.`', table_name,
    '` CLUSTER BY AUTO;'
) AS stmt
FROM needs_layout
ORDER BY table_name
```

If the orchestrator holds the probe results as `probe_detail`, the same generator can be written directly against it:

```sql
SELECT concat(
    'ALTER TABLE {{ catalog }}.{{ schema }}.`', table_name,
    '` CLUSTER BY AUTO;'
) AS stmt
FROM probe_detail
WHERE sizeInBytes >= {{ large_table_bytes }}
  AND size(clusteringColumns) = 0
  AND size(partitionColumns)  = 0
  AND LOWER(COALESCE(properties['clusterByAuto'], 'false')) <> 'true'
ORDER BY table_name
```

Show the generated statements to the user before executing them.

## Organizational guidance

Make layout a default, not a retrofit. Enable predictive optimization at the catalog level so every new schema inherits it, and put `CLUSTER BY AUTO` (or explicit keys for known serving tables) into the table-creation templates: Lakeflow Declarative Pipelines `cluster_by` on the table decorator, dbt `liquid_clustered_by` in model config, Terraform `databricks_sql_table` `cluster_keys`. Review the diagnostic quarterly for tables that crossed the size threshold since the last pass.
