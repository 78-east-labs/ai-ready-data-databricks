# Fix: point_lookup_availability

Cluster each table on its lookup key with liquid clustering and `OPTIMIZE` once; use a Bloom filter index only for high-cardinality equality columns on tables that must stay partitioned.

## Context

The preferred fix is liquid clustering on the lookup key. It gives file-level pruning on equality and range predicates, works with `MERGE`, needs no directory layout, and is maintained by `OPTIMIZE` (manual or predictive). Order of preference:

1. **`CLUSTER BY (key columns)`** when the lookup key is known. Put the PK (or the most selective lookup column) first; up to 4 columns. Then `OPTIMIZE` once so existing files are rewritten in key order.
2. **`CLUSTER BY AUTO`** when the key is not known; Databricks picks from query history. Needs predictive optimization enabled on the table.
3. **Bloom filter index** for a table that must keep Hive partitioning (Bloom filters and liquid clustering are mutually exclusive), where lookups hit a high-cardinality column that is not the partition key (a UUID, an email hash). It is a per-file index; it speeds equality only, and it is built for existing files by the `CREATE` statement and for new files on write.

None of these rewrites data at `ALTER` time; `OPTIMIZE` and `CREATE BLOOMFILTER INDEX` do the rewriting and are safe to re-run.

Guards: read `clusteringColumns`, `partitionColumns` and `properties['clusterByAuto']` from `DESCRIBE DETAIL`. If clustering keys already equal the intended keys, skip. If they differ, show both sets to the owner before replacing (a `CLUSTER BY` replaces, it does not append). If `partitionColumns` is non-empty, `CLUSTER BY` converts the table away from partitioning on current runtimes; confirm that is intended.

Permissions: ownership or `MODIFY` on the table.

## Fix: Cluster on the primary key

`{{ lookup_columns }}` defaults to the PK columns from the diagnostic.

```sql
ALTER TABLE {{ catalog }}.{{ schema }}.{{ asset }} CLUSTER BY ({{ lookup_columns }});

OPTIMIZE {{ catalog }}.{{ schema }}.{{ asset }};
```

Also turn on deletion vectors if the table takes `MERGE`/`UPDATE`/`DELETE`, so changes do not undo the layout (guard: property already `true`):

```sql
ALTER TABLE {{ catalog }}.{{ schema }}.{{ asset }}
SET TBLPROPERTIES ('delta.enableDeletionVectors' = 'true');
```

## Fix: Automatic clustering when the key is unknown

Requires predictive optimization on the table (guard: `DESCRIBE EXTENDED` row `Predictive Optimization`).

```sql
ALTER TABLE {{ catalog }}.{{ schema }}.{{ asset }} ENABLE PREDICTIVE OPTIMIZATION;
ALTER TABLE {{ catalog }}.{{ schema }}.{{ asset }} CLUSTER BY AUTO;
```

Keys are chosen after enough query history accumulates (days, not minutes). Re-run the diagnostic after a week; `clustering_columns` fills in when selection has happened.

## Fix: Bloom filter index on a partitioned table

Only for tables that keep Hive partitioning. Guard: the column is not already in `bloom_columns` from the diagnostic. `numItems` should be close to the number of distinct values per file times a safety factor; the default (1,000,000) is fine for most tables, and `fpp` 0.1 balances size and selectivity.

```sql
CREATE BLOOMFILTER INDEX
ON TABLE {{ catalog }}.{{ schema }}.{{ asset }}
FOR COLUMNS ({{ column }} OPTIONS (fpp = 0.1, numItems = 1000000));
```

The statement scans the table once to build the index for existing files. Re-running with the same options is a no-op in effect; with different options it rebuilds. Remove with `DROP BLOOMFILTER INDEX ON TABLE ... FOR COLUMNS ({{ column }})` if the table later moves to liquid clustering.

## Fix: Declare the lookup key first

For `LAYOUT_NO_PK` and `NO_LAYOUT` tables without a PK, declare it so the next person (and `CLUSTER BY AUTO`) know the key. Informational, not enforced; check uniqueness first with `SELECT COUNT(*) - COUNT(DISTINCT {{ lookup_columns }}) FROM ...` (must be 0). Guard: no `PRIMARY KEY` row in `table_constraints`.

```sql
ALTER TABLE {{ catalog }}.{{ schema }}.{{ asset }} ALTER COLUMN {{ lookup_columns }} SET NOT NULL;
ALTER TABLE {{ catalog }}.{{ schema }}.{{ asset }} ADD CONSTRAINT {{ asset }}_pk PRIMARY KEY ({{ lookup_columns }});
```

## Fix: Bulk-generate CLUSTER BY on the PK for every NO_LAYOUT table with a key

Feed the diagnostic output in as a temp view `lookup_diag(table_name, status, pk_columns, partition_columns)`.

```sql
SELECT concat(
    'ALTER TABLE {{ catalog }}.{{ schema }}.`', table_name,
    '` CLUSTER BY (', array_join(transform(pk_columns, c -> concat('`', c, '`')), ', '), '); ',
    'OPTIMIZE {{ catalog }}.{{ schema }}.`', table_name, '`;'
) AS stmt
FROM lookup_diag
WHERE status IN ('NO_LAYOUT', 'LAYOUT_OFF_KEY')
  AND pk_columns IS NOT NULL
  AND size(partition_columns) = 0
ORDER BY table_name
```

Partitioned tables are excluded so the conversion away from partitioning is an explicit decision. Show the generated statements to the user before executing them.

## Organizational guidance

Make "declare the key, cluster on the key" part of the table template: dbt models with `liquid_clustered_by` set to the unique key, Lakeflow tables with `cluster_by` on the primary key, Terraform `cluster_keys`. Enable predictive optimization at the catalog level so the layout stays maintained without per-table `OPTIMIZE` jobs. Treat a serving table with `LAYOUT_OFF_KEY` as a bug in review, not a tuning opportunity later.
