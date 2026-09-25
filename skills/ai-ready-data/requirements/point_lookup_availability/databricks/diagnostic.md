# Diagnostic: point_lookup_availability

One row per base Delta table with its declared primary key, clustering keys, partition columns, Bloom-filtered columns, deletion vector status, file count, and a status that says whether the layout covers the key.

## Context

Built from the check's enumeration, `DESCRIBE DETAIL` probes, the optional Bloom filter probe, and `key_column_usage` for the primary key. The status compares layout to key:

- `KEY_COVERED`: a clustering, partition or Bloom column is one of the PK columns. Point lookups on the key prune files.
- `LAYOUT_OFF_KEY`: the table has a layout but none of its columns is in the PK. Range queries may be fine; key lookups scan.
- `LAYOUT_NO_PK`: layout exists, no PK declared, so coverage cannot be judged. Declare the key (`entity_identifier_declaration`).
- `NO_LAYOUT`: nothing prunes. Fix candidates. Tables with a PK sort first inside this group because the fix is obvious (cluster on the PK).

`deletion_vectors` is reported because `MERGE`/`UPDATE`/`DELETE`-heavy tables without deletion vectors rewrite files on every change and defeat clustering over time. `avg_file_mb` under 16 MB on a table with a layout indicates it has never been `OPTIMIZE`d; the layout exists on paper only.

The last query measures a real lookup: run it against one table and key value and read the query profile (`read_files` versus `numFiles`) to confirm pruning is happening.

## SQL

### Per-table probe (run for each table from the check's enumeration)

```sql
DESCRIBE DETAIL {{ catalog }}.{{ schema }}.`{{ asset }}`
```

### Primary key columns (once per schema)

```sql
SELECT LOWER(k.table_name) AS table_name,
       array_sort(collect_list(struct(k.ordinal_position, LOWER(k.column_name)))).col2 AS pk_columns
FROM {{ catalog }}.information_schema.key_column_usage k
JOIN {{ catalog }}.information_schema.table_constraints c
  ON k.constraint_name = c.constraint_name
 AND k.table_schema = c.table_schema AND k.table_name = c.table_name
WHERE LOWER(k.table_schema) = LOWER('{{ schema }}')
  AND c.constraint_type = 'PRIMARY KEY'
GROUP BY LOWER(k.table_name)
```

### Assembled output (shape and status rules)

With the probe rows in a temp view `probe_detail(table_name, sizeInBytes, numFiles, clusteringColumns, partitionColumns, properties, bloom_columns)` and the PK rows in `pk_columns`:

```sql
WITH layout AS (
    SELECT
        p.table_name,
        transform(p.clusteringColumns, c -> LOWER(c)) AS clustering_columns,
        transform(p.partitionColumns,  c -> LOWER(c)) AS partition_columns,
        transform(COALESCE(p.bloom_columns, array()), c -> LOWER(c)) AS bloom_columns,
        LOWER(COALESCE(p.properties['clusterByAuto'], 'false')) = 'true' AS cluster_by_auto,
        LOWER(COALESCE(p.properties['delta.enableDeletionVectors'], 'false')) = 'true' AS deletion_vectors,
        p.numFiles,
        p.sizeInBytes,
        k.pk_columns
    FROM probe_detail p
    LEFT JOIN pk_columns k USING (table_name)
),
scored AS (
    SELECT *,
        size(clustering_columns) > 0 OR cluster_by_auto
            OR size(partition_columns) > 0 OR size(bloom_columns) > 0        AS has_layout,
        pk_columns IS NOT NULL AND (
               arrays_overlap(clustering_columns, pk_columns)
            OR arrays_overlap(partition_columns,  pk_columns)
            OR arrays_overlap(bloom_columns,      pk_columns))                AS key_covered
    FROM layout
)
SELECT
    table_name,
    pk_columns,
    clustering_columns,
    cluster_by_auto,
    partition_columns,
    bloom_columns,
    deletion_vectors,
    numFiles                                                      AS num_files,
    ROUND(sizeInBytes / 1073741824.0, 2)                          AS size_gb,
    ROUND(sizeInBytes / NULLIF(numFiles, 0) / 1048576.0, 1)       AS avg_file_mb,
    CASE
        WHEN key_covered                         THEN 'KEY_COVERED'
        WHEN has_layout AND pk_columns IS NOT NULL THEN 'LAYOUT_OFF_KEY'
        WHEN has_layout                          THEN 'LAYOUT_NO_PK'
        ELSE 'NO_LAYOUT'
    END AS status,
    CASE
        WHEN key_covered THEN 'Ready; run OPTIMIZE if avg_file_mb is small'
        WHEN has_layout AND pk_columns IS NOT NULL
             THEN concat('Add the PK to clustering keys: CLUSTER BY (', array_join(pk_columns, ', '), ')')
        WHEN has_layout THEN 'Declare a PRIMARY KEY so coverage can be judged'
        WHEN pk_columns IS NOT NULL
             THEN concat('CLUSTER BY (', array_join(pk_columns, ', '), ') then OPTIMIZE')
        ELSE 'Declare a PRIMARY KEY, then CLUSTER BY it'
    END AS recommendation
FROM scored
ORDER BY
    CASE status WHEN 'NO_LAYOUT' THEN 1 WHEN 'LAYOUT_OFF_KEY' THEN 2 WHEN 'LAYOUT_NO_PK' THEN 3 ELSE 4 END,
    pk_columns IS NULL,
    sizeInBytes DESC
```

### Confirm pruning on one table

Run once and open the query profile (Query History, or `EXPLAIN` output on a warehouse). If `files read` is close to `numFiles`, the layout is not pruning for this predicate.

```sql
SELECT *
FROM {{ catalog }}.{{ schema }}.{{ asset }}
WHERE {{ column }} = {{ probe_value }}
LIMIT 10
```

`system.query.history` records the same run: `read_files` and `read_bytes` on the statement row (columns present on current releases; probe `SELECT read_files FROM system.query.history LIMIT 1` to confirm).
