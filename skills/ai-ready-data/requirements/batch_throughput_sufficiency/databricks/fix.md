# Fix: batch_throughput_sufficiency

Raise write throughput on the tables and statements the diagnostic flagged as `SLOW` or `FAILED`.

## Context

There is no single statement that makes loads faster. The diagnostic tells you which of four causes applies, and each has a concrete remedy:

- **`MERGE` rescans the target** (`read_rows` far above `written_rows`): the target is not clustered on the merge key, so every merge reads the whole table. Liquid clustering on the merge key plus deletion vectors turns this into a targeted rewrite.
- **Small-file accumulation** (many `SLOW` appends on the same table, `avg_file_mb` low in `DESCRIBE DETAIL`): each write pays for the existing file layout. Enable predictive optimization or optimized writes.
- **Row-by-row or tiny-batch inserts** (hundreds of `INSERT` statements with `written_rows` in the tens): the fix is upstream, batch into `COPY INTO` or a Structured Streaming / Auto Loader ingest.
- **Compute under-sized or queued** (`total_duration_ms` much higher than `execution_duration_ms`): the statement waited for the warehouse. Scale the warehouse or move loads to serverless jobs.

Everything below is idempotent or guarded. None of it rewrites data outside `OPTIMIZE`.

Permissions: ownership or `MODIFY` on the table for `ALTER TABLE` / `OPTIMIZE`; `CAN MANAGE` on the warehouse to resize it.

## Fix: Cluster the target on the merge key and enable deletion vectors

Guard: read `clusteringColumns` and `properties['delta.enableDeletionVectors']` from `DESCRIBE DETAIL {{ catalog }}.{{ schema }}.{{ asset }}`. Skip the `CLUSTER BY` if the keys already match; warn if they differ. Skip the property if already `true`.

```sql
ALTER TABLE {{ catalog }}.{{ schema }}.{{ asset }} CLUSTER BY ({{ merge_key_columns }});

ALTER TABLE {{ catalog }}.{{ schema }}.{{ asset }}
SET TBLPROPERTIES ('delta.enableDeletionVectors' = 'true');

OPTIMIZE {{ catalog }}.{{ schema }}.{{ asset }};
```

Deletion vectors let `MERGE`, `UPDATE` and `DELETE` mark rows instead of rewriting whole files. Readers on very old runtimes (below DBR 12.2) cannot read tables with deletion vectors; check external readers before enabling.

## Fix: Turn on optimized writes and auto compaction

Cheaper than a full predictive optimization rollout when only a few tables are hot. Both are table properties, idempotent to re-set.

```sql
ALTER TABLE {{ catalog }}.{{ schema }}.{{ asset }}
SET TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact'   = 'true'
);
```

Optimized writes coalesce output into fewer, larger files at write time; auto compaction runs a small `OPTIMIZE` after writes that leave many small files. Each adds a little latency to the write and removes far more from the reads and the next merge.

## Fix: Enable predictive optimization on the schema

Preferred when the whole schema is loaded regularly. Guard: `DESCRIBE SCHEMA EXTENDED {{ catalog }}.{{ schema }}` row `Predictive Optimization`.

```sql
ALTER SCHEMA {{ catalog }}.{{ schema }} ENABLE PREDICTIVE OPTIMIZATION
```

## Fix: Replace row-level inserts with COPY INTO

For file-based sources. `COPY INTO` is idempotent by file: files already loaded are skipped on re-run, so it is safe to schedule.

```sql
COPY INTO {{ catalog }}.{{ schema }}.{{ asset }}
FROM '{{ source_path }}'
FILEFORMAT = {{ source_format }}
FORMAT_OPTIONS ('mergeSchema' = 'true')
COPY_OPTIONS  ('mergeSchema' = 'true');
```

For continuous arrival, Auto Loader (`cloudFiles`) in a Lakeflow pipeline or a streaming job gives the same idempotency with incremental listing, and its micro-batches show up in `DESCRIBE HISTORY` as `STREAMING UPDATE` commits.

## Fix: Give loads their own compute

Loads that queue behind dashboards inflate `total_duration_ms`. Either resize the warehouse used for loads or route load jobs to serverless job compute. Resizing through the CLI (the SQL `ALTER WAREHOUSE` statement does not exist on Databricks):

```bash
databricks warehouses edit {{ warehouse_id }} \
  --cluster-size "{{ warehouse_size }}" \
  --max-num-clusters {{ max_clusters }}
```

`cluster-size` takes values like `2X-Small`, `Small`, `Medium`, `Large`. Raising `max-num-clusters` addresses queueing (many concurrent loads); raising `cluster-size` addresses single large statements.

## Fix: Bulk-generate table property changes for slow-load tables

Feed the diagnostic's per-table roll-up in as a temp view `slow_tables(table_name)` (tables whose `sufficient_statements / write_statements` is below your bar).

```sql
SELECT concat(
    'ALTER TABLE {{ catalog }}.{{ schema }}.`', table_name,
    '` SET TBLPROPERTIES (''delta.autoOptimize.optimizeWrite'' = ''true'', ',
    '''delta.autoOptimize.autoCompact'' = ''true'', ',
    '''delta.enableDeletionVectors'' = ''true'');'
) AS stmt
FROM slow_tables
ORDER BY table_name
```

Show the generated statements to the user before executing them.

## Organizational guidance

Throughput problems recur when every team hand-rolls ingestion. Standardize on Auto Loader or `COPY INTO` for files and Lakeflow Declarative Pipelines for transformations, set `optimizeWrite`, `autoCompact` and deletion vectors as defaults in the table-creation template (dbt `tblproperties`, Terraform `properties`), enable predictive optimization at the catalog level, and give load jobs their own serverless compute so their duration reflects the write and not the queue. Track the check weekly; a drop usually means a new job started writing row-by-row.
