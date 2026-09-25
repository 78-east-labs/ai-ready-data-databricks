# Fix: serving_latency_compliance

Reduce SELECT latency on the schema: cluster the tables the slow queries filter on, materialize repeated expensive shapes, give serving traffic its own warehouse, and move true point lookups to an online store.

## Context

The diagnostic separates the causes; the fixes map onto them:

| Diagnostic signal | Cause | Fix |
|---|---|---|
| `read_rows` large, `produced_rows` small | No file pruning on the filter column | Liquid clustering on the filter column, `OPTIMIZE` |
| `p95_overhead_ms` dominates on a warehouse | Queueing or cold start | Dedicated serving warehouse, autoscaling, serverless |
| Same shape executed hundreds of times, slow | Repeated aggregation or join | Materialized view with a refresh schedule |
| Latency under 100 ms required, single-key reads | Wrong engine | Lakebase synced table (`feature_materialization_coverage`) |
| Slow queries all on one table with `avg_file_mb` small | Small files | Predictive optimization / `OPTIMIZE` (`search_optimization`) |

Everything here is idempotent or guarded. `ALTER TABLE ... CLUSTER BY` replaces keys; read `clusteringColumns` from `DESCRIBE DETAIL` first and confirm with the owner when they differ. Materialized views and warehouses are created with existence guards.

Permissions: ownership or `MODIFY` on tables; `CREATE MATERIALIZED VIEW` on the schema (serverless or Pro warehouse); `CAN MANAGE` on the warehouse or workspace admin to create one.

## Fix: Cluster on the filter columns of the slow queries

Take the filter columns from the slow statements' text (diagnostic query 1) or the primary key when lookups are by key. Then compact once so existing files are ordered.

```sql
ALTER TABLE {{ catalog }}.{{ schema }}.{{ asset }} CLUSTER BY ({{ filter_columns }});

OPTIMIZE {{ catalog }}.{{ schema }}.{{ asset }};
```

For tables with unknown or shifting patterns, `CLUSTER BY AUTO` with predictive optimization enabled picks keys from the same query history the diagnostic reads.

## Fix: Materialize a repeated slow shape

For a shape from the last diagnostic query. A materialized view serves the precomputed result and refreshes on a schedule; consumers switch their `FROM` clause. Choose a refresh interval at or below the freshness the consumer needs.

```sql
CREATE MATERIALIZED VIEW IF NOT EXISTS {{ catalog }}.{{ schema }}.{{ asset }}_mv
SCHEDULE EVERY {{ refresh_minutes }} MINUTES
CLUSTER BY ({{ filter_columns }})
COMMENT 'Precomputed serving shape for {{ asset }}; source query in repo {{ repo_path }}'
AS
{{ slow_query_text }};
```

Materialized views refresh incrementally when the query is incrementally computable (joins, aggregations over Delta sources with CDF); otherwise they recompute, which is still cheaper than every consumer recomputing.

## Fix: Give serving traffic a dedicated warehouse

Interactive exploration and scheduled loads should not share a warehouse with serving reads. A small serverless warehouse with autoscaling keeps p95 overhead low; serverless removes cold-start latency. Guard: `databricks warehouses list --output json | jq '.[] | select(.name == "{{ serving_warehouse_name }}")'` returns nothing.

```bash
databricks warehouses create --json '{
  "name": "{{ serving_warehouse_name }}",
  "cluster_size": "Small",
  "min_num_clusters": 1,
  "max_num_clusters": 4,
  "enable_serverless_compute": true,
  "auto_stop_mins": 0,
  "warehouse_type": "PRO",
  "tags": {"custom_tags": [{"key": "workload", "value": "serving"}]}
}'
```

`auto_stop_mins: 0` keeps it warm; set a non-zero value if idle cost matters more than first-query latency. Point the serving application at the new warehouse's HTTP path and tag its statements (`SET query_tags = 'workload:serving'` per session, or via the connector's `query_tags` option) so the serving-only variant of the check isolates them.

Resize an existing warehouse instead:

```bash
databricks warehouses edit {{ warehouse_id }} --cluster-size "Medium" --max-num-clusters 4
```

## Fix: Move key lookups to an online store

If the slow statements are `WHERE id = ?` with a strict sub-100 ms budget, a SQL warehouse is the wrong engine regardless of layout. Create a Lakebase synced table from the serving table (`feature_materialization_coverage` fix) and read it through the Postgres endpoint or a Feature Serving endpoint. The SQL check will keep scoring the warehouse reads that remain; the point lookups leave the population.

## Fix: Enable the result cache and statistics

Two cheap items that shave latency on every warehouse query:

```sql
ANALYZE TABLE {{ catalog }}.{{ schema }}.{{ asset }} COMPUTE STATISTICS FOR ALL COLUMNS;
```

Fresh column statistics improve join ordering and pruning. Predictive optimization runs `ANALYZE` on its own once enabled. Result caching on warehouses is on by default; if a client disables it (`use_cached_result = false`), turn it back on for serving sessions.

## Fix: Bulk-generate CLUSTER BY for the worst tables

Feed the per-table roll-up in as a temp view `latency_by_table(table_name, compliance, queries)` together with the PK columns from `key_column_usage`. Tables without a PK are listed for manual key selection rather than clustered blindly.

```sql
WITH pk AS (
    SELECT LOWER(k.table_name) AS table_name,
           array_sort(collect_list(struct(k.ordinal_position, k.column_name))).column_name AS pk_columns
    FROM {{ catalog }}.information_schema.key_column_usage k
    JOIN {{ catalog }}.information_schema.table_constraints c
      ON k.constraint_name = c.constraint_name
     AND k.table_schema = c.table_schema AND k.table_name = c.table_name
    WHERE LOWER(k.table_schema) = LOWER('{{ schema }}') AND c.constraint_type = 'PRIMARY KEY'
    GROUP BY LOWER(k.table_name)
)
SELECT
    l.table_name,
    l.compliance,
    l.queries,
    CASE WHEN p.pk_columns IS NOT NULL THEN concat(
        'ALTER TABLE {{ catalog }}.{{ schema }}.`', l.table_name, '` CLUSTER BY (',
        array_join(transform(p.pk_columns, c -> concat('`', c, '`')), ', '), '); ',
        'OPTIMIZE {{ catalog }}.{{ schema }}.`', l.table_name, '`;')
    ELSE '-- no PK: pick filter columns from the slow statements' END AS stmt
FROM latency_by_table l
LEFT JOIN pk p USING (table_name)
WHERE l.compliance < 0.95
ORDER BY l.queries DESC
```

Show the generated statements to the user before executing them.

## Organizational guidance

Latency SLAs hold when serving reads are isolated and measured. Give serving its own tagged warehouse, alert on the check's value dropping below target using a SQL alert over `system.query.history`, and require any new dashboard or agent that reads the schema to declare its latency budget so the table owner can cluster for it. Review the repeated-shape query monthly; the top three shapes are usually worth a materialized view each.
