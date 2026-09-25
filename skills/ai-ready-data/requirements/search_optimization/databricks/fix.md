# Fix: search_optimization

Put layout maintenance on autopilot with predictive optimization, or schedule `OPTIMIZE` for clustered tables where predictive optimization is not available.

## Context

The durable fix is predictive optimization: Databricks runs `OPTIMIZE`, `VACUUM` and `ANALYZE` when a table's write pattern warrants it, bills the work to serverless compute, and records every operation in `system.storage.predictive_optimization_operations_history`, which is exactly what the check reads. Enable it at the catalog or schema level so new tables inherit it. Requirements: Unity Catalog managed tables (external tables are not covered by predictive optimization), the account-level setting turned on by an account admin, and a region where serverless is available.

For tables predictive optimization cannot cover (external tables, workspaces without serverless), a scheduled `OPTIMIZE` job on the clustered tables is the fallback, and the check's second leg credits it. `OPTIMIZE` is safe to re-run; it touches only files that need compaction or re-clustering.

Order of operations for an `UNMAINTAINED` table:

1. Give it a layout if it has none (`access_optimization` fix: `CLUSTER BY` or `CLUSTER BY AUTO`).
2. Enable predictive optimization (schema level preferred).
3. Run `OPTIMIZE` once now so the check and the queries do not wait for the first automatic pass.
4. If predictive optimization does not apply, schedule the job below.

Guards: predictive optimization state from `DESCRIBE SCHEMA EXTENDED` / `DESCRIBE EXTENDED` (row `Predictive Optimization`); skip when `ENABLE` or `INHERIT (ENABLE)`. `OPTIMIZE` needs no guard.

Permissions: schema or catalog ownership (or `MANAGE`) for `ALTER SCHEMA ... ENABLE PREDICTIVE OPTIMIZATION`; ownership or `MODIFY` on tables for `OPTIMIZE`; job creation rights for the scheduled fallback.

## Fix: Enable predictive optimization on the schema

```sql
ALTER SCHEMA {{ catalog }}.{{ schema }} ENABLE PREDICTIVE OPTIMIZATION
```

Catalog-wide, so every schema inherits:

```sql
ALTER CATALOG {{ catalog }} ENABLE PREDICTIVE OPTIMIZATION
```

A single table can opt out afterwards with `ALTER TABLE ... DISABLE PREDICTIVE OPTIMIZATION` (rarely warranted; a table with a custom `OPTIMIZE ZORDER` job is the usual case). Operations begin appearing in `system.storage.predictive_optimization_operations_history` within a day for tables with write activity.

## Fix: Optimize a table now

```sql
OPTIMIZE {{ catalog }}.{{ schema }}.{{ asset }};
```

For a table that was just given clustering keys, or whose keys changed, a full rewrite establishes the layout in one pass (run off-peak on large tables):

```sql
OPTIMIZE {{ catalog }}.{{ schema }}.{{ asset }} FULL;
```

Follow with `ANALYZE` so the optimizer has fresh statistics for the query planner:

```sql
ANALYZE TABLE {{ catalog }}.{{ schema }}.{{ asset }} COMPUTE STATISTICS FOR ALL COLUMNS;
```

## Fix: Schedule OPTIMIZE for tables outside predictive optimization

A SQL task in a Lakeflow job, run daily (hourly for streaming targets). Iterate over the clustered tables from the diagnostic; the statement list can be generated with the bulk generator below and pasted into the task, or the task can run a notebook that reads the list from a control table.

```bash
databricks jobs create --json '{
  "name": "optimize-{{ catalog }}-{{ schema }}",
  "schedule": {"quartz_cron_expression": "0 0 3 * * ?", "timezone_id": "UTC"},
  "tasks": [{
    "task_key": "optimize",
    "sql_task": {
      "warehouse_id": "{{ warehouse_id }}",
      "query": {"query_id": "{{ saved_query_id }}"}
    }
  }]
}'
```

Where `{{ saved_query_id }}` is a saved SQL query containing the `OPTIMIZE` statements. Guard: `databricks jobs list --name optimize-{{ catalog }}-{{ schema }}` returns nothing before creating.

## Fix: Convert a VACUUM-less schedule to include VACUUM

Maintenance is not only `OPTIMIZE`. Files removed by `OPTIMIZE` stay on storage until `VACUUM`. Add to the same job, never with a retention under 7 days:

```sql
VACUUM {{ catalog }}.{{ schema }}.{{ asset }} RETAIN 168 HOURS;
```

Predictive optimization does this on its own; the statement is only for the manual path.

## Fix: Bulk-generate OPTIMIZE for every unmaintained or stale table

Feed the diagnostic output in as a temp view `maint_diag(table_name, status)`.

```sql
SELECT concat('OPTIMIZE {{ catalog }}.{{ schema }}.`', table_name, '`;') AS stmt
FROM maint_diag
WHERE status IN ('UNMAINTAINED', 'CLUSTERED_STALE', 'UNCLUSTERED_OPTIMIZED')
ORDER BY table_name
```

Show the generated statements to the user before executing them. On a schema with many large tables, run them in a job rather than an interactive session.

## Organizational guidance

Maintenance should not depend on a person remembering a job. Enable predictive optimization at the catalog level in every environment and make it part of catalog creation (Terraform `databricks_catalog` with `enable_predictive_optimization = "ENABLE"`). Keep manual `OPTIMIZE` jobs only for the documented exceptions (external tables, Z-order legacy tables) and register them in one place so the diagnostic's `MANUAL_MAINTAINED` rows can be traced to a job. Review `CLUSTERED_STALE` monthly; it is the signature of a job that silently stopped.
