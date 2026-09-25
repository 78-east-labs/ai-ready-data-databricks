# Check: serving_latency_compliance

Fraction of recent successful SELECT statements that read a table in the schema and completed within `{{ latency_threshold_ms }}` milliseconds.

## Context

Reads `system.query.history` for `SELECT` statements with `execution_status = 'FINISHED'` in the last `{{ lookback_days }}` days and compares `total_duration_ms` with the threshold. Attribution to the schema uses `system.access.table_lineage`: a statement counts when its `statement_id` matches a lineage row's `query_statement_id` whose source table is in `{{ catalog }}.{{ schema }}`. This credits a statement to the schema whose tables it read, regardless of the session's default catalog and schema, and follows reads through views down to the underlying tables (lineage records the base table).

Placeholders and defaults: `{{ latency_threshold_ms }}` = `1000`; `{{ lookback_days }}` = `7`.

`total_duration_ms` is wall-clock from submission to completion and includes queue time, compilation, execution and result fetch. That is the latency a consumer experienced, so it is the right number for an SLA. `execution_duration_ms` is the engine's share; the diagnostic shows both so a warehouse queueing problem is distinguishable from a slow query. Statements served from the result cache have very small durations and count as compliant, which is fair (the consumer got the answer) but flatters a schema whose dashboards repeat the same query; the no-cache variant removes them where the column is available.

Coverage limits, which matter for this check more than most:

- `system.query.history` covers SQL warehouses, serverless notebooks and jobs, and Lakeflow pipelines. Reads from classic all-purpose clusters, from model serving feature lookups (Lakebase), and from Vector Search queries do **not** appear. If "serving" in this schema means an online store or a vector index, this check measures only the SQL side; say so in the report.
- `table_lineage` records one row per (statement, source table); statements that read only views whose lineage was not captured, or that failed before planning, are missing. The statement-text variant is the fallback.
- Lag: `system.query.history` minutes; `system.access.table_lineage` up to a few hours. The most recent hours may be under-represented in the primary variant.

Permissions: `SELECT` on `system.query.history` and `system.access.table_lineage`. Both are metastore-wide; rows are not filtered to the caller's tables, so the check sees statements run by everyone.

Returns NULL (N/A) when no successful SELECT read a table in the schema during the window.

## SQL

### Lineage-attributed SELECTs (primary)

```sql
WITH schema_reads AS (
    SELECT DISTINCT query_statement_id AS statement_id
    FROM system.access.table_lineage
    WHERE LOWER(source_catalog) = LOWER('{{ catalog }}')
      AND LOWER(source_schema)  = LOWER('{{ schema }}')
      AND query_statement_id IS NOT NULL
      AND event_time >= current_timestamp() - INTERVAL {{ lookback_days }} DAYS
),
selects AS (
    SELECT qh.statement_id, qh.total_duration_ms
    FROM system.query.history qh
    JOIN schema_reads sr USING (statement_id)
    WHERE qh.statement_type = 'SELECT'
      AND qh.execution_status = 'FINISHED'
      AND qh.start_time >= current_timestamp() - INTERVAL {{ lookback_days }} DAYS
)
SELECT
    COUNT_IF(total_duration_ms <= {{ latency_threshold_ms }})            AS compliant_queries,
    COUNT(*)                                                             AS total_queries,
    COUNT_IF(total_duration_ms <= {{ latency_threshold_ms }})::DOUBLE
        / NULLIF(COUNT(*), 0)                                            AS value
FROM selects
```

### Statement-text attribution (variant, no lineage needed)

Matches statements whose text names `{{ catalog }}.{{ schema }}.<table>` or `{{ schema }}.<table>`. Misses reads through views and through a session default schema without qualification; over-matches statements that mention the schema in a comment or string. No lineage lag.

```sql
WITH selects AS (
    SELECT statement_id, total_duration_ms
    FROM system.query.history
    WHERE statement_type = 'SELECT'
      AND execution_status = 'FINISHED'
      AND start_time >= current_timestamp() - INTERVAL {{ lookback_days }} DAYS
      AND REGEXP_LIKE(LOWER(statement_text),
            LOWER('(^|[^a-z0-9_])(`?{{ catalog }}`?\\.)?`?{{ schema }}`?\\.`?[a-z0-9_]+`?'))
)
SELECT
    COUNT_IF(total_duration_ms <= {{ latency_threshold_ms }})            AS compliant_queries,
    COUNT(*)                                                             AS total_queries,
    COUNT_IF(total_duration_ms <= {{ latency_threshold_ms }})::DOUBLE
        / NULLIF(COUNT(*), 0)                                            AS value
FROM selects
```

### Serving traffic only (variant)

Interactive exploration in the SQL editor is not serving. This variant keeps statements issued by applications and jobs: `client_application` outside the Databricks UI clients, or a `query_source` that is a job, dashboard, alert or Genie space, or a `query_tags` entry marking the workload. Adjust the client list to the schema's consumers (`SELECT client_application, COUNT(*) FROM system.query.history WHERE start_time >= current_timestamp() - INTERVAL 7 DAYS GROUP BY 1` shows what is there).

```sql
WITH schema_reads AS (
    SELECT DISTINCT query_statement_id AS statement_id
    FROM system.access.table_lineage
    WHERE LOWER(source_catalog) = LOWER('{{ catalog }}')
      AND LOWER(source_schema)  = LOWER('{{ schema }}')
      AND query_statement_id IS NOT NULL
      AND event_time >= current_timestamp() - INTERVAL {{ lookback_days }} DAYS
),
selects AS (
    SELECT qh.statement_id, qh.total_duration_ms
    FROM system.query.history qh
    JOIN schema_reads sr USING (statement_id)
    WHERE qh.statement_type = 'SELECT'
      AND qh.execution_status = 'FINISHED'
      AND qh.start_time >= current_timestamp() - INTERVAL {{ lookback_days }} DAYS
      AND (
            qh.query_source.job_info.job_id IS NOT NULL
         OR qh.query_source.dashboard_id     IS NOT NULL
         OR qh.query_source.alert_id         IS NOT NULL
         OR qh.query_source.genie_space_id   IS NOT NULL
         OR qh.query_tags['workload'] = 'serving'
         OR NOT REGEXP_LIKE(LOWER(COALESCE(qh.client_application, '')),
                            '(databricks sql editor|databricks notebook|databricks query editor)')
      )
)
SELECT
    COUNT_IF(total_duration_ms <= {{ latency_threshold_ms }})            AS compliant_queries,
    COUNT(*)                                                             AS total_queries,
    COUNT_IF(total_duration_ms <= {{ latency_threshold_ms }})::DOUBLE
        / NULLIF(COUNT(*), 0)                                            AS value
FROM selects
```

### Per-table breakdown (variant, table-scoped)

The same measurement for one `{{ asset }}`, for orchestrators that score per table.

```sql
WITH table_reads AS (
    SELECT DISTINCT query_statement_id AS statement_id
    FROM system.access.table_lineage
    WHERE LOWER(source_table_full_name) = LOWER('{{ catalog }}.{{ schema }}.{{ asset }}')
      AND query_statement_id IS NOT NULL
      AND event_time >= current_timestamp() - INTERVAL {{ lookback_days }} DAYS
)
SELECT
    COUNT_IF(qh.total_duration_ms <= {{ latency_threshold_ms }})         AS compliant_queries,
    COUNT(*)                                                             AS total_queries,
    COUNT_IF(qh.total_duration_ms <= {{ latency_threshold_ms }})::DOUBLE
        / NULLIF(COUNT(*), 0)                                            AS value
FROM system.query.history qh
JOIN table_reads tr USING (statement_id)
WHERE qh.statement_type = 'SELECT'
  AND qh.execution_status = 'FINISHED'
  AND qh.start_time >= current_timestamp() - INTERVAL {{ lookback_days }} DAYS
```
