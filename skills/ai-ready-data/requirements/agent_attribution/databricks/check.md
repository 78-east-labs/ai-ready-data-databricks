# Check: agent_attribution

Fraction of recent write events against tables in the schema that are attributable to a named job, pipeline, notebook, dashboard or other Databricks entity, or to a statement carrying query tags, rather than to an anonymous session.

## Context

Reads `system.access.table_lineage`, which records one row per (source, target) pair for every read and write Unity Catalog observes. A row whose `target_table_full_name` is in `{{ catalog }}.{{ schema }}` is a write to that table. Each row names the compute entity that produced it: `entity_type` (observed values include `JOB`, `PIPELINE`, `NOTEBOOK`, `DASHBOARD`, `QUERY`, `GENIE_SPACE`, `ALERT`; the set is open and matched case-insensitively), `entity_id` and `entity_run_id`. A NULL `entity_type` means Unity Catalog could not tie the write to any workspace object: a JDBC/ODBC session, an external Delta writer, a bare SQL editor session on some runtimes, or an API caller. Those are the anonymous writes this check penalizes.

A write with a NULL entity can still be attributed if the statement set query tags. `system.query.history.query_tags` is a `map<string,string>` populated when the session ran `SET query_tags = 'pipeline=x,run=y'` (the Databricks analogue of Snowflake's `QUERY_TAG`; support depends on the warehouse type and runtime version, and classic all-purpose clusters do not write to `system.query.history` at all). The check joins `table_lineage.query_statement_id` to `query.history.statement_id` and counts a non-empty map as attribution.

The signal is native. `entity_type` proves which workspace object ran the write. It does not prove which human or agent was responsible: `user_identity.email` is always populated (it is the run-as identity) and is deliberately not counted, because a shared service principal running twenty pipelines gives twenty identical emails.

Writes from one statement produce one lineage row per source table, so rows are collapsed to distinct write events on (target, event_time, entity, statement) before counting.

Placeholder `{{ lookback_days }}` defaults to 30. `system.access.table_lineage` lags by up to a few hours and `system.query.history` by minutes, so writes from the last few hours may be missing or may temporarily appear unattributed. Reading both tables needs `USE SCHEMA` on `system.access` and `system.query` plus `SELECT` on the tables, and the schemas must be enabled by a metastore admin.

Returns NULL (N/A) when no writes to the schema were recorded in the window.

## SQL

### Lineage entity or query tag (primary)

```sql
WITH write_events AS (
    SELECT DISTINCT
        target_table_full_name,
        event_time,
        UPPER(entity_type)  AS entity_type,
        entity_id,
        entity_run_id,
        query_statement_id
    FROM system.access.table_lineage
    WHERE LOWER(target_catalog) = LOWER('{{ catalog }}')
      AND LOWER(target_schema)  = LOWER('{{ schema }}')
      AND target_table_full_name IS NOT NULL
      AND event_time >= current_timestamp() - INTERVAL {{ lookback_days }} DAYS
    ORDER BY event_time DESC
    LIMIT 100000
),
tagged_statements AS (
    SELECT statement_id
    FROM system.query.history
    WHERE start_time >= current_timestamp() - INTERVAL {{ lookback_days }} DAYS
      AND query_tags IS NOT NULL
      AND cardinality(query_tags) > 0
)
SELECT
    COUNT_IF(w.entity_type IS NOT NULL OR ts.statement_id IS NOT NULL)          AS attributed_writes,
    COUNT(*)                                                                    AS total_writes,
    COUNT_IF(w.entity_type IS NOT NULL OR ts.statement_id IS NOT NULL)::DOUBLE
        / NULLIF(COUNT(*), 0)                                                   AS value
FROM write_events w
LEFT JOIN tagged_statements ts
       ON w.query_statement_id = ts.statement_id
```

### Query history source or tags (variant)

Restricts the population to write statements that are visible in `system.query.history` (SQL warehouses, serverless notebooks and jobs, Lakeflow pipelines) and attributes them by `query_source` (job, notebook, dashboard, SQL query, alert or Genie space id) or by non-empty `query_tags`. Use this when the schema is written mainly through warehouses and you want the richer `query_source` breakdown. It misses writes from classic clusters entirely, so the denominator can be much smaller than the primary's.

```sql
WITH schema_write_statements AS (
    SELECT DISTINCT query_statement_id AS statement_id
    FROM system.access.table_lineage
    WHERE LOWER(target_catalog) = LOWER('{{ catalog }}')
      AND LOWER(target_schema)  = LOWER('{{ schema }}')
      AND target_table_full_name IS NOT NULL
      AND query_statement_id IS NOT NULL
      AND event_time >= current_timestamp() - INTERVAL {{ lookback_days }} DAYS
),
statements AS (
    SELECT
        h.statement_id,
        h.query_source,
        h.query_tags
    FROM system.query.history h
    JOIN schema_write_statements s USING (statement_id)
    WHERE h.start_time >= current_timestamp() - INTERVAL {{ lookback_days }} DAYS
      AND h.execution_status = 'FINISHED'
    ORDER BY h.start_time DESC
    LIMIT 100000
)
SELECT
    COUNT_IF(
        query_source.job_info.job_id IS NOT NULL
        OR query_source.notebook_id      IS NOT NULL
        OR query_source.dashboard_id     IS NOT NULL
        OR query_source.sql_query_id     IS NOT NULL
        OR query_source.alert_id         IS NOT NULL
        OR query_source.genie_space_id   IS NOT NULL
        OR (query_tags IS NOT NULL AND cardinality(query_tags) > 0)
    )                                                                   AS attributed_statements,
    COUNT(*)                                                            AS total_statements,
    COUNT_IF(
        query_source.job_info.job_id IS NOT NULL
        OR query_source.notebook_id      IS NOT NULL
        OR query_source.dashboard_id     IS NOT NULL
        OR query_source.sql_query_id     IS NOT NULL
        OR query_source.alert_id         IS NOT NULL
        OR query_source.genie_space_id   IS NOT NULL
        OR (query_tags IS NOT NULL AND cardinality(query_tags) > 0)
    )::DOUBLE / NULLIF(COUNT(*), 0)                                     AS value
FROM statements
```
