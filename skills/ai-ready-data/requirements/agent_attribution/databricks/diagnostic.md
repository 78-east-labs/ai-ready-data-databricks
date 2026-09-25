# Diagnostic: agent_attribution

Lists recent write events to the schema with the entity that produced them, the run-as identity, the statement's query tags and an attribution label, unattributed writes first.

## Context

Use this to find which sessions are writing anonymously. Each row is one distinct write event from `system.access.table_lineage` (the same population and window as the check), joined to `system.query.history` for the statement text, compute and query tags when the statement ran on a warehouse or serverless compute. Writes from classic clusters have no `query.history` row, so `statement_type`, `warehouse_id` and `query_tags` are NULL for them; that is itself a hint that the writer is a cluster job or an ad hoc notebook.

`attribution_status` is `ENTITY` when the lineage row names a workspace entity, `TAGGED` when only query tags identify it, and `UNATTRIBUTED` when neither is present. `user_identity.email` is shown for triage but does not count as attribution.

The second query aggregates the same events per target table so you can see which tables receive the most anonymous writes. Both tables lag (lineage up to a few hours, query history minutes). `{{ lookback_days }}` defaults to 30.

## SQL

### Recent write events (primary)

```sql
WITH write_events AS (
    SELECT DISTINCT
        target_table_name,
        event_time,
        UPPER(entity_type)  AS entity_type,
        entity_id,
        entity_run_id,
        query_statement_id,
        user_identity.email AS run_as
    FROM system.access.table_lineage
    WHERE LOWER(target_catalog) = LOWER('{{ catalog }}')
      AND LOWER(target_schema)  = LOWER('{{ schema }}')
      AND target_table_full_name IS NOT NULL
      AND event_time >= current_timestamp() - INTERVAL {{ lookback_days }} DAYS
    ORDER BY event_time DESC
    LIMIT 100000
),
statements AS (
    SELECT statement_id, statement_type, compute.warehouse_id, client_application, query_tags
    FROM system.query.history
    WHERE start_time >= current_timestamp() - INTERVAL {{ lookback_days }} DAYS
)
SELECT
    w.target_table_name,
    w.event_time,
    w.entity_type,
    w.entity_id,
    w.entity_run_id,
    w.run_as,
    s.statement_type,
    s.warehouse_id,
    s.client_application,
    to_json(s.query_tags)                       AS query_tags,
    CASE
        WHEN w.entity_type IS NOT NULL                                      THEN 'ENTITY'
        WHEN s.query_tags IS NOT NULL AND cardinality(s.query_tags) > 0     THEN 'TAGGED'
        ELSE 'UNATTRIBUTED'
    END                                          AS attribution_status
FROM write_events w
LEFT JOIN statements s
       ON w.query_statement_id = s.statement_id
ORDER BY
    CASE
        WHEN w.entity_type IS NOT NULL                                      THEN 2
        WHEN s.query_tags IS NOT NULL AND cardinality(s.query_tags) > 0     THEN 1
        ELSE 0
    END ASC,
    w.event_time DESC
LIMIT 100
```

### Per-table summary (variant)

```sql
WITH write_events AS (
    SELECT DISTINCT
        LOWER(target_table_name) AS table_name,
        event_time,
        UPPER(entity_type)       AS entity_type,
        entity_id,
        query_statement_id,
        user_identity.email      AS run_as
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
    w.table_name,
    COUNT(*)                                                             AS total_writes,
    COUNT_IF(w.entity_type IS NULL AND ts.statement_id IS NULL)          AS unattributed_writes,
    array_sort(collect_set(w.entity_type))                               AS entity_types_seen,
    array_sort(collect_set(CASE WHEN w.entity_type IS NULL
                                 AND ts.statement_id IS NULL
                                THEN w.run_as END))                      AS anonymous_run_as,
    MAX(w.event_time)                                                    AS last_write
FROM write_events w
LEFT JOIN tagged_statements ts
       ON w.query_statement_id = ts.statement_id
GROUP BY w.table_name
ORDER BY unattributed_writes DESC, total_writes DESC, w.table_name
```
