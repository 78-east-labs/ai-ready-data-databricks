# Check: batch_throughput_sufficiency

Fraction of recent bulk write statements into the schema that sustained at least `{{ min_rows_per_second }}` rows per second.

## Context

Reads `system.query.history` for successful write statements (`INSERT`, `MERGE`, `COPY`, `CREATE_TABLE_AS_SELECT`, `UPDATE`, `DELETE`, `REPLACE`) in the last `{{ lookback_days }}` days and computes per-statement throughput as `written_rows / (total_duration_ms / 1000)`. A statement is sufficient when that value is at or above `{{ min_rows_per_second }}`.

Placeholders and defaults: `{{ min_rows_per_second }}` = `10000`; `{{ lookback_days }}` = `7`.

Attribution to the schema is done by joining `system.query.history.statement_id` to `system.access.table_lineage.query_statement_id` where the lineage row's target is a table in `{{ catalog }}.{{ schema }}`. That is the accurate path: it credits a statement to the schema whose table it wrote, regardless of the session's default catalog and schema. The variant falls back to a regex on `statement_text` when lineage is not granted or has not caught up.

What the signal proves and does not prove. It is **native**: real statements, real durations, real row counts. `total_duration_ms` includes queueing and compilation, so a statement that waited for warehouse capacity looks slow even if the write itself was fast; that is still the throughput a consumer experienced. `written_rows` counts rows written by the statement, including rows rewritten by `MERGE` or `UPDATE` when Delta rewrites whole files, so a small targeted `UPDATE` on a large table can look like a fast bulk write. Zero-row statements are excluded so empty loads do not register as infinitely fast.

Coverage limits. `system.query.history` covers SQL warehouses, serverless notebooks and jobs, and Lakeflow pipelines. Writes from classic all-purpose or jobs clusters do not appear. Structured Streaming micro-batches appear as a stream of small commits in `DESCRIBE HISTORY` but not as statements here; if the schema is fed by streaming, this check under-reports and `DESCRIBE HISTORY ... operationMetrics.numOutputRows` is the better source (see the diagnostic). Lag: `system.query.history` minutes; `system.access.table_lineage` up to a few hours, so the most recent loads may be missing from the primary variant.

If `statement_type` values in your workspace differ from the list above, confirm with `SELECT DISTINCT statement_type FROM system.query.history WHERE start_time >= current_timestamp() - INTERVAL 7 DAYS`. `written_rows` is documented but has been null for some compute types in earlier releases; if the primary returns NULL on a schema you know is loaded, probe `SELECT COUNT_IF(written_rows IS NULL), COUNT(*) FROM system.query.history WHERE statement_type = 'INSERT' AND start_time >= current_timestamp() - INTERVAL 7 DAYS`.

Permissions: `SELECT` on `system.query.history` and `system.access.table_lineage` (metastore admin enables the `query` and `access` schemas).

Returns NULL (N/A) when no successful non-empty write statements into the schema occurred in the window.

## SQL

### Lineage-attributed writes (primary)

```sql
WITH schema_writes AS (
    SELECT DISTINCT query_statement_id AS statement_id
    FROM system.access.table_lineage
    WHERE LOWER(target_catalog) = LOWER('{{ catalog }}')
      AND LOWER(target_schema)  = LOWER('{{ schema }}')
      AND target_type IN ('TABLE', 'STREAMING_TABLE', 'MATERIALIZED_VIEW')
      AND query_statement_id IS NOT NULL
      AND event_time >= current_timestamp() - INTERVAL {{ lookback_days }} DAYS
),
write_statements AS (
    SELECT
        qh.statement_id,
        qh.written_rows,
        qh.total_duration_ms / 1000.0 AS elapsed_seconds
    FROM system.query.history qh
    JOIN schema_writes sw USING (statement_id)
    WHERE qh.statement_type IN ('INSERT', 'MERGE', 'COPY', 'CREATE_TABLE_AS_SELECT',
                                'UPDATE', 'DELETE', 'REPLACE')
      AND qh.execution_status = 'FINISHED'
      AND qh.start_time >= current_timestamp() - INTERVAL {{ lookback_days }} DAYS
      AND qh.total_duration_ms > 0
      AND qh.written_rows > 0
)
SELECT
    COUNT_IF(written_rows / NULLIF(elapsed_seconds, 0) >= {{ min_rows_per_second }})
                                                          AS sufficient_writes,
    COUNT(*)                                              AS total_writes,
    COUNT_IF(written_rows / NULLIF(elapsed_seconds, 0) >= {{ min_rows_per_second }})::DOUBLE
        / NULLIF(COUNT(*), 0)                             AS value
FROM write_statements
```

### Statement-text attribution (variant, no lineage needed)

Matches statements whose text names a table in the schema (`catalog.schema.table` or, when the session default catalog is `{{ catalog }}`, `schema.table`). Misses statements that reference the table through a view or a temp view, and can over-match a statement that only *reads* the schema while writing elsewhere. Faster to catch up because it does not wait for lineage.

```sql
WITH write_statements AS (
    SELECT
        statement_id,
        written_rows,
        total_duration_ms / 1000.0 AS elapsed_seconds
    FROM system.query.history
    WHERE statement_type IN ('INSERT', 'MERGE', 'COPY', 'CREATE_TABLE_AS_SELECT',
                             'UPDATE', 'DELETE', 'REPLACE')
      AND execution_status = 'FINISHED'
      AND start_time >= current_timestamp() - INTERVAL {{ lookback_days }} DAYS
      AND total_duration_ms > 0
      AND written_rows > 0
      AND REGEXP_LIKE(LOWER(statement_text),
            LOWER('(^|[^a-z0-9_])(`?{{ catalog }}`?\\.)?`?{{ schema }}`?\\.`?[a-z0-9_]+`?'))
)
SELECT
    COUNT_IF(written_rows / NULLIF(elapsed_seconds, 0) >= {{ min_rows_per_second }})
                                                          AS sufficient_writes,
    COUNT(*)                                              AS total_writes,
    COUNT_IF(written_rows / NULLIF(elapsed_seconds, 0) >= {{ min_rows_per_second }})::DOUBLE
        / NULLIF(COUNT(*), 0)                             AS value
FROM write_statements
```

### Delta commit throughput (variant, per table, covers streaming and cluster writes)

For a single `{{ asset }}`, `DESCRIBE HISTORY` exposes `operationMetrics.numOutputRows` and `operationMetrics.executionTimeMs` for `WRITE`, `MERGE`, `STREAMING UPDATE` and `COPY INTO` commits regardless of which compute produced them. Probe per table and aggregate across the schema as `sufficient_commits / total_commits`.

```sql
SELECT
    COUNT_IF(numOutputRows / NULLIF(executionTimeMs / 1000.0, 0) >= {{ min_rows_per_second }})
                                                          AS sufficient_writes,
    COUNT(*)                                              AS total_writes,
    COUNT_IF(numOutputRows / NULLIF(executionTimeMs / 1000.0, 0) >= {{ min_rows_per_second }})::DOUBLE
        / NULLIF(COUNT(*), 0)                             AS value
FROM (
    SELECT
        TRY_CAST(operationMetrics['numOutputRows']   AS BIGINT) AS numOutputRows,
        TRY_CAST(operationMetrics['executionTimeMs'] AS BIGINT) AS executionTimeMs
    FROM (DESCRIBE HISTORY {{ catalog }}.{{ schema }}.{{ asset }})
    WHERE operation IN ('WRITE', 'MERGE', 'STREAMING UPDATE', 'COPY INTO', 'CREATE TABLE AS SELECT')
      AND timestamp >= current_timestamp() - INTERVAL {{ lookback_days }} DAYS
)
WHERE numOutputRows > 0 AND executionTimeMs > 0
```

`executionTimeMs` is present for most write operations on current runtimes; commits that lack it fall out of both counts. Streaming micro-batches are individually small, so judge them by rows per second of the batch, not by absolute rows.
