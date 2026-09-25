# Diagnostic: incremental_update_coverage

Per-table summary of the last `{{ history_commits }}` commits: how many were incremental, how many were overwrites, which operations appeared, who made the last overwrite, and the disguised full-reload patterns the check cannot score.

## Context

Runs the same probe as the check and projects the fields that decide the fix. `overwrite_writers` (from `job.jobId`, `notebook.notebookId` and `userName` on the overwrite rows) names the pipeline to change. `full_delete_then_append` flags the `DELETE` with no predicate (`operationParameters['predicate']` missing or `[]`) followed by an `Append`; `partition_overwrites` counts `Overwrite` writes scoped by `replaceWhere` or `partitionBy`, which the check treats as incremental but which still rewrite whole partitions. `avg_rows_per_write` from `operationMetrics['numOutputRows']` helps tell "appends a day of data" from "appends the whole table again".

Status values: `INCREMENTAL`, `MIXED` (both kinds present), `FULL_RELOAD` (overwrites only), `NO_DATA_COMMITS` (nothing but maintenance in the window), `HISTORY_UNAVAILABLE` (probe failed). Sorted worst-first.

## SQL

### Per-table probe (run once per table from the check's enumeration)

```sql
DESCRIBE HISTORY {{ catalog }}.{{ schema }}.{{ asset }} LIMIT {{ history_commits }}
```

### Projection over the probe output

Apply to each table's rows (the `kind` expression is the classification from `check.md`):

```sql
WITH classified AS (
    SELECT
        version, timestamp, operation, operationParameters, operationMetrics, userName,
        job.jobId AS job_id, notebook.notebookId AS notebook_id,
        CASE
            WHEN version = 0 THEN 'maintenance'
            WHEN operation IN ('MERGE', 'UPDATE', 'DELETE', 'STREAMING UPDATE', 'COPY INTO') THEN 'incremental'
            WHEN operation = 'WRITE' AND operationParameters['mode'] = 'Append'               THEN 'incremental'
            WHEN operation = 'WRITE' AND operationParameters['mode'] = 'Overwrite'
                 AND COALESCE(operationParameters['predicate'], '') <> ''                      THEN 'incremental'
            WHEN operation = 'WRITE' AND operationParameters['mode'] = 'Overwrite'            THEN 'overwrite'
            WHEN operation IN ('CREATE OR REPLACE TABLE AS SELECT', 'REPLACE TABLE AS SELECT',
                               'TRUNCATE', 'RESTORE')                                          THEN 'overwrite'
            WHEN operation IN ('OPTIMIZE', 'VACUUM START', 'VACUUM END', 'SET TBLPROPERTIES',
                               'ADD COLUMNS', 'CHANGE COLUMN', 'DROP COLUMNS', 'RENAME COLUMN',
                               'CLUSTER BY', 'COMMENT ON', 'ADD CONSTRAINT', 'DROP CONSTRAINT',
                               'CREATE TABLE', 'CREATE TABLE AS SELECT', 'CONVERT', 'CLONE',
                               'UPGRADE PROTOCOL', 'ADD FEATURE', 'DROP FEATURE')              THEN 'maintenance'
            ELSE 'other'
        END AS kind,
        operation = 'DELETE' AND COALESCE(operationParameters['predicate'], '[]') IN ('[]', '') AS is_full_delete,
        operation = 'WRITE' AND operationParameters['mode'] = 'Overwrite'
            AND (COALESCE(operationParameters['predicate'], '') <> ''
                 OR COALESCE(operationParameters['partitionBy'], '[]') <> '[]')                AS is_partition_overwrite,
        LEAD(operation) OVER (ORDER BY version DESC)                                           AS previous_operation,
        TRY_CAST(operationMetrics['numOutputRows'] AS BIGINT)                                  AS rows_out
    FROM <probe output>
)
SELECT
    '{{ asset }}'                                                       AS table_name,
    COUNT_IF(kind = 'incremental')                                      AS incremental_commits,
    COUNT_IF(kind = 'overwrite')                                        AS overwrite_commits,
    COUNT_IF(kind = 'maintenance')                                      AS maintenance_commits,
    COUNT_IF(kind = 'other')                                            AS unclassified_commits,
    array_sort(collect_set(operation))                                  AS operations_seen,
    MAX(CASE WHEN kind = 'overwrite' THEN timestamp END)                AS last_overwrite_at,
    array_sort(collect_set(CASE WHEN kind = 'overwrite'
        THEN COALESCE(CAST(job_id AS STRING), CAST(notebook_id AS STRING), userName) END))
                                                                        AS overwrite_writers,
    COUNT_IF(is_full_delete AND previous_operation = 'WRITE')           AS full_delete_then_append,
    COUNT_IF(is_partition_overwrite)                                    AS partition_overwrites,
    ROUND(AVG(CASE WHEN kind IN ('incremental', 'overwrite') THEN rows_out END))
                                                                        AS avg_rows_per_write,
    MIN(timestamp)                                                      AS window_start,
    MAX(timestamp)                                                      AS window_end,
    CASE
        WHEN COUNT_IF(kind = 'overwrite') = 0 AND COUNT_IF(kind = 'incremental') > 0 THEN 'INCREMENTAL'
        WHEN COUNT_IF(kind = 'overwrite') > 0 AND COUNT_IF(kind = 'incremental') > 0 THEN 'MIXED'
        WHEN COUNT_IF(kind = 'overwrite') > 0                                        THEN 'FULL_RELOAD'
        ELSE 'NO_DATA_COMMITS'
    END                                                                 AS status
FROM classified
```

`previous_operation` uses `LEAD` over descending version, so it is the operation of the commit that came right after the `DELETE` in time. `operationParameters['predicate']` is a JSON-ish string (for example `["(id > 100)"]`); an unfiltered `DELETE` records `[]` or omits the key.

Collect one row per table and sort:

```
ORDER BY CASE status WHEN 'FULL_RELOAD' THEN 0 WHEN 'MIXED' THEN 1 WHEN 'NO_DATA_COMMITS' THEN 2 ELSE 3 END,
         overwrite_commits DESC, table_name
```

### Overwrite statements from query history (pure SQL, whole schema)

Complements the probe with the statement text of recent overwrites, so the operator can see the exact `INSERT OVERWRITE` or `CREATE OR REPLACE` and where it ran.

```sql
SELECT
    LOWER(l.target_table_full_name)                           AS full_name,
    q.start_time,
    q.statement_type,
    q.executed_by,
    q.query_source.job_info.job_id                            AS job_id,
    q.query_source.notebook_id                                AS notebook_id,
    q.written_rows,
    LEFT(q.statement_text, 300)                               AS statement_head
FROM system.access.table_lineage l
JOIN system.query.history q ON q.statement_id = l.query_statement_id
WHERE LOWER(l.target_catalog) = LOWER('{{ catalog }}')
  AND LOWER(l.target_schema)  = LOWER('{{ schema }}')
  AND l.target_table_full_name IS NOT NULL
  AND l.event_time >= current_timestamp() - INTERVAL {{ lookback_days }} DAYS
  AND q.start_time >= current_timestamp() - INTERVAL {{ lookback_days }} DAYS
  AND (q.statement_type IN ('REPLACE_TABLE_AS_SELECT', 'TRUNCATE')
       OR REGEXP_LIKE(LOWER(q.statement_text), 'insert\\s+overwrite|create\\s+or\\s+replace\\s+table'))
ORDER BY q.start_time DESC
LIMIT 200
```
