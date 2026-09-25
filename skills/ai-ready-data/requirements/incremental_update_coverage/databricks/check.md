# Check: incremental_update_coverage

Fraction of base Delta tables whose recent data-changing commits are all incremental (append, merge, update, delete, streaming update) rather than full overwrites.

## Context

Every Delta commit records how it changed the table. `DESCRIBE HISTORY` exposes it as `operation` plus `operationParameters`. An incremental pipeline leaves a trail of `WRITE` with `operationParameters['mode'] = 'Append'`, `MERGE`, `UPDATE`, `DELETE`, `STREAMING UPDATE` or `COPY INTO`. A full-reload pipeline leaves `WRITE` with `mode = 'Overwrite'`, `CREATE OR REPLACE TABLE AS SELECT`, `REPLACE TABLE AS SELECT`, `TRUNCATE` or `RESTORE`. The check reads the last `{{ history_commits }}` commits (default 20) of each table and passes the table when it has at least one data-changing commit and none of them is a full overwrite.

Maintenance commits (`OPTIMIZE`, `VACUUM START`, `VACUUM END`, `SET TBLPROPERTIES`, `ADD COLUMNS`, `CHANGE COLUMN`, `CLUSTER BY`, `COMMENT ON`, `ADD CONSTRAINT`, `DROP CONSTRAINT`) are ignored. Version 0 is ignored too: the commit that created the table is always a full write and says nothing about how it is maintained. A table whose last N commits are all maintenance or only version 0 has no evidence either way and is counted as failing (it is not being updated at all, incrementally or otherwise); the diagnostic labels these `NO_DATA_COMMITS`.

Two patterns this cannot see: a `DELETE` with no predicate followed by an `Append` is a full reload disguised as incremental commits, and an `INSERT OVERWRITE` into a partition is a partial overwrite recorded as `WRITE` / `Overwrite` with `partitionBy` or a `replaceWhere` predicate in `operationParameters`. The predicate below treats a `WRITE` with `operationParameters['predicate']` set (the `replaceWhere` case) as incremental, because it only replaces a slice; the diagnostic surfaces both patterns so a human can judge.

Strength is **native** and this is a **probe mode** check: `DESCRIBE HISTORY` is a metadata read, one call per table, needs `SELECT` on the table, and is available only as long as the transaction log is retained (`delta.logRetentionDuration`, default 30 days). Streaming tables and materialized views are excluded from the denominator: their pipeline decides incremental versus full recompute and `feature_refresh_compliance` covers them.

Returns NULL (N/A) when the schema contains no base Delta tables.

## SQL

### Recent commit operations (primary, probe mode)

**(a) Enumerate tables in scope**

```sql
SELECT LOWER(table_name) AS table_name,
       CONCAT('`{{ catalog }}`.`{{ schema }}`.`', table_name, '`') AS qualified_name
FROM {{ catalog }}.information_schema.tables
WHERE LOWER(table_schema) = LOWER('{{ schema }}')
  AND table_type IN ('MANAGED', 'EXTERNAL')
  AND UPPER(data_source_format) = 'DELTA'
ORDER BY table_name
```

**(b) Per-table probe statement**

```sql
DESCRIBE HISTORY {{ catalog }}.{{ schema }}.{{ asset }} LIMIT {{ history_commits }}
```

Output columns used: `version`, `timestamp`, `operation`, `operationParameters` (MAP<STRING,STRING>), `isBlindAppend`, `operationMetrics`.

**(c) Per-table predicate**

In words: among the returned rows with `version > 0`, classify each as incremental, overwrite or maintenance. The table passes when the count of incremental rows is at least one and the count of overwrite rows is zero.

Classification as a SQL expression over one probe row:

```sql
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
END
```

Table-level predicate over the classified rows:

```sql
COUNT_IF(kind = 'incremental') >= 1 AND COUNT_IF(kind = 'overwrite') = 0
```

`other` rows (operations not listed) neither pass nor fail the table; report them so the list can be extended.

**(d) Aggregation rule**

```
incremental_tables = probed tables where the table-level predicate is true
total_tables       = probed tables (DESCRIBE HISTORY errors are excluded and reported)
value              = incremental_tables / total_tables, NULL when total_tables = 0
```

### Write statements from query history (variant, pure SQL approximation)

Approximates the same signal from `system.query.history` joined to `system.access.table_lineage` on `statement_id = query_statement_id`, which attributes each SQL write to its target table. Overwrite statements are `REPLACE_TABLE_AS_SELECT`, `TRUNCATE`, and `INSERT` whose text contains `INSERT OVERWRITE`; incremental ones are `INSERT`, `MERGE`, `UPDATE`, `DELETE`, `COPY`. What it misses: writes from Structured Streaming and DataFrame APIs on classic clusters (never in `query.history`), pipeline writes, and anything older than the window. What it adds: it works after the Delta log has been truncated and needs no per-table call. Tables with no attributed write in the window fail, as in the primary.

`statement_type` tokens in `system.query.history` are believed to include `INSERT`, `MERGE`, `UPDATE`, `DELETE`, `COPY`, `CREATE_TABLE_AS_SELECT`, `REPLACE_TABLE_AS_SELECT`, `TRUNCATE`; confirm with `SELECT DISTINCT statement_type FROM system.query.history` before relying on the variant. `{{ lookback_days }}` defaults to 7.

```sql
WITH tables_in_scope AS (
    SELECT LOWER(table_name) AS table_name,
           CONCAT(LOWER('{{ catalog }}'), '.', LOWER('{{ schema }}'), '.', LOWER(table_name)) AS full_name
    FROM {{ catalog }}.information_schema.tables
    WHERE LOWER(table_schema) = LOWER('{{ schema }}')
      AND table_type IN ('MANAGED', 'EXTERNAL')
      AND UPPER(data_source_format) = 'DELTA'
),
writes AS (
    SELECT DISTINCT
        LOWER(l.target_table_full_name) AS full_name,
        q.statement_id,
        CASE
            WHEN q.statement_type IN ('REPLACE_TABLE_AS_SELECT', 'TRUNCATE')              THEN 'overwrite'
            WHEN q.statement_type = 'INSERT'
                 AND REGEXP_LIKE(LOWER(q.statement_text), 'insert\\s+overwrite')          THEN 'overwrite'
            WHEN q.statement_type IN ('INSERT', 'MERGE', 'UPDATE', 'DELETE', 'COPY')       THEN 'incremental'
            ELSE 'other'
        END AS kind
    FROM system.access.table_lineage l
    JOIN system.query.history q
      ON q.statement_id = l.query_statement_id
    WHERE LOWER(l.target_catalog) = LOWER('{{ catalog }}')
      AND LOWER(l.target_schema)  = LOWER('{{ schema }}')
      AND l.target_table_full_name IS NOT NULL
      AND l.event_time  >= current_timestamp() - INTERVAL {{ lookback_days }} DAYS
      AND q.start_time  >= current_timestamp() - INTERVAL {{ lookback_days }} DAYS
      AND q.execution_status = 'FINISHED'
),
per_table AS (
    SELECT t.table_name,
           COUNT_IF(w.kind = 'incremental') AS incremental_writes,
           COUNT_IF(w.kind = 'overwrite')   AS overwrite_writes
    FROM tables_in_scope t
    LEFT JOIN writes w USING (full_name)
    GROUP BY t.table_name
)
SELECT
    COUNT_IF(incremental_writes >= 1 AND overwrite_writes = 0)           AS incremental_tables,
    COUNT(*)                                                             AS total_tables,
    COUNT_IF(incremental_writes >= 1 AND overwrite_writes = 0)::DOUBLE
        / NULLIF(COUNT(*), 0)                                            AS value
FROM per_table
```
