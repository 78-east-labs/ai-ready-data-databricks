# Diagnostic: schema_evolution_tracking

Per-table view of effective log retention, the oldest version still readable, and the schema-changing commits currently visible in history.

## Context

Two probes per table. `DESCRIBE DETAIL` gives the retention property (and `createdAt`, useful because a table younger than the retention window necessarily has its whole history). `DESCRIBE HISTORY` gives what is actually still there: the oldest retained version and timestamp (how far back schema can be reconstructed today), and the list of schema-changing operations with the columns they touched. Together they answer "is history retained long enough" and "has the schema actually changed, and can we still see how".

Status values: `RETAINED` (effective retention meets `{{ min_history_days }}`), `SHORT_RETENTION` (explicit property below threshold), `DEFAULT_BELOW_THRESHOLD` (property unset and 30 days is below the threshold), `UNPARSEABLE` (property set to a value the check cannot read). Sorted worst-first, then by most schema churn.

## SQL

### Retention probe (per table from the check's enumeration)

```sql
DESCRIBE DETAIL {{ catalog }}.{{ schema }}.{{ asset }}
```

Project from the probe output:

```sql
WITH parsed AS (
    SELECT
        name                                                 AS full_name,
        createdAt                                            AS created_at,
        lastModified                                         AS last_modified,
        properties['delta.logRetentionDuration']             AS log_retention_raw,
        properties['delta.deletedFileRetentionDuration']     AS data_retention_raw,
        CASE
            WHEN properties['delta.logRetentionDuration'] IS NULL THEN 30.0
            WHEN REGEXP_LIKE(LOWER(properties['delta.logRetentionDuration']), 'interval\\s+[0-9]+\\s+day')
                THEN CAST(regexp_extract(LOWER(properties['delta.logRetentionDuration']), 'interval\\s+([0-9]+)', 1) AS DOUBLE)
            WHEN REGEXP_LIKE(LOWER(properties['delta.logRetentionDuration']), 'interval\\s+[0-9]+\\s+hour')
                THEN CAST(regexp_extract(LOWER(properties['delta.logRetentionDuration']), 'interval\\s+([0-9]+)', 1) AS DOUBLE) / 24.0
            WHEN REGEXP_LIKE(LOWER(properties['delta.logRetentionDuration']), 'interval\\s+[0-9]+\\s+week')
                THEN CAST(regexp_extract(LOWER(properties['delta.logRetentionDuration']), 'interval\\s+([0-9]+)', 1) AS DOUBLE) * 7.0
            ELSE NULL
        END                                                  AS effective_log_retention_days
    FROM <probe output>
)
SELECT
    full_name,
    created_at,
    last_modified,
    COALESCE(log_retention_raw, '(unset, default interval 30 days)') AS log_retention_raw,
    effective_log_retention_days,
    data_retention_raw,
    CASE
        WHEN effective_log_retention_days IS NULL                     THEN 'UNPARSEABLE'
        WHEN effective_log_retention_days >= {{ min_history_days }}   THEN 'RETAINED'
        WHEN log_retention_raw IS NULL                                THEN 'DEFAULT_BELOW_THRESHOLD'
        ELSE 'SHORT_RETENTION'
    END                                                              AS status
FROM parsed
```

### History probe (per table)

```sql
DESCRIBE HISTORY {{ catalog }}.{{ schema }}.{{ asset }}
```

This returns every retained commit (no `LIMIT`, since the oldest one is needed). For tables with tens of thousands of commits, run it with `LIMIT 1000` and accept that `oldest_retained_version` is then a lower bound. Project:

```sql
SELECT
    MIN(version)                                                      AS oldest_retained_version,
    MIN(timestamp)                                                    AS oldest_retained_at,
    MAX(version)                                                      AS current_version,
    timestampdiff(DAY, MIN(timestamp), current_timestamp())           AS observable_history_days,
    COUNT_IF(operation IN ('ADD COLUMNS', 'CHANGE COLUMN', 'DROP COLUMNS', 'RENAME COLUMN'))
                                                                      AS schema_change_commits,
    MAX(CASE WHEN operation IN ('ADD COLUMNS', 'CHANGE COLUMN', 'DROP COLUMNS', 'RENAME COLUMN')
             THEN timestamp END)                                      AS last_schema_change_at,
    COUNT_IF(operation = 'WRITE'
             AND (LOWER(COALESCE(operationParameters['mergeSchema'], '')) = 'true'
                  OR LOWER(COALESCE(operationParameters['overwriteSchema'], '')) = 'true'))
                                                                      AS implicit_schema_writes,
    array_sort(collect_set(CASE WHEN operation = 'ADD COLUMNS'
                                THEN operationParameters['columns'] END))
                                                                      AS added_column_specs
FROM <probe output>
```

`added_column_specs` holds the JSON that `ADD COLUMNS` records (`[{"column":{"name":"x","type":"string",...}}]`), which is the closest Databricks has to a per-column change log. `implicit_schema_writes` counts DataFrame writes that changed the schema without a DDL statement; these are the ones no audit or query log will show as an `ALTER`.

`operationParameters['mergeSchema']` and `['overwriteSchema']` are believed to be the recorded keys for schema-evolving writes; if they are always null in your history, run `DESCRIBE HISTORY` on a table you know evolved via `mergeSchema` and check the keys present.

### Schema at a retained version (per table, on demand)

To see the columns as they were at a version the history probe reports as retained:

```sql
DESCRIBE TABLE {{ catalog }}.{{ schema }}.{{ asset }} VERSION AS OF {{ version }}
```

Diff it against `DESCRIBE TABLE` on the current version to reconstruct the column-level change. This is what becomes impossible once the log entry for `{{ version }}` is cleaned up.

### Combined report

Join the two projections and the check's enumeration on the table name, and sort:

```
ORDER BY CASE status WHEN 'UNPARSEABLE' THEN 0 WHEN 'SHORT_RETENTION' THEN 1
                     WHEN 'DEFAULT_BELOW_THRESHOLD' THEN 2 ELSE 3 END,
         schema_change_commits DESC, full_name
```
