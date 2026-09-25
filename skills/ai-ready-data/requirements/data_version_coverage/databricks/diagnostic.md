# Diagnostic: data_version_coverage

Per-table view of the configured time-travel window (from the probe), the oldest version still addressable in history, and any explicit version column, shortest retention first.

## Context

The diagnostic has two parts because the retention properties are only reachable through a probe.

The **enumeration query** below is pure SQL and lists every Delta base table in scope with size hints and any explicit version-tracking column (`version`, `version_id`, `data_version`, `snapshot_id`, `batch_id`, `_commit_version`). A version column is not time travel, but for training-set reproducibility it is often the more useful artifact, and a table that has one may not need a long Delta retention.

The **per-table probe** returns the two retention properties. Run `DESCRIBE DETAIL` for the properties and `DESCRIBE HISTORY ... LIMIT 1000` if you also want the oldest commit still in the log (`MIN(timestamp)`), which shows how much history actually exists today versus how much the configuration guarantees. Merge the probe output onto the enumeration by `table_name` and sort by `effective_retention_days` ascending, so the tables with the shortest guaranteed window come first.

`effective_retention_days` is `LEAST(deleted_file_retention_days, log_retention_days)` after applying the defaults (7 and 30). `status` is `COVERED` when both are at least `{{ min_retention_days }}` (default 30), `LOG_ONLY` when the log is long enough but deleted files are not (the common default state), `UNPARSEABLE` when a property is set to something the regex does not understand, and `SHORT` otherwise.

## SQL

### Enumeration with version-column scan (run first)

```sql
WITH tables_in_scope AS (
    SELECT LOWER(table_name) AS table_name, table_owner, created, last_altered, data_source_format
    FROM {{ catalog }}.information_schema.tables
    WHERE LOWER(table_schema) = LOWER('{{ schema }}')
      AND table_type IN ('MANAGED', 'EXTERNAL')
      AND UPPER(data_source_format) = 'DELTA'
),
version_columns AS (
    SELECT LOWER(table_name) AS table_name,
           array_sort(collect_set(column_name)) AS version_columns
    FROM {{ catalog }}.information_schema.columns
    WHERE LOWER(table_schema) = LOWER('{{ schema }}')
      AND LOWER(column_name) IN ('version', 'version_id', 'data_version',
                                 'snapshot_id', 'batch_id', '_commit_version')
    GROUP BY LOWER(table_name)
)
SELECT
    t.table_name,
    t.table_owner,
    t.created,
    t.last_altered,
    COALESCE(vc.version_columns, array())                 AS version_columns,
    size(COALESCE(vc.version_columns, array())) > 0       AS has_version_column
FROM tables_in_scope t
LEFT JOIN version_columns vc USING (table_name)
ORDER BY t.table_name
```

### Per-table probe (run once per table from the list above)

```sql
DESCRIBE DETAIL {{ catalog }}.{{ schema }}.`{{ asset }}`
```

Optional, for the oldest reachable version:

```sql
DESCRIBE HISTORY {{ catalog }}.{{ schema }}.`{{ asset }}` LIMIT 1000
```

### Per-table columns to derive from the probe output

Apply these expressions to the `properties` map from `DESCRIBE DETAIL` (and `MIN(timestamp)` / `MIN(version)` over the `DESCRIBE HISTORY` rows) to fill the diagnostic row:

```sql
properties['delta.deletedFileRetentionDuration']                                    AS deleted_file_retention_raw,
properties['delta.logRetentionDuration']                                            AS log_retention_raw,
COALESCE(
    TRY_CAST(regexp_extract(LOWER(properties['delta.deletedFileRetentionDuration']),
                            'interval\\s+(\\d+)\\s+(hour|day|week)', 1) AS DOUBLE)
    * CASE regexp_extract(LOWER(properties['delta.deletedFileRetentionDuration']),
                          'interval\\s+(\\d+)\\s+(hour|day|week)', 2)
        WHEN 'hour' THEN 1.0 / 24 WHEN 'week' THEN 7.0 WHEN 'day' THEN 1.0 END,
    CASE WHEN properties['delta.deletedFileRetentionDuration'] IS NULL THEN 7.0 END)  AS deleted_file_retention_days,
COALESCE(
    TRY_CAST(regexp_extract(LOWER(properties['delta.logRetentionDuration']),
                            'interval\\s+(\\d+)\\s+(hour|day|week)', 1) AS DOUBLE)
    * CASE regexp_extract(LOWER(properties['delta.logRetentionDuration']),
                          'interval\\s+(\\d+)\\s+(hour|day|week)', 2)
        WHEN 'hour' THEN 1.0 / 24 WHEN 'week' THEN 7.0 WHEN 'day' THEN 1.0 END,
    CASE WHEN properties['delta.logRetentionDuration'] IS NULL THEN 30.0 END)         AS log_retention_days,
LEAST(deleted_file_retention_days, log_retention_days)                              AS effective_retention_days,
numFiles                                                                            AS num_files,
sizeInBytes                                                                         AS size_bytes,
lastModified                                                                        AS last_modified,
CASE
    WHEN deleted_file_retention_days IS NULL OR log_retention_days IS NULL             THEN 'UNPARSEABLE'
    WHEN deleted_file_retention_days >= {{ min_retention_days }}
     AND log_retention_days          >= {{ min_retention_days }}                       THEN 'COVERED'
    WHEN log_retention_days          >= {{ min_retention_days }}                       THEN 'LOG_ONLY'
    ELSE 'SHORT'
END                                                                                 AS status
```

Sort the merged result by `effective_retention_days ASC NULLS FIRST, size_bytes DESC` so unparseable and shortest-window tables, largest first, are at the top.
