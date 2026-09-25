# Diagnostic: change_detection

Per-table view of Change Data Feed status, the version it was enabled at, whether change files are still retained, and whether anything reads the feed.

## Context

Two parts. The first is the same probe the check runs, extended with the fields that decide the fix: `delta.deletedFileRetentionDuration` (change files are removed by `VACUUM` on the same schedule as deleted data files, so a short retention bounds how far back `table_changes()` can go), `tableFeatures` (shows `changeDataFeed` once the property has been set at least once), and `numFiles` / `sizeInBytes` (very large tables cost more to enable, since every future update writes pre- and post-images).

The second part is pure SQL and shows the consumption side from `system.query.history`: the last time each table's feed was read and by whom. Absence there does not mean absence of readers (streaming `readChangeFeed` and classic clusters are invisible), but presence is a strong signal that disabling CDF would break something.

Sorted so tables without CDF come first, largest first, because those are the ones where enabling is both most valuable and most expensive.

## SQL

### Per-table probe (run once per table from the check's enumeration)

```sql
DESCRIBE DETAIL {{ catalog }}.{{ schema }}.{{ asset }}
```

Project these columns from the probe output into the report:

```sql
SELECT
    name                                                                  AS full_name,
    LOWER(COALESCE(properties['delta.enableChangeDataFeed'], 'false')) = 'true' AS cdf_enabled,
    COALESCE(properties['delta.deletedFileRetentionDuration'], 'interval 7 days (default)')
                                                                          AS change_file_retention,
    array_contains(tableFeatures, 'changeDataFeed')                       AS cdf_feature_present,
    numFiles                                                              AS num_files,
    ROUND(sizeInBytes / 1024 / 1024 / 1024, 2)                            AS size_gb,
    lastModified                                                          AS last_modified,
    CASE
        WHEN LOWER(COALESCE(properties['delta.enableChangeDataFeed'], 'false')) = 'true' THEN 'ENABLED'
        WHEN array_contains(tableFeatures, 'changeDataFeed')                          THEN 'DISABLED_PREVIOUSLY_ENABLED'
        ELSE 'NEEDS_ENABLING'
    END                                                                   AS status
FROM <probe output>
```

`cdf_feature_present = true` with `cdf_enabled = false` means someone turned CDF off; check with the owner before re-enabling.

### Enabling version (optional, per table with CDF)

`table_changes()` only works from the commit that set the property. Find it so consumers know the earliest readable version:

```sql
SELECT version, timestamp, userName, operation
FROM (DESCRIBE HISTORY {{ catalog }}.{{ schema }}.{{ asset }})
WHERE operation = 'SET TBLPROPERTIES'
  AND LOWER(operationParameters['properties']) LIKE '%delta.enablechangedatafeed%'
ORDER BY version DESC
LIMIT 1
```

If the `FROM (DESCRIBE HISTORY ...)` form is rejected by the warehouse, run `DESCRIBE HISTORY` alone and filter the rows client-side.

### Change feed readers (pure SQL, whole schema)

```sql
WITH tables_in_scope AS (
    SELECT LOWER(table_name) AS table_name, table_owner
    FROM {{ catalog }}.information_schema.tables
    WHERE LOWER(table_schema) = LOWER('{{ schema }}')
      AND table_type IN ('MANAGED', 'EXTERNAL')
      AND UPPER(data_source_format) = 'DELTA'
),
cdf_reads AS (
    SELECT
        LOWER(regexp_extract(statement_text,
              '(?i)table_changes\\s*\\(\\s*[\'"]([^\'"]+)[\'"]', 1)) AS full_name,
        executed_by,
        start_time,
        query_source.job_info.job_id AS job_id,
        query_source.notebook_id     AS notebook_id
    FROM system.query.history
    WHERE start_time >= current_timestamp() - INTERVAL {{ lookback_days }} DAYS
      AND execution_status = 'FINISHED'
      AND REGEXP_LIKE(LOWER(statement_text), 'table_changes\\s*\\(')
),
readers AS (
    SELECT
        element_at(split(full_name, '\\.'), -1) AS table_name,
        COUNT(*)                                AS cdf_reads,
        MAX(start_time)                         AS last_cdf_read,
        array_sort(collect_set(executed_by))    AS reader_principals,
        array_sort(collect_set(CAST(job_id AS STRING))) AS reader_job_ids
    FROM cdf_reads
    WHERE full_name LIKE CONCAT(LOWER('{{ catalog }}'), '.', LOWER('{{ schema }}'), '.%')
       OR full_name LIKE CONCAT(LOWER('{{ schema }}'), '.%')
    GROUP BY 1
)
SELECT
    t.table_name,
    t.table_owner,
    COALESCE(r.cdf_reads, 0) AS cdf_reads,
    r.last_cdf_read,
    r.reader_principals,
    r.reader_job_ids
FROM tables_in_scope t
LEFT JOIN readers r USING (table_name)
ORDER BY cdf_reads DESC, t.table_name
```

`{{ lookback_days }}` defaults to 7. Join this result to the probe output on `table_name` to get one row per table with both the property state and the consumption evidence.
