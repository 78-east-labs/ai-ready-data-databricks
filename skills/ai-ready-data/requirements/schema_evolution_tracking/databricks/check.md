# Check: schema_evolution_tracking

Fraction of base Delta tables whose transaction log is retained for at least `{{ min_history_days }}` days, so column-level schema history is observable for that window.

## Context

Delta keeps the table schema in every commit of its transaction log. `DESCRIBE HISTORY` shows the schema-changing commits (`ADD COLUMNS`, `CHANGE COLUMN`, `DROP COLUMNS`, `RENAME COLUMN`, `REPLACE TABLE AS SELECT` with a new schema, `WRITE` with `mergeSchema` or `overwriteSchema`), `DESCRIBE TABLE t VERSION AS OF n` shows the columns as they were at any retained version, and `operationParameters['columns']` on an `ADD COLUMNS` commit names what was added. All of this exists only as long as the log entries exist. Log entries older than `delta.logRetentionDuration` are deleted at the next checkpoint. The default is `interval 30 days`; when the property is unset, that default applies.

So the requirement reduces to: is the log retained long enough for the window the organization needs to reason about? A table passes when its effective `delta.logRetentionDuration` (explicit or the 30-day default) is at least `{{ min_history_days }}` days (default 30). The property is a Delta table property, visible in `DESCRIBE DETAIL.properties` and `SHOW TBLPROPERTIES`, not in `information_schema`, so this is a **probe mode** check. Needs `SELECT` on the table; no data is scanned.

The value is an interval string: `interval 30 days`, `interval 720 hours`, `interval 4 weeks` are all valid spellings. The predicate normalizes to days. Also note `delta.deletedFileRetentionDuration`: it governs data files, not the log, and does not affect schema history; `data_version_coverage` measures it.

Strength is **native**. What it does not prove: that anyone is capturing schema changes into a durable record outside the log. A retention of 30 days means the history is observable for 30 days and then gone. The pure-SQL variant below shows schema-change activity that is observable right now through `system.query.history`, which complements but does not replace the retention check. Streaming tables and materialized views are excluded from the denominator; their schema is owned by the pipeline definition, which is versioned in source control.

Returns NULL (N/A) when the schema contains no base Delta tables.

## SQL

### Log retention property (primary, probe mode)

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
DESCRIBE DETAIL {{ catalog }}.{{ schema }}.{{ asset }}
```

Output columns used: `properties` (MAP<STRING,STRING>), `createdAt`, `lastModified`.

**(c) Per-table predicate**

In words: read `properties['delta.logRetentionDuration']`. If absent, the effective retention is 30 days. If present, parse the leading integer and the unit (`day`, `hour`, `week`, with or without a trailing `s`) and convert to days. The table passes when the effective days are at least `{{ min_history_days }}`.

As a SQL expression over the probe's output columns:

```sql
COALESCE(
    CASE
        WHEN properties['delta.logRetentionDuration'] IS NULL THEN 30.0
        WHEN REGEXP_LIKE(LOWER(properties['delta.logRetentionDuration']), 'interval\\s+[0-9]+\\s+day')
            THEN CAST(regexp_extract(LOWER(properties['delta.logRetentionDuration']), 'interval\\s+([0-9]+)', 1) AS DOUBLE)
        WHEN REGEXP_LIKE(LOWER(properties['delta.logRetentionDuration']), 'interval\\s+[0-9]+\\s+hour')
            THEN CAST(regexp_extract(LOWER(properties['delta.logRetentionDuration']), 'interval\\s+([0-9]+)', 1) AS DOUBLE) / 24.0
        WHEN REGEXP_LIKE(LOWER(properties['delta.logRetentionDuration']), 'interval\\s+[0-9]+\\s+week')
            THEN CAST(regexp_extract(LOWER(properties['delta.logRetentionDuration']), 'interval\\s+([0-9]+)', 1) AS DOUBLE) * 7.0
        ELSE NULL
    END, 0.0) >= {{ min_history_days }}
```

An unparseable value (for example `interval 1 month`, which Delta rejects anyway) scores 0 and fails; report it.

**(d) Aggregation rule**

```
tables_with_retention = probed tables where the predicate is true
total_tables          = probed tables (DESCRIBE DETAIL errors are excluded and reported)
value                 = tables_with_retention / total_tables, NULL when total_tables = 0
```

### SHOW TBLPROPERTIES probe (variant, probe mode)

Cheaper than `DESCRIBE DETAIL` on very large tables. Same enumeration and aggregation.

```sql
SHOW TBLPROPERTIES {{ catalog }}.{{ schema }}.{{ asset }} ('delta.logRetentionDuration')
```

Output columns: `key`, `value`. When the property is unset the `value` column carries the text `Table ... does not have property: delta.logRetentionDuration`; treat any value that does not start with `interval` as unset (30 days). Apply the same parsing to `value` as the primary applies to the map entry.

### Schema-change activity in query history (variant, pure SQL approximation)

Counts base tables that had a schema-changing statement (`ALTER TABLE ... ADD|DROP|RENAME|ALTER|CHANGE COLUMN`) recorded in `system.query.history` within `{{ lookback_days }}` days. This is not the retention check: it says which tables' schema changes are observable right now through the query log regardless of Delta retention, and it misses DDL from classic clusters, DataFrame `mergeSchema` writes, and anything before the window. Use it to size how much schema churn there is, and to find tables where the churn happened but the Delta log no longer shows it. A schema with no schema changes in the window scores NULL here, which is why this cannot be the primary.

```sql
WITH tables_in_scope AS (
    SELECT LOWER(table_name) AS table_name
    FROM {{ catalog }}.information_schema.tables
    WHERE LOWER(table_schema) = LOWER('{{ schema }}')
      AND table_type IN ('MANAGED', 'EXTERNAL')
      AND UPPER(data_source_format) = 'DELTA'
),
schema_changes AS (
    SELECT DISTINCT
        LOWER(regexp_extract(statement_text,
              '(?is)alter\\s+table\\s+(?:if\\s+exists\\s+)?`?([^`\\s(]+)`?(?:\\.`?([^`\\s(]+)`?)?(?:\\.`?([^`\\s(]+)`?)?', 3)) AS tbl3,
        LOWER(regexp_extract(statement_text,
              '(?is)alter\\s+table\\s+(?:if\\s+exists\\s+)?`?([^`\\s(]+)`?(?:\\.`?([^`\\s(]+)`?)?(?:\\.`?([^`\\s(]+)`?)?', 2)) AS tbl2,
        LOWER(regexp_extract(statement_text,
              '(?is)alter\\s+table\\s+(?:if\\s+exists\\s+)?`?([^`\\s(]+)`?(?:\\.`?([^`\\s(]+)`?)?(?:\\.`?([^`\\s(]+)`?)?', 1)) AS tbl1
    FROM system.query.history
    WHERE start_time >= current_timestamp() - INTERVAL {{ lookback_days }} DAYS
      AND execution_status = 'FINISHED'
      AND statement_type = 'ALTER'
      AND REGEXP_LIKE(LOWER(statement_text), 'alter\\s+table.*\\b(add|drop|rename|alter|change)\\s+column')
),
changed AS (
    SELECT DISTINCT COALESCE(NULLIF(tbl3, ''), NULLIF(tbl2, ''), tbl1) AS table_name
    FROM schema_changes
)
SELECT
    COUNT_IF(c.table_name IS NOT NULL)                             AS tables_with_observed_schema_change,
    COUNT(*)                                                        AS total_tables,
    COUNT_IF(c.table_name IS NOT NULL)::DOUBLE / NULLIF(COUNT(*), 0) AS value
FROM tables_in_scope t
LEFT JOIN changed c USING (table_name)
```

The regex picks the last name part of a one-, two- or three-part identifier and does not verify the catalog and schema, so a same-named table elsewhere can match. `{{ lookback_days }}` defaults to 7. `statement_type = 'ALTER'` is the expected token; confirm with `SELECT DISTINCT statement_type FROM system.query.history` if the count is zero.
