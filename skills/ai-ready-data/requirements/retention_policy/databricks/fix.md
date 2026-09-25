# Fix: retention_policy

Declare the retention period with the `retention_days` tag, align the Delta deleted-file retention with it, and make sure deletion and VACUUM actually run.

## Context

Three layers, and the score needs the first two:

1. **Declare.** `retention_days` records the retention period the organisation decided for that data: a records schedule, a contract clause, a regulatory minimum or maximum, or a privacy commitment. It is a human decision. Tagging every table `365` because it sounds reasonable is worse than leaving the tag off, because a tag is read as a commitment and will be used to justify deletions and to answer regulators. Tag only after the owner and, for personal data, the privacy function have decided, and record where (`retention_ref`).
2. **Align Delta retention.** `delta.deletedFileRetentionDuration` controls how long deleted rows stay recoverable through time travel after `VACUUM` is eligible to remove them. Set it to at most the declared retention, and never under `interval 7 days` (shorter values break running readers and streaming checkpoints, and Databricks refuses `VACUUM` below 7 days without a safety override you should not use). `ALTER TABLE ... SET TBLPROPERTIES` is metadata only; it does not delete anything by itself. Lowering it on a table someone relies on for long time travel (audit replay, ML reproducibility) is a real change; check `data_version_coverage` and the owners first, because that requirement pulls the other way.
3. **Enforce.** Rows past retention must be deleted by a job (`DELETE WHERE ts < now - retention`), and `VACUUM` must run so the deleted files leave storage. Predictive optimization runs VACUUM automatically on managed tables when enabled; otherwise schedule it.

Permissions: `APPLY TAG` for tags, ownership or `MODIFY` for properties and DELETE, ownership for VACUUM.

## Fix: Tag one table with its decided retention

Guard (skip if the same value is set; a different value is an earlier decision, confirm before overwriting):

```sql
SELECT tag_value
FROM {{ catalog }}.information_schema.table_tags
WHERE LOWER(schema_name) = LOWER('{{ schema }}')
  AND LOWER(table_name)  = LOWER('{{ asset }}')
  AND LOWER(tag_name)    = 'retention_days'
```

```sql
ALTER TABLE {{ catalog }}.{{ schema }}.{{ asset }}
SET TAGS ('retention_days' = '{{ retention_days }}',
          'retention_ref'  = '{{ retention_ref }}');
```

`{{ retention_days }}` is a positive integer of days, or `indefinite`. Do not write `2 years` or `per policy`; the check cannot parse them and neither can a deletion job. Omit `retention_ref` if there is no document; do not fill it with a placeholder.

## Fix: Align the Delta deleted-file retention with the tag

Guard: skip if the property already parses to a value less than or equal to the tag.

```sql
SHOW TBLPROPERTIES {{ catalog }}.{{ schema }}.{{ asset }} ('delta.deletedFileRetentionDuration');
```

Then set it to the smaller of the declared retention and your operational standard (7 to 30 days is typical; the retention tag bounds it from above, time-travel needs bound it from below):

```sql
ALTER TABLE {{ catalog }}.{{ schema }}.{{ asset }}
SET TBLPROPERTIES ('delta.deletedFileRetentionDuration' = 'interval {{ delta_retention_days }} days');
```

`{{ delta_retention_days }}` must be at least 7 and at most `{{ retention_days }}`. For `indefinite` tables leave the property alone. `delta.logRetentionDuration` (default 30 days) may stay as it is; it holds commit metadata, not rows, but if the policy requires that even the fact of a record's existence be gone, lower it to the same value.

## Fix: Generate property statements for tables whose property exceeds the tag

Pure SQL cannot read the property, so this generator emits a statement for every validly tagged Delta table and lets the probe result (or the guard above) filter out the ones already compliant. Run the check's probe first and drop rows whose `deleted_file_retention_days <= retention_days`.

```sql
WITH tables_in_scope AS (
    SELECT LOWER(table_name) AS table_name, table_name AS table_name_cased
    FROM {{ catalog }}.information_schema.tables
    WHERE LOWER(table_schema) = LOWER('{{ schema }}')
      AND table_type IN ('MANAGED', 'EXTERNAL')
      AND UPPER(COALESCE(data_source_format, 'DELTA')) = 'DELTA'
),
tagged AS (
    SELECT LOWER(table_name) AS table_name,
           TRY_CAST(trim(MAX(tag_value)) AS INT) AS retention_days
    FROM {{ catalog }}.information_schema.table_tags
    WHERE LOWER(schema_name) = LOWER('{{ schema }}')
      AND LOWER(tag_name) = 'retention_days'
    GROUP BY LOWER(table_name)
)
SELECT concat(
    'ALTER TABLE {{ catalog }}.{{ schema }}.`', t.table_name_cased,
    '` SET TBLPROPERTIES (''delta.deletedFileRetentionDuration'' = ''interval ',
    GREATEST(7, LEAST(g.retention_days, {{ delta_retention_days }})),
    ' days'');'
) AS stmt,
g.retention_days
FROM tables_in_scope t
JOIN tagged g USING (table_name)
WHERE g.retention_days > 0
ORDER BY t.table_name
```

`{{ delta_retention_days }}` defaults to 7. Show the statements to the user; anyone depending on long time travel for a listed table should object before they run.

## Fix: Delete rows past retention

The job that makes the policy true. Requires a timestamp column (`{{ timestamp_column }}`; the diagnostic lists candidates). Blast radius first:

```sql
SELECT COUNT(*) AS rows_to_delete,
       MIN({{ timestamp_column }}) AS oldest,
       MAX({{ timestamp_column }}) AS newest_to_delete
FROM {{ catalog }}.{{ schema }}.{{ asset }}
WHERE {{ timestamp_column }} < current_timestamp() - INTERVAL {{ retention_days }} DAYS;
```

If the count is what the owner expects, delete:

```sql
DELETE FROM {{ catalog }}.{{ schema }}.{{ asset }}
WHERE {{ timestamp_column }} < current_timestamp() - INTERVAL {{ retention_days }} DAYS;
```

Idempotent: a second run deletes nothing. Schedule it as a Lakeflow job at least daily for short retentions. If the table is a streaming target, prefer `DELETE` over rewriting so downstream Change Data Feed consumers see deletes. Deletion vectors (default on recent runtimes) make this cheap; the files are rewritten or removed at the next OPTIMIZE / VACUUM.

## Fix: Run VACUUM so deleted files leave storage

Safe to re-run. Never pass a `RETAIN` under 168 hours.

```sql
VACUUM {{ catalog }}.{{ schema }}.{{ asset }};
```

The default retention threshold is the table's `delta.deletedFileRetentionDuration`. Confirm with `DESCRIBE HISTORY` that a `VACUUM END` commit appears. For managed tables, enable predictive optimization on the schema or catalog instead of scheduling VACUUM by hand:

```sql
ALTER SCHEMA {{ catalog }}.{{ schema }} ENABLE PREDICTIVE OPTIMIZATION;
```

Guard: `DESCRIBE SCHEMA EXTENDED {{ catalog }}.{{ schema }}` shows the current setting; skip if already `ENABLE`.

## Fix: Generate tag statements from a retention schedule

The only safe bulk path for the tag. `{{ retention_register }}` has `table_name STRING`, `retention_days STRING`, `retention_ref STRING`, `decided_by STRING`. Emits statements for base tables that are untagged or disagree with the register.

```sql
WITH tables_in_scope AS (
    SELECT LOWER(table_name) AS table_name, table_name AS table_name_cased
    FROM {{ catalog }}.information_schema.tables
    WHERE LOWER(table_schema) = LOWER('{{ schema }}')
      AND table_type IN ('MANAGED', 'EXTERNAL')
),
current_tag AS (
    SELECT LOWER(table_name) AS table_name, MAX(tag_value) AS retention_days
    FROM {{ catalog }}.information_schema.table_tags
    WHERE LOWER(schema_name) = LOWER('{{ schema }}')
      AND LOWER(tag_name) = 'retention_days'
    GROUP BY LOWER(table_name)
)
SELECT concat(
    'ALTER TABLE {{ catalog }}.{{ schema }}.`', t.table_name_cased,
    '` SET TAGS (''retention_days'' = ''', trim(r.retention_days), '''',
    CASE WHEN r.retention_ref IS NOT NULL
         THEN concat(', ''retention_ref'' = ''', r.retention_ref, '''') ELSE '' END,
    ');'
) AS stmt,
r.decided_by
FROM tables_in_scope t
JOIN {{ retention_register }} r ON LOWER(r.table_name) = t.table_name
LEFT JOIN current_tag c USING (table_name)
WHERE (LOWER(trim(r.retention_days)) = 'indefinite' OR TRY_CAST(trim(r.retention_days) AS INT) > 0)
  AND (c.retention_days IS NULL OR LOWER(trim(c.retention_days)) <> LOWER(trim(r.retention_days)))
ORDER BY t.table_name
```

Show the statements to the user before running them. Tables not in the register need a decision, not a default.

## Organizational guidance

Retention is set at intake from the records schedule, with `legal_basis` and `ai_allowed_purposes`, and written into the pipeline definition so the table is created with the tag and the property together (Lakeflow table properties, dbt `meta` plus `tblproperties`, Terraform `databricks_sql_table`). Make `retention_days` a governed tag. Standardise one retention job template that reads the tag, deletes past-retention rows on the table's declared timestamp column, and runs daily; and enable predictive optimization so VACUUM is not a human chore. Keep `data_version_coverage` in the same conversation: a table cannot simultaneously promise 30 days of time travel and 7 days of deletion, and the owners should choose explicitly.
