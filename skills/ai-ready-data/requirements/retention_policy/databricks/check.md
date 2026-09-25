# Check: retention_policy

Fraction of base tables in the schema that declare a retention period (`retention_days` table tag) and whose Delta deleted-file retention is not longer than that period.

## Context

Two halves, one declared and one enforced.

**Declared (tag).** The table tag `retention_days` in `{{ catalog }}.information_schema.table_tags` records how long the data may be kept, as a positive integer number of days (`730`), or the literal `indefinite` for records that are kept permanently by policy. The tag is a human decision (records schedule, contract, regulation); the check verifies presence and a parseable value.

**Enforced (Delta property, probe).** Deleting a row from a Delta table does not remove it from storage. The old file stays readable through time travel until `VACUUM` removes files older than `delta.deletedFileRetentionDuration` (default `interval 7 days` when unset). If that property is set longer than the declared retention (say `interval 365 days` on a table tagged `retention_days = 90`), deleted personal data outlives its own policy by up to 275 days, and the declaration is not what happens. So the per-table predicate is: tag present, and deleted-file retention in days is less than or equal to the tag's days. `delta.logRetentionDuration` (default 30 days) governs commit history and is reported for context; it does not hold row data, though it does keep schema and operation metadata.

What the check does not prove: that rows older than the retention period are actually deleted (that needs a `DELETE` job keyed on a timestamp column; see the data variant and the fix), or that `VACUUM` runs (the diagnostic reports the last VACUUM from history and predictive optimization). A table can pass this check and still hold ten-year-old rows if nobody deletes them.

Strength: tag plus native. Execution: the tag half is SQL; the property half is probe mode because `DESCRIBE DETAIL` output cannot be joined in SQL. The SQL-only variant scores the tag alone and says so. `information_schema` reflects tags immediately; `DESCRIBE DETAIL` is current. `DESCRIBE DETAIL` needs `SELECT` on each table.

Returns NULL (N/A) when the schema contains no base tables.

## SQL

### Declared retention with consistent Delta retention (primary, probe mode)

(a) Enumeration. Base tables with their tag, parsed:

```sql
WITH tables_in_scope AS (
    SELECT LOWER(table_name) AS table_name, table_name AS table_name_cased
    FROM {{ catalog }}.information_schema.tables
    WHERE LOWER(table_schema) = LOWER('{{ schema }}')
      AND table_type IN ('MANAGED', 'EXTERNAL')
      AND UPPER(COALESCE(data_source_format, 'DELTA')) = 'DELTA'
),
retention_tag AS (
    SELECT LOWER(table_name) AS table_name,
           MAX(tag_value)    AS retention_value
    FROM {{ catalog }}.information_schema.table_tags
    WHERE LOWER(schema_name) = LOWER('{{ schema }}')
      AND LOWER(tag_name) = 'retention_days'
    GROUP BY LOWER(table_name)
)
SELECT
    t.table_name_cased                                        AS table_name,
    r.retention_value,
    CASE
        WHEN LOWER(trim(r.retention_value)) = 'indefinite' THEN -1
        ELSE TRY_CAST(trim(r.retention_value) AS INT)
    END                                                       AS retention_days,
    r.retention_value IS NOT NULL
        AND (LOWER(trim(r.retention_value)) = 'indefinite'
             OR TRY_CAST(trim(r.retention_value) AS INT) > 0) AS has_valid_tag
FROM tables_in_scope t
LEFT JOIN retention_tag r USING (table_name)
ORDER BY t.table_name
```

Non-Delta tables (Parquet, CSV external tables) have no Delta retention; they are excluded from the probe population and should be scored by the tag-only variant.

(b) Probe. For each enumerated table with `has_valid_tag = true`:

```sql
DESCRIBE DETAIL {{ catalog }}.{{ schema }}.`{table_name}`
```

Read `properties['delta.deletedFileRetentionDuration']` and `properties['delta.logRetentionDuration']` from the returned `properties` map. Values are strings like `interval 7 days`, `interval 168 hours`, `interval 2 weeks`; a missing key means the default (`interval 7 days` and `interval 30 days`).

(c) Predicate. In words: the table has a valid `retention_days` tag, and either the tag is `indefinite`, or the deleted-file retention converted to days is less than or equal to the tag's days. As a SQL expression over the enumeration columns and the probe's `properties` map (`prop` stands for `properties['delta.deletedFileRetentionDuration']`):

```sql
has_valid_tag
AND (
    retention_days = -1
    OR
    CASE
        WHEN prop IS NULL                        THEN 7.0
        WHEN REGEXP_LIKE(LOWER(prop), 'week')    THEN regexp_extract(prop, '([0-9]+)', 1)::DOUBLE * 7
        WHEN REGEXP_LIKE(LOWER(prop), 'day')     THEN regexp_extract(prop, '([0-9]+)', 1)::DOUBLE
        WHEN REGEXP_LIKE(LOWER(prop), 'hour')    THEN regexp_extract(prop, '([0-9]+)', 1)::DOUBLE / 24
        WHEN REGEXP_LIKE(LOWER(prop), 'minute')  THEN regexp_extract(prop, '([0-9]+)', 1)::DOUBLE / 1440
        ELSE NULL
    END <= retention_days
)
```

A table whose property does not parse (unrecognised unit) evaluates to NULL and counts as failing; report it separately so the user can inspect the raw string.

(d) Aggregation. `value = tables_passing / tables_enumerated`, with `tables_enumerated` the count from step (a) (tables without a valid tag are enumerated, not probed, and fail). NULL if the enumeration returns no rows. Report `tables_passing` and `tables_enumerated` as the numerator and denominator.

Reference helper (extends the SDK snippet in `platforms/DATABRICKS.md`):

```python
import re

UNIT_DAYS = {"week": 7.0, "day": 1.0, "hour": 1 / 24, "minute": 1 / 1440}

def interval_days(prop, default_days):
    if not prop:
        return default_days
    m = re.search(r"([0-9]+)\s*(week|day|hour|minute)", prop.lower())
    return float(m.group(1)) * UNIT_DAYS[m.group(2)] if m else None

def passes(row, detail):
    if not row["has_valid_tag"]:
        return False
    if row["retention_days"] == -1:
        return True
    d = interval_days(detail["properties"].get("delta.deletedFileRetentionDuration"), 7.0)
    return d is not None and d <= row["retention_days"]
```

### Declared retention only (variant, pure SQL)

Scores the tag alone. Misses the enforcement half: a table tagged `retention_days = 30` with `delta.deletedFileRetentionDuration = 'interval 365 days'` passes here and fails the primary.

```sql
WITH tables_in_scope AS (
    SELECT LOWER(table_name) AS table_name
    FROM {{ catalog }}.information_schema.tables
    WHERE LOWER(table_schema) = LOWER('{{ schema }}')
      AND table_type IN ('MANAGED', 'EXTERNAL')
),
declared AS (
    SELECT DISTINCT LOWER(table_name) AS table_name
    FROM {{ catalog }}.information_schema.table_tags
    WHERE LOWER(schema_name) = LOWER('{{ schema }}')
      AND LOWER(tag_name) = 'retention_days'
      AND (LOWER(trim(tag_value)) = 'indefinite' OR TRY_CAST(trim(tag_value) AS INT) > 0)
)
SELECT
    COUNT_IF(d.table_name IS NOT NULL)            AS tables_with_retention,
    COUNT(*)                                       AS total_tables,
    COUNT_IF(d.table_name IS NOT NULL)::DOUBLE
        / NULLIF(COUNT(*), 0)                      AS value
FROM tables_in_scope t
LEFT JOIN declared d USING (table_name)
```

### Oldest row within retention (variant, data, per table)

For one `{{ asset }}` with a known event timestamp column `{{ timestamp_column }}` and a numeric tag, checks that no live row is older than the declared retention. This is the row-level enforcement the primary cannot see. Table-scoped; the orchestrator aggregates across tables that have a configured timestamp column. Sampling is offered because `MIN` over a large unclustered table is a full scan; the sampled result can miss the oldest rows and should be treated as a lower bound.

```sql
WITH tag AS (
    SELECT TRY_CAST(trim(MAX(tag_value)) AS INT) AS retention_days
    FROM {{ catalog }}.information_schema.table_tags
    WHERE LOWER(schema_name) = LOWER('{{ schema }}')
      AND LOWER(table_name)  = LOWER('{{ asset }}')
      AND LOWER(tag_name)    = 'retention_days'
),
rows_scanned AS (
    SELECT COUNT(*) AS total_rows,
           COUNT_IF({{ timestamp_column }} >= current_timestamp() - make_interval(0, 0, 0, tag.retention_days)) AS rows_within_retention
    FROM {{ catalog }}.{{ schema }}.{{ asset }} TABLESAMPLE ({{ sample_rows }} ROWS)
    CROSS JOIN tag
    WHERE tag.retention_days IS NOT NULL
)
SELECT
    rows_within_retention,
    total_rows,
    rows_within_retention::DOUBLE / NULLIF(total_rows, 0) AS value
FROM rows_scanned
```

Drop `TABLESAMPLE (...)` for an exact answer. Returns NULL when the tag is missing or not numeric.
