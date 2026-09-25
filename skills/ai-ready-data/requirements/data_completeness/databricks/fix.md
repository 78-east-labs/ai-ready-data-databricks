# Fix: data_completeness

Fill, backfill or remove NULLs in a column, then enforce `NOT NULL` so they do not return.

## Context

Options, from least to most invasive:

1. **Backfill from a source that has the value.** The right fix when the NULL is a pipeline defect (a join that missed, a column added after the rows were loaded). `MERGE` from the upstream table on the business key.
2. **Fill a domain default** (`0`, `FALSE`, `''`, `DATE '9999-12-31'`). Only when the default is genuinely what a missing value means. A default that looks like data (0 revenue) poisons aggregates and features.
3. **Fill an explicit sentinel** (`'UNKNOWN'`, `-1`) for categorical columns whose consumers filter on it. Sentinels then fail `categorical_validity` and `value_range_validity` unless those vocabularies include them; choose deliberately.
4. **Quarantine and delete** rows that are meaningless without the value (a fact row with no measure, an event with no timestamp).
5. **`ALTER COLUMN ... SET NOT NULL`.** Enforced by Delta on every future write. The statement scans the column and fails if any NULL remains, so it always comes after one of the above.

Filling a NULL is a data change and only the deleted-file retention window (7 days by default) can undo it. Run the blast-radius query first. Every mutating statement is restricted to `WHERE {{ column }} IS NULL` and is therefore idempotent.

## Fix: Blast radius

```sql
SELECT
    COUNT_IF({{ column }} IS NULL)                  AS null_rows,
    COUNT(*)                                        AS total_rows,
    COUNT_IF({{ column }} IS NULL)::DOUBLE
        / NULLIF(COUNT(*), 0)                       AS null_rate
FROM {{ catalog }}.{{ schema }}.{{ asset }}
```

## Fix: Backfill from an upstream source

`{{ source_table }}` holds the correct value in `{{ source_column }}`, joinable on `{{ key_column }}`. Only NULL targets are updated, and only where the source has a non-null value, so a re-run changes nothing.

```sql
MERGE INTO {{ catalog }}.{{ schema }}.{{ asset }} t
USING (
    SELECT {{ key_column }}, MAX({{ source_column }}) AS v
    FROM {{ source_table }}
    WHERE {{ source_column }} IS NOT NULL
    GROUP BY {{ key_column }}
) s
ON t.{{ key_column }} = s.{{ key_column }} AND t.{{ column }} IS NULL
WHEN MATCHED THEN UPDATE SET t.{{ column }} = s.v
```

`MAX(...) GROUP BY` collapses duplicate source keys so `MERGE` does not fail on multiple matches; if the source key is unique, drop the aggregation.

## Fix: Fill a default value

`{{ default_value }}` is a typed literal matching the column.

```sql
UPDATE {{ catalog }}.{{ schema }}.{{ asset }}
SET {{ column }} = {{ default_value }}
WHERE {{ column }} IS NULL
```

To make the default apply to future inserts as well, Delta supports column defaults on tables with the `allowColumnDefaults` feature:

```sql
ALTER TABLE {{ catalog }}.{{ schema }}.{{ asset }}
SET TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported');

ALTER TABLE {{ catalog }}.{{ schema }}.{{ asset }}
ALTER COLUMN {{ column }} SET DEFAULT {{ default_value }};
```

Guard with `SHOW TBLPROPERTIES` first; the feature flag is a protocol upgrade (writer version 7) and cannot be removed without `DROP FEATURE`. Column defaults apply to `INSERT` statements that omit the column, not to `MERGE` sources or streaming writes that pass an explicit NULL.

## Fix: Fill a sentinel

```sql
UPDATE {{ catalog }}.{{ schema }}.{{ asset }}
SET {{ column }} = {{ sentinel_value }}
WHERE {{ column }} IS NULL
```

Add the sentinel to the column's `{{ allowed_values }}` or CHECK constraint in the same change, or `categorical_validity` will report it as a violation.

## Fix: Quarantine, then delete incomplete rows

```sql
CREATE TABLE IF NOT EXISTS {{ catalog }}.{{ schema }}.{{ asset }}_incomplete
AS SELECT *, '' AS missing_column, current_timestamp() AS quarantined_at
   FROM {{ catalog }}.{{ schema }}.{{ asset }} WHERE 1 = 0;

INSERT INTO {{ catalog }}.{{ schema }}.{{ asset }}_incomplete
SELECT *, '{{ column }}', current_timestamp()
FROM {{ catalog }}.{{ schema }}.{{ asset }}
WHERE {{ column }} IS NULL;

DELETE FROM {{ catalog }}.{{ schema }}.{{ asset }}
WHERE {{ column }} IS NULL;
```

## Fix: Enforce NOT NULL

Guard:

```sql
SELECT is_nullable
FROM {{ catalog }}.information_schema.columns
WHERE LOWER(table_schema) = LOWER('{{ schema }}')
  AND LOWER(table_name)   = LOWER('{{ asset }}')
  AND LOWER(column_name)  = LOWER('{{ column }}')
```

If `YES`, and the check returns 1.0:

```sql
ALTER TABLE {{ catalog }}.{{ schema }}.{{ asset }}
ALTER COLUMN {{ column }} SET NOT NULL
```

The statement fails with `DELTA_NOT_NULL_CONSTRAINT_VIOLATED` if any NULL exists. Afterwards any writer that sends a NULL fails; for tables fed by Auto Loader or a Lakeflow flow, prefer an expectation (`EXPECT (col IS NOT NULL) ON VIOLATION DROP ROW`) on the flow and `NOT NULL` on the downstream silver table.

## Fix: Bulk generation of SET NOT NULL for complete columns

Uses the generated one-pass profile from the diagnostic. Run that statement, save its output as a temporary view, then emit the `ALTER` statements for columns with zero nulls that are still declared nullable.

```sql
-- After running the diagnostic's generated statement:
-- CREATE OR REPLACE TEMP VIEW null_profile AS <emitted statement>;

SELECT concat(
    'ALTER TABLE {{ catalog }}.{{ schema }}.`{{ asset }}` ALTER COLUMN `', c.column_name, '` SET NOT NULL;'
) AS stmt
FROM {{ catalog }}.information_schema.columns c
JOIN null_profile p ON p.column_name = c.column_name
WHERE LOWER(c.table_schema) = LOWER('{{ schema }}')
  AND LOWER(c.table_name)   = LOWER('{{ asset }}')
  AND c.is_nullable = 'YES'
  AND p.null_rows = 0
ORDER BY c.ordinal_position
```

Declare `NOT NULL` only where the pipeline can keep the promise. A column that is complete today because the source happens to fill it is not a contract.

## Organizational guidance

NULLs should be caught at the write path. In Lakeflow, `CONSTRAINT not_null_<col> EXPECT (<col> IS NOT NULL) ON VIOLATION DROP ROW` on bronze-to-silver flows counts and drops incomplete rows and records the counts in the pipeline event log; `FAIL UPDATE` is right for gold tables. In dbt, `not_null` tests on every key and measure. Declare `NOT NULL` in the DDL template for keys, timestamps and measures so the guarantee travels with the table. Stop the practice of loading placeholders (`'N/A'`, `-1`, `1900-01-01`) for missing data; they hide the gap from this check and surface later as bad features.
