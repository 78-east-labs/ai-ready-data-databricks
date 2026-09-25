# Fix: uniqueness

Remove duplicate rows on the key columns and declare the key so the intent is visible to consumers and to the optimizer.

## Context

Ordered from least to most invasive:

1. **DELETE the surplus rows in place** when a `{{ tiebreaker_column }}` (ingest timestamp, version number, updated_at) distinguishes the rows in each group. This is a normal Delta commit: grants, tags, masks, row filters, constraints, comments, clustering and time travel all survive, and streaming readers see ordinary deletes.
2. **`INSERT OVERWRITE` with `DISTINCT`** when duplicates are whole-row copies with no distinguishing column. Also a normal Delta commit that keeps table identity and history, but it rewrites every file and appears as a full overwrite to streaming readers (they need `skipChangeCommits` or a restart). Never use `CREATE OR REPLACE TABLE` for this; it resets history and breaks Delta Sync indexes and streams.
3. **Declare the primary key** after the data is clean. `PRIMARY KEY` is informational in Unity Catalog and does not stop future duplicates; it documents the key, feeds the discovery variant of the check, and (with `RELY`) lets the optimizer eliminate joins. The key columns must be `NOT NULL` first.

Deleting data is irreversible in practice (time travel can restore within the retention window, which is 7 days of deleted files by default). Run the blast-radius query, and the diagnostic, before either mutating fix.

`{{ tiebreaker_column }}` has no default. If the diagnostic reports `tiebreaker_resolves_group = FALSE` for any group, the DELETE fix is not safe for that group; use the overwrite fix or add a tiebreaker (for example `_metadata.file_modification_time` captured into a real column at ingest).

## Fix: Blast radius

Run before any DELETE or overwrite and record the numbers.

```sql
WITH key_groups AS (
    SELECT COUNT(*) AS rows_in_group
    FROM {{ catalog }}.{{ schema }}.{{ asset }}
    GROUP BY {{ key_columns }}
)
SELECT
    SUM(rows_in_group)               AS total_rows,
    COUNT(*)                         AS distinct_keys,
    SUM(rows_in_group) - COUNT(*)    AS rows_to_delete
FROM key_groups
```

## Fix: Delete duplicates in place, keep latest

Keeps the row with the highest `{{ tiebreaker_column }}` per key and deletes the rest. Swap `DESC` for `ASC` to keep the earliest. Requires that `(key_columns, tiebreaker_column)` is unique within each group; check with the diagnostic.

```sql
DELETE FROM {{ catalog }}.{{ schema }}.{{ asset }}
WHERE ({{ key_columns }}, {{ tiebreaker_column }}) IN (
    SELECT {{ key_columns }}, {{ tiebreaker_column }}
    FROM (
        SELECT
            {{ key_columns }},
            {{ tiebreaker_column }},
            ROW_NUMBER() OVER (
                PARTITION BY {{ key_columns }}
                ORDER BY {{ tiebreaker_column }} DESC
            ) AS rn
        FROM {{ catalog }}.{{ schema }}.{{ asset }}
    )
    WHERE rn > 1
)
```

Re-running is a no-op once the duplicates are gone. Multi-column `IN` on a subquery is supported in Databricks SQL; NULL tiebreakers never match and are therefore never deleted, so fill or exclude them first.

## Fix: Overwrite with distinct rows (whole-row duplicates)

Use when duplicate rows are byte-for-byte identical. `DISTINCT` cannot compare MAP columns; if the table has one, list the columns explicitly instead of `*`.

```sql
INSERT OVERWRITE {{ catalog }}.{{ schema }}.{{ asset }}
SELECT DISTINCT * FROM {{ catalog }}.{{ schema }}.{{ asset }}
```

To keep one row per key (not just per identical row) without a tiebreaker, replace the SELECT with `SELECT * EXCEPT (rn) FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY {{ key_columns }} ORDER BY 1) AS rn FROM ...) WHERE rn = 1`; which duplicate survives is then arbitrary, say so in the change record.

## Fix: Declare the primary key

Guard first, since `ADD CONSTRAINT` fails on a duplicate name:

```sql
SELECT 1
FROM {{ catalog }}.information_schema.table_constraints
WHERE LOWER(table_schema) = LOWER('{{ schema }}')
  AND LOWER(table_name)   = LOWER('{{ asset }}')
  AND constraint_type = 'PRIMARY KEY'
```

If it returns no row, and the check now scores 1.0:

```sql
ALTER TABLE {{ catalog }}.{{ schema }}.{{ asset }}
ALTER COLUMN {{ column }} SET NOT NULL;   -- once per key column; skip if is_nullable = 'NO'

ALTER TABLE {{ catalog }}.{{ schema }}.{{ asset }}
ADD CONSTRAINT {{ asset }}_pk PRIMARY KEY ({{ key_columns }}) RELY;
```

`SET NOT NULL` scans the column and fails if any NULL exists. `RELY` tells the optimizer to trust the key for join elimination; leave it off if the pipeline cannot guarantee uniqueness going forward.

## Fix: Bulk generation of PRIMARY KEY statements

For every base table in the schema that has no primary key but has a single column named `id` or `{table}_id`, emit the constraint statement. Review before running; the name pattern is a guess.

```sql
WITH tables_in_scope AS (
    SELECT LOWER(table_name) AS table_name
    FROM {{ catalog }}.information_schema.tables
    WHERE LOWER(table_schema) = LOWER('{{ schema }}')
      AND table_type IN ('MANAGED', 'EXTERNAL')
),
has_pk AS (
    SELECT DISTINCT LOWER(table_name) AS table_name
    FROM {{ catalog }}.information_schema.table_constraints
    WHERE LOWER(table_schema) = LOWER('{{ schema }}')
      AND constraint_type = 'PRIMARY KEY'
),
candidate AS (
    SELECT LOWER(c.table_name) AS table_name, c.column_name, c.is_nullable
    FROM {{ catalog }}.information_schema.columns c
    WHERE LOWER(c.table_schema) = LOWER('{{ schema }}')
      AND LOWER(c.column_name) IN ('id', concat(LOWER(c.table_name), '_id'),
                                   concat(regexp_replace(LOWER(c.table_name), 's$', ''), '_id'))
)
SELECT concat(
    CASE WHEN c.is_nullable = 'YES'
         THEN concat('ALTER TABLE {{ catalog }}.{{ schema }}.`', t.table_name,
                     '` ALTER COLUMN `', c.column_name, '` SET NOT NULL; ')
         ELSE '' END,
    'ALTER TABLE {{ catalog }}.{{ schema }}.`', t.table_name,
    '` ADD CONSTRAINT `', t.table_name, '_pk` PRIMARY KEY (`', c.column_name, '`);'
) AS stmt
FROM tables_in_scope t
JOIN candidate c USING (table_name)
LEFT JOIN has_pk p USING (table_name)
WHERE p.table_name IS NULL
ORDER BY t.table_name
```

Run the uniqueness check on each candidate before executing its statement. A primary key declared over a column with duplicates is misinformation.

## Organizational guidance

Duplicates almost always enter through the write path: an append-only ingest that re-reads a source file, a job retry, or a `MERGE` whose `ON` clause is narrower than the real key. Fix the writer (Auto Loader with `cloudFiles.allowOverwrites = false`, `MERGE ... WHEN NOT MATCHED` keyed on the full business key, idempotent job runs keyed on `run_id`) rather than scheduling a dedup job. Declare `PRIMARY KEY ... RELY` in the table DDL template (dbt `constraints:` block, Lakeflow `CONSTRAINT` in the pipeline definition, Terraform `databricks_sql_table`) so every new table carries its key from day one and the check can discover it.
