# Fix: temporal_scope_declaration

Declare each table's temporal validity with the `temporal_scope` table tag, and describe validity columns in their comments.

## Context

The durable fix is the tag: `ALTER TABLE ... SET TAGS ('{{ temporal_tag_key }}' = '<scope>')` (default key `temporal_scope`). It is idempotent, needs `APPLY TAG` on the table or ownership, rewrites nothing, and is readable by anything that queries `information_schema.table_tags`. Use a small closed vocabulary and, if the account has governed tag policies, register it there:

| Value | Meaning |
|---|---|
| `current_snapshot` | One row per entity, overwritten in place; shows the latest state only |
| `daily_snapshot` (or `hourly_`, `monthly_`) | One row per entity per period; `snapshot_date` / `as_of` identifies the period |
| `scd2` | One row per entity per validity window; `valid_from` / `valid_to` (or `__START_AT` / `__END_AT`) bound it |
| `event_log` | Append-only facts; each row is an event at its own timestamp |
| `point_in_time` | A one-off extract as of a stated date; not refreshed |
| `static_reference` | Lookup data with no meaningful time dimension |

The tag documents a decision a human must make. Tagging every table `temporal_scope = 'unknown'` makes the score 1.0 and tells the next consumer nothing. The bulk variant therefore only emits tags where the column evidence is strong (`scd2` from an end-of-validity column, `daily_snapshot` from a `snapshot_date`), and asks you to review the rest table by table.

Column comments are the second half: a table tagged `scd2` should say in the comments of `valid_from` / `valid_to` whether the end bound is inclusive and what sentinel marks the current row (`NULL` or `9999-12-31`).

## Fix: Tag one table

Guard (skip if a row comes back with the desired value):

```sql
SELECT tag_value
FROM {{ catalog }}.information_schema.table_tags
WHERE LOWER(schema_name) = LOWER('{{ schema }}')
  AND LOWER(table_name)  = LOWER('{{ asset }}')
  AND LOWER(tag_name)    = LOWER('{{ temporal_tag_key }}')
```

```sql
ALTER TABLE {{ catalog }}.{{ schema }}.{{ asset }}
SET TAGS ('{{ temporal_tag_key }}' = '{{ temporal_scope }}')
```

## Fix: Comment the validity columns

```sql
COMMENT ON COLUMN {{ catalog }}.{{ schema }}.{{ asset }}.valid_from
IS 'Start of the validity window for this version of the row (inclusive, UTC).';

COMMENT ON COLUMN {{ catalog }}.{{ schema }}.{{ asset }}.valid_to
IS 'End of the validity window (exclusive, UTC). NULL for the current version.';
```

Adjust the column names, inclusivity and sentinel to the table's actual convention. `COMMENT ON COLUMN` replaces the existing text; check `information_schema.columns.comment` first.

## Fix: Bulk tag tables whose columns make the scope unambiguous

Emits one `SET TAGS` per untagged table where the column names give a confident answer: an end-of-validity column implies `scd2`; a `snapshot_date` / `as_of` column without an end column implies `daily_snapshot`. Tables that match neither are listed by the diagnostic as `UNDECLARED` and need a human.

```sql
WITH tables_in_scope AS (
    SELECT LOWER(t.table_name) AS table_name
    FROM {{ catalog }}.information_schema.tables t
    LEFT JOIN (
        SELECT DISTINCT LOWER(table_name) AS table_name
        FROM {{ catalog }}.information_schema.table_tags
        WHERE LOWER(schema_name) = LOWER('{{ schema }}')
          AND LOWER(tag_name) = LOWER('{{ temporal_tag_key }}')
    ) tg ON tg.table_name = LOWER(t.table_name)
    WHERE LOWER(t.table_schema) = LOWER('{{ schema }}')
      AND t.table_type IN ('MANAGED', 'EXTERNAL')
      AND tg.table_name IS NULL
),
evidence AS (
    SELECT t.table_name,
           bool_or(REGEXP_LIKE(LOWER(c.column_name), '(valid_to|valid_until|effective_(to|end)|expir(y|ation|es)_(date|at)|_end_date$|__end_at$|period_end|^is_current$)')) AS has_window_end,
           bool_or(REGEXP_LIKE(LOWER(c.column_name), '(as_of|snapshot_(date|ts|at))'))                                                                            AS has_snapshot_col
    FROM tables_in_scope t
    JOIN {{ catalog }}.information_schema.columns c
      ON LOWER(c.table_schema) = LOWER('{{ schema }}') AND LOWER(c.table_name) = t.table_name
    GROUP BY t.table_name
),
decided AS (
    SELECT table_name,
           CASE WHEN has_window_end   THEN 'scd2'
                WHEN has_snapshot_col THEN 'daily_snapshot'
           END AS temporal_scope
    FROM evidence
)
SELECT concat(
    'ALTER TABLE {{ catalog }}.{{ schema }}.`', table_name,
    '` SET TAGS (''{{ temporal_tag_key }}'' = ''', temporal_scope, ''');'
) AS stmt
FROM decided
WHERE temporal_scope IS NOT NULL
ORDER BY table_name
```

Show the generated statements to the user before executing them. `daily_snapshot` is a guess at the period; if the snapshot column is hourly or monthly, edit the value. For the remaining tables, the fastest way to decide is `DESCRIBE HISTORY` on each: a history of `WRITE` with `mode = Overwrite` is a `current_snapshot`, a history of appends is an `event_log` or a snapshot series, and a history of `MERGE` is usually `current_snapshot` or `scd2`.

## Fix: Tag Lakeflow SCD tables from their pipeline definition

Tables produced by `APPLY CHANGES INTO ... STORED AS SCD TYPE 2` (or `create_auto_cdc_flow` in Python) always carry `__START_AT` and `__END_AT`, and the bulk variant catches them. Tables produced with `SCD TYPE 1` have no window columns and are `current_snapshot`; tag them explicitly, because nothing in the column list says so. In the pipeline source, add the tag as a table property so it travels with the definition:

```sql
CREATE OR REFRESH STREAMING TABLE {{ asset }}
TBLPROPERTIES ('temporal_scope' = 'current_snapshot')
```

Table properties are not tags. Use this to record the intent in code, and keep the `SET TAGS` statement for the `information_schema`-visible declaration, or run the tag statement from the pipeline's post-deployment step.

## Organizational guidance

Temporal scope is a modelling decision and should be made when a table is designed. Put `temporal_scope` in the dbt `meta` block (dbt snapshots are always `scd2`; incremental models with `append` strategy are usually `event_log`) and render it to `SET TAGS` in a post-hook. In Lakeflow, decide it per `APPLY CHANGES` flow. Register `temporal_scope` as a governed tag with the vocabulary above so every team uses the same six values, and make the AI consumers (Genie space instructions, agent system prompts) read it, so that the declaration has a reader and stays maintained.
