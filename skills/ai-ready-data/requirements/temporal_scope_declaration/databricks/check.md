# Check: temporal_scope_declaration

Fraction of base tables in the schema that declare their temporal validity, either through a `temporal_scope` table tag or through columns that define a validity window (`valid_from`/`valid_to`, `effective_*`, `as_of`, `snapshot_date`, and similar).

## Context

A consumer, human or agent, needs to know whether a table is a current snapshot, a daily snapshot series, a slowly changing dimension with validity windows, an append-only event log, or a point-in-time extract. Nothing in Delta records that; it is a **tag / proxy** check with two signals:

1. **Table tag** `{{ temporal_tag_key }}` (default `temporal_scope`) in `information_schema.table_tags` with a non-empty value. Suggested values: `current_snapshot`, `daily_snapshot`, `scd2`, `event_log`, `point_in_time`, `static_reference`. Any non-empty value passes; the vocabulary is the team's.
2. **Validity-window columns** (proxy). The table has at least one column whose name matches `valid_from|valid_to|valid_until|effective_(from|to|date|start|end)|expir(y|ation|es)_(date|at)|as_of|snapshot_(date|ts|at)|period_(start|end)|_start_date$|_end_date$|is_current|__end_at$|__start_at$`. These are the conventions SCD2 tooling (dbt snapshots, Lakeflow `APPLY CHANGES ... STORED AS SCD TYPE 2`) and snapshot pipelines emit. A match means the table carries a window; it does not say whether the window is maintained.

A plain `created_at` or `updated_at` column does not count. Almost every table has one and it describes the row's write time, not the period the row is valid for. The stricter variant measures whether such temporal columns are at least commented.

Tags appear in `information_schema` immediately. Rows are filtered to objects the caller can see.

Placeholders beyond the standard set: `{{ temporal_tag_key }}` (default `temporal_scope`).

Returns NULL (N/A) when the schema contains no base tables (primary) or no temporal columns (variant).

## SQL

### Tag or validity-window columns (primary)

```sql
WITH tables_in_scope AS (
    SELECT LOWER(table_name) AS table_name
    FROM {{ catalog }}.information_schema.tables
    WHERE LOWER(table_schema) = LOWER('{{ schema }}')
      AND table_type IN ('MANAGED', 'EXTERNAL')
),
tagged AS (
    SELECT DISTINCT LOWER(table_name) AS table_name
    FROM {{ catalog }}.information_schema.table_tags
    WHERE LOWER(schema_name) = LOWER('{{ schema }}')
      AND LOWER(tag_name) = LOWER('{{ temporal_tag_key }}')
      AND tag_value IS NOT NULL AND trim(tag_value) <> ''
),
windowed AS (
    SELECT DISTINCT LOWER(table_name) AS table_name
    FROM {{ catalog }}.information_schema.columns
    WHERE LOWER(table_schema) = LOWER('{{ schema }}')
      AND REGEXP_LIKE(LOWER(column_name),
          '(valid_from|valid_to|valid_until|effective_(from|to|date|start|end)|expir(y|ation|es)_(date|at)'
       || '|as_of|snapshot_(date|ts|at)|period_(start|end)|_start_date$|_end_date$|^is_current$|__end_at$|__start_at$)')
)
SELECT
    COUNT_IF(tg.table_name IS NOT NULL OR w.table_name IS NOT NULL)                              AS tables_with_scope,
    COUNT(*)                                                                                     AS total_tables,
    COUNT_IF(tg.table_name IS NOT NULL OR w.table_name IS NOT NULL)::DOUBLE / NULLIF(COUNT(*), 0) AS value
FROM tables_in_scope t
LEFT JOIN tagged   tg USING (table_name)
LEFT JOIN windowed w  USING (table_name)
```

### Tag only (variant)

Strict: only an explicit `temporal_scope` tag counts. Use when the team has adopted the tag and wants the column proxy out of the score.

```sql
WITH tables_in_scope AS (
    SELECT LOWER(table_name) AS table_name
    FROM {{ catalog }}.information_schema.tables
    WHERE LOWER(table_schema) = LOWER('{{ schema }}')
      AND table_type IN ('MANAGED', 'EXTERNAL')
),
tagged AS (
    SELECT DISTINCT LOWER(table_name) AS table_name
    FROM {{ catalog }}.information_schema.table_tags
    WHERE LOWER(schema_name) = LOWER('{{ schema }}')
      AND LOWER(tag_name) = LOWER('{{ temporal_tag_key }}')
      AND tag_value IS NOT NULL AND trim(tag_value) <> ''
)
SELECT
    COUNT_IF(tg.table_name IS NOT NULL)                              AS tables_with_scope,
    COUNT(*)                                                         AS total_tables,
    COUNT_IF(tg.table_name IS NOT NULL)::DOUBLE / NULLIF(COUNT(*), 0) AS value
FROM tables_in_scope t
LEFT JOIN tagged tg USING (table_name)
```

### Commented temporal columns (variant, column-scoped)

Fraction of DATE / TIMESTAMP / TIMESTAMP_NTZ columns on base tables that have a non-empty comment. This is the weaker, per-column view: it asks whether each time column at least says what moment it records (event time, load time, validity start). Useful when the primary is near zero and you want a finer-grained progress measure.

```sql
WITH temporal_columns AS (
    SELECT c.comment
    FROM {{ catalog }}.information_schema.columns c
    JOIN {{ catalog }}.information_schema.tables t
      ON c.table_schema = t.table_schema AND c.table_name = t.table_name
    WHERE LOWER(c.table_schema) = LOWER('{{ schema }}')
      AND t.table_type IN ('MANAGED', 'EXTERNAL')
      AND c.data_type IN ('DATE', 'TIMESTAMP', 'TIMESTAMP_NTZ')
)
SELECT
    COUNT_IF(comment IS NOT NULL AND trim(comment) <> '')                              AS commented_temporal_columns,
    COUNT(*)                                                                           AS total_temporal_columns,
    COUNT_IF(comment IS NOT NULL AND trim(comment) <> '')::DOUBLE / NULLIF(COUNT(*), 0) AS value
FROM temporal_columns
```
