# Diagnostic: temporal_scope_declaration

Lists every base table in the schema with its `temporal_scope` tag, the validity-window and other temporal columns it has, and the scope the framework would infer from them.

## Context

One row per table. `temporal_scope_tag` is the declared value if any. `window_columns` are the columns that matched the validity-window pattern; `other_temporal_columns` are the remaining DATE / TIMESTAMP columns (event and load timestamps), listed so the human can see what the table actually carries. `inferred_scope` is a suggestion derived from column names only:

- `scd2`: has a start and an end of validity (`valid_from` + `valid_to`, `effective_from` + `effective_to`, dbt `dbt_valid_from`, Lakeflow `__START_AT`/`__END_AT`), or an `is_current` flag.
- `snapshot_series`: has `snapshot_date`, `as_of` or `period_start`, but no end-of-validity column.
- `event_log`: no window columns, has an event-like timestamp (`event_`, `occurred`, `_ts`, `timestamp`).
- `current_snapshot_or_reference`: no window columns and only `created_at`/`updated_at`-style columns, or none. Could be a live dimension or a static list; a human decides.

`declaration_status` is `TAGGED`, `INFERRED_FROM_COLUMNS` or `UNDECLARED`. `tag_matches_inference` flags tagged tables whose tag disagrees with the column evidence (for example tagged `current_snapshot` but carrying `valid_to`), which is worth a look. Sorted so undeclared tables come first.

## SQL

```sql
WITH tables_in_scope AS (
    SELECT LOWER(t.table_name) AS table_name, t.table_owner
    FROM {{ catalog }}.information_schema.tables t
    WHERE LOWER(t.table_schema) = LOWER('{{ schema }}')
      AND t.table_type IN ('MANAGED', 'EXTERNAL')
),
tagged AS (
    SELECT LOWER(table_name) AS table_name, max(tag_value) AS temporal_scope_tag
    FROM {{ catalog }}.information_schema.table_tags
    WHERE LOWER(schema_name) = LOWER('{{ schema }}')
      AND LOWER(tag_name) = LOWER('{{ temporal_tag_key }}')
      AND tag_value IS NOT NULL AND trim(tag_value) <> ''
    GROUP BY LOWER(table_name)
),
cols AS (
    SELECT LOWER(c.table_name) AS table_name, c.column_name, LOWER(c.column_name) AS col, c.data_type
    FROM {{ catalog }}.information_schema.columns c
    JOIN tables_in_scope t ON LOWER(c.table_name) = t.table_name
    WHERE LOWER(c.table_schema) = LOWER('{{ schema }}')
),
per_table AS (
    SELECT table_name,
           array_sort(collect_list(CASE WHEN REGEXP_LIKE(col,
               '(valid_from|valid_to|valid_until|effective_(from|to|date|start|end)|expir(y|ation|es)_(date|at)'
            || '|as_of|snapshot_(date|ts|at)|period_(start|end)|_start_date$|_end_date$|^is_current$|__end_at$|__start_at$)')
               THEN column_name END))                                                          AS window_columns,
           array_sort(collect_list(CASE WHEN data_type IN ('DATE', 'TIMESTAMP', 'TIMESTAMP_NTZ')
               AND NOT REGEXP_LIKE(col,
               '(valid_from|valid_to|valid_until|effective_(from|to|date|start|end)|expir(y|ation|es)_(date|at)'
            || '|as_of|snapshot_(date|ts|at)|period_(start|end)|_start_date$|_end_date$|__end_at$|__start_at$)')
               THEN column_name END))                                                          AS other_temporal_columns,
           bool_or(REGEXP_LIKE(col, '(valid_from|effective_(from|start)|_start_date$|__start_at$|period_start)')) AS has_window_start,
           bool_or(REGEXP_LIKE(col, '(valid_to|valid_until|effective_(to|end)|expir|_end_date$|__end_at$|period_end|^is_current$)')) AS has_window_end,
           bool_or(REGEXP_LIKE(col, '(as_of|snapshot_(date|ts|at))'))                          AS has_snapshot_col,
           bool_or(REGEXP_LIKE(col, '(event_|occurred|_ts$|timestamp)'))                       AS has_event_ts
    FROM cols
    GROUP BY table_name
),
inferred AS (
    SELECT *,
           CASE
               WHEN has_window_end                     THEN 'scd2'
               WHEN has_snapshot_col OR has_window_start THEN 'snapshot_series'
               WHEN has_event_ts                       THEN 'event_log'
               ELSE 'current_snapshot_or_reference'
           END AS inferred_scope
    FROM per_table
)
SELECT
    t.table_name,
    t.table_owner,
    tg.temporal_scope_tag,
    i.window_columns,
    i.other_temporal_columns,
    i.inferred_scope,
    CASE
        WHEN tg.temporal_scope_tag IS NOT NULL THEN 'TAGGED'
        WHEN size(i.window_columns) > 0         THEN 'INFERRED_FROM_COLUMNS'
        ELSE 'UNDECLARED'
    END                                                                        AS declaration_status,
    CASE WHEN tg.temporal_scope_tag IS NULL THEN NULL
         ELSE LOWER(tg.temporal_scope_tag) = i.inferred_scope
           OR (i.inferred_scope = 'current_snapshot_or_reference'
               AND LOWER(tg.temporal_scope_tag) IN ('current_snapshot', 'static_reference'))
    END                                                                        AS tag_matches_inference
FROM tables_in_scope t
LEFT JOIN tagged   tg USING (table_name)
LEFT JOIN inferred i  USING (table_name)
ORDER BY
    CASE WHEN tg.temporal_scope_tag IS NOT NULL THEN 2
         WHEN size(i.window_columns) > 0 THEN 1 ELSE 0 END,
    t.table_name
```
