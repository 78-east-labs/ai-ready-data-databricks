# Diagnostic: point_in_time_correctness

One row per feature table with its primary key columns and their types, the candidate timestamp columns on the table, whether the key already includes one, and (after the probe) whether `TIMESERIES` is declared.

## Context

The pure-SQL part reuses the check's population and shows what an `ALTER TABLE ... ADD CONSTRAINT ... PRIMARY KEY (..., ts TIMESERIES)` would need: the current key (`pk_columns`), whether any key column is time-typed (`pk_time_columns`), and the non-key timestamp columns that look like event time (`candidate_time_columns`, matched on `event|_at$|_ts$|_time$|timestamp|effective|valid_from|as_of`). It also reports whether those candidates are nullable, because a primary key column must be `NOT NULL`.

The probe part is `SHOW CREATE TABLE`, from which the exact constraint clause is extracted so the operator can see the current DDL before rewriting it.

Status values: `TIMESERIES` (from the probe), `TIME_IN_KEY_NO_FLAG` (key has a timestamp column but the probe did not find the flag), `NO_TIME_IN_KEY` (key is entity-only; a candidate column exists), `NO_TIME_COLUMN` (nothing on the table looks like an event time), `NO_PRIMARY_KEY` (feature table by tag only). Sorted worst-first.

## SQL

### Feature table inventory (pure SQL)

```sql
WITH tables_in_scope AS (
    SELECT LOWER(table_name) AS table_name, table_owner
    FROM {{ catalog }}.information_schema.tables
    WHERE LOWER(table_schema) = LOWER('{{ schema }}')
      AND table_type IN ('MANAGED', 'EXTERNAL')
      AND UPPER(data_source_format) = 'DELTA'
),
pk AS (
    SELECT LOWER(tc.table_name) AS table_name, tc.constraint_name
    FROM {{ catalog }}.information_schema.table_constraints tc
    WHERE LOWER(tc.table_schema) = LOWER('{{ schema }}')
      AND tc.constraint_type = 'PRIMARY KEY'
),
pk_cols AS (
    SELECT LOWER(k.table_name) AS table_name,
           array_join(transform(array_sort(collect_list(struct(k.ordinal_position, k.column_name))), x -> x.column_name), ', ')
               AS pk_columns,
           array_sort(collect_set(CASE WHEN UPPER(c.data_type) IN ('TIMESTAMP', 'TIMESTAMP_NTZ', 'DATE')
                                       THEN k.column_name END)) AS pk_time_columns
    FROM {{ catalog }}.information_schema.key_column_usage k
    JOIN pk ON pk.table_name = LOWER(k.table_name) AND pk.constraint_name = k.constraint_name
    JOIN {{ catalog }}.information_schema.columns c
      ON c.table_schema = k.table_schema AND c.table_name = k.table_name AND c.column_name = k.column_name
    WHERE LOWER(k.table_schema) = LOWER('{{ schema }}')
    GROUP BY LOWER(k.table_name)
),
tagged AS (
    SELECT DISTINCT LOWER(table_name) AS table_name
    FROM {{ catalog }}.information_schema.table_tags
    WHERE LOWER(schema_name) = LOWER('{{ schema }}')
      AND tag_name = 'feature_table' AND LOWER(tag_value) = 'true'
),
time_candidates AS (
    SELECT LOWER(c.table_name) AS table_name,
           array_sort(collect_set(concat(c.column_name, ' ', c.data_type,
                                         CASE WHEN c.is_nullable = 'YES' THEN ' NULLABLE' ELSE '' END)))
               AS candidate_time_columns
    FROM {{ catalog }}.information_schema.columns c
    WHERE LOWER(c.table_schema) = LOWER('{{ schema }}')
      AND UPPER(c.data_type) IN ('TIMESTAMP', 'TIMESTAMP_NTZ', 'DATE')
      AND REGEXP_LIKE(LOWER(c.column_name), 'event|_at$|_ts$|_time$|timestamp|effective|valid_from|as_of')
    GROUP BY LOWER(c.table_name)
)
SELECT
    t.table_name,
    t.table_owner,
    pk.constraint_name                                         AS pk_constraint,
    pc.pk_columns,
    pc.pk_time_columns,
    tc.candidate_time_columns,
    CASE
        WHEN pk.constraint_name IS NULL                         THEN 'NO_PRIMARY_KEY'
        WHEN size(pc.pk_time_columns) > 0                       THEN 'TIME_IN_KEY_NO_FLAG'
        WHEN size(COALESCE(tc.candidate_time_columns, array())) > 0 THEN 'NO_TIME_IN_KEY'
        ELSE 'NO_TIME_COLUMN'
    END                                                        AS status
FROM tables_in_scope t
LEFT JOIN pk              USING (table_name)
LEFT JOIN pk_cols   pc    USING (table_name)
LEFT JOIN tagged    tg    USING (table_name)
LEFT JOIN time_candidates tc USING (table_name)
WHERE pk.constraint_name IS NOT NULL OR tg.table_name IS NOT NULL
ORDER BY
    CASE status WHEN 'NO_PRIMARY_KEY' THEN 0 WHEN 'NO_TIME_COLUMN' THEN 1
                WHEN 'NO_TIME_IN_KEY' THEN 2 ELSE 3 END,
    t.table_name
```

`TIME_IN_KEY_NO_FLAG` is provisional until the probe runs: the SQL cannot see the flag, so every table with a time-typed key column lands here and the probe promotes the flagged ones to `TIMESERIES`.

### Constraint clause from the probe (per table)

```sql
SHOW CREATE TABLE {{ catalog }}.{{ schema }}.{{ asset }}
```

Project from the probe output:

```sql
SELECT
    regexp_extract(createtab_stmt, '(?is)(CONSTRAINT\\s+`?[^`\\s]+`?\\s+PRIMARY\\s+KEY\\s*\\([^)]*\\))', 1) AS pk_clause,
    REGEXP_LIKE(createtab_stmt, '(?is)PRIMARY\\s+KEY\\s*\\([^)]*\\bTIMESERIES\\b[^)]*\\)')            AS has_timeseries,
    regexp_extract(createtab_stmt, '(?is)PRIMARY\\s+KEY\\s*\\([^)]*?`?([^`,\\s]+)`?\\s+TIMESERIES', 1) AS timeseries_column
FROM <probe output>
```

Merge on `table_name` with the inventory: rows where `has_timeseries` is true get status `TIMESERIES`.

### Check the Feature Engineering client's view (optional, Python)

The client reports the same flag through `get_table`, which is the authoritative reading if the regex ever disagrees with the DDL text:

```python
from databricks.feature_engineering import FeatureEngineeringClient
fe = FeatureEngineeringClient()
info = fe.get_table(name="{{ catalog }}.{{ schema }}.{{ asset }}")
print(info.primary_keys, info.timeseries_columns)
```

`timeseries_columns` is empty for a table without the flag.
