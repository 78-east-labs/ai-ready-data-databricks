# Check: point_in_time_correctness

Fraction of feature tables in the schema whose primary key declares a `TIMESERIES` column, which is what lets Feature Engineering in Unity Catalog perform point-in-time joins.

## Context

On Databricks a feature table is any Delta table in Unity Catalog with a primary key constraint. To make it a time-series feature table, one key column is marked `TIMESERIES`: `PRIMARY KEY (customer_id, event_ts TIMESERIES)`. With that flag, `FeatureEngineeringClient.create_training_set` and feature serving look up, for each label row, the latest feature row whose timestamp is at or before the label's timestamp, which is the mechanism that prevents future-data leakage. A table that merely has a timestamp column, or even a timestamp in its primary key without the flag, is joined as an exact key match and leaks nothing but also cannot do a point-in-time lookup.

The `TIMESERIES` flag is stored on the constraint but is **not** exposed in `information_schema.table_constraints` or `key_column_usage`; those show the key columns only. The only SQL surface that prints it is `SHOW CREATE TABLE`, whose single output column `createtab_stmt` contains the DDL including the constraint clause. This is therefore a **probe mode** check: enumerate feature tables in SQL, run `SHOW CREATE TABLE` per table, match `TIMESERIES` inside the `PRIMARY KEY (...)` clause with a regex. Needs `SELECT` on the table. No data is read.

Denominator: base Delta tables that carry a `PRIMARY KEY` constraint or the table tag `feature_table = 'true'`. Tables with neither are not feature tables and are out of scope, which means a schema of plain fact tables returns NULL rather than 0.

Strength is **native**: the flag is exactly the thing the requirement asks for. The pure-SQL variant (a timestamp-typed column inside the primary key) is a **proxy** that can only say the table is one `ALTER` away from passing.

Returns NULL (N/A) when the schema has no feature tables.

## SQL

### TIMESERIES flag in SHOW CREATE TABLE (primary, probe mode)

**(a) Enumerate feature tables**

```sql
WITH tables_in_scope AS (
    SELECT LOWER(table_name) AS table_name
    FROM {{ catalog }}.information_schema.tables
    WHERE LOWER(table_schema) = LOWER('{{ schema }}')
      AND table_type IN ('MANAGED', 'EXTERNAL')
      AND UPPER(data_source_format) = 'DELTA'
),
with_pk AS (
    SELECT DISTINCT LOWER(table_name) AS table_name
    FROM {{ catalog }}.information_schema.table_constraints
    WHERE LOWER(table_schema) = LOWER('{{ schema }}')
      AND constraint_type = 'PRIMARY KEY'
),
tagged AS (
    SELECT DISTINCT LOWER(table_name) AS table_name
    FROM {{ catalog }}.information_schema.table_tags
    WHERE LOWER(schema_name) = LOWER('{{ schema }}')
      AND tag_name = 'feature_table'
      AND LOWER(tag_value) = 'true'
)
SELECT t.table_name,
       CONCAT('`{{ catalog }}`.`{{ schema }}`.`', t.table_name, '`') AS qualified_name,
       pk.table_name IS NOT NULL AS has_primary_key
FROM tables_in_scope t
LEFT JOIN with_pk pk USING (table_name)
LEFT JOIN tagged  tg USING (table_name)
WHERE pk.table_name IS NOT NULL OR tg.table_name IS NOT NULL
ORDER BY t.table_name
```

**(b) Per-table probe statement**

```sql
SHOW CREATE TABLE {{ catalog }}.{{ schema }}.{{ asset }}
```

Output: one row, one column `createtab_stmt` (STRING) holding the full DDL, for example `... CONSTRAINT customer_features_pk PRIMARY KEY (customer_id, event_ts TIMESERIES) ...`.

**(c) Per-table predicate**

In words: the DDL contains a `PRIMARY KEY (...)` clause and, inside that clause's parentheses, the keyword `TIMESERIES` follows a column name.

As a SQL expression over the probe's output column:

```sql
REGEXP_LIKE(createtab_stmt, '(?is)PRIMARY\\s+KEY\\s*\\([^)]*\\bTIMESERIES\\b[^)]*\\)')
```

The `(?is)` flags make the match case-insensitive and let `.` span lines; column names are backquoted in the output, which the character class `[^)]` tolerates.

**(d) Aggregation rule**

```
timeseries_feature_tables = probed tables where the predicate is true
total_feature_tables      = probed tables (SHOW CREATE TABLE errors are excluded and reported)
value                     = timeseries_feature_tables / total_feature_tables, NULL when total is 0
```

### Timestamp column inside the primary key (variant, pure SQL approximation)

Counts feature tables whose primary key includes a `TIMESTAMP`, `TIMESTAMP_NTZ` or `DATE` column. This is necessary for `TIMESERIES` (the flagged column must be one of those types and part of the key) but not sufficient: the flag itself is invisible here, so a table can pass this variant and still fail the primary. Use it to size the gap when the probe cannot be run, and report it as a proxy.

```sql
WITH feature_tables AS (
    SELECT DISTINCT LOWER(tc.table_name) AS table_name, tc.constraint_name
    FROM {{ catalog }}.information_schema.table_constraints tc
    JOIN {{ catalog }}.information_schema.tables t
      ON t.table_schema = tc.table_schema AND t.table_name = tc.table_name
    WHERE LOWER(tc.table_schema) = LOWER('{{ schema }}')
      AND tc.constraint_type = 'PRIMARY KEY'
      AND t.table_type IN ('MANAGED', 'EXTERNAL')
      AND UPPER(t.data_source_format) = 'DELTA'
),
pk_columns AS (
    SELECT LOWER(k.table_name) AS table_name, k.constraint_name, c.data_type
    FROM {{ catalog }}.information_schema.key_column_usage k
    JOIN {{ catalog }}.information_schema.columns c
      ON c.table_schema = k.table_schema AND c.table_name = k.table_name AND c.column_name = k.column_name
    WHERE LOWER(k.table_schema) = LOWER('{{ schema }}')
),
scored AS (
    SELECT f.table_name,
           MAX(CASE WHEN UPPER(p.data_type) IN ('TIMESTAMP', 'TIMESTAMP_NTZ', 'DATE') THEN 1 ELSE 0 END) = 1
               AS pk_has_time_column
    FROM feature_tables f
    LEFT JOIN pk_columns p ON p.table_name = f.table_name AND p.constraint_name = f.constraint_name
    GROUP BY f.table_name
)
SELECT
    COUNT_IF(pk_has_time_column)                              AS feature_tables_with_time_key,
    COUNT(*)                                                  AS total_feature_tables,
    COUNT_IF(pk_has_time_column)::DOUBLE / NULLIF(COUNT(*), 0) AS value
FROM scored
```

This variant's denominator omits tables that are feature tables by tag only (no primary key); such tables cannot have a time-series key at all and would fail the primary.
