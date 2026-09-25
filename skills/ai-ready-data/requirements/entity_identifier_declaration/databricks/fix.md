# Fix: entity_identifier_declaration

Declare a PRIMARY KEY on tables that lack one.

## Context

`ALTER TABLE ... ADD CONSTRAINT ... PRIMARY KEY (...)` on Databricks is informational: it does not scan or rewrite data and cannot fail because of duplicates. Two things can make it fail: a key column that is still nullable (PK columns must be NOT NULL, so `SET NOT NULL` comes first and that step does scan the table), and a constraint name that already exists on the table. Run the guard before the ALTER.

Because the constraint is not enforced, declaring the wrong grain is worse than declaring none: every tool that reads the PK will believe it. Run the uniqueness count first. Only add `RELY` when that count shows zero duplicates, since `RELY` lets the optimizer drop joins and aggregations on the assumption the key is unique.

Requires ownership of the table or `MODIFY` on it. The PK must be declared on the table itself; it cannot be added to a view. Streaming tables and materialized views get their PK through the pipeline definition (`CREATE STREAMING TABLE ... (..., CONSTRAINT pk PRIMARY KEY (...))`), not through `ALTER TABLE`.

Naming: `pk_{{ asset }}`. Constraint names are unique per schema for PK and FK constraints, so include the table name.

## Fix: Declare a PRIMARY KEY on one table

Uniqueness and null count (both `duplicate_keys` and `null_keys` should be 0; `{{ key_columns }}` is a comma-separated list such as `order_id` or `order_id, line_number`):

```sql
SELECT
    COUNT(*) - COUNT(DISTINCT {{ key_columns }})                  AS duplicate_keys,
    COUNT_IF({{ key_columns_null_predicate }})                    AS null_keys,
    COUNT(*)                                                      AS total_rows
FROM {{ catalog }}.{{ schema }}.{{ asset }}
```

`{{ key_columns_null_predicate }}` is `col IS NULL` for a single column or `col_a IS NULL OR col_b IS NULL` for a composite key. On very large tables add `TABLESAMPLE ({{ sample_rows }} ROWS)` for a first look (default 1,000,000), but confirm with a full count before adding `RELY`.

Guard (skip the ADD CONSTRAINT if a row comes back):

```sql
SELECT 1
FROM {{ catalog }}.information_schema.table_constraints
WHERE LOWER(table_schema) = LOWER('{{ schema }}')
  AND LOWER(table_name)   = LOWER('{{ asset }}')
  AND (constraint_type = 'PRIMARY KEY' OR LOWER(constraint_name) = LOWER('pk_{{ asset }}'))
```

Apply, one `SET NOT NULL` per key column (no-op when already NOT NULL, fails if NULLs exist), then the constraint:

```sql
ALTER TABLE {{ catalog }}.{{ schema }}.{{ asset }} ALTER COLUMN {{ column }} SET NOT NULL;

ALTER TABLE {{ catalog }}.{{ schema }}.{{ asset }}
ADD CONSTRAINT pk_{{ asset }} PRIMARY KEY ({{ key_columns }});
```

Add `RELY` after the column list (`PRIMARY KEY ({{ key_columns }}) RELY`) only when `duplicate_keys = 0` on a full count and the pipeline that writes the table guarantees it stays that way. For feature tables that need point-in-time lookups, append `TIMESERIES` to the timestamp key column: `PRIMARY KEY (entity_id, event_ts TIMESERIES)`.

## Fix: Bulk declare PKs for tables with one obvious identifier column

Emits `SET NOT NULL` plus `ADD CONSTRAINT` for every table without a PK that has exactly one identifier-like column (`id`, `{table}_id`, `{singular}_id`). Composite keys and ambiguous tables are left out on purpose; see the diagnostic's `MULTIPLE_CANDIDATES` rows for those.

Step 1, generate the uniqueness counts and run them:

```sql
WITH tables_in_scope AS (
    SELECT LOWER(t.table_name) AS table_name
    FROM {{ catalog }}.information_schema.tables t
    LEFT JOIN (
        SELECT DISTINCT LOWER(table_name) AS table_name
        FROM {{ catalog }}.information_schema.table_constraints
        WHERE LOWER(table_schema) = LOWER('{{ schema }}') AND constraint_type = 'PRIMARY KEY'
    ) pk ON pk.table_name = LOWER(t.table_name)
    WHERE LOWER(t.table_schema) = LOWER('{{ schema }}')
      AND t.table_type IN ('MANAGED', 'EXTERNAL')
      AND pk.table_name IS NULL
),
single_candidate AS (
    SELECT t.table_name, max(c.column_name) AS column_name
    FROM tables_in_scope t
    JOIN {{ catalog }}.information_schema.columns c
      ON LOWER(c.table_schema) = LOWER('{{ schema }}') AND LOWER(c.table_name) = t.table_name
    WHERE LOWER(c.column_name) IN ('id',
                                   concat(t.table_name, '_id'),
                                   concat(regexp_replace(t.table_name, 's$', ''), '_id'))
    GROUP BY t.table_name
    HAVING COUNT(*) = 1
)
SELECT concat(
    'SELECT ''', table_name, ''' AS table_name, ''', column_name, ''' AS column_name, ',
    'COUNT(*) - COUNT(DISTINCT `', column_name, '`) AS duplicate_keys, ',
    'COUNT_IF(`', column_name, '` IS NULL) AS null_keys, COUNT(*) AS total_rows ',
    'FROM {{ catalog }}.{{ schema }}.`', table_name, '`;'
) AS stmt
FROM single_candidate
ORDER BY table_name
```

Step 2, for the tables where step 1 returned `duplicate_keys = 0 AND null_keys = 0`, emit the ALTERs (`{{ verified_pairs }}` is written as `('orders','order_id'), ('customers','id')`):

```sql
SELECT concat(
    'ALTER TABLE {{ catalog }}.{{ schema }}.`', c.table_name, '` ALTER COLUMN `', c.column_name, '` SET NOT NULL; ',
    'ALTER TABLE {{ catalog }}.{{ schema }}.`', c.table_name,
    '` ADD CONSTRAINT pk_', c.table_name, ' PRIMARY KEY (`', c.column_name, '`);'
) AS stmt
FROM {{ catalog }}.information_schema.columns c
WHERE LOWER(c.table_schema) = LOWER('{{ schema }}')
  AND (LOWER(c.table_name), LOWER(c.column_name)) IN ( {{ verified_pairs }} )
ORDER BY c.table_name
```

Show the generated statements to the user before executing them. Tables where step 1 found duplicates are not PK candidates on that column; they either have a composite grain or a data quality problem that `uniqueness` should surface.

## Organizational guidance

Declare the key where the table is created. In dbt, `constraints: [{type: primary_key, columns: [...]}]` under the model contract renders to a UC PRIMARY KEY on Databricks. In Lakeflow declarative pipelines, put the `CONSTRAINT ... PRIMARY KEY` clause in `CREATE STREAMING TABLE` / `CREATE MATERIALIZED VIEW`. In Terraform, use the `constraint` block of `databricks_sql_table`. Pair every PK declaration with a `uniqueness` check in the pipeline (a dbt `unique` test or a Lakeflow expectation) so `RELY` stays truthful, and treat a gold table without a PK as a modelling gap in review, not a metadata chore.
