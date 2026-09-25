# Check: uniqueness

Fraction of rows in a table that are unique on the key columns (1.0 = no duplicate keys).

## Context

Groups the table by `{{ key_columns }}` and counts how many rows are surplus in their group: `duplicate_rows = total_rows - distinct_key_groups`. `value = 1 - duplicate_rows / total_rows`. This is the same number the window-function formulation (`ROW_NUMBER() > 1`) produces, without the sort.

Strength is **native + data**. Unity Catalog `PRIMARY KEY` constraints are informational: Databricks records them in `information_schema.table_constraints` but does not enforce them (the optional `RELY` keyword only tells the optimizer to trust them). So the declared key is a good source for `{{ key_columns }}`, and the data scan is still required to know whether the key actually holds.

Placeholders:

- `{{ key_columns }}`: comma-separated column list, for example `order_id` or `customer_id, order_date`. Default: the columns of the table's `PRIMARY KEY` constraint, read with the discovery variant below. If the table has no primary key and the caller supplies no list, the check returns NULL (nothing to measure), and `entity_identifier_declaration` is the requirement that flags the missing key.
- `{{ sample_rows }}`: sample size for the sampled variant. Default 1,000,000.

Rows whose key columns are all NULL group together under `GROUP BY` and therefore count as duplicates of each other. That matches primary-key semantics (a key must be non-null). If the key is legitimately nullable, filter them out or measure `data_completeness` on the key columns separately.

`TABLESAMPLE (n ROWS)` in Databricks SQL takes the first `n` rows the scan produces, not a random sample. It is cheap and biased toward older files. Duplicates that straddle the sample boundary are missed, so treat the sampled value as triage only.

Returns NULL when the table has no rows, or when no key columns are available.

## SQL

### Full scan with supplied key (primary)

```sql
WITH key_groups AS (
    SELECT COUNT(*) AS rows_in_group
    FROM {{ catalog }}.{{ schema }}.{{ asset }}
    GROUP BY {{ key_columns }}
)
SELECT
    '{{ asset }}'                                   AS table_name,
    '{{ key_columns }}'                             AS key_columns,
    SUM(rows_in_group) - COUNT(*)                   AS duplicate_rows,
    SUM(rows_in_group)                              AS total_rows,
    1.0 - (SUM(rows_in_group) - COUNT(*))::DOUBLE
        / NULLIF(SUM(rows_in_group), 0)             AS value
FROM key_groups
```

### Discover the key from the PRIMARY KEY constraint (variant)

Emits the primary statement with `{{ key_columns }}` filled from `information_schema`. Run it, then run the statement it returns. It returns no row when the table has no primary key.

```sql
WITH pk AS (
    SELECT tc.table_name, tc.constraint_name
    FROM {{ catalog }}.information_schema.table_constraints tc
    WHERE LOWER(tc.table_schema) = LOWER('{{ schema }}')
      AND LOWER(tc.table_name)   = LOWER('{{ asset }}')
      AND tc.constraint_type = 'PRIMARY KEY'
),
pk_cols AS (
    SELECT
        pk.table_name,
        pk.constraint_name,
        array_join(
            transform(
                array_sort(collect_list(struct(k.ordinal_position, k.column_name))),
                s -> concat('`', s.column_name, '`')
            ), ', ') AS key_columns
    FROM pk
    JOIN {{ catalog }}.information_schema.key_column_usage k
      ON k.constraint_name = pk.constraint_name
     AND LOWER(k.table_schema) = LOWER('{{ schema }}')
     AND LOWER(k.table_name)   = LOWER(pk.table_name)
    GROUP BY pk.table_name, pk.constraint_name
)
SELECT
    constraint_name,
    key_columns,
    concat(
        'WITH key_groups AS (SELECT COUNT(*) AS rows_in_group FROM {{ catalog }}.{{ schema }}.`', table_name,
        '` GROUP BY ', key_columns, ') ',
        'SELECT ''', table_name, ''' AS table_name, ''', key_columns, ''' AS key_columns, ',
        'SUM(rows_in_group) - COUNT(*) AS duplicate_rows, SUM(rows_in_group) AS total_rows, ',
        '1.0 - (SUM(rows_in_group) - COUNT(*))::DOUBLE / NULLIF(SUM(rows_in_group), 0) AS value ',
        'FROM key_groups'
    ) AS stmt
FROM pk_cols
```

To run this for every table in the schema at once, drop the `table_name` filter in the `pk` CTE; you get one statement per table that declares a primary key.

### Sampled (variant)

For tables over the `sample_rows` threshold. Same formula on a prefix sample.

```sql
WITH key_groups AS (
    SELECT COUNT(*) AS rows_in_group
    FROM {{ catalog }}.{{ schema }}.{{ asset }} TABLESAMPLE ({{ sample_rows }} ROWS)
    GROUP BY {{ key_columns }}
)
SELECT
    '{{ asset }}'                                   AS table_name,
    '{{ key_columns }}'                             AS key_columns,
    SUM(rows_in_group) - COUNT(*)                   AS duplicate_rows,
    SUM(rows_in_group)                              AS total_rows,
    1.0 - (SUM(rows_in_group) - COUNT(*))::DOUBLE
        / NULLIF(SUM(rows_in_group), 0)             AS value
FROM key_groups
```

### Whole-row duplicates (variant)

When no key exists and the question is "are there exact duplicate records", use every column as the key. `SELECT DISTINCT *` fails on MAP columns; exclude them with `EXCEPT (col)` if the table has any.

```sql
WITH distinct_rows AS (
    SELECT COUNT(*) AS distinct_count
    FROM (SELECT DISTINCT * FROM {{ catalog }}.{{ schema }}.{{ asset }})
),
all_rows AS (
    SELECT COUNT(*) AS total_rows FROM {{ catalog }}.{{ schema }}.{{ asset }}
)
SELECT
    '{{ asset }}'                                   AS table_name,
    '*'                                             AS key_columns,
    a.total_rows - d.distinct_count                 AS duplicate_rows,
    a.total_rows                                    AS total_rows,
    1.0 - (a.total_rows - d.distinct_count)::DOUBLE
        / NULLIF(a.total_rows, 0)                   AS value
FROM all_rows a CROSS JOIN distinct_rows d
```
