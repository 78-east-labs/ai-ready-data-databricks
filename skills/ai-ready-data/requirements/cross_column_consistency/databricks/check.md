# Check: cross_column_consistency

Fraction of rows in a table where logically related columns agree with a declared cross-column rule.

## Context

Table-scoped data scan. The caller supplies `{{ consistency_predicate }}`, a boolean SQL expression over the table's columns, injected as raw SQL. `value = consistent_rows / rows_in_scope`, where a row is consistent when the predicate is TRUE and in scope when `{{ null_filter }}` holds.

Examples of predicates:

- `end_date >= start_date`
- `abs(total_amount - quantity * unit_price) < 0.01`
- `status <> 'SHIPPED' OR shipped_at IS NOT NULL`
- `country_code <> 'US' OR postal_code RLIKE '^[0-9]{5}(-[0-9]{4})?$'`

Placeholders:

- `{{ consistency_predicate }}`: required, no default.
- `{{ null_filter }}`: rows to evaluate. Default `TRUE` (every row). Set it to `start_date IS NOT NULL AND end_date IS NOT NULL` when a NULL on either side should count as "no rule to apply" rather than a violation. With the default, a predicate that evaluates to NULL counts as inconsistent (`COUNT_IF(pred)` skips NULL, and the denominator does not), which is the stricter reading.
- `{{ sample_rows }}`: default 1,000,000.

Strength is **data**. Databricks enforces `CHECK` constraints on Delta tables, so a rule that already exists as a constraint on `{{ asset }}` scores 1.0 by construction. The discovery variant reads `information_schema.check_constraints` for constraints whose expression references two or more columns of the table, and emits the check with that expression as the predicate. Use it to (a) list the rules that are already guaranteed, and (b) apply a curated table's rule to a raw upstream table (`{{ source_asset }}`) that shares the column names, which is where violations actually are.

Returns NULL when no rows pass `{{ null_filter }}`.

## SQL

### Declared predicate (primary)

```sql
WITH scoped AS (
    SELECT
        COUNT(*)                                        AS rows_in_scope,
        COUNT_IF({{ consistency_predicate }})           AS consistent_rows
    FROM {{ catalog }}.{{ schema }}.{{ asset }}
    WHERE {{ null_filter }}
)
SELECT
    '{{ asset }}'                                       AS table_name,
    '{{ consistency_predicate }}'                       AS rule,
    consistent_rows,
    rows_in_scope,
    consistent_rows::DOUBLE / NULLIF(rows_in_scope, 0)  AS value
FROM scoped
```

Quote characters inside the predicate must be doubled in the `rule` label, or drop that column.

### Several rules in one pass (variant)

When a table has more than one rule, one scan is cheaper than N. Each rule gets its own row; the orchestrator averages or takes the minimum.

```sql
WITH scoped AS (
    SELECT
        COUNT(*)                                    AS rows_in_scope,
        COUNT_IF({{ predicate_1 }})                 AS ok_1,
        COUNT_IF({{ predicate_2 }})                 AS ok_2,
        COUNT_IF({{ predicate_3 }})                 AS ok_3
    FROM {{ catalog }}.{{ schema }}.{{ asset }}
    WHERE {{ null_filter }}
)
SELECT
    '{{ asset }}'                                   AS table_name,
    rule,
    consistent_rows,
    rows_in_scope,
    consistent_rows::DOUBLE / NULLIF(rows_in_scope, 0) AS value
FROM scoped
LATERAL VIEW explode(map(
    '{{ predicate_1 }}', ok_1,
    '{{ predicate_2 }}', ok_2,
    '{{ predicate_3 }}', ok_3
)) AS rule, consistent_rows
```

### Discover multi-column CHECK constraints (variant)

Lists CHECK constraints on `{{ asset }}` that reference at least two of its columns and emits the primary statement for each, targeted at `{{ source_asset }}` (default: `{{ asset }}`). Column references are detected by matching column names as whole words in the constraint text, which over-matches on very short column names (`id`, `a`); review `referenced_columns`.

```sql
WITH cols AS (
    SELECT column_name
    FROM {{ catalog }}.information_schema.columns
    WHERE LOWER(table_schema) = LOWER('{{ schema }}')
      AND LOWER(table_name)   = LOWER('{{ asset }}')
),
cc AS (
    SELECT tc.constraint_name, c.sql AS constraint_sql
    FROM {{ catalog }}.information_schema.table_constraints tc
    JOIN {{ catalog }}.information_schema.check_constraints c
      ON c.constraint_name = tc.constraint_name
     AND c.constraint_schema = tc.constraint_schema
    WHERE LOWER(tc.table_schema) = LOWER('{{ schema }}')
      AND LOWER(tc.table_name)   = LOWER('{{ asset }}')
      AND tc.constraint_type = 'CHECK'
),
refs AS (
    SELECT
        cc.constraint_name,
        cc.constraint_sql,
        array_sort(collect_set(cols.column_name)) AS referenced_columns
    FROM cc
    JOIN cols
      ON REGEXP_LIKE(LOWER(cc.constraint_sql),
                     concat('(^|[^a-z0-9_])`?', LOWER(cols.column_name), '`?([^a-z0-9_]|$)'))
    GROUP BY cc.constraint_name, cc.constraint_sql
    HAVING size(collect_set(cols.column_name)) >= 2
)
SELECT
    constraint_name,
    referenced_columns,
    constraint_sql,
    concat(
        'WITH scoped AS (SELECT COUNT(*) AS rows_in_scope, COUNT_IF(', constraint_sql,
        ') AS consistent_rows FROM {{ catalog }}.{{ schema }}.`{{ source_asset }}` WHERE {{ null_filter }}) ',
        'SELECT ''{{ source_asset }}'' AS table_name, ''', replace(constraint_sql, '''', ''''''), ''' AS rule, ',
        'consistent_rows, rows_in_scope, consistent_rows::DOUBLE / NULLIF(rows_in_scope, 0) AS value FROM scoped'
    ) AS stmt
FROM refs
ORDER BY constraint_name
```

No rows means the table has no multi-column CHECK constraint; the rule has to come from the caller (or from `constraint_declaration`'s guidance on where rules are documented).

### Sampled (variant)

```sql
WITH scoped AS (
    SELECT
        COUNT(*)                                        AS rows_in_scope,
        COUNT_IF({{ consistency_predicate }})           AS consistent_rows
    FROM {{ catalog }}.{{ schema }}.{{ asset }} TABLESAMPLE ({{ sample_rows }} ROWS)
    WHERE {{ null_filter }}
)
SELECT
    '{{ asset }}'                                       AS table_name,
    consistent_rows,
    rows_in_scope,
    consistent_rows::DOUBLE / NULLIF(rows_in_scope, 0)  AS value
FROM scoped
```

`TABLESAMPLE (n ROWS)` is a prefix sample; violations concentrated in recent loads are under-counted. Use for triage only.
