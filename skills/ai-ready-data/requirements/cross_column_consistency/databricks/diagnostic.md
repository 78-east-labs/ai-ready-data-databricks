# Diagnostic: cross_column_consistency

Shows the rows that violate a rule, how violations cluster by a grouping column and by load time, and which CHECK constraints the schema already enforces.

## Context

Three queries:

1. **Violating rows.** Up to 100 rows failing `{{ consistency_predicate }}`, with the columns you name in `{{ display_columns }}` (default `*`) and, when the table has row tracking or a load timestamp, the version that wrote them. Sorted by `{{ order_column }}` (default: the first display column).
2. **Violation clusters.** Violation rate per value of `{{ group_column }}` (a source system, a region, a status, a `_ingest_date`). One bucket at 100% and the rest at 0% points at a single upstream job or a single code path; a uniform rate points at the rule itself.
3. **Enforced rules in the schema.** Every CHECK constraint on base tables, with the columns it references, so you know which rules are guaranteed and which tables have none.

Placeholders `{{ null_filter }}` (default `TRUE`), `{{ display_columns }}`, `{{ order_column }}`, `{{ group_column }}` as described. `{{ group_column }}` has no default; pick one from the table.

## SQL

### Violating rows

```sql
SELECT
    {{ display_columns }},
    '{{ consistency_predicate }}'                       AS violated_rule,
    CASE WHEN ({{ consistency_predicate }}) IS NULL
         THEN 'NULL_IN_RULE' ELSE 'VIOLATED' END        AS status
FROM {{ catalog }}.{{ schema }}.{{ asset }}
WHERE {{ null_filter }}
  AND NOT COALESCE({{ consistency_predicate }}, FALSE)
ORDER BY {{ order_column }}
LIMIT 100
```

`NULL_IN_RULE` rows are those where a NULL operand made the predicate unknown; if they dominate, tighten `{{ null_filter }}` or decide that the NULL is itself the defect.

### Violation clusters

```sql
SELECT
    {{ group_column }}                                              AS group_value,
    COUNT(*)                                                        AS rows_in_scope,
    COUNT_IF(NOT COALESCE({{ consistency_predicate }}, FALSE))      AS violating_rows,
    COUNT_IF(NOT COALESCE({{ consistency_predicate }}, FALSE))::DOUBLE
        / NULLIF(COUNT(*), 0)                                       AS violation_rate
FROM {{ catalog }}.{{ schema }}.{{ asset }}
WHERE {{ null_filter }}
GROUP BY {{ group_column }}
HAVING COUNT_IF(NOT COALESCE({{ consistency_predicate }}, FALSE)) > 0
ORDER BY violation_rate DESC, violating_rows DESC
LIMIT 100
```

If the table has Change Data Feed enabled, `SELECT ... FROM table_changes('{{ catalog }}.{{ schema }}.{{ asset }}', <start_version>)` with the same predicate and `GROUP BY _commit_version` tells you which commit introduced the violations.

### Enforced CHECK constraints across the schema

```sql
WITH base_tables AS (
    SELECT LOWER(table_name) AS table_name
    FROM {{ catalog }}.information_schema.tables
    WHERE LOWER(table_schema) = LOWER('{{ schema }}')
      AND table_type IN ('MANAGED', 'EXTERNAL')
),
cc AS (
    SELECT LOWER(tc.table_name) AS table_name, tc.constraint_name, c.sql AS constraint_sql
    FROM {{ catalog }}.information_schema.table_constraints tc
    JOIN {{ catalog }}.information_schema.check_constraints c
      ON c.constraint_name = tc.constraint_name
     AND c.constraint_schema = tc.constraint_schema
    WHERE LOWER(tc.table_schema) = LOWER('{{ schema }}')
      AND tc.constraint_type = 'CHECK'
),
refs AS (
    SELECT cc.table_name, cc.constraint_name, cc.constraint_sql,
           array_sort(collect_set(col.column_name)) AS referenced_columns
    FROM cc
    JOIN {{ catalog }}.information_schema.columns col
      ON LOWER(col.table_schema) = LOWER('{{ schema }}')
     AND LOWER(col.table_name)   = cc.table_name
     AND REGEXP_LIKE(LOWER(cc.constraint_sql),
                     concat('(^|[^a-z0-9_])`?', LOWER(col.column_name), '`?([^a-z0-9_]|$)'))
    GROUP BY cc.table_name, cc.constraint_name, cc.constraint_sql
)
SELECT
    b.table_name,
    r.constraint_name,
    r.referenced_columns,
    size(r.referenced_columns) >= 2         AS is_cross_column,
    r.constraint_sql
FROM base_tables b
LEFT JOIN refs r USING (table_name)
ORDER BY r.constraint_name IS NOT NULL, b.table_name, r.constraint_name
```

Tables with `constraint_name` NULL have no enforced rule at all. Whether they need one is a question for the owner; date pairs (`start_/end_`, `created_/updated_`), amount decompositions (`net + tax = gross`) and status/timestamp pairs are the usual candidates.
