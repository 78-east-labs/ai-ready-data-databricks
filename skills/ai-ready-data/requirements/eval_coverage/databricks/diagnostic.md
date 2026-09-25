# Diagnostic: eval_coverage

One row per base table with the eval artifacts linked to it by name, by tag, and by MLflow evaluation dataset, plus an inventory of every eval artifact in the catalog and what it points at.

## Context

Reuses the check's scoping so the population matches. For each base table the first query shows `eval_by_name` (sibling tables matching the convention), `eval_by_tag` (tables carrying `{{ eval_tag_key }}` naming it), `eval_by_mlflow` (MLflow datasets whose name contains the table's name), how the link was established, and a status:

- `LINKED_EXPLICIT`: covered by tag (the strongest link).
- `LINKED_BY_NAME`: covered by naming convention or by an MLflow dataset name match only. Consider adding the tag so the link survives a rename.
- `NO_EVAL`: fix candidates.

The second query inventories eval artifacts in the whole catalog: every table that is eval-named, `eval_set_for`-tagged, or shaped like an MLflow evaluation dataset, with its row count where cheap to obtain (from `DESCRIBE DETAIL` in the orchestrator, not here), its tag targets, and whether any target resolves to an existing table. `DANGLING` means the tag names a table that does not exist, usually a rename.

Sorted `NO_EVAL` first.

## SQL

### Base tables and their eval links

```sql
WITH all_tables AS (
    SELECT LOWER(table_schema) AS table_schema, LOWER(table_name) AS table_name,
           table_owner, created, last_altered
    FROM {{ catalog }}.information_schema.tables
    WHERE table_type IN ('MANAGED', 'EXTERNAL')
),
mlflow_datasets AS (
    SELECT LOWER(table_schema) AS table_schema, LOWER(table_name) AS table_name
    FROM {{ catalog }}.information_schema.columns
    WHERE LOWER(column_name) IN ('inputs', 'expectations')
    GROUP BY LOWER(table_schema), LOWER(table_name)
    HAVING COUNT(DISTINCT LOWER(column_name)) = 2
),
eval_named AS (
    SELECT table_schema, table_name
    FROM all_tables
    WHERE REGEXP_LIKE(table_name, '(^eval_|_evals?$|_eval_|_evaluation$|_golden$|_testset$)')
),
eval_tagged AS (
    SELECT LOWER(schema_name) AS table_schema, LOWER(table_name) AS table_name,
           trim(target) AS target
    FROM {{ catalog }}.information_schema.table_tags
    LATERAL VIEW explode(split(LOWER(tag_value), ',')) t AS target
    WHERE LOWER(tag_name) = LOWER('{{ eval_tag_key }}')
),
base_tables AS (
    SELECT a.*
    FROM all_tables a
    WHERE a.table_schema = LOWER('{{ schema }}')
      AND NOT EXISTS (SELECT 1 FROM eval_named e
                      WHERE e.table_schema = a.table_schema AND e.table_name = a.table_name)
      AND NOT EXISTS (SELECT 1 FROM mlflow_datasets m
                      WHERE m.table_schema = a.table_schema AND m.table_name = a.table_name)
      AND NOT EXISTS (SELECT 1 FROM eval_tagged g
                      WHERE g.table_schema = a.table_schema AND g.table_name = a.table_name)
),
by_name AS (
    SELECT b.table_name, array_sort(collect_set(e.table_name)) AS eval_by_name
    FROM base_tables b
    JOIN eval_named e
      ON e.table_schema = LOWER('{{ schema }}')
     AND (e.table_name = concat(b.table_name, '_eval')
       OR e.table_name = concat(b.table_name, '_evals')
       OR e.table_name LIKE concat(b.table_name, '\\_eval\\_%')
       OR e.table_name = concat('eval_', b.table_name)
       OR e.table_name = concat(b.table_name, '_evaluation')
       OR e.table_name = concat(b.table_name, '_golden')
       OR e.table_name = concat(b.table_name, '_testset'))
    GROUP BY b.table_name
),
by_tag AS (
    SELECT b.table_name,
           array_sort(collect_set(concat(g.table_schema, '.', g.table_name))) AS eval_by_tag
    FROM base_tables b
    JOIN eval_tagged g
      ON g.target = b.table_name
      OR g.target = concat(LOWER('{{ schema }}'), '.', b.table_name)
      OR g.target = concat(LOWER('{{ catalog }}'), '.', LOWER('{{ schema }}'), '.', b.table_name)
    GROUP BY b.table_name
),
by_mlflow AS (
    SELECT b.table_name,
           array_sort(collect_set(concat(m.table_schema, '.', m.table_name))) AS eval_by_mlflow
    FROM base_tables b
    JOIN mlflow_datasets m ON m.table_name LIKE concat('%', b.table_name, '%')
    GROUP BY b.table_name
)
SELECT
    b.table_name,
    b.table_owner,
    n.eval_by_name,
    t.eval_by_tag,
    m.eval_by_mlflow,
    CASE
        WHEN t.eval_by_tag IS NOT NULL THEN 'LINKED_EXPLICIT'
        WHEN n.eval_by_name IS NOT NULL OR m.eval_by_mlflow IS NOT NULL THEN 'LINKED_BY_NAME'
        ELSE 'NO_EVAL'
    END AS status
FROM base_tables b
LEFT JOIN by_name   n USING (table_name)
LEFT JOIN by_tag    t USING (table_name)
LEFT JOIN by_mlflow m USING (table_name)
ORDER BY
    CASE status WHEN 'NO_EVAL' THEN 1 WHEN 'LINKED_BY_NAME' THEN 2 ELSE 3 END,
    b.table_name
```

### Inventory of eval artifacts in the catalog

```sql
WITH all_tables AS (
    SELECT LOWER(table_schema) AS table_schema, LOWER(table_name) AS table_name,
           table_owner, last_altered, comment
    FROM {{ catalog }}.information_schema.tables
    WHERE table_type IN ('MANAGED', 'EXTERNAL')
),
mlflow_datasets AS (
    SELECT LOWER(table_schema) AS table_schema, LOWER(table_name) AS table_name
    FROM {{ catalog }}.information_schema.columns
    WHERE LOWER(column_name) IN ('inputs', 'expectations')
    GROUP BY LOWER(table_schema), LOWER(table_name)
    HAVING COUNT(DISTINCT LOWER(column_name)) = 2
),
tags AS (
    SELECT LOWER(schema_name) AS table_schema, LOWER(table_name) AS table_name,
           array_sort(collect_set(trim(target))) AS targets
    FROM {{ catalog }}.information_schema.table_tags
    LATERAL VIEW explode(split(LOWER(tag_value), ',')) t AS target
    WHERE LOWER(tag_name) = LOWER('{{ eval_tag_key }}')
    GROUP BY LOWER(schema_name), LOWER(table_name)
),
artifacts AS (
    SELECT a.table_schema, a.table_name, a.table_owner, a.last_altered, a.comment,
           REGEXP_LIKE(a.table_name, '(^eval_|_evals?$|_eval_|_evaluation$|_golden$|_testset$)') AS eval_named,
           m.table_name IS NOT NULL AS mlflow_shaped,
           tg.targets
    FROM all_tables a
    LEFT JOIN mlflow_datasets m USING (table_schema, table_name)
    LEFT JOIN tags tg USING (table_schema, table_name)
    WHERE REGEXP_LIKE(a.table_name, '(^eval_|_evals?$|_eval_|_evaluation$|_golden$|_testset$)')
       OR m.table_name IS NOT NULL
       OR tg.targets IS NOT NULL
)
SELECT
    ar.table_schema, ar.table_name, ar.table_owner, ar.last_altered,
    ar.eval_named, ar.mlflow_shaped, ar.targets,
    CASE
        WHEN ar.targets IS NULL THEN 'UNTAGGED'
        WHEN EXISTS (
            SELECT 1 FROM all_tables x
            LATERAL VIEW explode(ar.targets) tt AS target
            WHERE target = x.table_name
               OR target = concat(x.table_schema, '.', x.table_name)
               OR target = concat(LOWER('{{ catalog }}'), '.', x.table_schema, '.', x.table_name)
        ) THEN 'RESOLVES'
        ELSE 'DANGLING'
    END AS tag_status,
    ar.comment
FROM artifacts ar
ORDER BY tag_status DESC, ar.table_schema, ar.table_name
```

### Record count for one eval artifact

```sql
SELECT numFiles, sizeInBytes, lastModified
FROM (DESCRIBE DETAIL {{ catalog }}.{{ schema }}.{{ asset }})
```

Row counts are not in `DESCRIBE DETAIL`; run `SELECT COUNT(*) FROM {{ catalog }}.{{ schema }}.{{ asset }}` on the eval table itself (eval tables are small by design). An eval set with under 20 records rarely covers the edge cases; note it in the report.
