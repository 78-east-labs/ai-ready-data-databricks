# Check: eval_coverage

Fraction of base tables in the schema that have an associated evaluation set: a table tagged `eval_set_for` naming them, a sibling table following an eval naming convention, or an MLflow evaluation dataset stored in Unity Catalog that references them.

## Context

Databricks has no "evaluation set" object type. Three signals, weakest to strongest:

- **Naming convention (proxy).** A base table `t` is covered when the schema also holds `t_eval`, `t_evals`, `t_eval_*`, `eval_t`, `t_evaluation`, `t_golden` or `t_testset`. Matches the upstream framework's convention so profiles port over.
- **Tag (`eval_set_for`).** A table anywhere in the catalog carries `eval_set_for = '<table>'` (or a comma-separated list of tables, each as `table` or `schema.table`). This is the explicit link and is the default tag key from `platforms/DATABRICKS.md`. Override the key with `{{ eval_tag_key }}`; default `eval_set_for`.
- **MLflow evaluation dataset in UC (native).** `mlflow.genai.datasets.create_dataset(uc_table_name=...)` writes a Delta table with the columns `dataset_record_id`, `inputs`, `expectations`, `source_type`, `source_id`, `create_time`, `created_by`. Any table in the catalog with `inputs` and `expectations` columns is treated as an MLflow evaluation dataset. It counts toward a base table when it is tagged `eval_set_for` that table, or when its name contains the base table's name (`orders_agent_eval`, `eval_orders`). The dataset alone does not say which table it exercises; the tag or the name does.

Eval tables and MLflow datasets are removed from the denominator, so a schema with `orders` and `orders_eval` scores 1.0, not 0.5, and a schema of nothing but eval tables returns NULL.

What it proves: that an evaluation artifact exists and is linked to the table. It does not prove the evals run, pass, or cover the table's edge cases. MLflow run history (`mlflow.search_runs` against the experiment) is where pass rates live; that is out of scope for a metadata check.

`information_schema.tables`, `table_tags` and `columns` are live. Rows are filtered to what the caller can see, so an eval table in a schema the caller cannot read will not count.

If you are not sure your MLflow version writes `inputs` / `expectations` (older `mlflow.evaluate` datasets used `inputs` / `outputs` / `targets`), confirm with `SELECT table_schema, table_name FROM {{ catalog }}.information_schema.columns WHERE LOWER(column_name) IN ('expectations','targets') GROUP BY 1,2`. The variant below accepts `targets` as well.

Returns NULL (N/A) when the schema contains no non-eval base tables.

## SQL

### Naming convention, tag, or MLflow dataset (primary)

```sql
WITH all_tables AS (
    SELECT LOWER(table_schema) AS table_schema, LOWER(table_name) AS table_name
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
    SELECT a.table_name
    FROM all_tables a
    WHERE a.table_schema = LOWER('{{ schema }}')
      AND NOT EXISTS (SELECT 1 FROM eval_named e
                      WHERE e.table_schema = a.table_schema AND e.table_name = a.table_name)
      AND NOT EXISTS (SELECT 1 FROM mlflow_datasets m
                      WHERE m.table_schema = a.table_schema AND m.table_name = a.table_name)
      AND NOT EXISTS (SELECT 1 FROM eval_tagged g
                      WHERE g.table_schema = a.table_schema AND g.table_name = a.table_name)
),
covered AS (
    SELECT DISTINCT b.table_name
    FROM base_tables b
    WHERE EXISTS (
            SELECT 1 FROM eval_named e
            WHERE e.table_schema = LOWER('{{ schema }}')
              AND (e.table_name = concat(b.table_name, '_eval')
                OR e.table_name = concat(b.table_name, '_evals')
                OR e.table_name LIKE concat(b.table_name, '\\_eval\\_%')
                OR e.table_name = concat('eval_', b.table_name)
                OR e.table_name = concat(b.table_name, '_evaluation')
                OR e.table_name = concat(b.table_name, '_golden')
                OR e.table_name = concat(b.table_name, '_testset')))
       OR EXISTS (
            SELECT 1 FROM eval_tagged g
            WHERE g.target = b.table_name
               OR g.target = concat(LOWER('{{ schema }}'), '.', b.table_name)
               OR g.target = concat(LOWER('{{ catalog }}'), '.', LOWER('{{ schema }}'), '.', b.table_name))
       OR EXISTS (
            SELECT 1 FROM mlflow_datasets m
            WHERE m.table_name LIKE concat('%', b.table_name, '%'))
)
SELECT
    COUNT_IF(c.table_name IS NOT NULL)            AS tables_with_eval,
    COUNT(*)                                       AS base_tables,
    COUNT_IF(c.table_name IS NOT NULL)::DOUBLE
        / NULLIF(COUNT(*), 0)                      AS value
FROM base_tables b
LEFT JOIN covered c USING (table_name)
```

### Explicit links only (variant, strict)

Drops the naming convention and the name-contains match for MLflow datasets. A table is covered only through the `{{ eval_tag_key }}` tag. Use when the schema's table names make the convention noisy (a table called `retrieval_eval_config` is not an eval set).

```sql
WITH all_tables AS (
    SELECT LOWER(table_schema) AS table_schema, LOWER(table_name) AS table_name
    FROM {{ catalog }}.information_schema.tables
    WHERE table_type IN ('MANAGED', 'EXTERNAL')
),
eval_tagged AS (
    SELECT LOWER(schema_name) AS table_schema, LOWER(table_name) AS table_name,
           trim(target) AS target
    FROM {{ catalog }}.information_schema.table_tags
    LATERAL VIEW explode(split(LOWER(tag_value), ',')) t AS target
    WHERE LOWER(tag_name) = LOWER('{{ eval_tag_key }}')
),
base_tables AS (
    SELECT a.table_name
    FROM all_tables a
    WHERE a.table_schema = LOWER('{{ schema }}')
      AND NOT EXISTS (SELECT 1 FROM eval_tagged g
                      WHERE g.table_schema = a.table_schema AND g.table_name = a.table_name)
),
covered AS (
    SELECT DISTINCT b.table_name
    FROM base_tables b
    JOIN eval_tagged g
      ON g.target = b.table_name
      OR g.target = concat(LOWER('{{ schema }}'), '.', b.table_name)
      OR g.target = concat(LOWER('{{ catalog }}'), '.', LOWER('{{ schema }}'), '.', b.table_name)
)
SELECT
    COUNT_IF(c.table_name IS NOT NULL)            AS tables_with_eval,
    COUNT(*)                                       AS base_tables,
    COUNT_IF(c.table_name IS NOT NULL)::DOUBLE
        / NULLIF(COUNT(*), 0)                      AS value
FROM base_tables b
LEFT JOIN covered c USING (table_name)
```
