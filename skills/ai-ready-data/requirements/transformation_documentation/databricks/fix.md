# Fix: transformation_documentation

Put a comment on every derived asset that explains what it computes and from what, and give writing jobs a description.

## Context

Comments are metadata; setting one creates no data change and, on Delta tables, one small commit. The right place to set the comment depends on who owns the object, which the diagnostic's `writer_entities` column tells you:

- **Views and job-written tables**: `COMMENT ON TABLE` (Databricks uses `COMMENT ON TABLE` for views as well). Needs ownership or `MODIFY` on the object. Guard: `information_schema.tables.comment` is empty; if it is not, prompt before overwriting.
- **Materialized views and streaming tables** managed by a Lakeflow Declarative Pipeline: the pipeline owns the object and re-applies its definition on each update, so set the `COMMENT` clause in the pipeline source. A `COMMENT ON` applied outside the pipeline may be overwritten at the next full refresh.
- **Job description**: `databricks jobs update` with a `description`. This documents the process rather than the table and is the weaker of the two; use it in addition to the table comment, not instead.

A useful transformation comment names the inputs, the grain, the main rule, and the owner: "One row per customer per day. Joins silver.orders to silver.customers, sums net_amount excluding cancelled orders, refreshed by job orders__customer_daily__build. Owner: growth-data." Unity Catalog can draft comments with AI (`ai_gen()` in SQL, or the AI-generated comment button in Catalog Explorer); drafts still need a human to confirm the rule is stated correctly before they count as documentation.

## Fix: Comment a view or a job-written table

```sql
COMMENT ON TABLE {{ catalog }}.{{ schema }}.{{ asset }} IS
  '{{ transformation_description }}'
```

Draft with AI when the definition is available, then edit before applying:

```sql
SELECT ai_gen(concat(
    'Write a two-sentence description of what this SQL view computes, naming its inputs and grain. SQL: ',
    view_definition
)) AS draft_comment
FROM {{ catalog }}.information_schema.views
WHERE LOWER(table_schema) = LOWER('{{ schema }}')
  AND LOWER(table_name)   = LOWER('{{ asset }}')
```

`ai_gen()` needs a serverless or Pro warehouse in a supported region.

## Fix: Comment a materialized view or streaming table in its pipeline source

Edit the pipeline's SQL (or Python decorator `comment=` argument) and run an update. `CREATE OR REFRESH` is the pipeline's own idempotent form; it is not `CREATE OR REPLACE TABLE`.

```sql
CREATE OR REFRESH MATERIALIZED VIEW {{ asset }}
COMMENT '{{ transformation_description }}'
AS SELECT ...
```

```sql
CREATE OR REFRESH STREAMING TABLE {{ asset }}
COMMENT '{{ transformation_description }}'
AS SELECT ... FROM STREAM ...
```

## Fix: Give a writing job a description

`jobs update` merges the supplied fields, so this touches nothing else in the job definition. Check the current value first with `databricks jobs get {{ job_id }} --output json | jq -r '.settings.description'` and prompt before overwriting a non-empty one.

```bash
databricks jobs update --json '{
  "job_id": {{ job_id }},
  "new_settings": {"description": "{{ transformation_description }}"}
}'
```

## Fix: Generate COMMENT statements for undocumented assets

Emits one `COMMENT ON TABLE` per view or job-written table with an empty comment. Pipeline-managed assets (`MATERIALIZED_VIEW`, `STREAMING_TABLE`) are listed separately with a reminder that the comment belongs in the pipeline source. The description is left as a marker; do not execute a statement that still contains `<fill_in>`.

```sql
WITH job_written AS (
    SELECT DISTINCT LOWER(target_table_name) AS table_name
    FROM system.access.table_lineage
    WHERE LOWER(target_catalog) = LOWER('{{ catalog }}')
      AND LOWER(target_schema)  = LOWER('{{ schema }}')
      AND target_table_full_name IS NOT NULL
      AND UPPER(entity_type) IN ('JOB', 'PIPELINE')
      AND event_time >= current_timestamp() - INTERVAL {{ lookback_days }} DAYS
),
targets AS (
    SELECT LOWER(t.table_name) AS table_name, t.table_type
    FROM {{ catalog }}.information_schema.tables t
    LEFT JOIN job_written jw ON LOWER(t.table_name) = jw.table_name
    WHERE LOWER(t.table_schema) = LOWER('{{ schema }}')
      AND (t.comment IS NULL OR trim(t.comment) = '')
      AND (t.table_type IN ('VIEW', 'MATERIALIZED_VIEW', 'STREAMING_TABLE')
           OR (t.table_type IN ('MANAGED', 'EXTERNAL') AND jw.table_name IS NOT NULL))
)
SELECT
    table_type,
    CASE
        WHEN table_type IN ('MATERIALIZED_VIEW', 'STREAMING_TABLE') THEN
            concat('-- ', table_name, ': set COMMENT in the Lakeflow pipeline source (', table_type, ')')
        ELSE
            concat('COMMENT ON TABLE {{ catalog }}.{{ schema }}.`', table_name,
                   '` IS ''<fill_in: inputs, grain, rule, owner>'';')
    END AS stmt
FROM targets
ORDER BY table_type, table_name
```

Show the generated statements to the user before executing any of them.

## Organizational guidance

Documentation survives only when it lives with the code. Make the `COMMENT` clause mandatory in the pipeline and dbt templates (dbt `description` becomes the Unity Catalog comment with `persist_docs` enabled), reject pull requests that add a view without one, and put the job description into the job's Terraform or Asset Bundle definition so it is versioned. Review AI-generated comments the way you review code: they are a draft of what the SQL does, not a statement of what it is for.
