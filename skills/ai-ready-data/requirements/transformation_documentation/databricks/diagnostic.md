# Diagnostic: transformation_documentation

One row per derived asset with its type, comment length, the jobs and pipelines that write it, whether the writing job has a description, and a status, undocumented assets first.

## Context

Same population as the check: declared derived objects (views, materialized views, streaming tables) plus base tables written by a `JOB` or `PIPELINE` in the last `{{ lookback_days }}` days (default 30). For each asset it shows the current comment (truncated to 200 characters), the writer entities with their names from `system.lakeflow.jobs` / `system.lakeflow.pipelines` (latest row per id), and whether any writing job carries a description. `view_definition` length is included for views so an operator can see that the SQL exists even when the comment does not.

`documentation_status`:

- `UNDOCUMENTED`: no comment, no job description
- `JOB_ONLY`: documented only through the writing job's description (the table itself says nothing)
- `SHORT`: a comment exists but is under `{{ min_comment_chars }}` characters (default 20)
- `DOCUMENTED`: a comment of at least the threshold

The `writer_entities` column is what the fix needs: a `PIPELINE:<id>` writer means the comment must be set in the pipeline source, a `JOB:<id>` writer means either the job description or a `COMMENT ON` will do.

## SQL

```sql
WITH declared_derived AS (
    SELECT LOWER(t.table_name) AS table_name, t.table_type, t.table_owner, t.comment,
           length(v.view_definition) AS view_definition_chars
    FROM {{ catalog }}.information_schema.tables t
    LEFT JOIN {{ catalog }}.information_schema.views v
           ON t.table_schema = v.table_schema AND t.table_name = v.table_name
    WHERE LOWER(t.table_schema) = LOWER('{{ schema }}')
      AND t.table_type IN ('VIEW', 'MATERIALIZED_VIEW', 'STREAMING_TABLE')
),
base_tables AS (
    SELECT LOWER(table_name) AS table_name, table_type, table_owner, comment,
           CAST(NULL AS INT) AS view_definition_chars
    FROM {{ catalog }}.information_schema.tables
    WHERE LOWER(table_schema) = LOWER('{{ schema }}')
      AND table_type IN ('MANAGED', 'EXTERNAL')
),
writers AS (
    SELECT DISTINCT
        LOWER(target_table_name)  AS table_name,
        UPPER(entity_type)        AS entity_type,
        CAST(entity_id AS STRING) AS entity_id
    FROM system.access.table_lineage
    WHERE LOWER(target_catalog) = LOWER('{{ catalog }}')
      AND LOWER(target_schema)  = LOWER('{{ schema }}')
      AND target_table_full_name IS NOT NULL
      AND UPPER(entity_type) IN ('JOB', 'PIPELINE')
      AND entity_id IS NOT NULL
      AND event_time >= current_timestamp() - INTERVAL {{ lookback_days }} DAYS
),
job_meta AS (
    SELECT CAST(job_id AS STRING) AS entity_id, name, description
    FROM (
        SELECT job_id, name, description,
               ROW_NUMBER() OVER (PARTITION BY job_id ORDER BY change_time DESC) AS rn
        FROM system.lakeflow.jobs
    ) WHERE rn = 1
),
pipeline_meta AS (
    SELECT CAST(pipeline_id AS STRING) AS entity_id, name
    FROM (
        SELECT pipeline_id, name,
               ROW_NUMBER() OVER (PARTITION BY pipeline_id ORDER BY change_time DESC) AS rn
        FROM system.lakeflow.pipelines
    ) WHERE rn = 1
),
writer_summary AS (
    SELECT
        w.table_name,
        array_sort(collect_set(concat_ws(':', w.entity_type, w.entity_id,
                                         COALESCE(jm.name, pm.name, ''))))          AS writer_entities,
        bool_or(jm.description IS NOT NULL AND trim(jm.description) <> '')          AS job_has_description,
        MAX(CASE WHEN jm.description IS NOT NULL AND trim(jm.description) <> ''
                 THEN left(jm.description, 200) END)                                AS job_description_sample
    FROM writers w
    LEFT JOIN job_meta      jm ON w.entity_type = 'JOB'      AND w.entity_id = jm.entity_id
    LEFT JOIN pipeline_meta pm ON w.entity_type = 'PIPELINE' AND w.entity_id = pm.entity_id
    GROUP BY w.table_name
),
derived_assets AS (
    SELECT d.table_name, d.table_type, d.table_owner, d.comment, d.view_definition_chars,
           ws.writer_entities, COALESCE(ws.job_has_description, false) AS job_has_description,
           ws.job_description_sample
    FROM declared_derived d
    LEFT JOIN writer_summary ws USING (table_name)
    UNION ALL
    SELECT b.table_name, b.table_type, b.table_owner, b.comment, b.view_definition_chars,
           ws.writer_entities, ws.job_has_description, ws.job_description_sample
    FROM base_tables b
    JOIN writer_summary ws USING (table_name)
)
SELECT
    table_name,
    table_type,
    table_owner,
    length(trim(COALESCE(comment, '')))            AS comment_chars,
    left(comment, 200)                             AS comment_sample,
    view_definition_chars,
    writer_entities,
    job_has_description,
    job_description_sample,
    CASE
        WHEN length(trim(COALESCE(comment, ''))) >= {{ min_comment_chars }} THEN 'DOCUMENTED'
        WHEN length(trim(COALESCE(comment, ''))) > 0                        THEN 'SHORT'
        WHEN job_has_description                                            THEN 'JOB_ONLY'
        ELSE 'UNDOCUMENTED'
    END                                            AS documentation_status
FROM derived_assets
ORDER BY
    CASE
        WHEN length(trim(COALESCE(comment, ''))) >= {{ min_comment_chars }} THEN 3
        WHEN length(trim(COALESCE(comment, ''))) > 0                        THEN 2
        WHEN job_has_description                                            THEN 1
        ELSE 0
    END ASC,
    table_type,
    table_name
```
