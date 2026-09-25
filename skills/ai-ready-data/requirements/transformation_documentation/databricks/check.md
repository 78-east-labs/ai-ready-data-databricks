# Check: transformation_documentation

Fraction of derived assets in the schema (views, materialized views, streaming tables, and base tables written by a job or pipeline) whose transformation is documented: the asset has a non-empty comment, or the job that writes it has a non-empty description.

## Context

A derived asset is one whose contents are computed from something else, so someone should be able to read what the computation is. Two populations are merged:

- **Declared derived objects**: `information_schema.tables` rows with `table_type IN ('VIEW', 'MATERIALIZED_VIEW', 'STREAMING_TABLE')`. Their definition is the transformation.
- **Pipeline outputs**: `MANAGED` / `EXTERNAL` tables that `system.access.table_lineage` shows being written by an entity of type `JOB` or `PIPELINE` in the last `{{ lookback_days }}` days (default 30). A table written only by notebooks or anonymous sessions is not counted as derived here; it is either a landing table or an attribution problem (`agent_attribution`).

Documentation counts when either the object's `comment` is non-empty after trimming, or, for job-written tables, the writing job's `description` in `system.lakeflow.jobs` (latest row per `job_id`) is non-empty. `system.lakeflow.pipelines` has no description column in the schema this framework relies on, so pipeline outputs are judged on their own comment; Lakeflow Declarative Pipelines set that comment from the `COMMENT` clause in the pipeline source, which is where the documentation belongs anyway. If your `pipelines` table does expose a description or a `settings.comment` field, confirm with `DESCRIBE TABLE system.lakeflow.pipelines` and extend the join.

The check measures presence, not quality. A view whose comment is "view" passes. The strict variant requires at least `{{ min_comment_chars }}` characters (default 20) to filter out placeholders. A view definition (`information_schema.views.view_definition`) is always present and is not counted as documentation, because it explains what the SQL does, not why.

`information_schema` is current. Lineage and `system.lakeflow.jobs` lag by up to a few hours; a table whose only job write is very recent may not yet be in the population, and a job whose description was just edited may still show the old row. Reading them needs `SELECT` on `system.access.table_lineage` and `system.lakeflow.jobs`. Returns NULL (N/A) when the schema has no derived assets.

## SQL

### Comment or writing-job description (primary)

```sql
WITH declared_derived AS (
    SELECT LOWER(table_name) AS table_name, table_type, comment
    FROM {{ catalog }}.information_schema.tables
    WHERE LOWER(table_schema) = LOWER('{{ schema }}')
      AND table_type IN ('VIEW', 'MATERIALIZED_VIEW', 'STREAMING_TABLE')
),
base_tables AS (
    SELECT LOWER(table_name) AS table_name, table_type, comment
    FROM {{ catalog }}.information_schema.tables
    WHERE LOWER(table_schema) = LOWER('{{ schema }}')
      AND table_type IN ('MANAGED', 'EXTERNAL')
),
pipeline_writers AS (
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
documented_jobs AS (
    SELECT CAST(job_id AS STRING) AS entity_id
    FROM (
        SELECT job_id, description,
               ROW_NUMBER() OVER (PARTITION BY job_id ORDER BY change_time DESC) AS rn
        FROM system.lakeflow.jobs
    )
    WHERE rn = 1 AND description IS NOT NULL AND trim(description) <> ''
),
derived_assets AS (
    SELECT table_name, table_type, comment, false AS job_documented
    FROM declared_derived
    UNION ALL
    SELECT b.table_name, b.table_type, b.comment,
           bool_or(dj.entity_id IS NOT NULL) AS job_documented
    FROM base_tables b
    JOIN pipeline_writers pw USING (table_name)
    LEFT JOIN documented_jobs dj
           ON pw.entity_type = 'JOB' AND pw.entity_id = dj.entity_id
    GROUP BY b.table_name, b.table_type, b.comment
)
SELECT
    COUNT_IF((comment IS NOT NULL AND trim(comment) <> '') OR job_documented)          AS documented_assets,
    COUNT(*)                                                                           AS total_derived_assets,
    COUNT_IF((comment IS NOT NULL AND trim(comment) <> '') OR job_documented)::DOUBLE
        / NULLIF(COUNT(*), 0)                                                          AS value
FROM derived_assets
```

### Declared derived objects only (variant)

Pure `information_schema`, no system tables: views, materialized views and streaming tables with a non-empty comment. Matches the upstream framework's population and has no lag or `system.*` permission requirement.

```sql
WITH declared_derived AS (
    SELECT LOWER(table_name) AS table_name, table_type, comment
    FROM {{ catalog }}.information_schema.tables
    WHERE LOWER(table_schema) = LOWER('{{ schema }}')
      AND table_type IN ('VIEW', 'MATERIALIZED_VIEW', 'STREAMING_TABLE')
)
SELECT
    COUNT_IF(comment IS NOT NULL AND trim(comment) <> '')            AS documented_assets,
    COUNT(*)                                                         AS total_derived_assets,
    COUNT_IF(comment IS NOT NULL AND trim(comment) <> '')::DOUBLE
        / NULLIF(COUNT(*), 0)                                        AS value
FROM declared_derived
```

### Strict length threshold (variant)

Same population as the primary, but a comment or description must be at least `{{ min_comment_chars }}` characters (default 20) to count. Filters out one-word placeholders.

```sql
WITH declared_derived AS (
    SELECT LOWER(table_name) AS table_name, table_type, comment
    FROM {{ catalog }}.information_schema.tables
    WHERE LOWER(table_schema) = LOWER('{{ schema }}')
      AND table_type IN ('VIEW', 'MATERIALIZED_VIEW', 'STREAMING_TABLE')
),
base_tables AS (
    SELECT LOWER(table_name) AS table_name, table_type, comment
    FROM {{ catalog }}.information_schema.tables
    WHERE LOWER(table_schema) = LOWER('{{ schema }}')
      AND table_type IN ('MANAGED', 'EXTERNAL')
),
pipeline_writers AS (
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
documented_jobs AS (
    SELECT CAST(job_id AS STRING) AS entity_id
    FROM (
        SELECT job_id, description,
               ROW_NUMBER() OVER (PARTITION BY job_id ORDER BY change_time DESC) AS rn
        FROM system.lakeflow.jobs
    )
    WHERE rn = 1 AND length(trim(COALESCE(description, ''))) >= {{ min_comment_chars }}
),
derived_assets AS (
    SELECT table_name, table_type, comment, false AS job_documented
    FROM declared_derived
    UNION ALL
    SELECT b.table_name, b.table_type, b.comment,
           bool_or(dj.entity_id IS NOT NULL) AS job_documented
    FROM base_tables b
    JOIN pipeline_writers pw USING (table_name)
    LEFT JOIN documented_jobs dj
           ON pw.entity_type = 'JOB' AND pw.entity_id = dj.entity_id
    GROUP BY b.table_name, b.table_type, b.comment
)
SELECT
    COUNT_IF(length(trim(COALESCE(comment, ''))) >= {{ min_comment_chars }} OR job_documented)          AS documented_assets,
    COUNT(*)                                                                                             AS total_derived_assets,
    COUNT_IF(length(trim(COALESCE(comment, ''))) >= {{ min_comment_chars }} OR job_documented)::DOUBLE
        / NULLIF(COUNT(*), 0)                                                                            AS value
FROM derived_assets
```
