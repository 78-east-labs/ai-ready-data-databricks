# Fix: data_freshness

Declare the SLA, then get the writer running again.

## Context

A stale table is a symptom. The table itself cannot be "fixed" by SQL; the pipeline that should write it has stopped, slowed, or never existed. There are three things an operator can do from here:

- **Declare the SLA** with the `freshness_sla_hours` tag so the check measures against the real expectation instead of the `{{ default_sla_hours }}` fallback. This is a decision the data owner makes (how old can this table be before consumers are harmed). Tagging every table `24` without asking is worse than leaving the tag empty, because the score then reports compliance with a number nobody agreed to.
- **Find and re-run the writer.** The diagnostic's `last_writer_type` / `last_writer_id` name the job or pipeline. Check its run history and trigger it.
- **Move the table onto a scheduled refresh.** For tables produced by a SQL transformation, a materialized view or streaming table with a schedule replaces an unowned job. This is a design change, not a one-line fix, and it is covered under `feature_refresh_compliance/databricks/fix.md`.

Nothing in this file rewrites data.

## Fix: Declare the SLA on a single table

Guard:

```sql
SELECT tag_value
FROM {{ catalog }}.information_schema.table_tags
WHERE LOWER(schema_name) = LOWER('{{ schema }}')
  AND LOWER(table_name)  = LOWER('{{ asset }}')
  AND tag_name = 'freshness_sla_hours'
```

Skip if a row exists with the intended value. Otherwise, with `{{ sla_hours }}` supplied by the table owner:

```sql
ALTER TABLE {{ catalog }}.{{ schema }}.{{ asset }}
SET TAGS ('freshness_sla_hours' = '{{ sla_hours }}')
```

Requires `APPLY TAG` on the table or ownership. If the account uses governed tags, confirm `freshness_sla_hours` is an allowed key and that numeric text is an allowed value.

## Fix: Generate SLA tag statements for undeclared tables

Emits one statement per base table without the tag. Leave the value as a placeholder the owner fills in; do not run these with a uniform default.

```sql
SELECT concat(
    'ALTER TABLE `{{ catalog }}`.`{{ schema }}`.`', t.table_name,
    '` SET TAGS (''freshness_sla_hours'' = ''<HOURS>'');  -- owner: ', t.table_owner
) AS stmt
FROM {{ catalog }}.information_schema.tables t
LEFT JOIN (
    SELECT DISTINCT LOWER(table_name) AS table_name
    FROM {{ catalog }}.information_schema.table_tags
    WHERE LOWER(schema_name) = LOWER('{{ schema }}')
      AND tag_name = 'freshness_sla_hours'
) tg ON LOWER(t.table_name) = tg.table_name
WHERE LOWER(t.table_schema) = LOWER('{{ schema }}')
  AND t.table_type IN ('MANAGED', 'EXTERNAL')
  AND tg.table_name IS NULL
ORDER BY t.table_name
```

## Fix: Locate and re-run the last writer

From the diagnostic take `last_writer_type` and `last_writer_id`. For a JOB:

```sql
SELECT run_id, period_start_time, period_end_time, result_state, termination_code, trigger_type
FROM system.lakeflow.job_run_timeline
WHERE job_id = '{{ job_id }}'
  AND period_start_time >= current_timestamp() - INTERVAL 30 DAYS
ORDER BY period_start_time DESC
LIMIT 20
```

Then trigger it (needs `CAN MANAGE RUN` on the job):

```bash
databricks jobs run-now --job-id {{ job_id }}
```

For a PIPELINE:

```sql
SELECT update_id, period_start_time, period_end_time, update_type, result_state
FROM system.lakeflow.pipeline_update_timeline
WHERE pipeline_id = '{{ pipeline_id }}'
  AND period_start_time >= current_timestamp() - INTERVAL 30 DAYS
ORDER BY period_start_time DESC
LIMIT 20
```

```bash
databricks pipelines start-update {{ pipeline_id }}
```

If the last writer was a NOTEBOOK or an ad-hoc QUERY run by a person, there is no scheduled writer at all; that is the finding to report.

## Fix: Refresh a streaming table or materialized view now

Only applies when the stale object is a `STREAMING_TABLE` or `MATERIALIZED_VIEW` (not counted by the primary check, but often the real upstream of a stale base table). Requires ownership.

```sql
REFRESH MATERIALIZED VIEW {{ catalog }}.{{ schema }}.{{ asset }}
```

```sql
REFRESH STREAMING TABLE {{ catalog }}.{{ schema }}.{{ asset }}
```

## Organizational guidance

Freshness is owned by the writer, not the table. Put the `freshness_sla_hours` tag in the same template that creates the table (Lakeflow `table_properties` plus a post-deploy `SET TAGS` step, dbt `meta` mapped to tags, Terraform `databricks_sql_table`), and wire a Databricks SQL alert on the diagnostic query so `STALE` rows page the owning team before consumers notice. Prefer scheduled materialized views and streaming tables over notebook jobs for derived tables: they carry their schedule in the object definition, expose refresh status in `DESCRIBE EXTENDED`, and show up in `system.lakeflow.pipeline_update_timeline` without any extra instrumentation.
