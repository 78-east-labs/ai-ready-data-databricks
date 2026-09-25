# Fix: feature_refresh_compliance

Refresh stale streaming tables and materialized views now, and give them a schedule so they stay fresh.

## Context

Three root causes map to three fixes:

- **No schedule.** A materialized view or streaming table created in a SQL warehouse without a `SCHEDULE` clause only refreshes on demand. Add one with `ALTER ... ADD SCHEDULE`.
- **Schedule too slow for the SLA.** Tighten it with `ALTER ... ALTER SCHEDULE`.
- **Pipeline failing.** A refresh is running on schedule and failing every time. `REFRESH` again will fail the same way; read the pipeline's event log first. The diagnostic's `consecutive_failures` and `pipeline_id` point at it.

A one-off `REFRESH` is safe to repeat and needs ownership of the object (or `MANAGE` on it). `ADD SCHEDULE` fails if a schedule already exists, so check `DESCRIBE EXTENDED` first and use `ALTER SCHEDULE` in that case. Schedules created this way run on serverless compute in the workspace of the warehouse that created the object.

Do not recreate the object with `CREATE OR REPLACE` to add a schedule; that triggers a full recompute and, for streaming tables, resets the stream's checkpoint.

## Fix: Refresh once

Materialized view (incremental when the definition allows it, otherwise a full recompute):

```sql
REFRESH MATERIALIZED VIEW {{ catalog }}.{{ schema }}.{{ asset }}
```

Streaming table (processes new source data since the last checkpoint):

```sql
REFRESH STREAMING TABLE {{ catalog }}.{{ schema }}.{{ asset }}
```

If the object's definition or upstream schema changed and an incremental refresh keeps failing, force a rebuild. This re-reads all source data; for a streaming table it also re-ingests everything the source still retains:

```sql
REFRESH MATERIALIZED VIEW {{ catalog }}.{{ schema }}.{{ asset }} FULL
```

```sql
REFRESH STREAMING TABLE {{ catalog }}.{{ schema }}.{{ asset }} FULL
```

## Fix: Add a schedule

Guard: run `DESCRIBE EXTENDED {{ catalog }}.{{ schema }}.{{ asset }}` and look at the `Refresh Schedule` row. If it is non-empty, use the `ALTER SCHEDULE` form below instead.

Interval form (pick an interval at or under the SLA; `EVERY` accepts HOUR, HOURS, DAY, DAYS, WEEK, WEEKS):

```sql
ALTER MATERIALIZED VIEW {{ catalog }}.{{ schema }}.{{ asset }}
ADD SCHEDULE EVERY {{ interval_hours }} HOURS
```

```sql
ALTER STREAMING TABLE {{ catalog }}.{{ schema }}.{{ asset }}
ADD SCHEDULE EVERY {{ interval_hours }} HOURS
```

Cron form (Quartz syntax, six fields, with an explicit time zone):

```sql
ALTER MATERIALIZED VIEW {{ catalog }}.{{ schema }}.{{ asset }}
ADD SCHEDULE CRON '0 0 */6 * * ?' AT TIME ZONE 'UTC'
```

## Fix: Tighten an existing schedule

```sql
ALTER MATERIALIZED VIEW {{ catalog }}.{{ schema }}.{{ asset }}
ALTER SCHEDULE EVERY {{ interval_hours }} HOURS
```

```sql
ALTER STREAMING TABLE {{ catalog }}.{{ schema }}.{{ asset }}
ALTER SCHEDULE EVERY {{ interval_hours }} HOURS
```

## Fix: Generate refresh statements for every stale object

Takes the diagnostic's population and emits one `REFRESH` per streaming table or materialized view. Run the diagnostic first and remove the `FRESH` rows; refreshing a fresh object is harmless but costs compute.

```sql
SELECT concat(
    CASE table_type
        WHEN 'MATERIALIZED_VIEW' THEN 'REFRESH MATERIALIZED VIEW `'
        ELSE 'REFRESH STREAMING TABLE `'
    END,
    '{{ catalog }}`.`{{ schema }}`.`', table_name, '`;'
) AS stmt
FROM {{ catalog }}.information_schema.tables
WHERE LOWER(table_schema) = LOWER('{{ schema }}')
  AND table_type IN ('STREAMING_TABLE', 'MATERIALIZED_VIEW')
ORDER BY table_type, table_name
```

Show the generated statements to the user before executing them.

## Fix: Investigate a failing pipeline

With `pipeline_id` from the diagnostic:

```bash
databricks pipelines get {{ pipeline_id }}
databricks pipelines list-pipeline-events {{ pipeline_id }} --max-results 50
```

Failure events carry the error message and the flow that failed. Common causes: an upstream table dropped a column the definition selects, a source table lost the `SELECT` grant for the pipeline's run-as principal, or a streaming table's source stopped being append-only (which forces `skipChangeCommits` or a full refresh). After fixing the cause, start an update:

```bash
databricks pipelines start-update {{ pipeline_id }}
```

## Organizational guidance

Put the schedule in the object's definition (`CREATE MATERIALIZED VIEW ... SCHEDULE EVERY 1 HOUR AS ...`) so it is versioned with the SQL rather than added afterwards. For feature tables that feed online serving, pick a schedule at or below the `freshness_sla_hours` tag on the same table and alert on `system.lakeflow.pipeline_update_timeline` rows with `result_state = 'FAILED'` for the pipelines that maintain them. Objects owned by a departed user's personal principal stop being refreshable when that principal is deactivated; own them with a service principal or group.
