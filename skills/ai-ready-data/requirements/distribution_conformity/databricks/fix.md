# Fix: distribution_conformity

Establish a monitor and a baseline so drift is measured continuously, then act on the cause of the drift rather than on the statistic.

## Context

Drift is not fixed with an UPDATE. A drifted column means one of three things, and the diagnostic's profile query usually tells you which:

- **The pipeline broke** (unit change, currency change, a source segment dropped or doubled, a join fanned out). Fix upstream and reprocess the affected window; `RESTORE` is a last resort for a table that was fully overwritten with bad data.
- **The world changed** (prices rose, users moved). The data is right; the baseline is stale. Re-baseline, and re-validate any model trained on the old distribution.
- **Outliers or bad records** pull the moments. Handle under `outlier_prevalence` and `value_range_validity`; the drift statistic follows.

The durable fix is the monitor itself. Without one, the check can only compare Delta versions. The options below create or adjust monitoring; none of them rewrites data except the explicit reprocessing option.

Creating a monitor needs `USE CATALOG`, `USE SCHEMA`, `SELECT` on the table and `CREATE TABLE` on the output schema. It runs on serverless compute and creates the `_profile_metrics` and `_drift_metrics` Delta tables plus a dashboard.

## Fix: Create a Lakehouse Monitoring monitor with a baseline

The SDK call. `baseline_table_name` is optional; without it the monitor computes `CONSECUTIVE` drift only. A good baseline is a snapshot of the table taken when a model was trained, or the training set itself.

```python
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import (
    MonitorSnapshot, MonitorCronSchedule)

w = WorkspaceClient()
table = "{{ catalog }}.{{ schema }}.{{ asset }}"

def monitor_exists(t):
    try:
        w.quality_monitors.get(table_name=t)
        return True
    except Exception:          # NotFound when no monitor is attached
        return False

if not monitor_exists(table):  # guard: create is not idempotent
    w.quality_monitors.create(
        table_name=table,
        assets_dir="/Workspace/Shared/monitors/{{ schema }}/{{ asset }}",
        output_schema_name="{{ monitor_schema }}",
        snapshot=MonitorSnapshot(),
        baseline_table_name="{{ catalog }}.{{ schema }}.{{ asset }}_baseline",
        schedule=MonitorCronSchedule(
            quartz_cron_expression="0 0 6 * * ?", timezone_id="UTC"),
    )
```

CLI equivalent:

```bash
databricks quality-monitors get {{ catalog }}.{{ schema }}.{{ asset }} >/dev/null 2>&1 || \
databricks quality-monitors create {{ catalog }}.{{ schema }}.{{ asset }} \
  /Workspace/Shared/monitors/{{ schema }}/{{ asset }} {{ monitor_schema }} \
  --json '{"snapshot": {}, "baseline_table_name": "{{ catalog }}.{{ schema }}.{{ asset }}_baseline",
           "schedule": {"quartz_cron_expression": "0 0 6 * * ?", "timezone_id": "UTC"}}'
```

Use `time_series={"timestamp_col": "...", "granularities": ["1 day"]}` instead of `snapshot` when the table has an event time; drift is then computed per day, which is what you want for append-only facts.

## Fix: Capture a baseline snapshot

A baseline is just a table with the same schema. Take it from the version the model was trained on. `CREATE TABLE IF NOT EXISTS` makes this idempotent; a new baseline gets a new name (`_baseline_2026q3`) and the monitor is updated to point at it.

```sql
CREATE TABLE IF NOT EXISTS {{ catalog }}.{{ schema }}.{{ asset }}_baseline
COMMENT 'Distribution baseline for {{ asset }}, captured from version {{ baseline_version }}'
AS SELECT * FROM {{ catalog }}.{{ schema }}.{{ asset }} VERSION AS OF {{ baseline_version }};

ALTER TABLE {{ catalog }}.{{ schema }}.{{ asset }}_baseline
SET TAGS ('baseline_for' = '{{ asset }}', 'baseline_version' = '{{ baseline_version }}');
```

Then `databricks quality-monitors update {{ catalog }}.{{ schema }}.{{ asset }} {{ monitor_schema }} --json '{"baseline_table_name": "...", "snapshot": {}}'` and `databricks quality-monitors run-refresh {{ catalog }}.{{ schema }}.{{ asset }}`.

## Fix: Re-baseline after a legitimate population change

Only after a human has confirmed the shift is real and the model (or downstream consumer) has been re-validated on the new distribution. Point the monitor at a fresh snapshot (previous option) and record why in the table comment and in the model's registry description:

```sql
COMMENT ON TABLE {{ catalog }}.{{ schema }}.{{ asset }}_baseline_{{ suffix }} IS
'Re-baselined {{ today }}: <reason, ticket, approver>. Supersedes {{ asset }}_baseline.';
```

## Fix: Reprocess a window that drifted because of a pipeline defect

When the diagnostic shows the drift started at a specific commit, and the cause was upstream, replay that window from the fixed source. This is a data change; run the blast radius first.

```sql
-- Blast radius
SELECT COUNT(*) AS rows_in_window
FROM {{ catalog }}.{{ schema }}.{{ asset }}
WHERE {{ timestamp_column }} >= '{{ window_start }}' AND {{ timestamp_column }} < '{{ window_end }}';

-- Replace the window (Delta replaceWhere semantics; one atomic commit)
INSERT INTO {{ catalog }}.{{ schema }}.{{ asset }}
REPLACE WHERE {{ timestamp_column }} >= '{{ window_start }}' AND {{ timestamp_column }} < '{{ window_end }}'
SELECT * FROM {{ fixed_source }}
WHERE {{ timestamp_column }} >= '{{ window_start }}' AND {{ timestamp_column }} < '{{ window_end }}';
```

`INSERT INTO ... REPLACE WHERE` is the Databricks SQL form of `replaceWhere`. If the table has no timestamp, `RESTORE TABLE ... TO VERSION AS OF <n>` followed by a re-run of the fixed job is the alternative; it is a new commit, not a history rewrite.

## Fix: Bulk generation of monitors for the schema

Emits a CLI line per base table that has at least one numeric column and no monitor output table yet. Review the list; monitors cost serverless compute on every refresh.

```sql
WITH candidates AS (
    SELECT DISTINCT LOWER(t.table_name) AS table_name
    FROM {{ catalog }}.information_schema.tables t
    JOIN {{ catalog }}.information_schema.columns c
      ON c.table_schema = t.table_schema AND c.table_name = t.table_name
    WHERE LOWER(t.table_schema) = LOWER('{{ schema }}')
      AND t.table_type IN ('MANAGED', 'EXTERNAL')
      AND c.data_type IN ('TINYINT', 'SMALLINT', 'INT', 'BIGINT', 'FLOAT', 'DOUBLE', 'DECIMAL')
),
monitored AS (
    SELECT DISTINCT regexp_replace(LOWER(table_name), '_drift_metrics$', '') AS table_name
    FROM {{ catalog }}.information_schema.tables
    WHERE LOWER(table_schema) = LOWER(split_part('{{ monitor_schema }}', '.', 2))
      AND REGEXP_LIKE(LOWER(table_name), '_drift_metrics$')
)
SELECT concat(
    'databricks quality-monitors create {{ catalog }}.{{ schema }}.', c.table_name,
    ' /Workspace/Shared/monitors/{{ schema }}/', c.table_name, ' {{ monitor_schema }} --json ''{"snapshot": {}}'''
) AS cmd
FROM candidates c
LEFT JOIN monitored m USING (table_name)
WHERE m.table_name IS NULL
ORDER BY c.table_name
```

## Organizational guidance

Every table that feeds a model or a retrieval index should have a monitor and a named baseline that is tied to the model version in MLflow (log `baseline_table` and `baseline_version` as run tags). Put monitor creation in the same deployment step as the table (Terraform `databricks_quality_monitor`, or the SDK call in the pipeline's setup notebook) so it is not an afterthought. Decide the tolerance per table with the model owner, and route drift alerts (a SQL alert on `_drift_metrics` where `drift_stat > tolerance`) to the team that owns the pipeline, not the team that owns the model. Re-baselining is a decision with an approver, not a button.
