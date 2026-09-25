# Fix: bias_testing_coverage

Make bias testing observable: run per-slice statistics on the training set (Lakehouse Monitoring or a query), record the outcome, then tag the table with the test date.

## Context

The `bias_tested_at` tag records that a human ran and reviewed a bias test on a specific date. It is a claim, and it must not be set by a script that has not seen a test. Setting it on every training table to make the score green destroys the only value the tag has; an empty tag is honest, a fabricated one is not. The durable fix is a Lakehouse Monitor with slices on protected attributes, because it keeps the per-group statistics refreshed and visible without anyone remembering to update a tag.

Three steps:

1. Decide the protected attributes for the dataset (the diagnostic's `candidate_attribute_columns` is a starting list, not a legal determination).
2. Produce the per-group statistics: a sliced monitor (preferred, refreshes on schedule) or the one-off query below.
3. Review the numbers against the acceptance criteria your governance process defines, record where the review lives (ticket, doc), and set the tag.

Monitors are created through the SDK or Catalog Explorer; there is no SQL DDL for them. Creating one needs ownership of the table (or `MANAGE`) and `USE SCHEMA` plus `CREATE TABLE` on the output schema. Tagging needs `APPLY TAG` on the table.

## Fix: Create a sliced Lakehouse Monitor

Guard: `w.quality_monitors.get(table_name=...)` raises `NotFound` when no monitor exists; if one exists, use `update` to add `slicing_exprs` rather than create.

```python
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound
from databricks.sdk.service.catalog import MonitorSnapshot

w = WorkspaceClient()
table = "{{ catalog }}.{{ schema }}.{{ asset }}"
slices = {{ slicing_exprs }}   # e.g. ["gender", "age_bucket", "region"]

try:
    m = w.quality_monitors.get(table_name=table)
    existing = list(m.slicing_exprs or [])
    merged = sorted(set(existing) | set(slices))
    if merged != sorted(existing):
        w.quality_monitors.update(
            table_name=table,
            output_schema_name=m.output_schema_name,
            slicing_exprs=merged,
            snapshot=MonitorSnapshot(),
        )
except NotFound:
    w.quality_monitors.create(
        table_name=table,
        assets_dir="/Shared/lakehouse_monitoring/{{ schema }}/{{ asset }}",
        output_schema_name="{{ catalog }}.{{ schema }}",
        slicing_exprs=slices,
        snapshot=MonitorSnapshot(),
    )
w.quality_monitors.run_refresh(table_name=table)
```

`{{ slicing_exprs }}` defaults to the diagnostic's candidate attribute columns for the table; slice expressions can be column names or SQL expressions (`"age >= 65"`). A slicing expression on a high-cardinality column (zip code) produces one row per value per metric and gets expensive; bucket it first. For an inference-log or time-series table, replace `snapshot=` with the matching profile type and its timestamp column. After the first refresh, `{{ asset }}_profile_metrics` has one row per `(window, slice_key, slice_value, column_name)` and the check's probe finds `slice_rows > 0`.

## Fix: One-off per-slice statistics in SQL

When a monitor is not warranted, this is the minimum a bias review needs: label rate and volume per group, plus the gap against the overall rate. Replace `{{ label_column }}` with the target (binary 0/1 or boolean) and `{{ column }}` with one protected attribute at a time. Sample large tables.

```sql
WITH base AS (
    SELECT {{ column }} AS grp, CAST({{ label_column }} AS DOUBLE) AS y
    FROM {{ catalog }}.{{ schema }}.{{ asset }} TABLESAMPLE ({{ sample_rows }} ROWS)
    WHERE {{ label_column }} IS NOT NULL
),
overall AS (SELECT AVG(y) AS overall_rate, COUNT(*) AS n FROM base)
SELECT
    b.grp,
    COUNT(*)                                   AS rows_in_group,
    COUNT(*)::DOUBLE / o.n                     AS share_of_rows,
    AVG(b.y)                                   AS positive_rate,
    AVG(b.y) - o.overall_rate                  AS rate_gap_vs_overall,
    AVG(b.y) / NULLIF(MAX(AVG(b.y)) OVER (), 0) AS disparate_impact_ratio
FROM base b CROSS JOIN overall o
GROUP BY b.grp, o.n, o.overall_rate
ORDER BY rows_in_group DESC
```

A `disparate_impact_ratio` under 0.8 for any group with meaningful volume is the conventional flag (the "four-fifths rule"); your policy may set a different threshold. Save the result to a review table or attach it to the ticket so the tag points at something.

## Fix: Tag a table after a reviewed test

Only after the review is done. Guard (skip if the same date is already set):

```sql
SELECT tag_value
FROM {{ catalog }}.information_schema.table_tags
WHERE LOWER(schema_name) = LOWER('{{ schema }}')
  AND LOWER(table_name)  = LOWER('{{ asset }}')
  AND LOWER(tag_name)    = 'bias_tested_at'
```

Then:

```sql
ALTER TABLE {{ catalog }}.{{ schema }}.{{ asset }}
SET TAGS ('bias_tested_at' = '{{ test_date }}',
          'bias_test_ref'  = '{{ review_reference }}');
```

`{{ test_date }}` is the review date in `YYYY-MM-DD`. `{{ review_reference }}` (ticket URL, document id) is optional but makes the tag auditable. Also set `training_set = 'true'` if the table was picked up only by name pattern, so future runs do not depend on the regex.

## Fix: Generate tag statements from a review log

For teams that keep a review table (`{{ review_table }}` with columns `table_name`, `reviewed_on DATE`, `reference STRING`), emit one statement per reviewed table that is still untagged or whose tag is older than the review. This is the only bulk path that is safe, because each row is a recorded human decision.

```sql
WITH current_tag AS (
    SELECT LOWER(table_name) AS table_name, TRY_CAST(MAX(tag_value) AS DATE) AS tagged_on
    FROM {{ catalog }}.information_schema.table_tags
    WHERE LOWER(schema_name) = LOWER('{{ schema }}')
      AND LOWER(tag_name) = 'bias_tested_at'
    GROUP BY LOWER(table_name)
)
SELECT concat(
    'ALTER TABLE {{ catalog }}.{{ schema }}.`', r.table_name,
    '` SET TAGS (''bias_tested_at'' = ''', date_format(r.reviewed_on, 'yyyy-MM-dd'),
    ''', ''bias_test_ref'' = ''', r.reference, ''');'
) AS stmt
FROM {{ review_table }} r
JOIN {{ catalog }}.information_schema.tables t
  ON LOWER(t.table_name) = LOWER(r.table_name)
 AND LOWER(t.table_schema) = LOWER('{{ schema }}')
 AND t.table_type IN ('MANAGED', 'EXTERNAL')
LEFT JOIN current_tag c ON c.table_name = LOWER(r.table_name)
WHERE c.tagged_on IS NULL OR c.tagged_on < r.reviewed_on
ORDER BY r.table_name
```

Show the statements to the user before running them.

## Organizational guidance

Put bias testing in the model release process, not in the catalog clean-up. Every training set registered for a model should get a sliced monitor at creation (a template in the feature pipeline or MLflow training job that calls `quality_monitors.create` with the dataset's protected attributes), and the review that reads those metrics should be the step that sets `bias_tested_at`. Declare `bias_tested_at` and `training_set` as governed tags so the keys are consistent, and set an expiry convention (re-test when the tag is older than a year or when the data distribution drifts, which the same monitor's `_drift_metrics` table reports).
