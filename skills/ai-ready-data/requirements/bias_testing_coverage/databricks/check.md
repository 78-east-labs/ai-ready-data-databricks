# Check: bias_testing_coverage

Fraction of training datasets in the schema that carry documented bias testing: a `bias_tested_at` tag, or a Lakehouse Monitoring profile computed with slices.

## Context

Bias testing happens outside the catalog (fairness toolkits, notebooks, review meetings). Two things on Databricks can show it happened:

- **Tag** (tag strength). The table tag `bias_tested_at` in `{{ catalog }}.information_schema.table_tags`, holding the date of the last test (`2026-08-30`). A human sets it after the test. The check confirms presence, not method or result.
- **Lakehouse Monitoring with slices** (proxy strength). When a monitor is created on the table with `slicing_exprs` (for example `gender`, `age_bucket`, `region`), its `{table}_profile_metrics` output table contains one row per slice per metric, with `slice_key` and `slice_value` populated. Per-slice statistics on the training set are exactly what a bias test consumes, so a sliced monitor is strong evidence that the analysis is at least possible and refreshed. A monitor without slices produces rows where `slice_key IS NULL` only and does not count.

Training datasets are identified by the table tag `training_set` (any value) or a name pattern. Extra placeholder `{{ training_patterns }}`, applied to the lowercased table name, default:

```
(^|_)(train|training|trainset|training_set|training_data|labels?|dataset|feature_set|ml)($|_)
```

Add `{{ training_patterns }} = '.'` to treat every base table as a training dataset when the schema is dedicated to ML.

Execution. The primary variant is pure SQL and treats the existence of a profile-metrics table as the monitoring signal (a proxy for "sliced", since SQL cannot open each output table dynamically). Monitor output tables are named `{table}_profile_metrics` and land in the monitor's output schema, which is usually but not always the table's own schema, so the lookup scans the whole catalog. The probe variant then confirms slices per table. `information_schema` reflects tags immediately; monitor output tables appear after the first refresh (minutes to an hour after creation).

Returns NULL (N/A) when the schema contains no training datasets.

## SQL

### Tag or monitor output table present (primary)

```sql
WITH training_tables AS (
    SELECT DISTINCT LOWER(t.table_name) AS table_name
    FROM {{ catalog }}.information_schema.tables t
    LEFT JOIN {{ catalog }}.information_schema.table_tags tg
      ON  LOWER(tg.schema_name) = LOWER(t.table_schema)
      AND LOWER(tg.table_name)  = LOWER(t.table_name)
      AND LOWER(tg.tag_name)    = 'training_set'
    WHERE LOWER(t.table_schema) = LOWER('{{ schema }}')
      AND t.table_type IN ('MANAGED', 'EXTERNAL')
      AND (tg.tag_name IS NOT NULL
           OR REGEXP_LIKE(LOWER(t.table_name), '{{ training_patterns }}'))
),
bias_tagged AS (
    SELECT DISTINCT LOWER(table_name) AS table_name
    FROM {{ catalog }}.information_schema.table_tags
    WHERE LOWER(schema_name) = LOWER('{{ schema }}')
      AND LOWER(tag_name) = 'bias_tested_at'
      AND tag_value IS NOT NULL AND tag_value <> ''
),
monitored AS (
    SELECT DISTINCT
        LOWER(regexp_replace(table_name, '_profile_metrics$', '')) AS table_name
    FROM {{ catalog }}.information_schema.tables
    WHERE REGEXP_LIKE(LOWER(table_name), '_profile_metrics$')
)
SELECT
    COUNT_IF(b.table_name IS NOT NULL OR m.table_name IS NOT NULL)  AS tested_training_tables,
    COUNT(*)                                                        AS training_tables,
    COUNT_IF(b.table_name IS NOT NULL OR m.table_name IS NOT NULL)::DOUBLE
        / NULLIF(COUNT(*), 0)                                       AS value
FROM training_tables tt
LEFT JOIN bias_tagged b USING (table_name)
LEFT JOIN monitored   m USING (table_name)
```

### Tag or sliced monitor (variant, probe mode)

Stricter: the monitor only counts when its profile table actually has slice rows.

(a) Enumeration. Training tables without the tag, paired with their candidate profile table:

```sql
WITH training_tables AS (
    SELECT DISTINCT LOWER(t.table_name) AS table_name
    FROM {{ catalog }}.information_schema.tables t
    LEFT JOIN {{ catalog }}.information_schema.table_tags tg
      ON  LOWER(tg.schema_name) = LOWER(t.table_schema)
      AND LOWER(tg.table_name)  = LOWER(t.table_name)
      AND LOWER(tg.tag_name)    = 'training_set'
    WHERE LOWER(t.table_schema) = LOWER('{{ schema }}')
      AND t.table_type IN ('MANAGED', 'EXTERNAL')
      AND (tg.tag_name IS NOT NULL
           OR REGEXP_LIKE(LOWER(t.table_name), '{{ training_patterns }}'))
),
bias_tagged AS (
    SELECT DISTINCT LOWER(table_name) AS table_name
    FROM {{ catalog }}.information_schema.table_tags
    WHERE LOWER(schema_name) = LOWER('{{ schema }}')
      AND LOWER(tag_name) = 'bias_tested_at'
      AND tag_value IS NOT NULL AND tag_value <> ''
),
profile_tables AS (
    SELECT LOWER(regexp_replace(table_name, '_profile_metrics$', '')) AS table_name,
           concat_ws('.', table_catalog, table_schema, table_name)   AS profile_table
    FROM {{ catalog }}.information_schema.tables
    WHERE REGEXP_LIKE(LOWER(table_name), '_profile_metrics$')
)
SELECT tt.table_name,
       b.table_name IS NOT NULL AS has_tag,
       p.profile_table
FROM training_tables tt
LEFT JOIN bias_tagged    b USING (table_name)
LEFT JOIN profile_tables p USING (table_name)
ORDER BY tt.table_name
```

(b) Probe. For each row with `has_tag = false AND profile_table IS NOT NULL`:

```sql
SELECT COUNT_IF(slice_key IS NOT NULL) AS slice_rows,
       array_sort(collect_set(slice_key)) AS slice_keys,
       MAX(window.end)                  AS latest_window_end
FROM {profile_table}
```

If `window` is not a struct in your release, replace `window.end` with `window_end` or drop that column; the predicate only needs `slice_rows`.

(c) Predicate. A training table passes when it has the tag, or its probe returned at least one sliced row. As a SQL expression over the enumeration and probe output: `has_tag OR COALESCE(slice_rows, 0) > 0`.

(d) Aggregation. `value = passing / enumerated training tables`, NULL if none were enumerated. Tables with `has_tag = true` need no probe and count as passing.

### Monitor definition via SDK (variant)

The monitor definition is authoritative about slices even before the first refresh. Requires the SDK and read access to the table (monitor metadata is visible to callers who can see the table; creating or editing needs ownership).

```python
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound

w = WorkspaceClient()

def sliced_monitor(full_name: str):
    """True if a Lakehouse Monitor with slicing_exprs exists, False if a monitor
    exists without slices, None if the table has no monitor."""
    try:
        m = w.quality_monitors.get(table_name=full_name)
    except NotFound:
        return None
    return bool(m.slicing_exprs)

# tables = list of 'catalog.schema.table' from the enumeration query
results = {t: sliced_monitor(t) for t in tables}
passing = sum(1 for v in results.values() if v)
value = passing / len(results) if results else None
```

CLI equivalent per table: `databricks quality-monitors get {{ catalog }}.{{ schema }}.{{ asset }}` (look at `slicing_exprs`). Combine with the tag as in the predicate above.
