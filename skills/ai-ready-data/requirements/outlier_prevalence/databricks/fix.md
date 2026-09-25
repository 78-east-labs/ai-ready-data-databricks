# Fix: outlier_prevalence

Remove sentinel values, correct unit errors, and give AI consumers a filtered or winsorized view, without destroying legitimate extremes.

## Context

An outlier by z-score is one of four things, and the diagnostic separates them:

- **A sentinel or cap** (`-1`, `0`, `9999`, `999999`, a repeated maximum). Not a measurement. NULL it.
- **A unit or scale error** (cents in a dollars column, milliseconds in a seconds column, a factor of 1000 from one source system). Correct it by the factor, scoped to the affected rows (usually one source or one load window).
- **A legitimate extreme** (a whale customer, a Black Friday day). The data is right. Do not modify the table; give models a view that winsorizes or excludes, and document the choice.
- **A genuine error with unknown truth.** Quarantine or NULL; clamping invents a number.

Never clamp before ruling out the first two; clamping a sentinel `-1` to the lower bound turns a "missing" marker into a plausible value.

Databricks `UPDATE` does not accept a `FROM` clause, so bounds computed from the data are passed as scalar subqueries (evaluated once each) or pasted as literals after running the diagnostic. Every mutating statement is restricted to affected rows and is idempotent. Deleted-file retention (7 days by default) is the undo window.

## Fix: Blast radius

```sql
WITH stats AS (
    SELECT AVG({{ column }})::DOUBLE AS mean_val, STDDEV({{ column }})::DOUBLE AS sd_val
    FROM {{ catalog }}.{{ schema }}.{{ asset }}
    WHERE {{ column }} IS NOT NULL
)
SELECT
    COUNT_IF(s.sd_val > 0 AND ABS(t.{{ column }} - s.mean_val) > {{ z_threshold }} * s.sd_val) AS outlier_rows,
    COUNT_IF(t.{{ column }} IN ({{ sentinel_values }}))                                          AS sentinel_rows,
    COUNT(*)                                                                                     AS non_null_rows,
    MAX(s.mean_val - {{ z_threshold }} * s.sd_val)                                               AS lower_bound,
    MAX(s.mean_val + {{ z_threshold }} * s.sd_val)                                               AS upper_bound
FROM {{ catalog }}.{{ schema }}.{{ asset }} t CROSS JOIN stats s
WHERE t.{{ column }} IS NOT NULL
```

`{{ sentinel_values }}` is a comma-separated numeric list from the diagnostic's repeated-values query, for example `-1, 9999, 999999`.

## Fix: NULL sentinel values

```sql
UPDATE {{ catalog }}.{{ schema }}.{{ asset }}
SET {{ column }} = NULL
WHERE {{ column }} IN ({{ sentinel_values }})
```

Then fix the source mapping so the sentinel is written as NULL at bronze-to-silver.

## Fix: Correct a unit or scale error for a scoped set of rows

`{{ scope_predicate }}` identifies the affected rows (a `source_system`, a load date range, a file name); `{{ factor }}` is the correction (for example `/ 100` for cents to dollars). Guard with the value range so a second run cannot double-apply.

```sql
UPDATE {{ catalog }}.{{ schema }}.{{ asset }}
SET {{ column }} = {{ column }} {{ factor }}
WHERE {{ scope_predicate }}
  AND {{ column }} > {{ plausible_max }}
```

`{{ plausible_max }}` is the largest value the column can legitimately hold in the correct unit; after correction no row satisfies the predicate, which is what makes the statement safe to re-run.

## Fix: Winsorize (clamp) to the z bounds

Only for rows confirmed to be errors with unknown truth, after sentinels are gone. Bounds are evaluated once via scalar subqueries; paste literals from the blast-radius output if you want the bounds frozen in the change record.

```sql
UPDATE {{ catalog }}.{{ schema }}.{{ asset }}
SET {{ column }} = GREATEST(
        (SELECT AVG({{ column }}) - {{ z_threshold }} * STDDEV({{ column }}) FROM {{ catalog }}.{{ schema }}.{{ asset }} WHERE {{ column }} IS NOT NULL),
        LEAST(
            (SELECT AVG({{ column }}) + {{ z_threshold }} * STDDEV({{ column }}) FROM {{ catalog }}.{{ schema }}.{{ asset }} WHERE {{ column }} IS NOT NULL),
            {{ column }}))
WHERE {{ column }} IS NOT NULL
  AND ABS({{ column }} - (SELECT AVG({{ column }}) FROM {{ catalog }}.{{ schema }}.{{ asset }} WHERE {{ column }} IS NOT NULL))
      > {{ z_threshold }} * (SELECT STDDEV({{ column }}) FROM {{ catalog }}.{{ schema }}.{{ asset }} WHERE {{ column }} IS NOT NULL)
```

Clamping shrinks the stddev, so a second run finds new "outliers" at the tightened bounds. Run it once, record the bounds, and do not schedule it.

## Fix: Quarantine, then NULL genuine errors

```sql
CREATE TABLE IF NOT EXISTS {{ catalog }}.{{ schema }}.{{ asset }}_outliers
AS SELECT *, '' AS outlier_column, CAST(NULL AS DOUBLE) AS z_score, current_timestamp() AS quarantined_at
   FROM {{ catalog }}.{{ schema }}.{{ asset }} WHERE 1 = 0;

INSERT INTO {{ catalog }}.{{ schema }}.{{ asset }}_outliers
SELECT t.*, '{{ column }}', (t.{{ column }} - s.mean_val) / s.sd_val, current_timestamp()
FROM {{ catalog }}.{{ schema }}.{{ asset }} t
CROSS JOIN (SELECT AVG({{ column }})::DOUBLE AS mean_val, STDDEV({{ column }})::DOUBLE AS sd_val
            FROM {{ catalog }}.{{ schema }}.{{ asset }} WHERE {{ column }} IS NOT NULL) s
WHERE t.{{ column }} IS NOT NULL
  AND s.sd_val > 0
  AND ABS(t.{{ column }} - s.mean_val) > {{ z_threshold }} * s.sd_val;

UPDATE {{ catalog }}.{{ schema }}.{{ asset }}
SET {{ column }} = NULL
WHERE {{ column }} IS NOT NULL
  AND ABS({{ column }} - (SELECT AVG({{ column }}) FROM {{ catalog }}.{{ schema }}.{{ asset }} WHERE {{ column }} IS NOT NULL))
      > {{ z_threshold }} * (SELECT STDDEV({{ column }}) FROM {{ catalog }}.{{ schema }}.{{ asset }} WHERE {{ column }} IS NOT NULL);
```

## Fix: Serve a filtered view for AI consumption (legitimate extremes)

Leaves the table intact. The view excludes rows beyond the threshold and is what training jobs and feature pipelines should read. Recomputing the bounds on every read is expensive; for a large table materialize them into a small stats table refreshed by the pipeline, or use a materialized view.

```sql
CREATE VIEW IF NOT EXISTS {{ catalog }}.{{ schema }}.{{ asset }}_trimmed
COMMENT 'Rows of {{ asset }} with |z({{ column }})| <= {{ z_threshold }}. Legitimate extremes excluded for model training; see {{ asset }} for the full population.'
AS
WITH stats AS (
    SELECT AVG({{ column }})::DOUBLE AS mean_val, STDDEV({{ column }})::DOUBLE AS sd_val
    FROM {{ catalog }}.{{ schema }}.{{ asset }}
    WHERE {{ column }} IS NOT NULL
)
SELECT t.*
FROM {{ catalog }}.{{ schema }}.{{ asset }} t CROSS JOIN stats s
WHERE t.{{ column }} IS NULL
   OR s.sd_val IS NULL OR s.sd_val = 0
   OR ABS(t.{{ column }} - s.mean_val) <= {{ z_threshold }} * s.sd_val
```

`CREATE VIEW IF NOT EXISTS` is the idempotency guard; to change the definition later use `CREATE OR REPLACE VIEW` (views have no history to lose). A winsorized variant replaces the `WHERE` with a `LEAST(GREATEST(...))` in the select list.

## Fix: Bulk generation of sentinel scans

Emits a query per numeric column that lists values repeated at least 100 times beyond the threshold. Run them to build `{{ sentinel_values }}` per column.

```sql
SELECT concat(
    'WITH s AS (SELECT AVG(`', column_name, '`)::DOUBLE AS m, STDDEV(`', column_name, '`)::DOUBLE AS sd ',
    'FROM {{ catalog }}.{{ schema }}.`{{ asset }}` WHERE `', column_name, '` IS NOT NULL) ',
    'SELECT ''', column_name, ''' AS column_name, t.`', column_name, '` AS value, COUNT(*) AS occurrences ',
    'FROM {{ catalog }}.{{ schema }}.`{{ asset }}` t CROSS JOIN s ',
    'WHERE s.sd > 0 AND ABS(t.`', column_name, '` - s.m) > {{ z_threshold }} * s.sd ',
    'GROUP BY t.`', column_name, '` HAVING COUNT(*) >= 100 ORDER BY occurrences DESC'
) AS stmt
FROM {{ catalog }}.information_schema.columns
WHERE LOWER(table_schema) = LOWER('{{ schema }}')
  AND LOWER(table_name)   = LOWER('{{ asset }}')
  AND data_type IN ('TINYINT', 'SMALLINT', 'INT', 'BIGINT', 'FLOAT', 'DOUBLE', 'DECIMAL')
  AND NOT REGEXP_LIKE(LOWER(column_name), '(^id$|_id$|_key$)')
ORDER BY ordinal_position
```

## Organizational guidance

Outlier handling is a modelling decision and belongs with the feature pipeline, not in the source table. Keep the silver table faithful to the source, and do trimming or winsorization in the feature table or training view with the rule written down (in the view comment, the feature's description in Unity Catalog, or the model card). Stop sentinels at bronze-to-silver with an explicit mapping to NULL and a Lakeflow expectation on the plausible range (`value_range_validity`). Attach a Lakehouse Monitoring monitor so a new sentinel or unit error shows up as a jump in `max` or `stddev` in the next window rather than in the next assessment.
