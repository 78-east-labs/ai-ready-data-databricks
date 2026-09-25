# Fix: demographic_representation

Compute the training set's distribution over its demographic attributes, compare it with the target population, write down the verdict, then tag the table with `demographic_profile`.

## Context

The tag is the last step, not the fix. `demographic_profile` records a human conclusion ("this dataset's age and gender mix is within 3 points of the 2024 customer base", or "it over-represents one region and we accepted that for this use case") reached by comparing the dataset with a defined target population. Setting the tag on tables that have not been compared to anything fabricates that conclusion; an unset tag is the truthful state until the comparison is done. Do not bulk-stamp it.

What the comparison needs:

1. A target population reference: a small table `{{ reference_table }}` with `attribute`, `group_value`, `target_share` (0 to 1) per demographic dimension, sourced from a census, the customer base or the eligible population. Building it is a governance task; the SQL below assumes it exists.
2. The dataset's own shares per group, from the query below or from a sliced Lakehouse Monitor.
3. An acceptance threshold (`{{ max_share_gap }}`, default 0.05 absolute) and a person who signs off.

Demographic columns are sensitive. Run the profile queries under the same access controls as the data, aggregate only (no row output), and store results in a governance schema, not next to the training data.

Tags need `APPLY TAG` on the table or ownership. `ALTER TABLE ... SET TAGS` is idempotent for the same value.

## Fix: Compute dataset shares for one attribute

Replace `{{ column }}` with one demographic attribute column at a time. Sampled for large tables; drop `TABLESAMPLE` for exact shares.

```sql
SELECT
    '{{ column }}'                                AS attribute,
    CAST({{ column }} AS STRING)                  AS group_value,
    COUNT(*)                                      AS rows_in_group,
    COUNT(*)::DOUBLE / SUM(COUNT(*)) OVER ()      AS dataset_share
FROM {{ catalog }}.{{ schema }}.{{ asset }} TABLESAMPLE ({{ sample_rows }} ROWS)
GROUP BY {{ column }}
ORDER BY rows_in_group DESC
```

Continuous attributes (age, income) should be bucketed first (`CASE WHEN age < 25 THEN '18-24' ...`) so the groups match the reference table's groups.

## Fix: Compare against the target population

Joins the dataset shares to `{{ reference_table }}` and flags gaps beyond `{{ max_share_gap }}`. Groups present in the reference but missing from the dataset show `dataset_share = 0`, which is usually the finding that matters most.

```sql
WITH dataset AS (
    SELECT '{{ column }}' AS attribute,
           CAST({{ column }} AS STRING) AS group_value,
           COUNT(*)::DOUBLE / SUM(COUNT(*)) OVER () AS dataset_share
    FROM {{ catalog }}.{{ schema }}.{{ asset }} TABLESAMPLE ({{ sample_rows }} ROWS)
    GROUP BY {{ column }}
),
reference AS (
    SELECT attribute, group_value, target_share
    FROM {{ reference_table }}
    WHERE LOWER(attribute) = LOWER('{{ column }}')
)
SELECT
    COALESCE(r.attribute, d.attribute)     AS attribute,
    COALESCE(r.group_value, d.group_value) AS group_value,
    COALESCE(d.dataset_share, 0)           AS dataset_share,
    r.target_share,
    COALESCE(d.dataset_share, 0) - r.target_share AS share_gap,
    ABS(COALESCE(d.dataset_share, 0) - r.target_share) > {{ max_share_gap }} AS exceeds_threshold
FROM reference r
FULL OUTER JOIN dataset d
  ON LOWER(r.group_value) = LOWER(d.group_value)
ORDER BY ABS(COALESCE(d.dataset_share, 0) - COALESCE(r.target_share, 0)) DESC
```

Save the output (for example into `{{ catalog }}.{{ governance_schema }}.demographic_profiles` with `table_name`, `attribute`, `group_value`, `dataset_share`, `target_share`, `profiled_on`) so the tag has something to reference.

## Fix: Keep the dataset side fresh with a sliced monitor

If the training set is rebuilt regularly, a Lakehouse Monitor sliced on the attribute columns recomputes the per-group counts on every refresh, so the comparison can be re-run without new profiling code. Creation is through the SDK; see the `bias_testing_coverage` fix for the guarded `quality_monitors.create` / `update` snippet and pass the attribute columns as `slicing_exprs`. The per-slice `count` in `{{ asset }}_profile_metrics` then replaces the `dataset` CTE above.

## Fix: Tag the table after sign-off

Guard (skip if the same value is present; a different value is an earlier decision, confirm before overwriting):

```sql
SELECT tag_name, tag_value
FROM {{ catalog }}.information_schema.table_tags
WHERE LOWER(schema_name) = LOWER('{{ schema }}')
  AND LOWER(table_name)  = LOWER('{{ asset }}')
  AND LOWER(tag_name) IN ('demographic_profile', 'demographic_profile_ref')
```

```sql
ALTER TABLE {{ catalog }}.{{ schema }}.{{ asset }}
SET TAGS ('demographic_profile'     = '{{ profile_summary }}',
          'demographic_profile_ref' = '{{ profile_reference }}',
          'training_set'            = 'true');
```

`{{ profile_summary }}` is a short verdict a reader can act on: `matches_us_census_2024`, `within_5pct_customer_base_2026q2`, `overweights_region_west_accepted`, `not_representative_do_not_deploy`. `{{ profile_reference }}` points at the saved comparison (table name and `profiled_on`, or a document). Setting `training_set` at the same time stops the table from depending on the name regex.

## Fix: Generate tag statements from the saved profiles

Bulk path that is safe because each row is a completed, signed-off comparison. Assumes the governance table from the comparison step plus a `verdict` and `signed_off_by` column filled in by the reviewer.

```sql
WITH latest AS (
    SELECT table_name, verdict, signed_off_by, MAX(profiled_on) AS profiled_on
    FROM {{ catalog }}.{{ governance_schema }}.demographic_profiles
    WHERE verdict IS NOT NULL AND signed_off_by IS NOT NULL
    GROUP BY table_name, verdict, signed_off_by
),
current_tag AS (
    SELECT LOWER(table_name) AS table_name, MAX(tag_value) AS demographic_profile
    FROM {{ catalog }}.information_schema.table_tags
    WHERE LOWER(schema_name) = LOWER('{{ schema }}')
      AND LOWER(tag_name) = 'demographic_profile'
    GROUP BY LOWER(table_name)
)
SELECT concat(
    'ALTER TABLE {{ catalog }}.{{ schema }}.`', l.table_name,
    '` SET TAGS (''demographic_profile'' = ''', l.verdict,
    ''', ''demographic_profile_ref'' = ''{{ catalog }}.{{ governance_schema }}.demographic_profiles@',
    date_format(l.profiled_on, 'yyyy-MM-dd'), ''');'
) AS stmt,
l.signed_off_by
FROM latest l
JOIN {{ catalog }}.information_schema.tables t
  ON  LOWER(t.table_schema) = LOWER('{{ schema }}')
  AND LOWER(t.table_name)   = LOWER(l.table_name)
  AND t.table_type IN ('MANAGED', 'EXTERNAL')
LEFT JOIN current_tag c ON c.table_name = LOWER(l.table_name)
WHERE c.demographic_profile IS NULL OR c.demographic_profile <> l.verdict
ORDER BY l.table_name
```

Show the statements to the user before executing.

## Organizational guidance

Representation is a property of a dataset relative to a use, so it belongs in the model card and the dataset intake, not in an after-the-fact tag sweep. Maintain one governed reference table of target populations per product line, make the comparison query part of the training-set build job (write its output to the governance table on every rebuild), and have the model review read that table before approving deployment. Declare `demographic_profile`, `demographic_profile_ref` and `training_set` as governed tags. When a dataset cannot be profiled because it has no demographic columns by design, record that explicitly (`demographic_profile = 'no_demographic_attributes_by_design'`) after someone has confirmed proxies (zip code, name, language) are not present either.
