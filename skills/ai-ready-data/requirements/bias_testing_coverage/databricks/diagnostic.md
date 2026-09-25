# Diagnostic: bias_testing_coverage

Per-training-table view of the bias tag, the monitor output table if any, and the columns that look like protected attributes, so the reviewer can see what was tested and what could be sliced.

## Context

Reuses the check's training-table CTE (`training_set` tag or `{{ training_patterns }}`). For each table:

- `how_identified`: `TAG` if `training_set` is set, otherwise `NAME_PATTERN`. Name-pattern hits should be sanity-checked; `dataset_versions` is not a training set.
- `bias_tested_at`: the tag value. A date more than `{{ max_test_age_days }}` days old (default 365) is flagged `STALE` in the status, since the tag is only useful if it is refreshed when the data changes.
- `profile_table`: the `{table}_profile_metrics` table found anywhere in the catalog. Run the check's probe against it to see the slice keys.
- `candidate_attribute_columns`: columns on the table whose names match protected-attribute patterns (`gender|sex|age|race|ethnic|religion|disab|national|marital|income|zip|postcode`). These are what a bias test slices on; if the list is empty the table may hold no protected attributes, or they may be encoded under other names.
- `status`: `TESTED_TAG`, `TESTED_MONITOR`, `STALE_TAG`, `MONITOR_UNVERIFIED` (profile table exists, slices not yet probed) or `NOT_TESTED`.

Sorted worst-first.

## SQL

```sql
WITH training_tables AS (
    SELECT DISTINCT LOWER(t.table_name) AS table_name,
           t.table_owner,
           CASE WHEN tg.tag_name IS NOT NULL THEN 'TAG' ELSE 'NAME_PATTERN' END AS how_identified
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
bias_tag AS (
    SELECT LOWER(table_name) AS table_name,
           MAX(tag_value)    AS bias_tested_at
    FROM {{ catalog }}.information_schema.table_tags
    WHERE LOWER(schema_name) = LOWER('{{ schema }}')
      AND LOWER(tag_name) = 'bias_tested_at'
    GROUP BY LOWER(table_name)
),
profile_tables AS (
    SELECT LOWER(regexp_replace(table_name, '_profile_metrics$', '')) AS table_name,
           MIN(concat_ws('.', table_catalog, table_schema, table_name)) AS profile_table
    FROM {{ catalog }}.information_schema.tables
    WHERE REGEXP_LIKE(LOWER(table_name), '_profile_metrics$')
    GROUP BY LOWER(regexp_replace(table_name, '_profile_metrics$', ''))
),
attribute_columns AS (
    SELECT LOWER(table_name) AS table_name,
           array_sort(collect_set(column_name)) AS candidate_attribute_columns
    FROM {{ catalog }}.information_schema.columns
    WHERE LOWER(table_schema) = LOWER('{{ schema }}')
      AND REGEXP_LIKE(LOWER(column_name),
          '(^|_)(gender|sex|age|age_group|age_bucket|dob|race|ethnicity|ethnic|religion|disability|disabled|nationality|citizenship|marital|income|zip|zipcode|postcode)($|_)')
    GROUP BY LOWER(table_name)
)
SELECT
    tt.table_name,
    tt.table_owner,
    tt.how_identified,
    b.bias_tested_at,
    p.profile_table,
    a.candidate_attribute_columns,
    CASE
        WHEN b.bias_tested_at IS NOT NULL
             AND TRY_CAST(b.bias_tested_at AS DATE) IS NOT NULL
             AND TRY_CAST(b.bias_tested_at AS DATE) < date_sub(current_date(), {{ max_test_age_days }})
                                                THEN 'STALE_TAG'
        WHEN b.bias_tested_at IS NOT NULL       THEN 'TESTED_TAG'
        WHEN p.profile_table IS NOT NULL        THEN 'MONITOR_UNVERIFIED'
        ELSE 'NOT_TESTED'
    END AS status
FROM training_tables tt
LEFT JOIN bias_tag          b USING (table_name)
LEFT JOIN profile_tables    p USING (table_name)
LEFT JOIN attribute_columns a USING (table_name)
ORDER BY
    CASE status
        WHEN 'NOT_TESTED'         THEN 0
        WHEN 'STALE_TAG'          THEN 1
        WHEN 'MONITOR_UNVERIFIED' THEN 2
        ELSE 3
    END,
    tt.table_name
```

To turn `MONITOR_UNVERIFIED` into `TESTED_MONITOR`, run the check's probe on each `profile_table` and look for `slice_rows > 0`.
