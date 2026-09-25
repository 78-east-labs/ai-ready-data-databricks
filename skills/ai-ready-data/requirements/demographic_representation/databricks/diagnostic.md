# Diagnostic: demographic_representation

Per-training-table view of the `demographic_profile` tag, the demographic attribute columns available for profiling, and whether a sliced Lakehouse Monitor already produces per-group counts.

## Context

Reuses the check's training-table population. For each table:

- `how_identified`: `TAG` (`training_set` set) or `NAME_PATTERN`. Confirm the name-pattern hits are really training data.
- `demographic_profile`: the tag value, or NULL. `demographic_profile_ref`, if set, is where the comparison against the target population is written up.
- `attribute_columns`: columns matching `{{ demographic_patterns }}` (default in the check). These are the columns a profile would group by. Empty means the reviewer must decide whether the dataset has no demographic dimension or encodes it under other names.
- `profile_table`: the `{table}_profile_metrics` output of a Lakehouse Monitor, if one exists anywhere in the catalog. If that monitor slices on an attribute column, its `count` per `slice_value` is the dataset side of the profile already computed.
- `status`: `PROFILED`, `PROFILED_NO_REF` (tag present, no reference to the write-up), `NOT_PROFILED_HAS_ATTRIBUTES` or `NOT_PROFILED_NO_ATTRIBUTES`.

Reads only metadata. Sorted worst-first, with tables that have attributes but no profile at the top because they are the actionable ones.

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
profile_tags AS (
    SELECT LOWER(table_name) AS table_name,
           MAX(CASE WHEN LOWER(tag_name) = 'demographic_profile'     THEN tag_value END) AS demographic_profile,
           MAX(CASE WHEN LOWER(tag_name) = 'demographic_profile_ref' THEN tag_value END) AS demographic_profile_ref
    FROM {{ catalog }}.information_schema.table_tags
    WHERE LOWER(schema_name) = LOWER('{{ schema }}')
    GROUP BY LOWER(table_name)
),
attribute_columns AS (
    SELECT LOWER(table_name) AS table_name,
           array_sort(collect_set(column_name)) AS attribute_columns
    FROM {{ catalog }}.information_schema.columns
    WHERE LOWER(table_schema) = LOWER('{{ schema }}')
      AND REGEXP_LIKE(LOWER(column_name), '{{ demographic_patterns }}')
    GROUP BY LOWER(table_name)
),
profile_tables AS (
    SELECT LOWER(regexp_replace(table_name, '_profile_metrics$', '')) AS table_name,
           MIN(concat_ws('.', table_catalog, table_schema, table_name)) AS profile_table
    FROM {{ catalog }}.information_schema.tables
    WHERE REGEXP_LIKE(LOWER(table_name), '_profile_metrics$')
    GROUP BY LOWER(regexp_replace(table_name, '_profile_metrics$', ''))
)
SELECT
    tt.table_name,
    tt.table_owner,
    tt.how_identified,
    pt.demographic_profile,
    pt.demographic_profile_ref,
    a.attribute_columns,
    m.profile_table,
    CASE
        WHEN pt.demographic_profile IS NOT NULL AND trim(pt.demographic_profile) <> ''
             AND pt.demographic_profile_ref IS NOT NULL                  THEN 'PROFILED'
        WHEN pt.demographic_profile IS NOT NULL AND trim(pt.demographic_profile) <> ''
                                                                         THEN 'PROFILED_NO_REF'
        WHEN a.attribute_columns IS NOT NULL                             THEN 'NOT_PROFILED_HAS_ATTRIBUTES'
        ELSE 'NOT_PROFILED_NO_ATTRIBUTES'
    END AS status
FROM training_tables tt
LEFT JOIN profile_tags      pt USING (table_name)
LEFT JOIN attribute_columns a  USING (table_name)
LEFT JOIN profile_tables    m  USING (table_name)
ORDER BY
    CASE status
        WHEN 'NOT_PROFILED_HAS_ATTRIBUTES' THEN 0
        WHEN 'NOT_PROFILED_NO_ATTRIBUTES'  THEN 1
        WHEN 'PROFILED_NO_REF'             THEN 2
        ELSE 3
    END,
    tt.table_name
```

To see the dataset-side distribution from an existing sliced monitor without touching the source table, query its profile table:

```sql
SELECT slice_key, slice_value, MAX(count) AS rows_in_slice
FROM {profile_table}
WHERE column_name = ':table' AND slice_key IS NOT NULL
GROUP BY slice_key, slice_value
ORDER BY slice_key, rows_in_slice DESC
```

`column_name = ':table'` selects the table-level row count per slice; if your monitor version labels it differently, drop that predicate and pick any column's `count`.
