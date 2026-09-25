# Diagnostic: row_access_policy

Per-table view of the attached row filter (function, columns, body), whether the table looks like it needs one, and an inventory of filter functions available for reuse.

## Context

Reuses the check's population (all base tables). For each table:

- `filter_function` and `filter_col_usage`: from `information_schema.row_filters`. `filter_col_usage` is the list of table columns passed to the function in `ON (...)`.
- `filter_body`: `routine_definition` from `information_schema.routines`. Read it: `RETURN TRUE`, or a function that only checks `current_user()` against a literal, is not real row-level security although it scores as such. A healthy body gates on `is_account_group_member()` and compares a column to the caller's entitlement, often via a small mapping table.
- `pii_columns`: count of columns tagged `{{ pii_tag_key }}`.
- `tenancy_columns`: columns matching `{{ tenancy_patterns }}` (default in the check), which are the natural filter keys.
- `needs_filter_hint`: `LIKELY` (PII or tenancy columns present), `UNLIKELY` (neither; often a reference table). A hint, not a verdict.
- `status`: `FILTERED`, `UNFILTERED_LIKELY_NEEDED`, `UNFILTERED`.

The second query lists every BOOLEAN-returning SQL function in the catalog's governance schema and any function already used as a filter, so a new attachment can reuse an existing function instead of adding a near-duplicate.

Sorted worst-first.

## SQL

### Tables and their row filters

```sql
WITH tables_in_scope AS (
    SELECT LOWER(table_name) AS table_name, table_name AS table_name_cased, table_owner
    FROM {{ catalog }}.information_schema.tables
    WHERE LOWER(table_schema) = LOWER('{{ schema }}')
      AND table_type IN ('MANAGED', 'EXTERNAL')
),
filters AS (
    SELECT LOWER(rf.table_name) AS table_name,
           concat_ws('.', rf.filter_catalog, rf.filter_schema, rf.filter_name) AS filter_function,
           rf.filter_col_usage,
           r.routine_definition AS filter_body
    FROM {{ catalog }}.information_schema.row_filters rf
    LEFT JOIN system.information_schema.routines r
      ON  LOWER(r.routine_catalog) = LOWER(rf.filter_catalog)
      AND LOWER(r.routine_schema)  = LOWER(rf.filter_schema)
      AND LOWER(r.routine_name)    = LOWER(rf.filter_name)
    WHERE LOWER(rf.schema_name) = LOWER('{{ schema }}')
),
pii AS (
    SELECT LOWER(table_name) AS table_name, COUNT(DISTINCT column_name) AS pii_columns
    FROM {{ catalog }}.information_schema.column_tags
    WHERE LOWER(schema_name) = LOWER('{{ schema }}')
      AND LOWER(tag_name) = LOWER('{{ pii_tag_key }}')
    GROUP BY LOWER(table_name)
),
tenancy AS (
    SELECT LOWER(table_name) AS table_name,
           array_sort(collect_set(column_name)) AS tenancy_columns
    FROM {{ catalog }}.information_schema.columns
    WHERE LOWER(table_schema) = LOWER('{{ schema }}')
      AND REGEXP_LIKE(LOWER(column_name), '{{ tenancy_patterns }}')
    GROUP BY LOWER(table_name)
)
SELECT
    t.table_name_cased AS table_name,
    t.table_owner,
    f.filter_function,
    f.filter_col_usage,
    f.filter_body,
    COALESCE(p.pii_columns, 0) AS pii_columns,
    n.tenancy_columns,
    CASE WHEN p.pii_columns > 0 OR n.tenancy_columns IS NOT NULL THEN 'LIKELY' ELSE 'UNLIKELY' END AS needs_filter_hint,
    CASE
        WHEN f.filter_function IS NOT NULL THEN 'FILTERED'
        WHEN p.pii_columns > 0 OR n.tenancy_columns IS NOT NULL THEN 'UNFILTERED_LIKELY_NEEDED'
        ELSE 'UNFILTERED'
    END AS status
FROM tables_in_scope t
LEFT JOIN filters f USING (table_name)
LEFT JOIN pii     p USING (table_name)
LEFT JOIN tenancy n USING (table_name)
ORDER BY
    CASE status WHEN 'UNFILTERED_LIKELY_NEEDED' THEN 0 WHEN 'UNFILTERED' THEN 1 ELSE 2 END,
    COALESCE(p.pii_columns, 0) DESC, t.table_name
```

`filter_body` is NULL when the function lives in a catalog the caller cannot see. If `routine_catalog` is not accepted, use `{{ catalog }}.information_schema.routines` and drop the catalog predicate.

### Reusable filter functions

```sql
WITH in_use AS (
    SELECT DISTINCT LOWER(filter_catalog) AS c, LOWER(filter_schema) AS s, LOWER(filter_name) AS n
    FROM {{ catalog }}.information_schema.row_filters
)
SELECT
    concat_ws('.', r.routine_catalog, r.routine_schema, r.routine_name) AS function_name,
    r.data_type            AS returns,
    r.routine_definition   AS body,
    u.n IS NOT NULL        AS currently_attached_somewhere
FROM {{ catalog }}.information_schema.routines r
LEFT JOIN in_use u
  ON  LOWER(r.routine_catalog) = u.c
  AND LOWER(r.routine_schema)  = u.s
  AND LOWER(r.routine_name)    = u.n
WHERE UPPER(r.data_type) = 'BOOLEAN'
  AND (LOWER(r.routine_schema) = LOWER('{{ governance_schema }}') OR u.n IS NOT NULL)
ORDER BY currently_attached_somewhere DESC, function_name
```

`data_type` on `information_schema.routines` is the return type in the standard view; if it is absent in your metastore, drop that predicate and filter on `routine_definition` containing `is_account_group_member` instead.
