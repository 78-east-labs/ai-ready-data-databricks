# Check: row_access_policy

Fraction of base tables in the schema that have a Unity Catalog row filter attached.

## Context

A row filter is a SQL UDF returning BOOLEAN that Unity Catalog applies as an implicit `WHERE` on every read of the table, attached with `ALTER TABLE ... SET ROW FILTER fn ON (col, ...)`. Attached filters are listed in `{{ catalog }}.information_schema.row_filters` (one row per table; a table carries at most one filter). The signal is native: a `row_filters` row means row-level security is enforced on SQL warehouses and UC-enabled compute for every caller, including the table owner unless the function says otherwise.

The check does not read the function body. A filter whose body is `RETURN TRUE` counts. The diagnostic prints the body from `information_schema.routines` and the columns it reads (`filter_col_usage`) so a human can judge. The check also does not decide which tables need a filter; a small dimension table of currency codes will score as unprotected. That is why the default population is all base tables (matching the upstream framework) and the variant narrows to tables that plausibly need row-level security: those with PII-tagged columns or with a column that looks like a tenancy or region key.

ABAC row-filter policies (`CREATE POLICY ... ROW FILTER ... FOR TABLES ...`) apply filters to matching tables at query time; whether they appear in `row_filters` depends on the release. A best-effort union with `information_schema.abac_policy_definitions` is included as a third variant; if the view does not exist, the primary result stands.

`information_schema` reflects `SET ROW FILTER` immediately. Rows are limited to objects the caller can see. Views, materialized views and streaming tables are excluded; a view over a filtered table inherits the filter through the underlying table.

Returns NULL (N/A) when the schema contains no base tables (primary) or no candidate tables (variant).

## SQL

### Base tables with a row filter (primary)

```sql
WITH tables_in_scope AS (
    SELECT LOWER(table_name) AS table_name
    FROM {{ catalog }}.information_schema.tables
    WHERE LOWER(table_schema) = LOWER('{{ schema }}')
      AND table_type IN ('MANAGED', 'EXTERNAL')
),
filtered AS (
    SELECT DISTINCT LOWER(table_name) AS table_name
    FROM {{ catalog }}.information_schema.row_filters
    WHERE LOWER(schema_name) = LOWER('{{ schema }}')
)
SELECT
    COUNT_IF(f.table_name IS NOT NULL)            AS tables_with_row_filter,
    COUNT(*)                                       AS total_tables,
    COUNT_IF(f.table_name IS NOT NULL)::DOUBLE
        / NULLIF(COUNT(*), 0)                      AS value
FROM tables_in_scope t
LEFT JOIN filtered f USING (table_name)
```

### Tables that plausibly need row-level security (variant)

Population: base tables with at least one column tagged `{{ pii_tag_key }}` (default `pii`), or with a column whose name matches `{{ tenancy_patterns }}` (default `(^|_)(tenant|tenant_id|org_id|organization_id|account_id|customer_id|region|country|country_code|business_unit|department|legal_entity)($|_)`).

```sql
WITH tables_in_scope AS (
    SELECT LOWER(table_name) AS table_name
    FROM {{ catalog }}.information_schema.tables
    WHERE LOWER(table_schema) = LOWER('{{ schema }}')
      AND table_type IN ('MANAGED', 'EXTERNAL')
),
candidates AS (
    SELECT DISTINCT LOWER(table_name) AS table_name
    FROM {{ catalog }}.information_schema.column_tags
    WHERE LOWER(schema_name) = LOWER('{{ schema }}')
      AND LOWER(tag_name) = LOWER('{{ pii_tag_key }}')
    UNION
    SELECT DISTINCT LOWER(table_name) AS table_name
    FROM {{ catalog }}.information_schema.columns
    WHERE LOWER(table_schema) = LOWER('{{ schema }}')
      AND REGEXP_LIKE(LOWER(column_name), '{{ tenancy_patterns }}')
),
filtered AS (
    SELECT DISTINCT LOWER(table_name) AS table_name
    FROM {{ catalog }}.information_schema.row_filters
    WHERE LOWER(schema_name) = LOWER('{{ schema }}')
)
SELECT
    COUNT_IF(f.table_name IS NOT NULL)            AS tables_with_row_filter,
    COUNT(*)                                       AS candidate_tables,
    COUNT_IF(f.table_name IS NOT NULL)::DOUBLE
        / NULLIF(COUNT(*), 0)                      AS value
FROM tables_in_scope t
JOIN candidates c USING (table_name)
LEFT JOIN filtered f USING (table_name)
```

### Including ABAC row-filter policies (variant, best-effort)

Counts a table as filtered when a row filter is attached directly or an ABAC policy of type row filter is bound to the table, its schema or its catalog. Confirm the view's columns first with `DESCRIBE {{ catalog }}.information_schema.abac_policy_definitions`; the names `policy_type` and `securable_fullname` are the expected ones. Does not evaluate the policy's `FOR TABLES` / `MATCH` clause, so it can over-count.

```sql
WITH tables_in_scope AS (
    SELECT LOWER(table_name) AS table_name
    FROM {{ catalog }}.information_schema.tables
    WHERE LOWER(table_schema) = LOWER('{{ schema }}')
      AND table_type IN ('MANAGED', 'EXTERNAL')
),
direct AS (
    SELECT DISTINCT LOWER(table_name) AS table_name
    FROM {{ catalog }}.information_schema.row_filters
    WHERE LOWER(schema_name) = LOWER('{{ schema }}')
),
abac_scope AS (
    SELECT COUNT_IF(LOWER(securable_fullname) IN (LOWER('{{ catalog }}'), LOWER('{{ catalog }}.{{ schema }}'))) > 0 AS container_policy,
           array_sort(collect_set(
               CASE WHEN LOWER(securable_fullname) LIKE LOWER('{{ catalog }}.{{ schema }}.%')
                    THEN LOWER(element_at(split(securable_fullname, '\\.'), 3)) END)) AS table_policies
    FROM {{ catalog }}.information_schema.abac_policy_definitions
    WHERE UPPER(policy_type) = 'ROW_FILTER'
)
SELECT
    COUNT_IF(d.table_name IS NOT NULL OR a.container_policy
             OR array_contains(a.table_policies, t.table_name))     AS tables_with_row_filter,
    COUNT(*)                                                        AS total_tables,
    COUNT_IF(d.table_name IS NOT NULL OR a.container_policy
             OR array_contains(a.table_policies, t.table_name))::DOUBLE
        / NULLIF(COUNT(*), 0)                                       AS value
FROM tables_in_scope t
CROSS JOIN abac_scope a
LEFT JOIN direct d USING (table_name)
```
