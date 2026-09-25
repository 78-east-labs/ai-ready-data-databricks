# Fix: row_access_policy

Create row filter functions and attach them to the tables that need row-level security.

## Context

A row filter is a SQL UDF returning BOOLEAN. Unity Catalog evaluates it per row on every read and drops rows where it returns FALSE or NULL, for every caller and every query shape (SELECT, MERGE source, CTAS, streaming reads). Attach with `ALTER TABLE ... SET ROW FILTER fn ON (col, ...)`; the columns in `ON (...)` are passed as the function's arguments in order. One filter per table; a second `SET ROW FILTER` replaces the first, which is why the guard matters.

Design rules that keep filters correct and fast:

- Gate on `is_account_group_member('group')`; it resolves nested account groups. Avoid `current_user() = 'a@b.com'` lists.
- Keep the function deterministic and cheap. It runs per row. Reading a small entitlement mapping table (`user_email, tenant_id`) inside the function is supported and is the standard pattern for per-tenant access; keep that table small and clustered on the lookup key.
- Return TRUE for a privileged group first so administrators and pipelines are not locked out, then apply the restriction. Service principals that run pipelines must be in the privileged group or they will silently read an empty table and write empty outputs.
- The filter applies to the owner too. Test as a member of the restricted group and as a non-member before rolling out.
- Not every table needs a filter. Reference and lookup tables with no PII or tenancy key are legitimately unfiltered; use the check's candidate variant to score only the tables that need one, rather than filtering everything to reach 1.0.

Permissions: creating the function needs `CREATE FUNCTION` on the schema; attaching needs ownership of the table (or `MANAGE`) plus `EXECUTE` on the function for the table owner. Callers do not need `EXECUTE`.

ABAC row-filter policies (`CREATE POLICY ... ROW FILTER`) let one statement cover every table in a schema that has a given tag or column. The syntax is newer and still changing; a best-effort form is given last. Verify it against the current reference.

## Fix: Create filter functions

Guard: skip functions that exist.

```sql
SELECT routine_name
FROM {{ catalog }}.information_schema.routines
WHERE LOWER(routine_schema) = LOWER('{{ governance_schema }}')
  AND LOWER(routine_name) IN ('rf_group_only', 'rf_region', 'rf_tenant')
```

`{{ governance_schema }}` defaults to `governance`, `{{ privileged_group }}` to `data_admins`.

```sql
-- Whole-table gate: only members of one group see any rows.
CREATE FUNCTION {{ catalog }}.{{ governance_schema }}.rf_group_only()
RETURNS BOOLEAN
RETURN is_account_group_member('{{ allowed_group }}');

-- Region-based: a group per region, admins see everything.
CREATE FUNCTION {{ catalog }}.{{ governance_schema }}.rf_region(region STRING)
RETURNS BOOLEAN
RETURN is_account_group_member('{{ privileged_group }}')
    OR (region = 'EU'   AND is_account_group_member('analysts_eu'))
    OR (region = 'US'   AND is_account_group_member('analysts_us'))
    OR (region = 'APAC' AND is_account_group_member('analysts_apac'));

-- Tenant-based via an entitlement table (user_email STRING, tenant_id STRING).
CREATE FUNCTION {{ catalog }}.{{ governance_schema }}.rf_tenant(tenant_id STRING)
RETURNS BOOLEAN
RETURN is_account_group_member('{{ privileged_group }}')
    OR EXISTS (
        SELECT 1
        FROM {{ catalog }}.{{ governance_schema }}.tenant_entitlements e
        WHERE e.user_email = current_user()
          AND e.tenant_id  = rf_tenant.tenant_id
    );
```

Create the entitlement table if it does not exist (idempotent):

```sql
CREATE TABLE IF NOT EXISTS {{ catalog }}.{{ governance_schema }}.tenant_entitlements (
    user_email STRING NOT NULL,
    tenant_id  STRING NOT NULL,
    granted_by STRING,
    granted_at TIMESTAMP
) CLUSTER BY (user_email);
```

Grant execution to the table owners who will attach the filter:

```sql
GRANT EXECUTE ON FUNCTION {{ catalog }}.{{ governance_schema }}.rf_region TO `{{ table_owner_group }}`;
```

Use `CREATE OR REPLACE FUNCTION` only when the user confirms the body is unchanged; a replaced filter changes what every attached table returns immediately.

## Fix: Attach a filter to one table

Guard (a returned row means the table already has a filter; combine logic into that function rather than replacing it blindly):

```sql
SELECT filter_name, filter_col_usage
FROM {{ catalog }}.information_schema.row_filters
WHERE LOWER(schema_name) = LOWER('{{ schema }}')
  AND LOWER(table_name)  = LOWER('{{ asset }}')
```

```sql
ALTER TABLE {{ catalog }}.{{ schema }}.{{ asset }}
SET ROW FILTER {{ catalog }}.{{ governance_schema }}.rf_region ON ({{ column }});
```

For a no-argument function use `ON ()`. The column types must match the function's parameter types. To remove: `ALTER TABLE ... DROP ROW FILTER`.

## Fix: Generate SET ROW FILTER statements for unfiltered candidate tables

Emits one statement per unfiltered base table that has a column named `{{ column }}` (the filter key, for example `region` or `tenant_id`), bound to `{{ filter_function }}`. Tables without that column are listed separately so they get a hand-picked function.

```sql
WITH tables_in_scope AS (
    SELECT LOWER(table_name) AS table_name, table_name AS table_name_cased
    FROM {{ catalog }}.information_schema.tables
    WHERE LOWER(table_schema) = LOWER('{{ schema }}')
      AND table_type IN ('MANAGED', 'EXTERNAL')
),
filtered AS (
    SELECT DISTINCT LOWER(table_name) AS table_name
    FROM {{ catalog }}.information_schema.row_filters
    WHERE LOWER(schema_name) = LOWER('{{ schema }}')
),
key_column AS (
    SELECT LOWER(table_name) AS table_name, column_name, data_type
    FROM {{ catalog }}.information_schema.columns
    WHERE LOWER(table_schema) = LOWER('{{ schema }}')
      AND LOWER(column_name) = LOWER('{{ column }}')
)
SELECT
    t.table_name_cased AS table_name,
    k.data_type        AS key_type,
    CASE WHEN k.column_name IS NOT NULL THEN concat(
        'ALTER TABLE {{ catalog }}.{{ schema }}.`', t.table_name_cased,
        '` SET ROW FILTER {{ catalog }}.{{ governance_schema }}.{{ filter_function }} ON (`',
        k.column_name, '`);')
    END AS stmt
FROM tables_in_scope t
LEFT JOIN filtered   f USING (table_name)
LEFT JOIN key_column k USING (table_name)
WHERE f.table_name IS NULL
ORDER BY stmt IS NULL, t.table_name
```

Show the statements to the user first. Rows with `stmt` NULL are unfiltered tables without the key column; decide per table whether they need a different filter or none.

## Fix: Verify enforcement

From a SQL warehouse, as a member of a restricted group and then as a non-member:

```sql
SELECT is_account_group_member('{{ privileged_group }}') AS am_admin,
       COUNT(*) AS visible_rows,
       collect_set({{ column }}) AS visible_keys
FROM {{ catalog }}.{{ schema }}.{{ asset }};
```

`visible_keys` should contain only the caller's entitled values. Then run the pipelines that read the table as their service principal and confirm their outputs are not empty.

## Fix: Row filter by tag with an ABAC policy (best-effort)

One policy on the schema attaches `rf_region` to every table that has a `region` column, for everyone except the privileged group. Requires ABAC enabled for the metastore and `MANAGE` on the schema. Confirm the clause order against the current `CREATE POLICY` reference.

Guard (view is new; if it does not exist, check the schema's Policies tab in Catalog Explorer):

```sql
SELECT policy_name
FROM {{ catalog }}.information_schema.abac_policy_definitions
WHERE LOWER(policy_name) = LOWER('rf_region_{{ schema }}')
```

```sql
CREATE POLICY rf_region_{{ schema }}
ON SCHEMA {{ catalog }}.{{ schema }}
ROW FILTER {{ catalog }}.{{ governance_schema }}.rf_region
TO `account users`
EXCEPT `{{ privileged_group }}`
FOR TABLES
MATCH COLUMNS hasTag('{{ region_tag_key }}') AS region_col
USING COLUMNS (region_col);
```

`{{ region_tag_key }}` is a column tag (for example `row_filter_key = 'region'`) applied to the filter-key column on each table; tagging the column is what opts the table in. ABAC-filtered tables may not appear in `information_schema.row_filters`, so verify with a query as a restricted user rather than expecting the primary check to move.

## Organizational guidance

Row-level security should follow a data model, not a scoreboard. Decide the tenancy or region key per domain, put it on every fact table as a first-class column, tag it, and bind one ABAC policy (or one generated `SET ROW FILTER` batch) to that tag so new tables inherit the filter. Keep filter functions and entitlement tables in a governance schema owned by the security team and deployed by Terraform; keep pipeline service principals in the privileged group and document it, so a filter never turns a production job into a silent empty write. Score the check's candidate variant in profiles so reference tables do not drag the number down and nobody is tempted to filter them for the sake of it.
