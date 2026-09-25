# Check: purpose_limitation

Fraction of base tables in the schema that declare the AI processing purposes they may be used for, through the `ai_allowed_purposes` table tag.

## Context

Purpose limitation asks that data collected for one reason not be silently reused for another, which for AI means a table sourced for billing should not become training data without a decision. Databricks has no primitive for this; the framework measures whether the decision was recorded, through the table tag `ai_allowed_purposes` in `{{ catalog }}.information_schema.table_tags`. The value is a comma-separated list from a small vocabulary, for example `rag,analytics`, `training,evaluation`, or `none` for tables that must not feed AI workloads at all. The check verifies presence and a non-empty value; it does not check that AI pipelines honour it (that is enforced, if at all, by grants or a row filter keyed on the tag, see the fix).

Strength: tag. `information_schema` reflects tags immediately. Rows are limited to what the caller can see.

Two populations are offered. The primary is every base table, matching the upstream framework: any table an AI workload can reach should say what it may be used for, and `none` is a valid declaration. The variant narrows to tables that hold personal data (a column tagged `{{ pii_tag_key }}`, default `pii`), where purpose limitation is a legal obligation rather than good hygiene; use it when the schema is large and the team wants to sequence the work.

Returns NULL (N/A) when the schema contains no base tables (primary) or no tables with PII-tagged columns (variant).

## SQL

### All base tables with ai_allowed_purposes (primary)

```sql
WITH tables_in_scope AS (
    SELECT LOWER(table_name) AS table_name
    FROM {{ catalog }}.information_schema.tables
    WHERE LOWER(table_schema) = LOWER('{{ schema }}')
      AND table_type IN ('MANAGED', 'EXTERNAL')
),
declared AS (
    SELECT DISTINCT LOWER(table_name) AS table_name
    FROM {{ catalog }}.information_schema.table_tags
    WHERE LOWER(schema_name) = LOWER('{{ schema }}')
      AND LOWER(tag_name) = 'ai_allowed_purposes'
      AND tag_value IS NOT NULL AND trim(tag_value) <> ''
)
SELECT
    COUNT_IF(d.table_name IS NOT NULL)            AS tables_with_purpose,
    COUNT(*)                                       AS total_tables,
    COUNT_IF(d.table_name IS NOT NULL)::DOUBLE
        / NULLIF(COUNT(*), 0)                      AS value
FROM tables_in_scope t
LEFT JOIN declared d USING (table_name)
```

### Personal-data tables only (variant)

```sql
WITH personal_data_tables AS (
    SELECT DISTINCT LOWER(ct.table_name) AS table_name
    FROM {{ catalog }}.information_schema.column_tags ct
    JOIN {{ catalog }}.information_schema.tables t
      ON  LOWER(t.table_schema) = LOWER(ct.schema_name)
      AND LOWER(t.table_name)   = LOWER(ct.table_name)
    WHERE LOWER(ct.schema_name) = LOWER('{{ schema }}')
      AND t.table_type IN ('MANAGED', 'EXTERNAL')
      AND LOWER(ct.tag_name) = LOWER('{{ pii_tag_key }}')
),
declared AS (
    SELECT DISTINCT LOWER(table_name) AS table_name
    FROM {{ catalog }}.information_schema.table_tags
    WHERE LOWER(schema_name) = LOWER('{{ schema }}')
      AND LOWER(tag_name) = 'ai_allowed_purposes'
      AND tag_value IS NOT NULL AND trim(tag_value) <> ''
)
SELECT
    COUNT_IF(d.table_name IS NOT NULL)            AS tables_with_purpose,
    COUNT(*)                                       AS personal_data_tables,
    COUNT_IF(d.table_name IS NOT NULL)::DOUBLE
        / NULLIF(COUNT(*), 0)                      AS value
FROM personal_data_tables p
LEFT JOIN declared d USING (table_name)
```

### Value vocabulary check (variant, stricter)

Counts a declaration only if every token in the list is in `{{ purpose_vocabulary }}` (default `training,fine_tuning,evaluation,rag,analytics,feature_engineering,agent_tools,none`). Catches free-text values like `tbd` or `ask legal` that satisfy the primary but decide nothing.

```sql
WITH tables_in_scope AS (
    SELECT LOWER(table_name) AS table_name
    FROM {{ catalog }}.information_schema.tables
    WHERE LOWER(table_schema) = LOWER('{{ schema }}')
      AND table_type IN ('MANAGED', 'EXTERNAL')
),
declared AS (
    SELECT LOWER(table_name) AS table_name,
           transform(split(LOWER(tag_value), '\\s*,\\s*'), x -> trim(x)) AS purposes
    FROM {{ catalog }}.information_schema.table_tags
    WHERE LOWER(schema_name) = LOWER('{{ schema }}')
      AND LOWER(tag_name) = 'ai_allowed_purposes'
      AND tag_value IS NOT NULL AND trim(tag_value) <> ''
),
valid AS (
    SELECT DISTINCT table_name
    FROM declared
    WHERE forall(purposes, p -> array_contains(split('{{ purpose_vocabulary }}', ','), p))
)
SELECT
    COUNT_IF(v.table_name IS NOT NULL)            AS tables_with_valid_purpose,
    COUNT(*)                                       AS total_tables,
    COUNT_IF(v.table_name IS NOT NULL)::DOUBLE
        / NULLIF(COUNT(*), 0)                      AS value
FROM tables_in_scope t
LEFT JOIN valid v USING (table_name)
```
