# Check: column_masking

Fraction of columns tagged as PII on base tables in the schema that carry a Unity Catalog column mask.

## Context

Starts from what the organisation has declared to be PII: rows in `{{ catalog }}.information_schema.column_tags` whose `tag_name` is `{{ pii_tag_key }}` (default `pii`; the profile can point it at `sensitivity` or whatever the governed tag policy uses). A tagged column counts as protected when `{{ catalog }}.information_schema.column_masks` has a row for it, meaning a mask function was attached with `ALTER TABLE ... ALTER COLUMN ... SET MASK`. Both sides are native Unity Catalog metadata and update immediately.

The signal is native. A mask is enforced at query time on SQL warehouses and UC-enabled compute regardless of who wrote the query, so a `column_masks` row is real protection. The check does not read the function body, so a mask that returns the value to everyone still counts; the diagnostic prints the body.

Compared with `anonymization_effectiveness`, which guesses PII from column names, this check has a narrower and higher-confidence population: only columns someone tagged. That makes it the right score for "is our declared PII masked" and the wrong score for "did we find all the PII". If nothing is tagged the check returns NULL, and the honest next step is `classification` (or enabling Databricks Data Classification, whose output is column tags) before this one.

Data Classification writes its results as column tags. The exact tag keys it applies are workspace-configurable and have changed across previews; run the diagnostic's tag-key inventory and set `{{ pii_tag_key }}` accordingly rather than assuming `pii`. The variant below accepts any tag key that matches a regex, which is convenient while the vocabulary is still settling.

ABAC column-mask policies (`CREATE POLICY ... COLUMN MASK ... MATCH COLUMNS hasTag('pii')`) protect tagged columns without a per-column `SET MASK`. Whether they appear in `column_masks` depends on the release; the `anonymization_effectiveness` check has a best-effort variant reading `information_schema.abac_policy_definitions` that can be adapted here with the same CTE.

Returns NULL (N/A) when no column in the schema carries the PII tag.

## SQL

### Columns tagged with the PII key that have a mask (primary)

```sql
WITH pii_columns AS (
    SELECT DISTINCT LOWER(ct.table_name) AS table_name, LOWER(ct.column_name) AS column_name
    FROM {{ catalog }}.information_schema.column_tags ct
    JOIN {{ catalog }}.information_schema.tables t
      ON  LOWER(t.table_schema) = LOWER(ct.schema_name)
      AND LOWER(t.table_name)   = LOWER(ct.table_name)
    WHERE LOWER(ct.schema_name) = LOWER('{{ schema }}')
      AND t.table_type IN ('MANAGED', 'EXTERNAL')
      AND LOWER(ct.tag_name) = LOWER('{{ pii_tag_key }}')
),
masked AS (
    SELECT DISTINCT LOWER(table_name) AS table_name, LOWER(column_name) AS column_name
    FROM {{ catalog }}.information_schema.column_masks
    WHERE LOWER(schema_name) = LOWER('{{ schema }}')
)
SELECT
    COUNT_IF(m.column_name IS NOT NULL)           AS masked_pii_columns,
    COUNT(*)                                       AS total_pii_columns,
    COUNT_IF(m.column_name IS NOT NULL)::DOUBLE
        / NULLIF(COUNT(*), 0)                      AS value
FROM pii_columns p
LEFT JOIN masked m USING (table_name, column_name)
```

### Any PII-like tag key (variant)

Accepts any column tag whose key matches `{{ pii_tag_pattern }}` (default `pii|sensitiv|personal|confidential|classification`), which covers hand-applied keys and Data Classification output at once. Wider population, same numerator logic.

```sql
WITH pii_columns AS (
    SELECT DISTINCT LOWER(ct.table_name) AS table_name, LOWER(ct.column_name) AS column_name
    FROM {{ catalog }}.information_schema.column_tags ct
    JOIN {{ catalog }}.information_schema.tables t
      ON  LOWER(t.table_schema) = LOWER(ct.schema_name)
      AND LOWER(t.table_name)   = LOWER(ct.table_name)
    WHERE LOWER(ct.schema_name) = LOWER('{{ schema }}')
      AND t.table_type IN ('MANAGED', 'EXTERNAL')
      AND REGEXP_LIKE(LOWER(ct.tag_name), '{{ pii_tag_pattern }}')
      AND NOT LOWER(ct.tag_value) IN ('none', 'no', 'false', 'public', 'not_pii')
),
masked AS (
    SELECT DISTINCT LOWER(table_name) AS table_name, LOWER(column_name) AS column_name
    FROM {{ catalog }}.information_schema.column_masks
    WHERE LOWER(schema_name) = LOWER('{{ schema }}')
)
SELECT
    COUNT_IF(m.column_name IS NOT NULL)           AS masked_pii_columns,
    COUNT(*)                                       AS total_pii_columns,
    COUNT_IF(m.column_name IS NOT NULL)::DOUBLE
        / NULLIF(COUNT(*), 0)                      AS value
FROM pii_columns p
LEFT JOIN masked m USING (table_name, column_name)
```

The `NOT IN` list drops columns explicitly tagged as not sensitive (for example `sensitivity = 'public'`), which would otherwise be demanded a mask.
