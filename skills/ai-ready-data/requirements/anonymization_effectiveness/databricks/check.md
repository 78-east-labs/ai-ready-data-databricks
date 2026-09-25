# Check: anonymization_effectiveness

Fraction of PII-candidate columns (by column-name pattern) on base tables in the schema that are protected by a Unity Catalog column mask.

## Context

PII candidates are found by a name regex over `{{ catalog }}.information_schema.columns`. Protection is a row in `{{ catalog }}.information_schema.column_masks`, which lists every column that has a mask function attached with `ALTER TABLE ... ALTER COLUMN ... SET MASK`. The mask is native and enforced at query time on SQL warehouses and UC-enabled clusters, so a `column_masks` row is real protection, not a declaration. The candidate list is a proxy: it catches obviously named columns and misses PII in generic ones (`value`, `payload`, `notes`).

This check is deliberately broader than `column_masking`. `column_masking` starts from columns a human (or Databricks Data Classification) tagged as PII; this check starts from what the column is called, so it also surfaces PII nobody has tagged yet. A column that carries a `pii` tag but no mask counts as unprotected here: the tag records awareness, the mask does the work.

Limits of the signal. The check sees that a mask exists, not what it does. A mask function whose body returns the input for everyone still counts. The diagnostic prints the function body from `information_schema.routines` so a human can judge. Masks defined through ABAC policies (`CREATE POLICY ... COLUMN MASK`) are a newer mechanism; they are applied to matching columns at query time and may or may not be materialised into `column_masks` depending on release. The variant below unions `information_schema.abac_policy_definitions` on a best-effort basis; if that view does not exist in your metastore the variant fails and the primary result stands.

`information_schema` reflects `SET MASK` immediately. Rows are limited to objects the caller can see.

Extra placeholder `{{ pii_patterns }}`, a regex applied to the lowercased column name. Default:

```
(^|_)(email|e_mail|phone|mobile|ssn|passport|dob|birth|birthdate|address|street|postcode|zip|first_name|last_name|full_name|surname|given_name|ip_address|credit_card|card_number|iban|national_id|tax_id)($|_)
```

Returns NULL (N/A) when no column in the schema matches the pattern.

## SQL

### Name-pattern PII columns with a column mask (primary)

```sql
WITH pii_columns AS (
    SELECT LOWER(c.table_name) AS table_name, LOWER(c.column_name) AS column_name
    FROM {{ catalog }}.information_schema.columns c
    JOIN {{ catalog }}.information_schema.tables t
      ON c.table_schema = t.table_schema AND c.table_name = t.table_name
    WHERE LOWER(c.table_schema) = LOWER('{{ schema }}')
      AND t.table_type IN ('MANAGED', 'EXTERNAL')
      AND REGEXP_LIKE(LOWER(c.column_name), '{{ pii_patterns }}')
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

### Including ABAC column-mask policies (variant, best-effort)

Adds columns covered by an ABAC policy of type column mask that targets this schema. The `abac_policy_definitions` view is new; confirm its columns first with `DESCRIBE {{ catalog }}.information_schema.abac_policy_definitions`. ABAC policies match columns by tag (`hasTag('pii')`), so this variant treats a column as covered when a column-mask policy is attached at the catalog or schema level and the column carries the tag key named in `{{ pii_tag_key }}` (default `pii`). It does not evaluate the policy's `MATCH COLUMNS` expression, so it can over-count.

```sql
WITH pii_columns AS (
    SELECT LOWER(c.table_name) AS table_name, LOWER(c.column_name) AS column_name
    FROM {{ catalog }}.information_schema.columns c
    JOIN {{ catalog }}.information_schema.tables t
      ON c.table_schema = t.table_schema AND c.table_name = t.table_name
    WHERE LOWER(c.table_schema) = LOWER('{{ schema }}')
      AND t.table_type IN ('MANAGED', 'EXTERNAL')
      AND REGEXP_LIKE(LOWER(c.column_name), '{{ pii_patterns }}')
),
direct_masks AS (
    SELECT DISTINCT LOWER(table_name) AS table_name, LOWER(column_name) AS column_name
    FROM {{ catalog }}.information_schema.column_masks
    WHERE LOWER(schema_name) = LOWER('{{ schema }}')
),
abac_mask_policy_exists AS (
    SELECT COUNT(*) > 0 AS present
    FROM {{ catalog }}.information_schema.abac_policy_definitions
    WHERE UPPER(policy_type) = 'COLUMN_MASK'
      AND (
            LOWER(securable_fullname) = LOWER('{{ catalog }}')
         OR LOWER(securable_fullname) = LOWER('{{ catalog }}.{{ schema }}')
         OR LOWER(securable_fullname) LIKE LOWER('{{ catalog }}.{{ schema }}.%')
      )
),
abac_tagged AS (
    SELECT DISTINCT LOWER(ct.table_name) AS table_name, LOWER(ct.column_name) AS column_name
    FROM {{ catalog }}.information_schema.column_tags ct
    CROSS JOIN abac_mask_policy_exists p
    WHERE LOWER(ct.schema_name) = LOWER('{{ schema }}')
      AND LOWER(ct.tag_name) = LOWER('{{ pii_tag_key }}')
      AND p.present
),
covered AS (
    SELECT table_name, column_name FROM direct_masks
    UNION
    SELECT table_name, column_name FROM abac_tagged
)
SELECT
    COUNT_IF(m.column_name IS NOT NULL)           AS masked_pii_columns,
    COUNT(*)                                       AS total_pii_columns,
    COUNT_IF(m.column_name IS NOT NULL)::DOUBLE
        / NULLIF(COUNT(*), 0)                      AS value
FROM pii_columns p
LEFT JOIN covered m USING (table_name, column_name)
```
