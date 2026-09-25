# Check: consent_coverage

Fraction of base tables holding personal data (at least one column tagged as PII) that carry a `legal_basis` table tag declaring the lawful basis for processing.

## Context

Databricks has no primitive for consent or legal basis, so the framework measures it through a table tag. The tag key is `legal_basis` (the platform default from `platforms/DATABRICKS.md`), read from `{{ catalog }}.information_schema.table_tags`. Typical values follow GDPR Article 6: `consent`, `contract`, `legal_obligation`, `vital_interest`, `public_task`, `legitimate_interest`. The tag records that a human made and documented that determination; the check verifies presence and a non-empty value, not whether the determination is correct or whether consent records exist.

The denominator is not every table. It is tables that hold personal data, identified as tables with at least one column tagged `{{ pii_tag_key }}` (default `pii`) in `information_schema.column_tags`. A reference table of country codes needs no legal basis, and demanding one from it would only encourage blanket tagging. This makes the check depend on `classification`: if nothing is tagged PII, the denominator is empty and the result is NULL, which is the right answer (you cannot know what needs a legal basis until you know where personal data is). The variant below widens the population to tables with PII-looking column names for schemas where tagging has not happened yet.

Strength: tag. `information_schema` updates immediately after `ALTER TABLE ... SET TAGS`. Rows are limited to objects the caller can see.

Returns NULL (N/A) when no table in the schema has a PII-tagged column (primary) or a PII-named column (variant).

## SQL

### Tables with PII-tagged columns that declare legal_basis (primary)

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
with_basis AS (
    SELECT DISTINCT LOWER(table_name) AS table_name
    FROM {{ catalog }}.information_schema.table_tags
    WHERE LOWER(schema_name) = LOWER('{{ schema }}')
      AND LOWER(tag_name) = 'legal_basis'
      AND tag_value IS NOT NULL AND trim(tag_value) <> ''
)
SELECT
    COUNT_IF(b.table_name IS NOT NULL)            AS tables_with_legal_basis,
    COUNT(*)                                       AS personal_data_tables,
    COUNT_IF(b.table_name IS NOT NULL)::DOUBLE
        / NULLIF(COUNT(*), 0)                      AS value
FROM personal_data_tables p
LEFT JOIN with_basis b USING (table_name)
```

### Tables with PII-named columns (variant)

For schemas with no PII tagging yet. Uses the same name regex as `anonymization_effectiveness` (`{{ pii_patterns }}`, default there) to find tables that probably hold personal data. Over-counts tables where a column merely contains a matching token.

```sql
WITH personal_data_tables AS (
    SELECT DISTINCT LOWER(c.table_name) AS table_name
    FROM {{ catalog }}.information_schema.columns c
    JOIN {{ catalog }}.information_schema.tables t
      ON c.table_schema = t.table_schema AND c.table_name = t.table_name
    WHERE LOWER(c.table_schema) = LOWER('{{ schema }}')
      AND t.table_type IN ('MANAGED', 'EXTERNAL')
      AND REGEXP_LIKE(LOWER(c.column_name), '{{ pii_patterns }}')
),
with_basis AS (
    SELECT DISTINCT LOWER(table_name) AS table_name
    FROM {{ catalog }}.information_schema.table_tags
    WHERE LOWER(schema_name) = LOWER('{{ schema }}')
      AND LOWER(tag_name) = 'legal_basis'
      AND tag_value IS NOT NULL AND trim(tag_value) <> ''
)
SELECT
    COUNT_IF(b.table_name IS NOT NULL)            AS tables_with_legal_basis,
    COUNT(*)                                       AS personal_data_tables,
    COUNT_IF(b.table_name IS NOT NULL)::DOUBLE
        / NULLIF(COUNT(*), 0)                      AS value
FROM personal_data_tables p
LEFT JOIN with_basis b USING (table_name)
```
