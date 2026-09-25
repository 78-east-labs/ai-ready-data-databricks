# Diagnostic: consent_coverage

Per-table view of personal-data tables with their PII columns, the `legal_basis` tag value, related governance tags, and a status.

## Context

Reuses the check's population (tables with a column tagged `{{ pii_tag_key }}`). For each table:

- `pii_columns`: the tagged columns with their tag values, so the reviewer sees what kind of personal data is present (an `email` needs a different conversation than a `health_condition`).
- `legal_basis`: the tag value, or NULL.
- `related_tags`: values of `ai_allowed_purposes`, `retention_days`, `data_controller`, `dpia_ref` and `consent_ref` if present. These usually get decided in the same review, so their absence suggests the table has never been through one.
- `status`: `HAS_LEGAL_BASIS`, `UNRECOGNISED_VALUE` (a value outside the GDPR Article 6 list plus `not_applicable`; not wrong, but worth a look) or `NO_LEGAL_BASIS`.

Also lists, as a second query, tables that have a `legal_basis` tag but no PII-tagged column, which can mean the basis was applied by blanket script or the PII tagging is incomplete.

Sorted worst-first.

## SQL

### Personal-data tables and their legal basis

```sql
WITH personal_data_tables AS (
    SELECT LOWER(ct.table_name) AS table_name,
           array_sort(collect_list(concat(ct.column_name, '=', ct.tag_value))) AS pii_columns
    FROM {{ catalog }}.information_schema.column_tags ct
    JOIN {{ catalog }}.information_schema.tables t
      ON  LOWER(t.table_schema) = LOWER(ct.schema_name)
      AND LOWER(t.table_name)   = LOWER(ct.table_name)
    WHERE LOWER(ct.schema_name) = LOWER('{{ schema }}')
      AND t.table_type IN ('MANAGED', 'EXTERNAL')
      AND LOWER(ct.tag_name) = LOWER('{{ pii_tag_key }}')
    GROUP BY LOWER(ct.table_name)
),
table_tags AS (
    SELECT LOWER(table_name) AS table_name,
           MAX(CASE WHEN LOWER(tag_name) = 'legal_basis' THEN tag_value END) AS legal_basis,
           array_sort(collect_list(
               CASE WHEN LOWER(tag_name) IN ('ai_allowed_purposes', 'retention_days',
                                             'data_controller', 'dpia_ref', 'consent_ref')
                    THEN concat(tag_name, '=', tag_value) END)) AS related_tags
    FROM {{ catalog }}.information_schema.table_tags
    WHERE LOWER(schema_name) = LOWER('{{ schema }}')
    GROUP BY LOWER(table_name)
)
SELECT
    p.table_name,
    t.table_owner,
    p.pii_columns,
    tt.legal_basis,
    tt.related_tags,
    CASE
        WHEN tt.legal_basis IS NULL OR trim(tt.legal_basis) = '' THEN 'NO_LEGAL_BASIS'
        WHEN LOWER(tt.legal_basis) IN ('consent', 'contract', 'legal_obligation',
                                       'vital_interest', 'public_task', 'legitimate_interest',
                                       'not_applicable')
                                                                  THEN 'HAS_LEGAL_BASIS'
        ELSE 'UNRECOGNISED_VALUE'
    END AS status
FROM personal_data_tables p
JOIN {{ catalog }}.information_schema.tables t
  ON LOWER(t.table_schema) = LOWER('{{ schema }}') AND LOWER(t.table_name) = p.table_name
LEFT JOIN table_tags tt USING (table_name)
ORDER BY
    CASE status WHEN 'NO_LEGAL_BASIS' THEN 0 WHEN 'UNRECOGNISED_VALUE' THEN 1 ELSE 2 END,
    p.table_name
```

### Tables tagged legal_basis without any PII-tagged column

```sql
WITH pii_tables AS (
    SELECT DISTINCT LOWER(table_name) AS table_name
    FROM {{ catalog }}.information_schema.column_tags
    WHERE LOWER(schema_name) = LOWER('{{ schema }}')
      AND LOWER(tag_name) = LOWER('{{ pii_tag_key }}')
)
SELECT LOWER(tt.table_name) AS table_name, tt.tag_value AS legal_basis
FROM {{ catalog }}.information_schema.table_tags tt
LEFT JOIN pii_tables p ON p.table_name = LOWER(tt.table_name)
WHERE LOWER(tt.schema_name) = LOWER('{{ schema }}')
  AND LOWER(tt.tag_name) = 'legal_basis'
  AND p.table_name IS NULL
ORDER BY table_name
```

A long list here with identical values is the signature of a blanket tagging script; each of those tables should either get its PII columns tagged (so the basis attaches to something) or have the tag reviewed.
