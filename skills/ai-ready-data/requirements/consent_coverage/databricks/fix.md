# Fix: consent_coverage

Record the lawful basis for processing on each table that holds personal data, using the `legal_basis` table tag.

## Context

The `legal_basis` tag is a statement of a legal determination: that someone accountable (privacy counsel, the DPO, the data owner under a documented policy) decided which Article 6 ground, or which local equivalent, covers processing of the personal data in that table, including its use in AI training or retrieval. Applying the tag without that decision is worse than leaving it empty, because it turns an honest gap into a false record that auditors and downstream teams will rely on. Do not run a bulk statement that stamps `legal_basis = 'legitimate_interest'` on every table to move the score.

Concretely, the tag is applied per table after a review that answers: what personal data is in it (the diagnostic's `pii_columns`), what it is processed for, and on which basis. Where the answer is that the table should not hold personal data at all, the fix is to remove or mask the columns, not to tag a basis.

Allowed values (keep them short and machine-comparable): `consent`, `contract`, `legal_obligation`, `vital_interest`, `public_task`, `legitimate_interest`. Add `not_applicable` only for tables whose PII tags turn out to be false positives, and fix the PII tag at the same time. Companion tags that usually come out of the same review: `consent_ref` (where the consent records or the LIA live), `data_controller`, `dpia_ref`, `ai_allowed_purposes` (see `purpose_limitation`), `retention_days` (see `retention_policy`).

Tags are metadata only. `ALTER TABLE ... SET TAGS` needs `APPLY TAG` on the table or ownership, is idempotent for the same value, and overwrites a different value. Governed tag policies may restrict `legal_basis` to an allowed list; check the policy before choosing values.

## Fix: Tag one table after review

Guard (a returned row with the same value means nothing to do; a different value means someone decided before you, so confirm before overwriting):

```sql
SELECT tag_value
FROM {{ catalog }}.information_schema.table_tags
WHERE LOWER(schema_name) = LOWER('{{ schema }}')
  AND LOWER(table_name)  = LOWER('{{ asset }}')
  AND LOWER(tag_name)    = 'legal_basis'
```

```sql
ALTER TABLE {{ catalog }}.{{ schema }}.{{ asset }}
SET TAGS ('legal_basis' = '{{ legal_basis }}',
          'consent_ref' = '{{ consent_ref }}');
```

`{{ legal_basis }}` is one of the allowed values above. `{{ consent_ref }}` is the pointer to the consent records, legitimate-interest assessment or contract clause; omit the second pair if there is none, do not fill it with a placeholder.

## Fix: Generate statements from a review register

The only safe bulk path. `{{ review_table }}` is a table the privacy team maintains with one row per decided table: `table_name STRING`, `legal_basis STRING`, `consent_ref STRING`, `decided_on DATE`, `decided_by STRING`. The query emits statements only for personal-data tables in scope that are missing the tag or whose tag disagrees with the register.

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
current_tag AS (
    SELECT LOWER(table_name) AS table_name, MAX(tag_value) AS legal_basis
    FROM {{ catalog }}.information_schema.table_tags
    WHERE LOWER(schema_name) = LOWER('{{ schema }}')
      AND LOWER(tag_name) = 'legal_basis'
    GROUP BY LOWER(table_name)
)
SELECT concat(
    'ALTER TABLE {{ catalog }}.{{ schema }}.`', p.table_name,
    '` SET TAGS (''legal_basis'' = ''', r.legal_basis, '''',
    CASE WHEN r.consent_ref IS NOT NULL
         THEN concat(', ''consent_ref'' = ''', r.consent_ref, '''') ELSE '' END,
    ');'
) AS stmt,
r.decided_by, r.decided_on
FROM personal_data_tables p
JOIN {{ review_table }} r ON LOWER(r.table_name) = p.table_name
LEFT JOIN current_tag c USING (table_name)
WHERE r.legal_basis IN ('consent', 'contract', 'legal_obligation',
                        'vital_interest', 'public_task', 'legitimate_interest', 'not_applicable')
  AND (c.legal_basis IS NULL OR LOWER(c.legal_basis) <> LOWER(r.legal_basis))
ORDER BY p.table_name
```

Show the statements and the `decided_by` column to the user before running them. Personal-data tables absent from the register are the remaining worklist; they need a review, not a default.

## Fix: Produce the review worklist for the privacy team

Not a tag operation; it exports what the reviewers need. Run the diagnostic and hand over the `NO_LEGAL_BASIS` rows with `pii_columns`, `table_owner` and the table comment. If the team wants it as a table:

```sql
CREATE TABLE IF NOT EXISTS {{ catalog }}.{{ governance_schema }}.legal_basis_review (
    table_catalog STRING, table_schema STRING, table_name STRING,
    table_owner STRING, pii_columns ARRAY<STRING>,
    legal_basis STRING, consent_ref STRING,
    decided_on DATE, decided_by STRING
);
```

Then `INSERT` the diagnostic's `NO_LEGAL_BASIS` rows with the decision columns left NULL. `CREATE TABLE IF NOT EXISTS` is idempotent.

## Organizational guidance

Legal basis is decided when a dataset is brought into the platform, not discovered afterwards. Make `legal_basis` a governed tag with the six Article 6 values as its allowed list, require it (with `pii` column tags and `retention_days`) in the intake checklist for any table sourced from customer, employee or user systems, and store the determination in the same register the tag statements are generated from. Pipeline templates (Lakeflow, dbt `meta`, Terraform) should carry the tag from the intake record so it lands with the table. Re-review when the purpose changes: a table cleared under `contract` for billing is not thereby cleared for model training, which is what `purpose_limitation` tracks.
