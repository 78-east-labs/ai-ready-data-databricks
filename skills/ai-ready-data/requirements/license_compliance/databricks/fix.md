# Fix: license_compliance

Find the terms each external dataset came with, decide whether they permit the intended AI use, and record the result as a `license` tag on the table (or on the shared schema or catalog when the table cannot be altered).

## Context

The `license` tag records that someone read the terms and classified them. That is a legal reading, not a lookup: a Marketplace listing's terms, a vendor contract's permitted-use clause, or an open-data license's attribution and share-alike conditions each decide whether the data may be used for model training, retrieval, or redistribution in model outputs. Applying `license = 'unknown'` or a guessed identifier to make the score move is worse than leaving the tag empty, because downstream teams will treat a tagged table as cleared. Do not bulk-tag from a default.

Where the terms live, by population:

- **Delta Sharing and Marketplace**: the provider and share name identify the listing. `databricks providers list-shares <provider>` shows shares; the Marketplace listing page carries the terms. Record the listing id or contract in `license_ref`.
- **Federated tables**: usually the organisation's own systems. Confirm, then either tag `license = 'proprietary_internal'` or, if it is a vendor system (a partner's Snowflake share, a SaaS replica), treat it as vendor data.
- **Tagged managed tables**: the ingestion pipeline or its author knows the source URL; the license is on that page or in the download's LICENSE file.

Tag mechanics. `ALTER TABLE ... SET TAGS` needs `APPLY TAG` or ownership and is idempotent for the same value. Whether a recipient can set tags on individual tables inside a Delta Sharing catalog depends on the release; if `ALTER TABLE` is rejected on a shared table, set the tag on the schema or catalog instead (the recipient owns those objects) and use the check's inherited variant. Use short, comparable values: SPDX identifiers for public licenses (`CC-BY-4.0`, `ODbL-1.0`, `CDLA-Permissive-2.0`, `MIT`), `vendor_contract` plus a `license_ref` for private terms, `proprietary_internal` for the organisation's own data that was merely external in location. Add `license_ai_training = 'allowed' | 'prohibited' | 'conditional'` when the terms speak to it, because that is the question the AI team will actually ask.

## Fix: Tag one table

Guard (skip if the same value is set; a different value is an earlier reading, confirm before overwriting):

```sql
SELECT tag_name, tag_value
FROM {{ catalog }}.information_schema.table_tags
WHERE LOWER(schema_name) = LOWER('{{ schema }}')
  AND LOWER(table_name)  = LOWER('{{ asset }}')
  AND LOWER(tag_name) IN ('license', 'license_ref', 'license_expires', 'license_ai_training')
```

```sql
ALTER TABLE {{ catalog }}.{{ schema }}.{{ asset }}
SET TAGS ('license'             = '{{ license }}',
          'license_ref'         = '{{ license_ref }}',
          'license_ai_training' = '{{ license_ai_training }}');
```

Add `'license_expires' = 'YYYY-MM-DD'` for contracts with a term. Omit pairs you do not have rather than filling them with placeholders.

## Fix: Tag a shared schema or catalog when tables cannot be altered

For a Marketplace delivery or vendor share under one set of terms:

```sql
ALTER SCHEMA {{ catalog }}.{{ schema }}
SET TAGS ('license' = '{{ license }}', 'license_ref' = '{{ license_ref }}');
```

or, for the whole shared catalog:

```sql
ALTER CATALOG {{ catalog }}
SET TAGS ('license' = '{{ license }}', 'license_ref' = '{{ license_ref }}');
```

Guard with `SELECT tag_value FROM {{ catalog }}.information_schema.schema_tags WHERE LOWER(schema_name) = LOWER('{{ schema }}') AND LOWER(tag_name) = 'license'` (or `catalog_tags`). The primary check reads table tags only; run the inherited variant to see the effect.

## Fix: Mark ingested copies of external data as external

Before a license can be demanded, the population has to include the table. For managed tables that the diagnostic's heuristic (or the pipeline owner) identifies as third-party copies, set `source_type` so `license_compliance` and `data_provenance` both see them. This is a factual statement about origin, which the pipeline author can make confidently; it is not the license decision.

```sql
ALTER TABLE {{ catalog }}.{{ schema }}.{{ asset }}
SET TAGS ('source_type'       = 'external',
          'source_system'     = '{{ source_system }}',
          'collection_method' = '{{ collection_method }}');
```

## Fix: Generate license tag statements from a license register

The safe bulk path. `{{ license_register }}` is maintained by whoever reviews data contracts, with `table_name STRING`, `license STRING`, `license_ref STRING`, `license_ai_training STRING`, `license_expires DATE`, `reviewed_by STRING`. Emits statements for external tables in scope that are untagged or disagree with the register.

```sql
WITH external_tables AS (
    SELECT DISTINCT LOWER(t.table_name) AS table_name, t.table_type
    FROM {{ catalog }}.information_schema.tables t
    LEFT JOIN {{ catalog }}.information_schema.table_tags st
      ON  LOWER(st.schema_name) = LOWER(t.table_schema)
      AND LOWER(st.table_name)  = LOWER(t.table_name)
      AND LOWER(st.tag_name)    = 'source_type'
      AND LOWER(st.tag_value) IN ('external', 'third_party', 'vendor', 'purchased',
                                  'licensed', 'public', 'open_data')
    WHERE LOWER(t.table_schema) = LOWER('{{ schema }}')
      AND t.table_type IN ('MANAGED', 'EXTERNAL', 'FOREIGN')
      AND (t.table_type = 'FOREIGN' OR st.tag_name IS NOT NULL)
),
current_tag AS (
    SELECT LOWER(table_name) AS table_name, MAX(tag_value) AS license
    FROM {{ catalog }}.information_schema.table_tags
    WHERE LOWER(schema_name) = LOWER('{{ schema }}')
      AND LOWER(tag_name) = 'license'
    GROUP BY LOWER(table_name)
)
SELECT concat(
    'ALTER TABLE {{ catalog }}.{{ schema }}.`', e.table_name,
    '` SET TAGS (''license'' = ''', r.license, '''',
    CASE WHEN r.license_ref IS NOT NULL
         THEN concat(', ''license_ref'' = ''', r.license_ref, '''') ELSE '' END,
    CASE WHEN r.license_ai_training IS NOT NULL
         THEN concat(', ''license_ai_training'' = ''', r.license_ai_training, '''') ELSE '' END,
    CASE WHEN r.license_expires IS NOT NULL
         THEN concat(', ''license_expires'' = ''', date_format(r.license_expires, 'yyyy-MM-dd'), '''') ELSE '' END,
    ');'
) AS stmt,
e.table_type, r.reviewed_by
FROM external_tables e
JOIN {{ license_register }} r ON LOWER(r.table_name) = e.table_name
LEFT JOIN current_tag c USING (table_name)
WHERE r.license IS NOT NULL AND trim(r.license) <> ''
  AND (c.license IS NULL OR c.license <> r.license)
ORDER BY e.table_name
```

Show the statements and `reviewed_by` to the user first. Statements for `FOREIGN` tables may be rejected on shared catalogs; fall back to the schema-level tag for those. External tables absent from the register are the worklist for the contract reviewer, not candidates for a default.

## Organizational guidance

License review belongs at acquisition. Every Marketplace install, share acceptance or third-party ingestion should go through an intake that records the terms in the license register before the data is granted to anyone, and the tag should be written from that register by the same automation that creates the catalog grant. Declare `license`, `license_ref`, `license_ai_training` and `source_type` as governed tags with allowed values (SPDX list plus `vendor_contract` and `proprietary_internal`). Put `license_expires` on contracts and schedule a job that lists tables whose expiry is within 60 days. For training pipelines, add a pre-flight query that refuses any source with `license_ai_training = 'prohibited'` or with no license tag at all, which converts this check from a report into an enforced control.
