# Diagnostic: license_compliance

Per-table view of every externally sourced dataset in the schema with how it was identified as external, its license tag, and the provenance available to trace the terms.

## Context

Reuses the check's population. For each external table:

- `external_reason`: `DELTA_SHARING` (foreign table with `data_source_format = 'DELTASHARING'`), `FEDERATED:<format>` (foreign table from a Lakehouse Federation connection), or `TAG:<source_type value>` (managed or external table tagged as external).
- `license`: the tag value, or NULL. `license_ref` and `license_expires` if set (contract id, renewal date).
- `provenance`: `source_system` and `collection_method` tags, plus the table comment, which for Marketplace deliveries often carries the provider's description. Enough to find the listing or contract.
- `status`: `LICENSED`, `LICENSED_EXPIRED` (the `license_expires` tag is a past date), `LICENSED_NO_REF` (a license value like `vendor_contract` with no pointer to the document), or `NO_LICENSE`.

For shared tables the provider and share name identify where the terms live. `information_schema` does not carry them; get them with `databricks providers list` and `databricks providers list-shares <provider>` (SDK: `w.providers.list()`, `w.providers.list_shares(name)`), or from Catalog Explorer under the shared catalog's details. Marketplace installations are listed with `databricks consumer-installations list` (best-effort; the command group name has changed between CLI versions, so check `databricks --help` under Marketplace).

Sorted worst-first.

## SQL

```sql
WITH external_tables AS (
    SELECT DISTINCT LOWER(t.table_name) AS table_name,
           t.table_owner,
           t.table_type,
           t.data_source_format,
           t.comment,
           CASE
               WHEN t.table_type = 'FOREIGN' AND UPPER(t.data_source_format) = 'DELTASHARING'
                    THEN 'DELTA_SHARING'
               WHEN t.table_type = 'FOREIGN'
                    THEN concat('FEDERATED:', COALESCE(t.data_source_format, 'UNKNOWN'))
               ELSE concat('TAG:', st.tag_value)
           END AS external_reason
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
tags AS (
    SELECT LOWER(table_name) AS table_name,
           MAX(CASE WHEN LOWER(tag_name) = 'license'           THEN tag_value END) AS license,
           MAX(CASE WHEN LOWER(tag_name) = 'license_ref'       THEN tag_value END) AS license_ref,
           MAX(CASE WHEN LOWER(tag_name) = 'license_expires'   THEN tag_value END) AS license_expires,
           MAX(CASE WHEN LOWER(tag_name) = 'source_system'     THEN tag_value END) AS source_system,
           MAX(CASE WHEN LOWER(tag_name) = 'collection_method' THEN tag_value END) AS collection_method
    FROM {{ catalog }}.information_schema.table_tags
    WHERE LOWER(schema_name) = LOWER('{{ schema }}')
    GROUP BY LOWER(table_name)
)
SELECT
    e.table_name,
    e.table_owner,
    e.external_reason,
    g.license,
    g.license_ref,
    g.license_expires,
    g.source_system,
    g.collection_method,
    e.comment,
    CASE
        WHEN g.license IS NULL OR trim(g.license) = ''                       THEN 'NO_LICENSE'
        WHEN TRY_CAST(g.license_expires AS DATE) < current_date()            THEN 'LICENSED_EXPIRED'
        WHEN g.license_ref IS NULL
             AND NOT REGEXP_LIKE(LOWER(g.license), '^(cc|odbl|odc|pddl|mit|apache|bsd|gpl|lgpl|agpl|cdla|public_domain|proprietary_internal)')
                                                                              THEN 'LICENSED_NO_REF'
        ELSE 'LICENSED'
    END AS status
FROM external_tables e
LEFT JOIN tags g USING (table_name)
ORDER BY
    CASE status
        WHEN 'NO_LICENSE'       THEN 0
        WHEN 'LICENSED_EXPIRED' THEN 1
        WHEN 'LICENSED_NO_REF'  THEN 2
        ELSE 3
    END,
    e.external_reason, e.table_name
```

The `LICENSED_NO_REF` rule treats well-known public license identifiers as self-describing and asks for a `license_ref` only for private terms (vendor contracts). Adjust the regex to your vocabulary.

### Managed tables that look external but are untagged

Catches ingested copies of third-party data that the population misses because nobody set `source_type`. Heuristic on names and comments; review before tagging.

```sql
SELECT LOWER(t.table_name) AS table_name, t.table_owner, t.comment
FROM {{ catalog }}.information_schema.tables t
LEFT JOIN {{ catalog }}.information_schema.table_tags st
  ON  LOWER(st.schema_name) = LOWER(t.table_schema)
  AND LOWER(st.table_name)  = LOWER(t.table_name)
  AND LOWER(st.tag_name)    = 'source_type'
WHERE LOWER(t.table_schema) = LOWER('{{ schema }}')
  AND t.table_type IN ('MANAGED', 'EXTERNAL')
  AND st.tag_name IS NULL
  AND (
        REGEXP_LIKE(LOWER(t.table_name),
            '(^|_)(vendor|external|ext|third_party|3p|public|open|census|osm|wiki|kaggle|huggingface|hf|common_crawl|imdb|nyc_taxi|marketplace|purchased|licensed)($|_)')
     OR REGEXP_LIKE(LOWER(COALESCE(t.comment, '')),
            'license|licence|vendor|third.party|marketplace|downloaded from|source: http')
  )
ORDER BY t.table_name
```
