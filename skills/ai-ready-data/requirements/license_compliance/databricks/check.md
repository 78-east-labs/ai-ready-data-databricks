# Check: license_compliance

Fraction of externally sourced datasets in the schema (Delta Sharing and federated tables, Marketplace deliveries, or tables tagged as external) that carry a `license` table tag.

## Context

A dataset that came from outside the organisation has terms attached: a Marketplace listing's terms, a vendor contract, an open-data license such as CC-BY-4.0 or ODbL, or a research license that forbids commercial model training. Databricks records none of that in metadata, so the framework measures whether someone did, through the `license` table tag in `{{ catalog }}.information_schema.table_tags`. The tag records a human reading of the terms (`CC-BY-4.0`, `ODbL-1.0`, `vendor_contract_2026-0142`, `proprietary_internal`); the check verifies presence and a non-empty value only.

The population is what makes this check useful, and Databricks gives a partly native handle on it:

- `table_type = 'FOREIGN'` in `information_schema.tables` marks objects that Unity Catalog does not store itself: tables received through Delta Sharing (Marketplace deliveries arrive this way, as a shared catalog) and tables surfaced by Lakehouse Federation from another system. `data_source_format` tells them apart: `DELTASHARING` for shares, and a connector name (`MYSQL`, `POSTGRESQL`, `SNOWFLAKE`, `SQLSERVER`, `BIGQUERY`, `REDSHIFT`, `DATABRICKS`) for federation. Shared tables are almost always third-party data; federated tables are often the organisation's own data in another system, so the variant lets you exclude them.
- A **table tag** `source_type` with a value in `external`, `third_party`, `vendor`, `purchased`, `licensed`, `public`, `open_data` marks copies of external data that were ingested into managed tables (downloaded CSVs, API pulls). These are invisible to the native signal and depend on `data_provenance` tagging.

Strength: tag for the license, native plus tag for the population. If the schema sits inside a Delta Sharing catalog (every table shared), all its tables will be `FOREIGN` and the check works unchanged; to confirm the catalog kind, use `databricks catalogs get {{ catalog }}` and look at `catalog_type` (`DELTASHARING_CATALOG`), since `information_schema.catalogs` does not expose the type.

Base-table filter is deliberately not applied here: the objects of interest are `FOREIGN` by nature, plus `MANAGED`/`EXTERNAL` tables that carry the tag. Views are excluded. `information_schema` reflects tags immediately.

Returns NULL (N/A) when the schema contains no externally sourced datasets. That is a legitimate result for a schema of purely internal data, not a gap.

## SQL

### External datasets with a license tag (primary)

```sql
WITH external_tables AS (
    SELECT DISTINCT LOWER(t.table_name) AS table_name
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
licensed AS (
    SELECT DISTINCT LOWER(table_name) AS table_name
    FROM {{ catalog }}.information_schema.table_tags
    WHERE LOWER(schema_name) = LOWER('{{ schema }}')
      AND LOWER(tag_name) = 'license'
      AND tag_value IS NOT NULL AND trim(tag_value) <> ''
)
SELECT
    COUNT_IF(l.table_name IS NOT NULL)            AS licensed_external_tables,
    COUNT(*)                                       AS external_tables,
    COUNT_IF(l.table_name IS NOT NULL)::DOUBLE
        / NULLIF(COUNT(*), 0)                      AS value
FROM external_tables e
LEFT JOIN licensed l USING (table_name)
```

### Shared and tagged only, federation excluded (variant)

Drops `FOREIGN` tables whose `data_source_format` is a federation connector, keeping Delta Sharing (`DELTASHARING`) and tagged tables. Use when federated sources are known to be the organisation's own systems.

```sql
WITH external_tables AS (
    SELECT DISTINCT LOWER(t.table_name) AS table_name
    FROM {{ catalog }}.information_schema.tables t
    LEFT JOIN {{ catalog }}.information_schema.table_tags st
      ON  LOWER(st.schema_name) = LOWER(t.table_schema)
      AND LOWER(st.table_name)  = LOWER(t.table_name)
      AND LOWER(st.tag_name)    = 'source_type'
      AND LOWER(st.tag_value) IN ('external', 'third_party', 'vendor', 'purchased',
                                  'licensed', 'public', 'open_data')
    WHERE LOWER(t.table_schema) = LOWER('{{ schema }}')
      AND t.table_type IN ('MANAGED', 'EXTERNAL', 'FOREIGN')
      AND (
            (t.table_type = 'FOREIGN' AND UPPER(t.data_source_format) = 'DELTASHARING')
         OR st.tag_name IS NOT NULL
      )
),
licensed AS (
    SELECT DISTINCT LOWER(table_name) AS table_name
    FROM {{ catalog }}.information_schema.table_tags
    WHERE LOWER(schema_name) = LOWER('{{ schema }}')
      AND LOWER(tag_name) = 'license'
      AND tag_value IS NOT NULL AND trim(tag_value) <> ''
)
SELECT
    COUNT_IF(l.table_name IS NOT NULL)            AS licensed_external_tables,
    COUNT(*)                                       AS external_tables,
    COUNT_IF(l.table_name IS NOT NULL)::DOUBLE
        / NULLIF(COUNT(*), 0)                      AS value
FROM external_tables e
LEFT JOIN licensed l USING (table_name)
```

If `data_source_format` for shared tables is not `DELTASHARING` in your metastore, confirm with `SELECT DISTINCT table_type, data_source_format FROM {{ catalog }}.information_schema.tables WHERE table_type = 'FOREIGN'` and adjust the literal.

### Accept a license inherited from the schema or catalog (variant)

A whole Marketplace delivery or vendor share usually comes under one set of terms, and the recipient may not be able to alter individual shared tables. This variant counts a table as licensed when the `license` tag is set on the table, on its schema (`information_schema.schema_tags`) or on the catalog (`information_schema.catalog_tags`). Same population as the primary.

```sql
WITH external_tables AS (
    SELECT DISTINCT LOWER(t.table_name) AS table_name
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
table_licensed AS (
    SELECT DISTINCT LOWER(table_name) AS table_name
    FROM {{ catalog }}.information_schema.table_tags
    WHERE LOWER(schema_name) = LOWER('{{ schema }}')
      AND LOWER(tag_name) = 'license'
      AND tag_value IS NOT NULL AND trim(tag_value) <> ''
),
container_licensed AS (
    SELECT COUNT(*) > 0 AS present
    FROM (
        SELECT tag_value FROM {{ catalog }}.information_schema.schema_tags
        WHERE LOWER(schema_name) = LOWER('{{ schema }}') AND LOWER(tag_name) = 'license'
        UNION ALL
        SELECT tag_value FROM {{ catalog }}.information_schema.catalog_tags
        WHERE LOWER(tag_name) = 'license'
    )
    WHERE tag_value IS NOT NULL AND trim(tag_value) <> ''
)
SELECT
    COUNT_IF(l.table_name IS NOT NULL OR c.present)   AS licensed_external_tables,
    COUNT(*)                                          AS external_tables,
    COUNT_IF(l.table_name IS NOT NULL OR c.present)::DOUBLE
        / NULLIF(COUNT(*), 0)                         AS value
FROM external_tables e
CROSS JOIN container_licensed c
LEFT JOIN table_licensed l USING (table_name)
```
