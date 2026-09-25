# Fix: classification

Apply Unity Catalog tags to untagged tables.

## Context

Three paths, from least to most effort:

- **Enable Data Classification** on the catalog. Databricks scans columns for PII and applies tags automatically. Best first step for compliance-oriented classification; it does not cover business tags (domain, owner, tier).
- **Tag tables by hand or in bulk.** `ALTER TABLE ... SET TAGS` is idempotent; re-running with the same key/value is a no-op, and a different value overwrites. Requires `APPLY TAG` on the table (or ownership).
- **Adopt governed tags.** If the account uses governed tag policies, only allowed keys and values can be applied. Run the diagnostic first to see the keys already in use and check the tag policy before inventing new ones.

Tags are metadata only. No data is rewritten and no history is created.

## Fix: Enable Data Classification on the catalog

Turn on automatic classification in Catalog Explorer (catalog → Details → Data classification), or via the API:

```bash
databricks api patch /api/2.1/unity-catalog/catalogs/{{ catalog }} \
  --json '{"enable_auto_classification": true}'
```

Results land as column tags within the scan interval. Re-run the check afterwards; expect the column-level variant to move first.

## Fix: Tag a single table

Substitute `{{ tag_key }}` / `{{ tag_value }}` with the schema's convention (from the diagnostic), for example `data_domain = 'orders'` or `sensitivity = 'internal'`.

```sql
ALTER TABLE {{ catalog }}.{{ schema }}.{{ asset }}
SET TAGS ('{{ tag_key }}' = '{{ tag_value }}')
```

## Fix: Tag every untagged table with a default

Generate one statement per untagged table and run them in a batch. Use a neutral default such as `classification_status = 'unreviewed'` so the tag is truthful and can be replaced by a real value later.

```sql
SELECT concat(
    'ALTER TABLE {{ catalog }}.{{ schema }}.`', t.table_name,
    '` SET TAGS (''classification_status'' = ''unreviewed'');'
) AS stmt
FROM {{ catalog }}.information_schema.tables t
LEFT JOIN (
    SELECT DISTINCT LOWER(table_name) AS table_name
    FROM {{ catalog }}.information_schema.table_tags
    WHERE LOWER(schema_name) = LOWER('{{ schema }}')
) tg ON LOWER(t.table_name) = tg.table_name
WHERE LOWER(t.table_schema) = LOWER('{{ schema }}')
  AND t.table_type IN ('MANAGED', 'EXTERNAL')
  AND tg.table_name IS NULL
ORDER BY t.table_name
```

Show the generated statements to the user before executing them.

## Organizational guidance

Classification only pays off when the tag vocabulary is shared. Agree on a small set of keys (`sensitivity`, `data_domain`, `owner_team`, `pii`) and encode them as governed tags so every team applies the same ones. Wire the tagging step into the table creation path (Lakeflow pipeline settings, dbt `meta`, Terraform `databricks_sql_table` resources) so new tables arrive tagged instead of being back-filled.
