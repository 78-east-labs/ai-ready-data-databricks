# Fix: data_provenance

Declare each table's origin with the `source_system` and `collection_method` tags, and make ingestion paths leave lineage.

## Context

The tags record a decision a human makes: which system this data came from and how it was collected. Applying them with a guessed or placeholder value is worse than leaving them empty, because downstream consumers (and agents) will trust them. Use the diagnostic first: `external_sources` usually names the system (an `s3://acme-salesforce-export/` prefix is Salesforce), `upstream_tables` shows tables that are derived rather than landed, and `writer_entity_types` tells you whether the writer is a pipeline you can inspect.

Recommended values: `source_system` is the producing system in lowercase (`salesforce`, `postgres_orders`, `kafka_clickstream`, `manual_upload`); `collection_method` is the mechanism (`auto_loader`, `copy_into`, `fivetran`, `jdbc_extract`, `api_extract`, `manual_upload`, `derived`). If the account uses governed tag policies, check the allowed values before tagging.

`ALTER TABLE ... SET TAGS` is idempotent (same key and value is a no-op, different value overwrites) and needs `APPLY TAG` on the table or ownership. It rewrites no data.

## Fix: Tag a single table

```sql
ALTER TABLE {{ catalog }}.{{ schema }}.{{ asset }}
SET TAGS ('{{ source_system_tag }}'     = '{{ source_system }}',
          '{{ collection_method_tag }}' = '{{ collection_method }}')
```

## Fix: Generate tag statements for tables that lack them

Emits one `ALTER TABLE` per base table missing either tag. Tables that lineage shows being loaded from a path get `collection_method = 'file_ingest'` and the first external path in a trailing comment so the operator can name the system; tables fed only from other tables get `derived`. `source_system` is left as a `<fill_in>` marker on purpose: the statement must not be run until the marker is replaced.

```sql
WITH tables_in_scope AS (
    SELECT LOWER(table_name) AS table_name
    FROM {{ catalog }}.information_schema.tables
    WHERE LOWER(table_schema) = LOWER('{{ schema }}')
      AND table_type IN ('MANAGED', 'EXTERNAL')
),
tags AS (
    SELECT LOWER(table_name) AS table_name,
           COUNT_IF(LOWER(tag_name) = LOWER('{{ source_system_tag }}')     AND trim(tag_value) <> '') AS has_source,
           COUNT_IF(LOWER(tag_name) = LOWER('{{ collection_method_tag }}') AND trim(tag_value) <> '') AS has_method
    FROM {{ catalog }}.information_schema.table_tags
    WHERE LOWER(schema_name) = LOWER('{{ schema }}')
    GROUP BY LOWER(table_name)
),
lineage AS (
    SELECT LOWER(target_table_name) AS table_name,
           MIN(CASE WHEN source_path IS NOT NULL OR UPPER(source_type) = 'PATH' THEN source_path END) AS example_path,
           COUNT_IF(source_table_full_name IS NOT NULL)                                                AS upstream_table_edges
    FROM system.access.table_lineage
    WHERE LOWER(target_catalog) = LOWER('{{ catalog }}')
      AND LOWER(target_schema)  = LOWER('{{ schema }}')
      AND target_table_full_name IS NOT NULL
      AND event_time >= current_timestamp() - INTERVAL {{ lookback_days }} DAYS
    GROUP BY LOWER(target_table_name)
)
SELECT concat(
    'ALTER TABLE {{ catalog }}.{{ schema }}.`', t.table_name, '` SET TAGS (',
    '''{{ source_system_tag }}'' = ''<fill_in>'', ',
    '''{{ collection_method_tag }}'' = ''',
    CASE WHEN l.example_path IS NOT NULL THEN 'file_ingest'
         WHEN l.upstream_table_edges > 0  THEN 'derived'
         ELSE '<fill_in>' END,
    '''); -- ', COALESCE(l.example_path, 'no external path seen in window')
) AS stmt
FROM tables_in_scope t
LEFT JOIN tags    tg USING (table_name)
LEFT JOIN lineage l  USING (table_name)
WHERE COALESCE(tg.has_source, 0) = 0 OR COALESCE(tg.has_method, 0) = 0
ORDER BY t.table_name
```

Show the generated statements to the user, have them replace every `<fill_in>`, and refuse to execute any statement that still contains the marker.

## Fix: Record provenance in the table comment as well

Tags are machine-readable; a comment is what people and AI-generated documentation read. Only run this when the current comment is empty (check `information_schema.tables.comment` first; overwriting an existing comment needs a prompt).

```sql
COMMENT ON TABLE {{ catalog }}.{{ schema }}.{{ asset }} IS
  'Source: {{ source_system }} | Method: {{ collection_method }} | Upstream: {{ upstream_description }}'
```

## Fix: Make file ingestion leave lineage

Tables loaded by copying files onto a cluster and writing a DataFrame from local memory, or by an external Delta writer, have no `source_path` in lineage. Loading through Auto Loader or `COPY INTO` from an external location records the path automatically, and Auto Loader can persist the per-row origin too (see `record_level_traceability`).

```sql
COPY INTO {{ catalog }}.{{ schema }}.{{ asset }}
FROM '{{ source_location }}'
FILEFORMAT = PARQUET
COPY_OPTIONS ('mergeSchema' = 'false')
```

`COPY INTO` is idempotent per file (already-loaded files are skipped), so re-running it is safe.

## Organizational guidance

Provenance is cheapest at creation time. Put the two tags into the ingestion template so every landing table is tagged by the pipeline that creates it: Lakeflow Declarative Pipelines accept `TBLPROPERTIES` and can be followed by a `SET TAGS` step, dbt models carry `meta` that a post-hook turns into tags, and Terraform `databricks_sql_table` resources can set them declaratively. Register `source_system` and `collection_method` as governed tags with an allowed-values list so the vocabulary does not drift across teams.
