# Check: embedding_coverage

Fraction of text-bearing base tables in the schema that also hold a pre-computed embedding column (`ARRAY<FLOAT>` or `ARRAY<DOUBLE>`) in the same table.

## Context

Databricks has no dedicated vector type. Embeddings are stored as `ARRAY<FLOAT>` or `ARRAY<DOUBLE>` columns, which is also what Mosaic AI Vector Search accepts for self-managed embeddings. `information_schema.columns.full_data_type` records the full type string in lowercase (`array<float>`, `array<double>`), while `data_type` says only `ARRAY`, so the check filters on `full_data_type`.

Text-bearing tables are base tables with a `STRING` column whose name matches `text|content|description|body|message|comment|summary|abstract|document|article|note|transcript|review|chunk`. A text-bearing table counts as covered when it has at least one embedding-typed column.

The signal is **native** on the metadata side (the column exists, with the right type) but says nothing about the values. A table with an `ARRAY<FLOAT>` column that is all NULL scores the same as a fully embedded one. The diagnostic includes a null-ratio probe per column for that. The other blind spot: `ARRAY<FLOAT>` also fits non-embedding data (sensor readings, price histories). The name-tightened variant limits the match to columns named `embedding|embeddings|vector|emb|_vec`; use it when the primary variant looks inflated.

A common Databricks layout stores embeddings in a **separate** chunk table (`orders_chunks.embedding`) rather than on the source table. The primary variant treats the chunk table as its own text-bearing table (it has `chunk_text`), so it is covered; the source table is not, which is the intended reading: consumers of the source table get no embedding without a join. If the schema follows that layout deliberately, the sibling variant credits a source table when a table named `{table}_chunks` or `{table}_embeddings` carries the embedding.

Vector Search Delta Sync indexes with **Databricks-computed embeddings** (`embedding_source_columns`) hold their vectors inside the index, not in the source table, so those tables score 0 here even though embeddings exist. `vector_index_coverage` measures that case; read the two together.

`information_schema` is live; no lag. Rows are filtered to tables the caller can see.

Returns NULL (N/A) when the schema has no text-bearing base tables.

## SQL

### Embedding column present on the text table (primary)

```sql
WITH text_tables AS (
    SELECT DISTINCT LOWER(c.table_name) AS table_name
    FROM {{ catalog }}.information_schema.columns c
    JOIN {{ catalog }}.information_schema.tables t
      ON c.table_schema = t.table_schema AND c.table_name = t.table_name
    WHERE LOWER(c.table_schema) = LOWER('{{ schema }}')
      AND t.table_type IN ('MANAGED', 'EXTERNAL')
      AND c.data_type = 'STRING'
      AND REGEXP_LIKE(LOWER(c.column_name),
          '(text|content|description|body|message|comment|summary|abstract|document|article|note|transcript|review|chunk)')
),
embedding_tables AS (
    SELECT DISTINCT LOWER(table_name) AS table_name
    FROM {{ catalog }}.information_schema.columns
    WHERE LOWER(table_schema) = LOWER('{{ schema }}')
      AND LOWER(full_data_type) IN ('array<float>', 'array<double>')
)
SELECT
    COUNT_IF(e.table_name IS NOT NULL)            AS tables_with_embeddings,
    COUNT(*)                                       AS text_tables,
    COUNT_IF(e.table_name IS NOT NULL)::DOUBLE
        / NULLIF(COUNT(*), 0)                      AS value
FROM text_tables tt
LEFT JOIN embedding_tables e USING (table_name)
```

### Name-tightened embedding columns (variant)

Only counts float or double arrays whose name looks like an embedding. Use when the schema stores other array data.

```sql
WITH text_tables AS (
    SELECT DISTINCT LOWER(c.table_name) AS table_name
    FROM {{ catalog }}.information_schema.columns c
    JOIN {{ catalog }}.information_schema.tables t
      ON c.table_schema = t.table_schema AND c.table_name = t.table_name
    WHERE LOWER(c.table_schema) = LOWER('{{ schema }}')
      AND t.table_type IN ('MANAGED', 'EXTERNAL')
      AND c.data_type = 'STRING'
      AND REGEXP_LIKE(LOWER(c.column_name),
          '(text|content|description|body|message|comment|summary|abstract|document|article|note|transcript|review|chunk)')
),
embedding_tables AS (
    SELECT DISTINCT LOWER(table_name) AS table_name
    FROM {{ catalog }}.information_schema.columns
    WHERE LOWER(table_schema) = LOWER('{{ schema }}')
      AND LOWER(full_data_type) IN ('array<float>', 'array<double>')
      AND REGEXP_LIKE(LOWER(column_name), '(embedding|embeddings|vector|_vec$|^emb$|_emb$)')
)
SELECT
    COUNT_IF(e.table_name IS NOT NULL)            AS tables_with_embeddings,
    COUNT(*)                                       AS text_tables,
    COUNT_IF(e.table_name IS NOT NULL)::DOUBLE
        / NULLIF(COUNT(*), 0)                      AS value
FROM text_tables tt
LEFT JOIN embedding_tables e USING (table_name)
```

### Sibling chunk or embedding table counts (variant)

Credits a text table when it, or a sibling named `{table}_chunks`, `{table}_chunk`, `{table}_embeddings` or `{table}_emb`, carries an embedding column. Sibling tables themselves are removed from the denominator so they are not double-counted.

```sql
WITH text_tables AS (
    SELECT DISTINCT LOWER(c.table_name) AS table_name
    FROM {{ catalog }}.information_schema.columns c
    JOIN {{ catalog }}.information_schema.tables t
      ON c.table_schema = t.table_schema AND c.table_name = t.table_name
    WHERE LOWER(c.table_schema) = LOWER('{{ schema }}')
      AND t.table_type IN ('MANAGED', 'EXTERNAL')
      AND c.data_type = 'STRING'
      AND REGEXP_LIKE(LOWER(c.column_name),
          '(text|content|description|body|message|comment|summary|abstract|document|article|note|transcript|review|chunk)')
      AND NOT REGEXP_LIKE(LOWER(c.table_name), '_(chunks?|embeddings?|emb)$')
),
embedding_tables AS (
    SELECT DISTINCT LOWER(table_name) AS table_name,
           regexp_replace(LOWER(table_name), '_(chunks?|embeddings?|emb)$', '') AS base_name
    FROM {{ catalog }}.information_schema.columns
    WHERE LOWER(table_schema) = LOWER('{{ schema }}')
      AND LOWER(full_data_type) IN ('array<float>', 'array<double>')
)
SELECT
    COUNT_IF(e.base_name IS NOT NULL)             AS tables_with_embeddings,
    COUNT(*)                                       AS text_tables,
    COUNT_IF(e.base_name IS NOT NULL)::DOUBLE
        / NULLIF(COUNT(*), 0)                      AS value
FROM text_tables tt
LEFT JOIN (SELECT DISTINCT base_name FROM embedding_tables) e
       ON tt.table_name = e.base_name
```
