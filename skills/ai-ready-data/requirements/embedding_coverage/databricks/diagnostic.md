# Diagnostic: embedding_coverage

Lists every text-bearing table with its text columns, its embedding columns (if any), sibling chunk or embedding tables, and a status; plus a per-column probe for how many embeddings are actually populated.

## Context

Same text-table scoping as the check. `embedding_columns` are the `array<float>` / `array<double>` columns on the table itself; `sibling_embedding_tables` are tables named `{table}_chunks`, `{table}_embeddings` or similar that carry an embedding column. `text_max_length` is the declared `VARCHAR(n)` limit when one exists, taken from `full_data_type`; plain `STRING` columns show NULL.

Status:

- `HAS_EMBEDDING`: embedding column on the table.
- `SIBLING_EMBEDDING`: no embedding on the table, but a sibling table carries one.
- `NO_EMBEDDING`: neither. These are the fix candidates.

The second query is the data probe the check cannot do from metadata: for one embedding column, how many rows have a non-null vector and what the observed dimension is. A high null ratio means the column exists but the backfill never finished.

Sorted so `NO_EMBEDDING` comes first.

## SQL

### Text tables and their embedding status

```sql
WITH text_tables AS (
    SELECT LOWER(c.table_name) AS table_name,
           array_sort(collect_set(c.column_name)) AS text_columns,
           MAX(TRY_CAST(regexp_extract(LOWER(c.full_data_type), 'varchar\\((\\d+)\\)', 1) AS INT))
                                                  AS text_max_length
    FROM {{ catalog }}.information_schema.columns c
    JOIN {{ catalog }}.information_schema.tables t
      ON c.table_schema = t.table_schema AND c.table_name = t.table_name
    WHERE LOWER(c.table_schema) = LOWER('{{ schema }}')
      AND t.table_type IN ('MANAGED', 'EXTERNAL')
      AND c.data_type = 'STRING'
      AND REGEXP_LIKE(LOWER(c.column_name),
          '(text|content|description|body|message|comment|summary|abstract|document|article|note|transcript|review|chunk)')
    GROUP BY LOWER(c.table_name)
),
embedding_columns AS (
    SELECT LOWER(table_name) AS table_name,
           array_sort(collect_set(concat(column_name, ' ', LOWER(full_data_type)))) AS embedding_columns
    FROM {{ catalog }}.information_schema.columns
    WHERE LOWER(table_schema) = LOWER('{{ schema }}')
      AND LOWER(full_data_type) IN ('array<float>', 'array<double>')
    GROUP BY LOWER(table_name)
),
siblings AS (
    SELECT regexp_replace(table_name, '_(chunks?|embeddings?|emb)$', '') AS base_name,
           array_sort(collect_set(table_name)) AS sibling_embedding_tables
    FROM embedding_columns
    WHERE REGEXP_LIKE(table_name, '_(chunks?|embeddings?|emb)$')
    GROUP BY regexp_replace(table_name, '_(chunks?|embeddings?|emb)$', '')
)
SELECT
    tt.table_name,
    tt.text_columns,
    tt.text_max_length,
    ec.embedding_columns,
    s.sibling_embedding_tables,
    CASE
        WHEN ec.embedding_columns IS NOT NULL       THEN 'HAS_EMBEDDING'
        WHEN s.sibling_embedding_tables IS NOT NULL THEN 'SIBLING_EMBEDDING'
        ELSE 'NO_EMBEDDING'
    END AS status,
    CASE
        WHEN ec.embedding_columns IS NOT NULL       THEN 'Run the population probe below to confirm the column is filled'
        WHEN s.sibling_embedding_tables IS NOT NULL THEN 'Embeddings live in the sibling table; document the join key'
        ELSE 'Add an ARRAY<FLOAT> column and populate with ai_query() or a Vector Search Delta Sync index (see fix)'
    END AS recommendation
FROM text_tables tt
LEFT JOIN embedding_columns ec USING (table_name)
LEFT JOIN siblings s ON tt.table_name = s.base_name
ORDER BY
    CASE status WHEN 'NO_EMBEDDING' THEN 1 WHEN 'SIBLING_EMBEDDING' THEN 2 ELSE 3 END,
    tt.table_name
```

### Population probe for one embedding column

```sql
SELECT
    COUNT(*)                                         AS total_rows,
    COUNT_IF({{ column }} IS NOT NULL)               AS embedded_rows,
    COUNT_IF({{ column }} IS NOT NULL)::DOUBLE
        / NULLIF(COUNT(*), 0)                        AS embedded_fraction,
    percentile_approx(size({{ column }}), 0.5)       AS median_dimension,
    MIN(size({{ column }}))                          AS min_dimension,
    MAX(size({{ column }}))                          AS max_dimension,
    COUNT_IF(size({{ column }}) = 0)                 AS empty_vectors
FROM {{ catalog }}.{{ schema }}.{{ asset }} TABLESAMPLE ({{ sample_rows }} ROWS)
```

Drop the `TABLESAMPLE` clause for an exact count on smaller tables. `min_dimension <> max_dimension` is a finding for `embedding_dimension_consistency`.

### Indexes that hold embeddings outside the table

Tables with Databricks-computed embeddings in a Vector Search index score `NO_EMBEDDING` here but are covered. List those with the `vector_index_coverage` check's SDK snippet and read `delta_sync_index_spec.embedding_source_columns`; an index with `embedding_source_columns` set computes vectors itself.
