# Diagnostic: chunk_readiness

Per-table inventory of text columns with chunk structure flags, and a per-column length distribution with a chunking recommendation.

## Context

The first query lists every text-bearing base table (same scoping as the check) with its text columns, whether a chunk index column and a parent-document reference exist, and whether an embedding column is already present. Tables with no chunk structure and no embedding sort first: those are the ones a RAG pipeline would have to chunk at query time.

The second query is a single-column deep dive. It buckets rows by character length and attaches a recommendation per bucket. Buckets are anchored on `{{ max_chunk_chars }}` (default 4000) so the labels move with the profile:

- `NULL` / `EMPTY`: exclude from embedding.
- `TOO_SHORT (<100)`: usually lacks enough context to embed well; concatenate with a title or parent field.
- `OK (100..max)`: fits the window as-is.
- `LONG (max..4x max)`: needs chunking with overlap.
- `TOO_LONG (>4x max)`: must chunk; also review whether the row is really one document or a concatenation.

Run the deep dive on a sample for large tables by adding `TABLESAMPLE ({{ sample_rows }} ROWS)` after the table name.

## SQL

### Text tables and their chunk structure

```sql
WITH text_columns AS (
    SELECT LOWER(c.table_name) AS table_name, c.column_name, c.full_data_type
    FROM {{ catalog }}.information_schema.columns c
    JOIN {{ catalog }}.information_schema.tables t
      ON c.table_schema = t.table_schema AND c.table_name = t.table_name
    WHERE LOWER(c.table_schema) = LOWER('{{ schema }}')
      AND t.table_type IN ('MANAGED', 'EXTERNAL')
      AND c.data_type = 'STRING'
      AND REGEXP_LIKE(LOWER(c.column_name),
          '(text|content|description|body|message|comment|summary|abstract|document|article|note|transcript|review|chunk)')
),
all_columns AS (
    SELECT LOWER(table_name) AS table_name, LOWER(column_name) AS column_name,
           LOWER(full_data_type) AS full_data_type
    FROM {{ catalog }}.information_schema.columns
    WHERE LOWER(table_schema) = LOWER('{{ schema }}')
),
flags AS (
    SELECT table_name,
           array_sort(collect_set(column_name)) FILTER (WHERE REGEXP_LIKE(column_name,
               '^(chunk_id|chunk_index|chunk_idx|chunk_no|chunk_num|chunk_number|chunk_seq|chunk_position|chunk_offset)$'))
                                                              AS chunk_index_columns,
           array_sort(collect_set(column_name)) FILTER (WHERE REGEXP_LIKE(column_name,
               '^(doc_id|document_id|parent_id|parent_doc_id|source_id|source_doc_id|file_id|page_id)$'))
                                                              AS parent_ref_columns,
           array_sort(collect_set(column_name)) FILTER (WHERE full_data_type IN ('array<float>', 'array<double>'))
                                                              AS embedding_columns
    FROM all_columns
    GROUP BY table_name
)
SELECT
    tc.table_name,
    array_sort(collect_set(tc.column_name))          AS text_columns,
    f.chunk_index_columns,
    f.parent_ref_columns,
    f.embedding_columns,
    size(f.chunk_index_columns) > 0                  AS has_chunk_structure,
    CASE
        WHEN size(f.chunk_index_columns) > 0 THEN 'CHUNKED'
        WHEN size(f.embedding_columns) > 0   THEN 'EMBEDDED_UNCHUNKED'
        ELSE 'RAW_TEXT'
    END AS status
FROM text_columns tc
JOIN flags f USING (table_name)
GROUP BY tc.table_name, f.chunk_index_columns, f.parent_ref_columns, f.embedding_columns
ORDER BY
    CASE status WHEN 'RAW_TEXT' THEN 1 WHEN 'EMBEDDED_UNCHUNKED' THEN 2 ELSE 3 END,
    tc.table_name
```

`EMBEDDED_UNCHUNKED` deserves attention: someone embedded the column as-is, and if the length deep dive shows `LONG` rows those embeddings represent truncated text.

### Length distribution for one text column

```sql
WITH lengths AS (
    SELECT length({{ column }}) AS len
    FROM {{ catalog }}.{{ schema }}.{{ asset }}
),
buckets AS (
    SELECT
        CASE
            WHEN len IS NULL                       THEN 'NULL'
            WHEN len = 0                           THEN 'EMPTY'
            WHEN len < 100                         THEN 'TOO_SHORT (<100)'
            WHEN len <= {{ max_chunk_chars }}      THEN 'OK (100..max)'
            WHEN len <= {{ max_chunk_chars }} * 4  THEN 'LONG (max..4x max)'
            ELSE 'TOO_LONG (>4x max)'
        END AS length_bucket,
        CASE
            WHEN len IS NULL THEN 1 WHEN len = 0 THEN 2 WHEN len < 100 THEN 3
            WHEN len <= {{ max_chunk_chars }} THEN 4
            WHEN len <= {{ max_chunk_chars }} * 4 THEN 5 ELSE 6
        END AS bucket_order,
        len
    FROM lengths
)
SELECT
    length_bucket,
    COUNT(*)                                              AS row_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2)    AS pct_rows,
    MIN(len)                                              AS min_chars,
    ROUND(AVG(len), 0)                                    AS avg_chars,
    MAX(len)                                              AS max_chars,
    CASE length_bucket
        WHEN 'NULL'               THEN 'Missing content, exclude or impute'
        WHEN 'EMPTY'              THEN 'Empty strings, exclude from embedding'
        WHEN 'TOO_SHORT (<100)'   THEN 'Concatenate with title / parent context before embedding'
        WHEN 'OK (100..max)'      THEN 'Fits the window, no chunking needed'
        WHEN 'LONG (max..4x max)' THEN 'Chunk with overlap (see fix)'
        ELSE                           'Must chunk; also check whether rows are concatenated documents'
    END AS recommendation
FROM buckets
GROUP BY length_bucket, bucket_order
ORDER BY bucket_order
```

### Percentiles for one text column (quick read)

```sql
SELECT
    COUNT(*)                                         AS text_rows,
    percentile_approx(length({{ column }}), 0.50)    AS p50_chars,
    percentile_approx(length({{ column }}), 0.95)    AS p95_chars,
    percentile_approx(length({{ column }}), 0.99)    AS p99_chars,
    MAX(length({{ column }}))                        AS max_chars,
    COUNT_IF(length({{ column }}) > {{ max_chunk_chars }}) AS rows_over_limit
FROM {{ catalog }}.{{ schema }}.{{ asset }}
WHERE {{ column }} IS NOT NULL AND length({{ column }}) > 0
```
