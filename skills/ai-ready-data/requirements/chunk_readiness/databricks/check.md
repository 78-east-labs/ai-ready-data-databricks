# Check: chunk_readiness

Fraction of text-bearing tables in the schema whose text is already chunked to at most `{{ max_chunk_chars }}` characters, or that carry an explicit chunk structure.

## Context

Two signals, one structural and one from the data:

- **Structural (proxy).** A text-bearing table that also has a column named like `chunk_id`, `chunk_index`, `chunk_no`, `chunk_seq`, `chunk_text`, `chunk_number`, or a parent-document reference column (`doc_id`, `document_id`, `parent_id`, `source_id`) alongside a text column, was produced by a chunking step. This is metadata-only, one statement for the whole schema, and is the primary variant at schema scope.
- **Data.** For a given text column, the 95th percentile of `length()` is at or below `{{ max_chunk_chars }}`. This is the measurement that matters for an embedding model's context window. It scans rows (sampled variant offered) and runs per column; the generator below assembles one statement for the whole schema.

Text-bearing tables are base tables with a `STRING` column whose name matches `text|content|description|body|message|comment|summary|abstract|document|article|note|transcript|review|chunk`. `information_schema.columns.data_type` is `STRING` for both `STRING` and `VARCHAR(n)` columns on Databricks (the length shows only in `full_data_type`).

Placeholder default: `{{ max_chunk_chars }}` = `4000` (roughly 1,000 tokens for English text, which fits every mainstream embedding model's window with room for metadata). `{{ sample_rows }}` = `1000000`.

What it proves: the structural variant proves a chunking step exists, not that chunk sizes are right. The data variant proves size compliance for the column it was run on, not that chunks respect sentence or section boundaries. A table whose rows are naturally short (product titles, support ticket subjects) passes the data variant without any chunking, which is correct: it needs none. Length is measured in characters; a table with CJK or emoji-heavy text has a higher token-per-character ratio, so lower `{{ max_chunk_chars }}` for those.

Neither variant lags; `information_schema` is live and the data variant reads the table directly.

Returns NULL (N/A) when the schema has no text-bearing base tables.

## SQL

### Structural chunk signal (primary, metadata proxy)

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
chunk_structured AS (
    SELECT DISTINCT LOWER(table_name) AS table_name
    FROM {{ catalog }}.information_schema.columns
    WHERE LOWER(table_schema) = LOWER('{{ schema }}')
      AND REGEXP_LIKE(LOWER(column_name),
          '^(chunk_id|chunk_index|chunk_idx|chunk_no|chunk_num|chunk_number|chunk_seq|chunk_position|chunk_offset)$')
)
SELECT
    COUNT_IF(cs.table_name IS NOT NULL)           AS chunk_ready_tables,
    COUNT(*)                                       AS text_tables,
    COUNT_IF(cs.table_name IS NOT NULL)::DOUBLE
        / NULLIF(COUNT(*), 0)                      AS value
FROM text_tables tt
LEFT JOIN chunk_structured cs USING (table_name)
```

### Chunk-size compliance for one text column (variant, data)

Run per `{{ asset }}.{{ column }}`. Passes (value 1.0) when the 95th percentile length is within the limit; the numerator and denominator here are rows, so the orchestrator can also report the fraction of rows within limit.

```sql
WITH lengths AS (
    SELECT length({{ column }}) AS len
    FROM {{ catalog }}.{{ schema }}.{{ asset }}
    WHERE {{ column }} IS NOT NULL AND length({{ column }}) > 0
)
SELECT
    COUNT_IF(len <= {{ max_chunk_chars }})                    AS rows_within_limit,
    COUNT(*)                                                  AS text_rows,
    percentile_approx(len, 0.95)                              AS p95_chars,
    CASE WHEN percentile_approx(len, 0.95) <= {{ max_chunk_chars }} THEN 1.0 ELSE 0.0 END
                                                              AS value
FROM lengths
```

### Sampled chunk-size compliance (variant)

Same measurement on a `TABLESAMPLE`. `percentile_approx` on a million rows is within a few percent of the full-table value for any realistic length distribution.

```sql
WITH lengths AS (
    SELECT length({{ column }}) AS len
    FROM {{ catalog }}.{{ schema }}.{{ asset }} TABLESAMPLE ({{ sample_rows }} ROWS)
    WHERE {{ column }} IS NOT NULL AND length({{ column }}) > 0
)
SELECT
    COUNT_IF(len <= {{ max_chunk_chars }})                    AS rows_within_limit,
    COUNT(*)                                                  AS text_rows,
    percentile_approx(len, 0.95)                              AS p95_chars,
    CASE WHEN percentile_approx(len, 0.95) <= {{ max_chunk_chars }} THEN 1.0 ELSE 0.0 END
                                                              AS value
FROM lengths
```

### Schema-wide data variant (generator)

Emits one statement that measures every text column in the schema with a `UNION ALL` and aggregates to the table level: a table passes when **every** one of its text columns has p95 within the limit. Run the generator, then run the statement it returns.

```sql
WITH text_columns AS (
    SELECT c.table_name, c.column_name
    FROM {{ catalog }}.information_schema.columns c
    JOIN {{ catalog }}.information_schema.tables t
      ON c.table_schema = t.table_schema AND c.table_name = t.table_name
    WHERE LOWER(c.table_schema) = LOWER('{{ schema }}')
      AND t.table_type IN ('MANAGED', 'EXTERNAL')
      AND c.data_type = 'STRING'
      AND REGEXP_LIKE(LOWER(c.column_name),
          '(text|content|description|body|message|comment|summary|abstract|document|article|note|transcript|review|chunk)')
)
SELECT concat(
    'WITH per_column AS (\n',
    array_join(collect_list(concat(
        '  SELECT ''', LOWER(table_name), ''' AS table_name, ''', column_name, ''' AS column_name, ',
        'percentile_approx(length(`', column_name, '`), 0.95) AS p95_chars ',
        'FROM {{ catalog }}.{{ schema }}.`', table_name, '` TABLESAMPLE ({{ sample_rows }} ROWS) ',
        'WHERE `', column_name, '` IS NOT NULL AND length(`', column_name, '`) > 0'
    )), '\n  UNION ALL\n'),
    '\n), per_table AS (\n',
    '  SELECT table_name, MAX(COALESCE(p95_chars, 0)) AS max_p95_chars FROM per_column GROUP BY table_name\n',
    ')\n',
    'SELECT COUNT_IF(max_p95_chars <= {{ max_chunk_chars }}) AS chunk_ready_tables, ',
    'COUNT(*) AS text_tables, ',
    'COUNT_IF(max_p95_chars <= {{ max_chunk_chars }})::DOUBLE / NULLIF(COUNT(*), 0) AS value ',
    'FROM per_table'
) AS stmt
FROM text_columns
```

For schemas with many wide text tables, run the generated statement on a Medium or larger warehouse; each branch is a sampled scan of one column.
