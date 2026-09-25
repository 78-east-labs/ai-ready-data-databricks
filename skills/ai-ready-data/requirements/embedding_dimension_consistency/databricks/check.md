# Check: embedding_dimension_consistency

Fraction of embedding columns in the schema whose non-null vectors all share one dimensionality.

## Context

Databricks stores embeddings as `ARRAY<FLOAT>` or `ARRAY<DOUBLE>`, and arrays carry no declared length. Nothing in the type system stops a 768-dimension vector and a 1024-dimension vector from landing in the same column, and Vector Search will reject the index sync or return garbage similarities when that happens. So this is a **data** check: for each embedding column, `COUNT(DISTINCT size(col))` over non-null rows must be exactly 1.

Embedding columns are found from `information_schema.columns` where `LOWER(full_data_type) IN ('array<float>', 'array<double>')` on base tables. As in `embedding_coverage`, that also matches non-embedding float arrays; the name-tightened filter (`embedding|vector|_vec|emb`) is available in the generator by uncommenting one line.

Per column the check is one aggregate scan. The schema-level score is assembled by the generator below into a single `UNION ALL` statement, so the orchestrator runs two statements total: the generator, then its output. The sampled variant uses `TABLESAMPLE ({{ sample_rows }} ROWS)`; a dimension mismatch that affects less than one row in a million can slip through a sample, so run the full variant on columns that back a production index.

What passes: a column with zero non-null vectors has no dimension to be inconsistent about and is excluded from the denominator (reported separately as `empty_columns`). A column whose vectors are all 1024 passes even if the neighbouring column is all 768; cross-column convergence is a different question, answered by the schema convergence variant, which is the closest analogue to how the upstream framework scored this on typed vector columns.

`size(NULL)` returns -1 on Spark, so the query filters `col IS NOT NULL` before aggregating. Zero-length arrays (`size = 0`) count as a distinct dimension and therefore fail the column, which is the right outcome: an empty vector is a broken embedding.

Placeholder default: `{{ sample_rows }}` = `1000000`.

Returns NULL (N/A) when the schema has no embedding columns with at least one non-null vector.

## SQL

### One embedding column (primary, per `{{ asset }}.{{ column }}`)

```sql
WITH dims AS (
    SELECT size({{ column }}) AS dim
    FROM {{ catalog }}.{{ schema }}.{{ asset }}
    WHERE {{ column }} IS NOT NULL
)
SELECT
    COUNT(DISTINCT dim)                                        AS distinct_dimensions,
    COUNT(*)                                                   AS embedded_rows,
    MIN(dim)                                                   AS min_dimension,
    MAX(dim)                                                   AS max_dimension,
    CASE WHEN COUNT(*) = 0 THEN NULL
         WHEN COUNT(DISTINCT dim) = 1 THEN 1.0 ELSE 0.0 END    AS value
FROM dims
```

### One embedding column, sampled (variant)

```sql
WITH dims AS (
    SELECT size({{ column }}) AS dim
    FROM {{ catalog }}.{{ schema }}.{{ asset }} TABLESAMPLE ({{ sample_rows }} ROWS)
    WHERE {{ column }} IS NOT NULL
)
SELECT
    COUNT(DISTINCT dim)                                        AS distinct_dimensions,
    COUNT(*)                                                   AS embedded_rows,
    MIN(dim)                                                   AS min_dimension,
    MAX(dim)                                                   AS max_dimension,
    CASE WHEN COUNT(*) = 0 THEN NULL
         WHEN COUNT(DISTINCT dim) = 1 THEN 1.0 ELSE 0.0 END    AS value
FROM dims
```

### Schema-wide (generator)

Emits one statement that evaluates every embedding column and returns `consistent_columns`, `embedding_columns`, `empty_columns`, `value`. Run the generator, then run the statement it returns. Remove `TABLESAMPLE` from the template for the exact variant.

```sql
WITH embedding_columns AS (
    SELECT c.table_name, c.column_name
    FROM {{ catalog }}.information_schema.columns c
    JOIN {{ catalog }}.information_schema.tables t
      ON c.table_schema = t.table_schema AND c.table_name = t.table_name
    WHERE LOWER(c.table_schema) = LOWER('{{ schema }}')
      AND t.table_type IN ('MANAGED', 'EXTERNAL')
      AND LOWER(c.full_data_type) IN ('array<float>', 'array<double>')
      -- AND REGEXP_LIKE(LOWER(c.column_name), '(embedding|embeddings|vector|_vec$|^emb$|_emb$)')
)
SELECT concat(
    'WITH per_column AS (\n',
    array_join(collect_list(concat(
        '  SELECT ''', LOWER(table_name), ''' AS table_name, ''', column_name, ''' AS column_name, ',
        'COUNT(DISTINCT size(`', column_name, '`)) AS distinct_dimensions, ',
        'COUNT(*) AS embedded_rows ',
        'FROM {{ catalog }}.{{ schema }}.`', table_name, '` TABLESAMPLE ({{ sample_rows }} ROWS) ',
        'WHERE `', column_name, '` IS NOT NULL'
    )), '\n  UNION ALL\n'),
    '\n)\n',
    'SELECT COUNT_IF(embedded_rows > 0 AND distinct_dimensions = 1) AS consistent_columns, ',
    'COUNT_IF(embedded_rows > 0) AS embedding_columns, ',
    'COUNT_IF(embedded_rows = 0) AS empty_columns, ',
    'COUNT_IF(embedded_rows > 0 AND distinct_dimensions = 1)::DOUBLE ',
    '/ NULLIF(COUNT_IF(embedded_rows > 0), 0) AS value ',
    'FROM per_column'
) AS stmt
FROM embedding_columns
```

If the generator returns NULL, the schema has no embedding columns and the check is N/A.

### Schema convergence on one dimension (variant, generator)

Stricter: fraction of embedding columns whose (single) dimension equals the most common dimension across the schema. Columns that are internally inconsistent fail automatically. Ties on the most common dimension break toward the larger dimension so the result is deterministic. Use this when every table in the schema is expected to feed the same Vector Search endpoint or the same model.

```sql
WITH embedding_columns AS (
    SELECT c.table_name, c.column_name
    FROM {{ catalog }}.information_schema.columns c
    JOIN {{ catalog }}.information_schema.tables t
      ON c.table_schema = t.table_schema AND c.table_name = t.table_name
    WHERE LOWER(c.table_schema) = LOWER('{{ schema }}')
      AND t.table_type IN ('MANAGED', 'EXTERNAL')
      AND LOWER(c.full_data_type) IN ('array<float>', 'array<double>')
)
SELECT concat(
    'WITH per_column AS (\n',
    array_join(collect_list(concat(
        '  SELECT ''', LOWER(table_name), ''' AS table_name, ''', column_name, ''' AS column_name, ',
        'COUNT(DISTINCT size(`', column_name, '`)) AS distinct_dimensions, ',
        'MAX(size(`', column_name, '`)) AS dimension, COUNT(*) AS embedded_rows ',
        'FROM {{ catalog }}.{{ schema }}.`', table_name, '` TABLESAMPLE ({{ sample_rows }} ROWS) ',
        'WHERE `', column_name, '` IS NOT NULL'
    )), '\n  UNION ALL\n'),
    '\n), populated AS (SELECT * FROM per_column WHERE embedded_rows > 0),\n',
    'common AS (SELECT dimension FROM populated WHERE distinct_dimensions = 1 ',
    'GROUP BY dimension ORDER BY COUNT(*) DESC, dimension DESC LIMIT 1)\n',
    'SELECT COUNT_IF(p.distinct_dimensions = 1 AND p.dimension = c.dimension) AS consistent_columns, ',
    'COUNT(*) AS embedding_columns, ',
    'COUNT_IF(p.distinct_dimensions = 1 AND p.dimension = c.dimension)::DOUBLE / NULLIF(COUNT(*), 0) AS value ',
    'FROM populated p CROSS JOIN common c'
) AS stmt
FROM embedding_columns
```
