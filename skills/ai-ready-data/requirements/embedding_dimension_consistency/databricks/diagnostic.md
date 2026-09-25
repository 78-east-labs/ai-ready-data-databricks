# Diagnostic: embedding_dimension_consistency

One row per embedding column with its observed dimensions, row counts per dimension, the schema's most common dimension, and a status; plus a per-column breakdown of which rows carry the odd-sized vectors.

## Context

The first generator emits a statement that profiles every embedding column in the schema (same scoping as the check). Each output row shows `distinct_dimensions`, the full histogram of dimensions as `dimension_counts` (a map from dimension to row count), the column's comment (where the model name usually lives), and a status:

- `EMPTY`: no non-null vectors. Excluded from the score; the column exists but was never populated.
- `MIXED`: more than one dimension inside the column. Always a defect.
- `OFF_SCHEMA`: internally consistent but different from the schema's most common dimension. A defect only if the schema is meant to share one model.
- `CONSISTENT`: one dimension, matching the schema mode.

Sorted `MIXED` first, then `OFF_SCHEMA`, then by row count descending.

The second query is the drill-down for one `MIXED` column: which dimensions appear, how many rows each, and the earliest and latest rows per dimension using `{{ updated_at_column }}` when the table has one. A dimension that only appears in recent rows means the writer switched models without a backfill.

## SQL

### Schema-wide profile (generator)

Run the generator, then run the statement it returns.

```sql
WITH embedding_columns AS (
    SELECT c.table_name, c.column_name, c.full_data_type,
           replace(COALESCE(c.comment, ''), '''', '''''') AS column_comment
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
        '''', LOWER(full_data_type), ''' AS full_data_type, ''', column_comment, ''' AS column_comment, ',
        'COALESCE(SUM(cnt), 0) AS embedded_rows, ',
        'COUNT(dim) AS distinct_dimensions, ',
        'map_from_entries(collect_list(struct(dim, cnt))) AS dimension_counts ',
        'FROM (SELECT size(`', column_name, '`) AS dim, COUNT(*) AS cnt ',
        '      FROM {{ catalog }}.{{ schema }}.`', table_name, '` TABLESAMPLE ({{ sample_rows }} ROWS) ',
        '      WHERE `', column_name, '` IS NOT NULL GROUP BY size(`', column_name, '`))'
    )), '\n  UNION ALL\n'),
    '\n), populated AS (SELECT * FROM per_column WHERE embedded_rows > 0),\n',
    'common AS (SELECT try_element_at(map_keys(dimension_counts), 1) AS dimension FROM populated ',
    'WHERE distinct_dimensions = 1 GROUP BY 1 ORDER BY COUNT(*) DESC, 1 DESC LIMIT 1)\n',
    'SELECT p.table_name, p.column_name, p.full_data_type, p.embedded_rows, ',
    'p.distinct_dimensions, p.dimension_counts, c.dimension AS schema_common_dimension, ',
    'CASE WHEN p.embedded_rows = 0 THEN ''EMPTY'' ',
    '     WHEN p.distinct_dimensions > 1 THEN ''MIXED'' ',
    '     WHEN try_element_at(map_keys(p.dimension_counts), 1) <> c.dimension THEN ''OFF_SCHEMA'' ',
    '     ELSE ''CONSISTENT'' END AS status, ',
    'p.column_comment ',
    'FROM per_column p LEFT JOIN common c ON true ',
    'ORDER BY CASE WHEN p.distinct_dimensions > 1 THEN 1 ',
    '              WHEN p.embedded_rows > 0 AND try_element_at(map_keys(p.dimension_counts), 1) <> c.dimension THEN 2 ',
    '              WHEN p.embedded_rows = 0 THEN 4 ELSE 3 END, p.embedded_rows DESC'
) AS stmt
FROM embedding_columns
```

The inner subquery groups by dimension, so `SUM(cnt)` is the row count and `COUNT(dim)` is the number of distinct dimensions. An empty column yields one row with `embedded_rows = 0` and an empty map.

### Drill-down for one MIXED column

```sql
SELECT
    size({{ column }})                     AS dimension,
    COUNT(*)                               AS row_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS pct_rows,
    MIN({{ updated_at_column }})           AS first_seen,
    MAX({{ updated_at_column }})           AS last_seen
FROM {{ catalog }}.{{ schema }}.{{ asset }}
WHERE {{ column }} IS NOT NULL
GROUP BY size({{ column }})
ORDER BY row_count DESC
```

If the table has no timestamp column, drop `first_seen` / `last_seen` or use `_metadata.file_modification_time` as the proxy.

### Sample rows carrying the minority dimension

```sql
WITH counts AS (
    SELECT size({{ column }}) AS dimension, COUNT(*) AS n
    FROM {{ catalog }}.{{ schema }}.{{ asset }}
    WHERE {{ column }} IS NOT NULL
    GROUP BY size({{ column }})
),
majority AS (SELECT dimension FROM counts ORDER BY n DESC, dimension DESC LIMIT 1)
SELECT t.{{ key_column }}, size(t.{{ column }}) AS dimension
FROM {{ catalog }}.{{ schema }}.{{ asset }} t
CROSS JOIN majority m
WHERE t.{{ column }} IS NOT NULL
  AND size(t.{{ column }}) <> m.dimension
LIMIT 100
```
