# Check: categorical_validity

Fraction of non-null values in a categorical column that belong to the declared controlled vocabulary.

## Context

Column-scoped data scan. `value = valid_rows / non_null_rows`, where a value is valid when it is in `{{ allowed_values }}` (primary) or exists in a reference table (variant). NULLs are excluded from both counts; `data_completeness` covers them.

Strength is **data**. Databricks has no ENUM type. The closest native declaration is an enforced `CHECK (col IN (...))` constraint on a Delta table, which is why a column that already carries one scores 1.0 by construction (the constraint rejected violating writes when it was added and on every write since). The discovery variant reads such constraints from `information_schema.check_constraints` so the assessment can reuse a curated table's vocabulary against a raw upstream table with the same column name.

Placeholders:

- `{{ allowed_values }}`: comma-separated quoted literals, `'active','inactive','pending'`. No default. Must not contain `NULL`: `IN` with a NULL member returns UNKNOWN for non-members and inflates the score.
- `{{ reference_table }}`, `{{ reference_key }}`: fully qualified dimension or code table and its code column, for the reference variant. No default.
- `{{ sample_rows }}`: default 1,000,000.

The comparison is exact. `'Active'`, `'active '` and `'active'` are three values. That is deliberate: case and whitespace drift are real defects that break group-bys and one-hot encodings. The diagnostic separates "would match after `TRIM`/`LOWER`" from genuinely unknown codes so the fix can be a normalization instead of a deletion.

Returns NULL when the column has no non-null values.

## SQL

### Declared allowed values (primary)

```sql
WITH col_check AS (
    SELECT
        COUNT(*)                                                AS non_null_rows,
        COUNT_IF({{ column }} IN ({{ allowed_values }}))        AS valid_rows
    FROM {{ catalog }}.{{ schema }}.{{ asset }}
    WHERE {{ column }} IS NOT NULL
)
SELECT
    '{{ asset }}'                                       AS table_name,
    '{{ column }}'                                      AS column_name,
    valid_rows,
    non_null_rows,
    valid_rows::DOUBLE / NULLIF(non_null_rows, 0)       AS value
FROM col_check
```

### Values present in a reference table (variant)

Use when the vocabulary is maintained as data (a dimension, a code table, a Lakebase-synced lookup). A `LEFT SEMI JOIN` is the cheapest membership test in Spark and does not multiply rows if the reference has duplicates.

```sql
WITH src AS (
    SELECT {{ column }} AS v
    FROM {{ catalog }}.{{ schema }}.{{ asset }}
    WHERE {{ column }} IS NOT NULL
),
matched AS (
    SELECT COUNT(*) AS valid_rows
    FROM src
    LEFT SEMI JOIN {{ reference_table }} r
      ON src.v = r.{{ reference_key }}
),
total AS (
    SELECT COUNT(*) AS non_null_rows FROM src
)
SELECT
    '{{ asset }}'                                       AS table_name,
    '{{ column }}'                                      AS column_name,
    m.valid_rows,
    t.non_null_rows,
    m.valid_rows::DOUBLE / NULLIF(t.non_null_rows, 0)   AS value
FROM matched m CROSS JOIN total t
```

### Discover the vocabulary from a CHECK constraint (variant)

Finds a `CHECK (... col IN ('a','b',...) ...)` constraint on the table for this column, extracts the list, and emits the primary statement with it. Point `{{ source_asset }}` (default `{{ asset }}`) at the table you want to measure. Returns no rows when no such constraint exists.

```sql
WITH cc AS (
    SELECT tc.constraint_name, c.sql AS constraint_sql
    FROM {{ catalog }}.information_schema.table_constraints tc
    JOIN {{ catalog }}.information_schema.check_constraints c
      ON c.constraint_name = tc.constraint_name
     AND c.constraint_schema = tc.constraint_schema
    WHERE LOWER(tc.table_schema) = LOWER('{{ schema }}')
      AND LOWER(tc.table_name)   = LOWER('{{ asset }}')
      AND tc.constraint_type = 'CHECK'
),
parsed AS (
    SELECT
        constraint_name,
        constraint_sql,
        NULLIF(regexp_extract(constraint_sql,
            concat('(?i)`?', '{{ column }}', '`?\\s+in\\s*\\(([^)]*)\\)'), 1), '') AS allowed_values
    FROM cc
)
SELECT
    constraint_name,
    constraint_sql,
    allowed_values,
    size(split(allowed_values, ',')) AS vocabulary_size,
    concat(
        'WITH col_check AS (SELECT COUNT(*) AS non_null_rows, COUNT_IF(`{{ column }}` IN (', allowed_values,
        ')) AS valid_rows FROM {{ catalog }}.{{ schema }}.`{{ source_asset }}` WHERE `{{ column }}` IS NOT NULL) ',
        'SELECT ''{{ source_asset }}'' AS table_name, ''{{ column }}'' AS column_name, valid_rows, non_null_rows, ',
        'valid_rows::DOUBLE / NULLIF(non_null_rows, 0) AS value FROM col_check'
    ) AS stmt
FROM parsed
WHERE allowed_values IS NOT NULL
```

The extractor takes everything between the parentheses after `IN`, so a list containing a `)` inside a quoted literal is truncated; check `vocabulary_size` against what you expect.

### Sampled (variant)

```sql
WITH col_check AS (
    SELECT
        COUNT(*)                                                AS non_null_rows,
        COUNT_IF({{ column }} IN ({{ allowed_values }}))        AS valid_rows
    FROM {{ catalog }}.{{ schema }}.{{ asset }} TABLESAMPLE ({{ sample_rows }} ROWS)
    WHERE {{ column }} IS NOT NULL
)
SELECT
    '{{ asset }}'                                       AS table_name,
    '{{ column }}'                                      AS column_name,
    valid_rows,
    non_null_rows,
    valid_rows::DOUBLE / NULLIF(non_null_rows, 0)       AS value
FROM col_check
```

`TABLESAMPLE (n ROWS)` is a prefix sample, not random; legacy codes that live only in old files are over-represented, new codes under-represented. Good enough for triage.
