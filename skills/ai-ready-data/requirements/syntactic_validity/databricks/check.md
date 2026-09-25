# Check: syntactic_validity

Fraction of raw records in a column that parse without structural errors: well-formed JSON payloads, or ingested rows with nothing rescued or marked corrupt.

## Context

Column-scoped data scan. Two shapes, chosen by what the column is:

- **A STRING column holding serialized JSON.** A value is valid when `try_parse_json({{ column }})` returns non-NULL. `value = valid_rows / non_null_rows`; NULLs are excluded (a missing payload is `data_completeness`). Note that any JSON literal parses, including scalars like `42` or `"text"`, so this only means something on columns whose contract is "a JSON document". A stricter variant requires an object or array.
- **An ingested table with a rescue or corrupt-record column.** A row is valid when `{{ rescue_column }}` is NULL. This is the right shape for bronze tables loaded by Auto Loader (`_rescued_data`) or from CSV/JSON with `columnNameOfCorruptRecord` (`_corrupt_record`), where the ingestion engine already did the parsing and recorded what failed. `value = rows_without_rescue / total_rows`.

A VARIANT column has no invalid values by construction (parsing happened at write time and failures were rescued or rejected then), so for VARIANT columns use the rescue shape or `is_variant_null()` for a payload that parsed to JSON `null`.

Strength is **data**. `try_parse_json()` and the VARIANT type need DBR 15.3+ (all current SQL warehouses). On older runtimes the fallback uses `get_json_object({{ column }}, '$')`, which returns NULL for input it cannot parse; it accepts a few things a strict parser rejects (trailing garbage after a complete document in some versions), so treat it as an approximation.

Placeholders:

- `{{ rescue_column }}`: default `_rescued_data`; pass `_corrupt_record` for CSV/JSON tables that used that option.
- `{{ sample_rows }}`: default 1,000,000.

CSV rows with the wrong number of delimiters are not visible in a STRING column after the fact; they are visible only as `_corrupt_record` or as rescued fields at ingest, which is why the rescue shape exists.

Returns NULL when there are no non-null values (JSON shape) or no rows (rescue shape).

## SQL

### JSON payload parses (primary)

```sql
WITH col_check AS (
    SELECT
        COUNT(*)                                                    AS non_null_rows,
        COUNT_IF(try_parse_json({{ column }}) IS NOT NULL)          AS valid_rows
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

### JSON payload is an object or array (variant)

Stricter: a bare scalar or string does not count as a document.

```sql
WITH col_check AS (
    SELECT
        COUNT(*)                                                                    AS non_null_rows,
        COUNT_IF(schema_of_variant(try_parse_json({{ column }})) RLIKE '^(OBJECT|ARRAY|STRUCT)') AS valid_rows
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

`schema_of_variant` returns a type string such as `OBJECT<...>`, `ARRAY<...>`, `STRING`, `BIGINT`; if your runtime prints `STRUCT<...>` for objects the pattern covers it. Confirm with `SELECT schema_of_variant(parse_json('{"a":1}'))`.

### Fallback without try_parse_json (variant)

DBR below 15.3.

```sql
WITH col_check AS (
    SELECT
        COUNT(*)                                                    AS non_null_rows,
        COUNT_IF(get_json_object({{ column }}, '$') IS NOT NULL)    AS valid_rows
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

### Ingested rows with nothing rescued or corrupt (variant)

Table-level. The right shape for bronze tables.

```sql
SELECT
    '{{ asset }}'                                       AS table_name,
    '{{ rescue_column }}'                               AS rescue_column,
    COUNT_IF({{ rescue_column }} IS NULL)               AS valid_rows,
    COUNT(*)                                            AS total_rows,
    COUNT_IF({{ rescue_column }} IS NULL)::DOUBLE
        / NULLIF(COUNT(*), 0)                           AS value
FROM {{ catalog }}.{{ schema }}.{{ asset }}
```

To find which tables in the schema have a rescue column at all:

```sql
SELECT LOWER(c.table_name) AS table_name, c.column_name AS rescue_column
FROM {{ catalog }}.information_schema.columns c
JOIN {{ catalog }}.information_schema.tables t
  ON t.table_schema = c.table_schema AND t.table_name = c.table_name
WHERE LOWER(c.table_schema) = LOWER('{{ schema }}')
  AND t.table_type IN ('MANAGED', 'EXTERNAL')
  AND LOWER(c.column_name) IN ('_rescued_data', '_corrupt_record')
ORDER BY table_name
```

### Sampled (variant)

```sql
WITH col_check AS (
    SELECT
        COUNT(*)                                                    AS non_null_rows,
        COUNT_IF(try_parse_json({{ column }}) IS NOT NULL)          AS valid_rows
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

Malformed payloads come from producer changes and arrive in the newest files, which `TABLESAMPLE (n ROWS)` reads last. Triage only.
