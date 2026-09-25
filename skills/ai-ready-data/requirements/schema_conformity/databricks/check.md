# Check: schema_conformity

Fraction of rows in a column that conform to the declared schema: nothing was rescued at ingest, and the value casts cleanly to its intended type.

## Context

Column-scoped data scan. A row conforms when both hold:

- `{{ rescue_column }}` is NULL for the row. Auto Loader, `read_files` and `from_json` in rescue mode put any field that did not fit the declared schema (type mismatch, unexpected column, case mismatch) into `_rescued_data` as a JSON string instead of failing. A non-null value there means the row arrived with something the schema could not hold.
- `TRY_CAST({{ column }} AS {{ target_type }})` is not NULL when `{{ column }}` is not NULL. This catches values stored in a permissive type (a STRING holding numbers, dates, booleans) that would break when the consumer casts them.

`value = conforming_rows / total_rows`. Rows where `{{ column }}` is NULL count as conforming for the cast test (NULL casts to NULL without error); completeness is a separate requirement.

Strength is **data**. Two Databricks-native inputs make it precise: the rescue column is written by the ingestion engine itself, and `TRY_CAST` is the same function the consumer will use.

Placeholders:

- `{{ target_type }}`: the type the column should be, for example `BIGINT`, `DATE`, `TIMESTAMP`, `DECIMAL(18,2)`, `BOOLEAN`. Default: the column's own `full_data_type` from `information_schema.columns`, in which case the cast is trivially valid and only the rescue test contributes. The check is most useful when the caller supplies the intended type for a column that is declared STRING; the discovery variant proposes one from the column name.
- `{{ rescue_column }}`: default `_rescued_data`. If the table has no such column (the schema query below tells you), use the cast-only variant. Tables read from CSV with `columnNameOfCorruptRecord` use `_corrupt_record` instead; pass that name.
- `{{ sample_rows }}`: default 1,000,000.

`TRY_CAST` semantics matter: `'1e3'` casts to `DOUBLE` but not to `BIGINT`; `'2024-13-01'` fails as `DATE`; `'yes'` fails as `BOOLEAN` (`'true'`, `'t'`, `'1'`, `'y'` succeed). Pick the type the consumer actually uses. For a STRING declared as the intended type already (a `DATE` column), `TRY_CAST` is a no-op and the check reduces to the rescue test.

Returns NULL when the table is empty.

## SQL

### Rescue column and type cast (primary)

```sql
WITH col_check AS (
    SELECT
        COUNT(*)                                                                    AS total_rows,
        COUNT_IF({{ rescue_column }} IS NULL
                 AND ({{ column }} IS NULL OR TRY_CAST({{ column }} AS {{ target_type }}) IS NOT NULL)) AS conforming_rows,
        COUNT_IF({{ rescue_column }} IS NOT NULL)                                   AS rescued_rows,
        COUNT_IF({{ column }} IS NOT NULL AND TRY_CAST({{ column }} AS {{ target_type }}) IS NULL) AS uncastable_rows
    FROM {{ catalog }}.{{ schema }}.{{ asset }}
)
SELECT
    '{{ asset }}'                                           AS table_name,
    '{{ column }}'                                          AS column_name,
    '{{ target_type }}'                                     AS target_type,
    conforming_rows,
    total_rows,
    rescued_rows,
    uncastable_rows,
    conforming_rows::DOUBLE / NULLIF(total_rows, 0)         AS value
FROM col_check
```

### Cast only, no rescue column (variant)

For tables that were not ingested with a rescue column.

```sql
WITH col_check AS (
    SELECT
        COUNT(*)                                                                    AS total_rows,
        COUNT_IF({{ column }} IS NULL OR TRY_CAST({{ column }} AS {{ target_type }}) IS NOT NULL) AS conforming_rows
    FROM {{ catalog }}.{{ schema }}.{{ asset }}
)
SELECT
    '{{ asset }}'                                           AS table_name,
    '{{ column }}'                                          AS column_name,
    '{{ target_type }}'                                     AS target_type,
    conforming_rows,
    total_rows,
    conforming_rows::DOUBLE / NULLIF(total_rows, 0)         AS value
FROM col_check
```

### Rescue column only, table level (variant)

Table-scoped: fraction of rows with nothing rescued, regardless of column. Cheapest signal for bronze tables and a good first pass across a schema.

```sql
SELECT
    '{{ asset }}'                                           AS table_name,
    COUNT_IF({{ rescue_column }} IS NULL)                   AS conforming_rows,
    COUNT(*)                                                AS total_rows,
    COUNT_IF({{ rescue_column }} IS NULL)::DOUBLE
        / NULLIF(COUNT(*), 0)                               AS value
FROM {{ catalog }}.{{ schema }}.{{ asset }}
```

### Discover target types and emit per-column statements (variant)

Finds the table's rescue column (if any) and, for each STRING column whose name implies a stricter type, proposes a `{{ target_type }}` and emits the primary statement. Proposals: `_id`/`_count`/`_qty` → `BIGINT`, `_amount`/`_price`/`_total`/`_rate`/`_pct` → `DECIMAL(18,4)`, `_date` → `DATE`, `_at`/`_ts`/`_time`/`_timestamp` → `TIMESTAMP`, `is_`/`has_`/`_flag` → `BOOLEAN`. UUID-named identifiers are left as STRING. Review the proposals; a `zip_code` stored as STRING is correct.

```sql
WITH cols AS (
    SELECT column_name, data_type, full_data_type, ordinal_position
    FROM {{ catalog }}.information_schema.columns
    WHERE LOWER(table_schema) = LOWER('{{ schema }}')
      AND LOWER(table_name)   = LOWER('{{ asset }}')
),
rescue AS (
    SELECT MAX(column_name) AS rescue_column
    FROM cols
    WHERE LOWER(column_name) IN ('_rescued_data', '_corrupt_record')
),
proposed AS (
    SELECT
        c.column_name,
        c.full_data_type,
        CASE
            WHEN REGEXP_LIKE(LOWER(c.column_name), '(uuid|guid)')                          THEN NULL
            WHEN REGEXP_LIKE(LOWER(c.column_name), '(^is_|^has_|_flag$|^flag_)')           THEN 'BOOLEAN'
            WHEN REGEXP_LIKE(LOWER(c.column_name), '(_at$|_ts$|_time$|_timestamp$)')       THEN 'TIMESTAMP'
            WHEN REGEXP_LIKE(LOWER(c.column_name), '(_date$|^date_|_dt$)')                 THEN 'DATE'
            WHEN REGEXP_LIKE(LOWER(c.column_name), '(_amount$|_amt$|_price$|_total$|_rate$|_pct$|_percent$|_cost$|_revenue$)') THEN 'DECIMAL(18,4)'
            WHEN REGEXP_LIKE(LOWER(c.column_name), '(_count$|_cnt$|_qty$|_quantity$|_num$|_number$)') THEN 'BIGINT'
            WHEN REGEXP_LIKE(LOWER(c.column_name), '(^id$|_id$)')                          THEN 'BIGINT'
        END AS target_type
    FROM cols c
    WHERE c.data_type = 'STRING'
)
SELECT
    p.column_name,
    p.full_data_type                                        AS declared_type,
    p.target_type,
    r.rescue_column,
    concat(
        'WITH col_check AS (SELECT COUNT(*) AS total_rows, COUNT_IF(',
        CASE WHEN r.rescue_column IS NOT NULL THEN concat('`', r.rescue_column, '` IS NULL AND ') ELSE '' END,
        '(`', p.column_name, '` IS NULL OR TRY_CAST(`', p.column_name, '` AS ', p.target_type, ') IS NOT NULL)) AS conforming_rows ',
        'FROM {{ catalog }}.{{ schema }}.`{{ asset }}`) ',
        'SELECT ''{{ asset }}'' AS table_name, ''', p.column_name, ''' AS column_name, ''', p.target_type, ''' AS target_type, ',
        'conforming_rows, total_rows, conforming_rows::DOUBLE / NULLIF(total_rows, 0) AS value FROM col_check'
    ) AS stmt
FROM proposed p
CROSS JOIN rescue r
WHERE p.target_type IS NOT NULL
ORDER BY p.column_name
```

### Sampled (variant)

```sql
WITH col_check AS (
    SELECT
        COUNT(*)                                                                    AS total_rows,
        COUNT_IF({{ rescue_column }} IS NULL
                 AND ({{ column }} IS NULL OR TRY_CAST({{ column }} AS {{ target_type }}) IS NOT NULL)) AS conforming_rows
    FROM {{ catalog }}.{{ schema }}.{{ asset }} TABLESAMPLE ({{ sample_rows }} ROWS)
)
SELECT
    '{{ asset }}'                                           AS table_name,
    '{{ column }}'                                          AS column_name,
    conforming_rows,
    total_rows,
    conforming_rows::DOUBLE / NULLIF(total_rows, 0)         AS value
FROM col_check
```

Schema drift arrives with new loads and `TABLESAMPLE (n ROWS)` reads the oldest files first, so a sample can score 1.0 on a table whose last week of data is all rescued. Triage only.
