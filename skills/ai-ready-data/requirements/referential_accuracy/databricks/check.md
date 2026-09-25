# Check: referential_accuracy

Fraction of a table's values in one column that agree with an authoritative reference source when the two are joined on a key.

## Context

Table-scoped data scan against a reference. Two shapes are supported:

- **Value comparison (primary).** Join `{{ asset }}` to `{{ reference_table }}` on `{{ join_key }} = {{ reference_key }}` and compare `{{ column }}` to `{{ reference_column }}`. A row is accurate when the reference has a matching key and the two values are equal. Use it when the reference is a system of record for the same entities (a CRM export against the CRM, a derived customer segment against the golden record, a re-priced fact against the finance ledger).
- **Membership (variant).** The value itself must exist in the reference (`{{ column }} IN reference.{{ reference_key }}`). Use it for code lists and lookups: country codes, currency codes, postal codes. This overlaps with `categorical_validity`'s reference variant; prefer that requirement when the reference is a static vocabulary and this one when the reference is an authoritative record that changes.

`value = accurate_rows / rows_with_value`. Rows where `{{ column }}` is NULL are excluded from both counts (measured by `data_completeness`). Rows whose key has no match in the reference count as inaccurate in the primary shape, because "we cannot verify it" is not the same as "it is right"; the diagnostic splits unmatched from mismatched so the fix can tell a stale reference from wrong data.

Strength is **data**. Databricks does not know which table is authoritative; that is a declaration the caller makes by choosing the reference. Lakehouse Federation makes an external system of record (Postgres, SQL Server, Snowflake, Salesforce Data Cloud) queryable directly as `{{ reference_table }}` without copying it, which is the cleanest way to run this check against the actual source.

Placeholders:

- `{{ reference_table }}`: fully qualified, for example `prod.golden.customers` or a federated `sfdc_conn.public.account`. Required.
- `{{ reference_key }}`: key column in the reference. Required.
- `{{ reference_column }}`: column in the reference to compare against. Required for the primary shape; unused by the membership variant.
- `{{ join_key }}`: key column in `{{ asset }}`. Default: same name as `{{ reference_key }}`.
- `{{ column }}`: column in `{{ asset }}` under test. Required for the primary shape.
- `{{ comparison }}`: how to compare. Default `exact` (`<=>`, null-safe equality). Alternatives given inline: `normalized` (trim, lower) and `numeric_tolerance` with `{{ tolerance }}` (default `0.01`).
- `{{ sample_rows }}`: default 1,000,000.

If the reference has duplicate keys the join fans out and counts are inflated; the query aggregates the reference to one row per key first (`MAX`) so the denominator stays the source row count. If two reference rows disagree the `MAX` is arbitrary; the diagnostic reports duplicate keys in the reference.

Returns NULL when no source row has a non-null `{{ column }}`.

## SQL

### Value comparison against the reference (primary)

```sql
WITH ref AS (
    SELECT {{ reference_key }} AS k, MAX({{ reference_column }}) AS v
    FROM {{ reference_table }}
    WHERE {{ reference_key }} IS NOT NULL
    GROUP BY {{ reference_key }}
),
compared AS (
    SELECT
        COUNT(*)                                            AS rows_with_value,
        COUNT_IF(r.k IS NOT NULL AND s.{{ column }} <=> r.v) AS accurate_rows,
        COUNT_IF(r.k IS NULL)                               AS unmatched_rows
    FROM {{ catalog }}.{{ schema }}.{{ asset }} s
    LEFT JOIN ref r ON s.{{ join_key }} = r.k
    WHERE s.{{ column }} IS NOT NULL
)
SELECT
    '{{ asset }}'                                           AS table_name,
    '{{ column }}'                                          AS column_name,
    '{{ reference_table }}.{{ reference_column }}'          AS reference,
    accurate_rows,
    rows_with_value,
    unmatched_rows,
    accurate_rows::DOUBLE / NULLIF(rows_with_value, 0)      AS value
FROM compared
```

Comparison alternatives, replacing `s.{{ column }} <=> r.v`:

- normalized text: `LOWER(TRIM(CAST(s.{{ column }} AS STRING))) <=> LOWER(TRIM(CAST(r.v AS STRING)))`
- numeric tolerance: `ABS(s.{{ column }} - r.v) <= {{ tolerance }}`
- dates ignoring time: `CAST(s.{{ column }} AS DATE) <=> CAST(r.v AS DATE)`

### Membership in the reference (variant)

```sql
WITH src AS (
    SELECT {{ column }} AS v
    FROM {{ catalog }}.{{ schema }}.{{ asset }}
    WHERE {{ column }} IS NOT NULL
),
matched AS (
    SELECT COUNT(*) AS accurate_rows
    FROM src
    LEFT SEMI JOIN {{ reference_table }} r ON src.v = r.{{ reference_key }}
),
total AS (
    SELECT COUNT(*) AS rows_with_value FROM src
)
SELECT
    '{{ asset }}'                                           AS table_name,
    '{{ column }}'                                          AS column_name,
    '{{ reference_table }}.{{ reference_key }}'             AS reference,
    m.accurate_rows,
    t.rows_with_value,
    m.accurate_rows::DOUBLE / NULLIF(t.rows_with_value, 0)  AS value
FROM matched m CROSS JOIN total t
```

### Several columns against one reference row (variant)

When a table mirrors a record from the system of record, check the columns together in one scan. Each column gets its own row; the orchestrator averages or takes the minimum.

```sql
WITH ref AS (
    SELECT {{ reference_key }} AS k,
           MAX({{ ref_col_1 }}) AS v1, MAX({{ ref_col_2 }}) AS v2, MAX({{ ref_col_3 }}) AS v3
    FROM {{ reference_table }}
    WHERE {{ reference_key }} IS NOT NULL
    GROUP BY {{ reference_key }}
),
compared AS (
    SELECT
        COUNT_IF(s.{{ col_1 }} IS NOT NULL)                              AS n1,
        COUNT_IF(s.{{ col_1 }} IS NOT NULL AND s.{{ col_1 }} <=> r.v1)    AS ok1,
        COUNT_IF(s.{{ col_2 }} IS NOT NULL)                              AS n2,
        COUNT_IF(s.{{ col_2 }} IS NOT NULL AND s.{{ col_2 }} <=> r.v2)    AS ok2,
        COUNT_IF(s.{{ col_3 }} IS NOT NULL)                              AS n3,
        COUNT_IF(s.{{ col_3 }} IS NOT NULL AND s.{{ col_3 }} <=> r.v3)    AS ok3
    FROM {{ catalog }}.{{ schema }}.{{ asset }} s
    LEFT JOIN ref r ON s.{{ join_key }} = r.k
)
SELECT
    '{{ asset }}'                                   AS table_name,
    column_name,
    accurate_rows,
    rows_with_value,
    accurate_rows::DOUBLE / NULLIF(rows_with_value, 0) AS value
FROM compared
LATERAL VIEW inline(array(
    named_struct('column_name', '{{ col_1 }}', 'accurate_rows', ok1, 'rows_with_value', n1),
    named_struct('column_name', '{{ col_2 }}', 'accurate_rows', ok2, 'rows_with_value', n2),
    named_struct('column_name', '{{ col_3 }}', 'accurate_rows', ok3, 'rows_with_value', n3)
)) AS column_name, accurate_rows, rows_with_value
```

### Sampled (variant)

Samples the source only; the reference is read in full.

```sql
WITH ref AS (
    SELECT {{ reference_key }} AS k, MAX({{ reference_column }}) AS v
    FROM {{ reference_table }}
    WHERE {{ reference_key }} IS NOT NULL
    GROUP BY {{ reference_key }}
),
compared AS (
    SELECT
        COUNT(*)                                            AS rows_with_value,
        COUNT_IF(r.k IS NOT NULL AND s.{{ column }} <=> r.v) AS accurate_rows
    FROM {{ catalog }}.{{ schema }}.{{ asset }} TABLESAMPLE ({{ sample_rows }} ROWS) s
    LEFT JOIN ref r ON s.{{ join_key }} = r.k
    WHERE s.{{ column }} IS NOT NULL
)
SELECT
    '{{ asset }}'                                           AS table_name,
    '{{ column }}'                                          AS column_name,
    accurate_rows,
    rows_with_value,
    accurate_rows::DOUBLE / NULLIF(rows_with_value, 0)      AS value
FROM compared
```

`TABLESAMPLE (n ROWS)` is a prefix sample; recent rows, which are the ones most likely to disagree with a reference that has since been corrected, are under-represented.
