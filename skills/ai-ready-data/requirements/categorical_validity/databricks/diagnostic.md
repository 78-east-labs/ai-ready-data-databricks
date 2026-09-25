# Diagnostic: categorical_validity

Distinct value distribution of the column, each value labelled as allowed, fixable by normalization, or unknown.

## Context

One row per distinct non-null value with its row count and share, capped at 200. The `status` column classifies each value:

- `ALLOWED`: exact member of `{{ allowed_values }}`.
- `CASE_OR_WHITESPACE`: matches an allowed value after `LOWER(TRIM(...))`. A normalization `UPDATE` fixes these without judgment calls.
- `NEAR_MATCH`: within Levenshtein distance 2 of an allowed value (typos such as `activ`, `pendng`). Needs a human to confirm the mapping.
- `UNKNOWN`: nothing close. Either a legitimate new category (extend the vocabulary) or garbage (null or quarantine).

A column with more than 200 distinct values is probably not categorical; check `schema_type_coverage` instead.

`{{ allowed_values }}` has no default. To profile before any vocabulary exists, pass `''` and every row comes back `UNKNOWN`, which still gives you the distribution.

## SQL

### Value distribution with classification

```sql
WITH allowed AS (
    SELECT explode(array({{ allowed_values }})) AS a
),
dist AS (
    SELECT
        {{ column }}                                    AS category_value,
        COUNT(*)                                        AS row_count
    FROM {{ catalog }}.{{ schema }}.{{ asset }}
    WHERE {{ column }} IS NOT NULL
    GROUP BY {{ column }}
),
classified AS (
    SELECT
        d.category_value,
        d.row_count,
        MAX(CASE WHEN d.category_value = a.a THEN 1 ELSE 0 END)                                          AS exact_hit,
        MAX(CASE WHEN LOWER(TRIM(CAST(d.category_value AS STRING))) = LOWER(TRIM(a.a)) THEN a.a END)     AS normalized_match,
        MIN(CASE WHEN levenshtein(LOWER(TRIM(CAST(d.category_value AS STRING))), LOWER(a.a)) <= 2
                 THEN a.a END)                                                                           AS near_match
    FROM dist d
    LEFT JOIN allowed a ON TRUE
    GROUP BY d.category_value, d.row_count
)
SELECT
    category_value,
    row_count,
    row_count::DOUBLE / SUM(row_count) OVER ()          AS share_of_rows,
    CASE
        WHEN exact_hit = 1               THEN 'ALLOWED'
        WHEN normalized_match IS NOT NULL THEN 'CASE_OR_WHITESPACE'
        WHEN near_match IS NOT NULL       THEN 'NEAR_MATCH'
        ELSE 'UNKNOWN'
    END                                                 AS status,
    COALESCE(normalized_match, near_match)              AS suggested_value,
    length(CAST(category_value AS STRING)) <> length(TRIM(CAST(category_value AS STRING))) AS has_edge_whitespace
FROM classified
ORDER BY
    CASE WHEN exact_hit = 1 THEN 3 WHEN normalized_match IS NOT NULL THEN 1 WHEN near_match IS NOT NULL THEN 2 ELSE 0 END,
    row_count DESC
LIMIT 200
```

Unknown values sort first, then case/whitespace variants (cheapest to fix in bulk), then near matches, then allowed values.

### Values missing from a reference table

Same idea for the reference-table variant of the check: distinct source values with no match, worst-first.

```sql
SELECT
    s.{{ column }}                                      AS category_value,
    COUNT(*)                                            AS row_count,
    MAX(r.{{ reference_key }})                          AS case_insensitive_match
FROM {{ catalog }}.{{ schema }}.{{ asset }} s
LEFT JOIN {{ reference_table }} r
  ON LOWER(TRIM(CAST(s.{{ column }} AS STRING))) = LOWER(TRIM(CAST(r.{{ reference_key }} AS STRING)))
WHERE s.{{ column }} IS NOT NULL
  AND NOT EXISTS (
      SELECT 1 FROM {{ reference_table }} x WHERE x.{{ reference_key }} = s.{{ column }}
  )
GROUP BY s.{{ column }}
ORDER BY row_count DESC
LIMIT 200
```

A non-null `case_insensitive_match` means normalization fixes the value; NULL means it is absent from the reference entirely.
