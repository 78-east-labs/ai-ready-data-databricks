# Diagnostic: referential_accuracy

Splits inaccurate rows into unmatched keys, exact mismatches and near-mismatches, lists the most frequent disagreements, and reports whether the reference itself is fit to be a reference.

## Context

Three queries:

1. **Disagreement breakdown.** Of the rows with a value, how many match exactly, match after normalization (case, whitespace, numeric tolerance, date-only), differ outright, or have no key in the reference. Normalized matches point at a formatting fix; outright differences at a stale copy or a wrong transformation; unmatched keys at a stale reference or a key-format mismatch.
2. **Top disagreements.** The most common (source value, reference value) pairs among mismatches, with counts. A handful of pairs covering most mismatches is a mapping error (one code renamed); a long tail is drift between two systems that are both being edited.
3. **Reference health.** Duplicate keys, NULL keys and NULL values in the reference. A reference with duplicate keys that disagree cannot adjudicate anything, and the check's `MAX` picks one arbitrarily.

Placeholders as in the check, plus `{{ key_columns }}` for the sample rows (default `{{ join_key }}`) and `{{ tolerance }}` (default 0.01) for the numeric closeness test.

## SQL

### Disagreement breakdown

```sql
WITH ref AS (
    SELECT {{ reference_key }} AS k, MAX({{ reference_column }}) AS v
    FROM {{ reference_table }}
    WHERE {{ reference_key }} IS NOT NULL
    GROUP BY {{ reference_key }}
),
joined AS (
    SELECT s.{{ column }} AS sv, r.v AS rv, r.k IS NOT NULL AS matched
    FROM {{ catalog }}.{{ schema }}.{{ asset }} s
    LEFT JOIN ref r ON s.{{ join_key }} = r.k
    WHERE s.{{ column }} IS NOT NULL
)
SELECT
    '{{ asset }}'                                                                       AS table_name,
    '{{ column }}'                                                                      AS column_name,
    COUNT(*)                                                                            AS rows_with_value,
    COUNT_IF(NOT matched)                                                               AS unmatched_key,
    COUNT_IF(matched AND sv <=> rv)                                                     AS exact_match,
    COUNT_IF(matched AND NOT (sv <=> rv)
             AND LOWER(TRIM(CAST(sv AS STRING))) <=> LOWER(TRIM(CAST(rv AS STRING))))  AS match_after_normalization,
    COUNT_IF(matched AND NOT (sv <=> rv)
             AND TRY_CAST(sv AS DOUBLE) IS NOT NULL AND TRY_CAST(rv AS DOUBLE) IS NOT NULL
             AND ABS(TRY_CAST(sv AS DOUBLE) - TRY_CAST(rv AS DOUBLE)) <= {{ tolerance }}) AS match_within_tolerance,
    COUNT_IF(matched AND rv IS NULL)                                                    AS reference_value_null,
    COUNT_IF(matched AND rv IS NOT NULL AND NOT (sv <=> rv)
             AND NOT (LOWER(TRIM(CAST(sv AS STRING))) <=> LOWER(TRIM(CAST(rv AS STRING))))
             AND NOT (TRY_CAST(sv AS DOUBLE) IS NOT NULL AND TRY_CAST(rv AS DOUBLE) IS NOT NULL
                      AND ABS(TRY_CAST(sv AS DOUBLE) - TRY_CAST(rv AS DOUBLE)) <= {{ tolerance }})) AS outright_mismatch
FROM joined
```

### Top disagreements with sample keys

```sql
WITH ref AS (
    SELECT {{ reference_key }} AS k, MAX({{ reference_column }}) AS v
    FROM {{ reference_table }}
    WHERE {{ reference_key }} IS NOT NULL
    GROUP BY {{ reference_key }}
)
SELECT
    s.{{ column }}                                          AS source_value,
    r.v                                                     AS reference_value,
    CASE WHEN r.k IS NULL THEN 'UNMATCHED_KEY' ELSE 'MISMATCH' END AS status,
    COUNT(*)                                                AS rows,
    slice(collect_set(CAST(s.{{ key_columns }} AS STRING)), 1, 5) AS sample_keys
FROM {{ catalog }}.{{ schema }}.{{ asset }} s
LEFT JOIN ref r ON s.{{ join_key }} = r.k
WHERE s.{{ column }} IS NOT NULL
  AND NOT (r.k IS NOT NULL AND s.{{ column }} <=> r.v)
GROUP BY s.{{ column }}, r.v, r.k IS NULL
ORDER BY rows DESC
LIMIT 100
```

### Reference health

```sql
SELECT
    '{{ reference_table }}'                                 AS reference_table,
    COUNT(*)                                                AS reference_rows,
    COUNT(DISTINCT {{ reference_key }})                     AS distinct_keys,
    COUNT(*) - COUNT(DISTINCT {{ reference_key }})          AS surplus_rows_on_key,
    COUNT_IF({{ reference_key }} IS NULL)                   AS null_keys,
    COUNT_IF({{ reference_column }} IS NULL)                AS null_reference_values,
    (SELECT COUNT(*) FROM (
        SELECT {{ reference_key }}
        FROM {{ reference_table }}
        GROUP BY {{ reference_key }}
        HAVING COUNT(DISTINCT {{ reference_column }}) > 1
    ))                                                      AS keys_with_conflicting_values
FROM {{ reference_table }}
```

`keys_with_conflicting_values > 0` means the reference disagrees with itself for those keys; fix the reference (or pick a deterministic rule such as latest `updated_at`) before trusting the check.

If the reference is a federated table, this query runs on the remote system through Lakehouse Federation; keep it to the key and value columns so predicate pushdown keeps it cheap.
