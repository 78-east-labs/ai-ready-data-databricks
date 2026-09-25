# Diagnostic: anonymization_effectiveness

Per-column list of PII candidates with their mask (if any), the mask function body, and any privacy tags, so a human can decide which columns need a mask and whether existing masks actually redact.

## Context

Reuses the check's candidate CTE (same `{{ pii_patterns }}` default) so the population matches. For each candidate it shows:

- `mask_function`: the fully qualified function attached through `SET MASK`, from `information_schema.column_masks`.
- `mask_body`: the function's `routine_definition` from `information_schema.routines`, joined on catalog, schema and name. Read it. A body that returns the input unconditionally, or gates only on `current_user()` equality with a hard-coded email, is not effective anonymization even though it scores as masked.
- `mask_col_usage`: extra columns the mask reads (for example a `country` column used to decide whether to redact).
- `privacy_tags`: column tags whose key is `pii`, `sensitivity`, `anonymized`, `privacy_category` or the configured `{{ pii_tag_key }}`, with values. Tags do not count toward the score.
- `status`: `MASKED`, `TAGGED_NOT_MASKED` or `UNPROTECTED`.

False positives are expected from a name heuristic (`address` also matches `ip_address_hash`, `shipping_address_id`). Note them; do not mask surrogate keys.

Sorted so unprotected columns come first, then tagged-but-unmasked, then masked.

## SQL

```sql
WITH pii_columns AS (
    SELECT LOWER(c.table_name) AS table_name,
           LOWER(c.column_name) AS column_name,
           c.column_name        AS column_name_cased,
           c.data_type,
           c.comment
    FROM {{ catalog }}.information_schema.columns c
    JOIN {{ catalog }}.information_schema.tables t
      ON c.table_schema = t.table_schema AND c.table_name = t.table_name
    WHERE LOWER(c.table_schema) = LOWER('{{ schema }}')
      AND t.table_type IN ('MANAGED', 'EXTERNAL')
      AND REGEXP_LIKE(LOWER(c.column_name), '{{ pii_patterns }}')
),
masks AS (
    SELECT LOWER(cm.table_name)  AS table_name,
           LOWER(cm.column_name) AS column_name,
           concat_ws('.', cm.mask_catalog, cm.mask_schema, cm.mask_name) AS mask_function,
           cm.mask_col_usage,
           r.routine_definition  AS mask_body
    FROM {{ catalog }}.information_schema.column_masks cm
    LEFT JOIN system.information_schema.routines r
      ON  LOWER(r.routine_catalog) = LOWER(cm.mask_catalog)
      AND LOWER(r.routine_schema)  = LOWER(cm.mask_schema)
      AND LOWER(r.routine_name)    = LOWER(cm.mask_name)
    WHERE LOWER(cm.schema_name) = LOWER('{{ schema }}')
),
privacy_tags AS (
    SELECT LOWER(table_name)  AS table_name,
           LOWER(column_name) AS column_name,
           array_sort(collect_list(concat(tag_name, '=', tag_value))) AS privacy_tags
    FROM {{ catalog }}.information_schema.column_tags
    WHERE LOWER(schema_name) = LOWER('{{ schema }}')
      AND LOWER(tag_name) IN ('pii', 'sensitivity', 'anonymized', 'privacy_category',
                              LOWER('{{ pii_tag_key }}'))
    GROUP BY LOWER(table_name), LOWER(column_name)
)
SELECT
    p.table_name,
    p.column_name_cased AS column_name,
    p.data_type,
    m.mask_function,
    m.mask_col_usage,
    m.mask_body,
    t.privacy_tags,
    p.comment,
    CASE
        WHEN m.mask_function IS NOT NULL THEN 'MASKED'
        WHEN t.privacy_tags  IS NOT NULL THEN 'TAGGED_NOT_MASKED'
        ELSE 'UNPROTECTED'
    END AS status
FROM pii_columns p
LEFT JOIN masks        m USING (table_name, column_name)
LEFT JOIN privacy_tags t USING (table_name, column_name)
ORDER BY
    CASE status WHEN 'UNPROTECTED' THEN 0 WHEN 'TAGGED_NOT_MASKED' THEN 1 ELSE 2 END,
    p.table_name, p.column_name
```

If the mask functions live in a catalog the caller cannot see, `mask_body` is NULL; the join to `system.information_schema.routines` is metastore-wide precisely because masks are often centralised in a governance catalog. `routine_catalog` is a standard column on that view; if the join errors, replace it with `{{ catalog }}.information_schema.routines` and drop the catalog predicate.
