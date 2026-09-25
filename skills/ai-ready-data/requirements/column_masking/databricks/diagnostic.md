# Diagnostic: column_masking

Three views: the worklist of PII-tagged columns without a mask, the inventory of masks and row filters already in the schema, and the tag-key vocabulary in use.

## Context

**Unmasked PII columns** reuses the check's population (`{{ pii_tag_key }}`, default `pii`) and lists what needs a mask, with data type (masks are type-specific) and comment.

**Mask and filter inventory** lists every column mask and row filter attached in the schema, with the function body from `information_schema.routines`, so existing functions can be reused and weak ones can be spotted. A body that never redacts, or that gates on `current_user() = 'someone@example.com'`, deserves attention even though it scores as masked.

**Tag-key inventory** shows the distinct column tag keys and their value sets in the schema. Use it to set `{{ pii_tag_key }}` correctly, in particular after enabling Databricks Data Classification, whose output keys you should read here rather than assume.

All three are read-only over `information_schema`, which is current within seconds.

## SQL

### Unmasked PII columns (worklist)

```sql
WITH pii_columns AS (
    SELECT DISTINCT LOWER(ct.table_name) AS table_name,
           LOWER(ct.column_name) AS column_name,
           ct.column_name        AS column_name_cased,
           ct.tag_value          AS pii_value
    FROM {{ catalog }}.information_schema.column_tags ct
    JOIN {{ catalog }}.information_schema.tables t
      ON  LOWER(t.table_schema) = LOWER(ct.schema_name)
      AND LOWER(t.table_name)   = LOWER(ct.table_name)
    WHERE LOWER(ct.schema_name) = LOWER('{{ schema }}')
      AND t.table_type IN ('MANAGED', 'EXTERNAL')
      AND LOWER(ct.tag_name) = LOWER('{{ pii_tag_key }}')
),
masked AS (
    SELECT DISTINCT LOWER(table_name) AS table_name, LOWER(column_name) AS column_name
    FROM {{ catalog }}.information_schema.column_masks
    WHERE LOWER(schema_name) = LOWER('{{ schema }}')
)
SELECT
    p.table_name,
    p.column_name_cased AS column_name,
    p.pii_value,
    c.data_type,
    c.comment,
    'NEEDS_MASK' AS status
FROM pii_columns p
LEFT JOIN masked m USING (table_name, column_name)
JOIN {{ catalog }}.information_schema.columns c
  ON  LOWER(c.table_schema) = LOWER('{{ schema }}')
  AND LOWER(c.table_name)   = p.table_name
  AND LOWER(c.column_name)  = p.column_name
WHERE m.column_name IS NULL
ORDER BY p.table_name, p.column_name
```

### Mask and row-filter inventory

```sql
SELECT
    'COLUMN_MASK'                                                AS policy_kind,
    cm.table_name,
    cm.column_name,
    concat_ws('.', cm.mask_catalog, cm.mask_schema, cm.mask_name) AS function_name,
    cm.mask_col_usage                                            AS extra_columns,
    r.routine_definition                                         AS function_body
FROM {{ catalog }}.information_schema.column_masks cm
LEFT JOIN system.information_schema.routines r
  ON  LOWER(r.routine_catalog) = LOWER(cm.mask_catalog)
  AND LOWER(r.routine_schema)  = LOWER(cm.mask_schema)
  AND LOWER(r.routine_name)    = LOWER(cm.mask_name)
WHERE LOWER(cm.schema_name) = LOWER('{{ schema }}')

UNION ALL

SELECT
    'ROW_FILTER'                                                 AS policy_kind,
    rf.table_name,
    NULL                                                         AS column_name,
    concat_ws('.', rf.filter_catalog, rf.filter_schema, rf.filter_name) AS function_name,
    rf.filter_col_usage                                          AS extra_columns,
    r.routine_definition                                         AS function_body
FROM {{ catalog }}.information_schema.row_filters rf
LEFT JOIN system.information_schema.routines r
  ON  LOWER(r.routine_catalog) = LOWER(rf.filter_catalog)
  AND LOWER(r.routine_schema)  = LOWER(rf.filter_schema)
  AND LOWER(r.routine_name)    = LOWER(rf.filter_name)
WHERE LOWER(rf.schema_name) = LOWER('{{ schema }}')

ORDER BY policy_kind, table_name, column_name
```

`function_body` is NULL when the function lives in a catalog the caller cannot see. If `routine_catalog` is rejected on your metastore, switch the join to `{{ catalog }}.information_schema.routines` and remove the catalog predicate.

### Column tag-key inventory

```sql
SELECT
    tag_name,
    COUNT(DISTINCT concat(table_name, '.', column_name)) AS columns_tagged,
    array_sort(collect_set(tag_value))                    AS values_seen
FROM {{ catalog }}.information_schema.column_tags
WHERE LOWER(schema_name) = LOWER('{{ schema }}')
GROUP BY tag_name
ORDER BY columns_tagged DESC, tag_name
```
