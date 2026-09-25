# Diagnostic: purpose_limitation

Per-table view of the declared AI purposes, whether the table holds personal data, its legal basis, and evidence of actual AI consumption from lineage, so declarations can be checked against use.

## Context

Reuses the check's population (all base tables). For each table:

- `ai_allowed_purposes`: the tag value split into tokens; `invalid_tokens` lists any outside `{{ purpose_vocabulary }}` (default in the check).
- `has_pii_columns`: whether any column carries `{{ pii_tag_key }}`. These are the tables where purpose limitation is a legal duty.
- `legal_basis`: from `consent_coverage`; a table with `legal_basis = 'contract'` and `ai_allowed_purposes = 'training'` deserves a second look.
- `ai_consumers_30d`: distinct downstream entities that read the table in the last `{{ lookback_days }}` days per `system.access.table_lineage` (`entity_type` JOB, PIPELINE, NOTEBOOK, DASHBOARD, QUERY) whose name or the target table name suggests an AI workload (`train|model|feature|embed|vector|rag|agent|llm|genie`). It is a heuristic, useful for spotting a table tagged `none` that a training job reads anyway. Lineage lags up to a few hours and needs the `system.access` schema.
- `status`: `DECLARED`, `DECLARED_INVALID_VALUE`, `UNDECLARED_PII` (worst: personal data, no purpose) or `UNDECLARED`.

`{{ lookback_days }}` defaults to 30. Sorted worst-first.

## SQL

```sql
WITH tables_in_scope AS (
    SELECT LOWER(table_name) AS table_name, table_owner, comment
    FROM {{ catalog }}.information_schema.tables
    WHERE LOWER(table_schema) = LOWER('{{ schema }}')
      AND table_type IN ('MANAGED', 'EXTERNAL')
),
tags AS (
    SELECT LOWER(table_name) AS table_name,
           MAX(CASE WHEN LOWER(tag_name) = 'ai_allowed_purposes' THEN tag_value END) AS ai_allowed_purposes,
           MAX(CASE WHEN LOWER(tag_name) = 'legal_basis'         THEN tag_value END) AS legal_basis
    FROM {{ catalog }}.information_schema.table_tags
    WHERE LOWER(schema_name) = LOWER('{{ schema }}')
    GROUP BY LOWER(table_name)
),
pii AS (
    SELECT DISTINCT LOWER(table_name) AS table_name
    FROM {{ catalog }}.information_schema.column_tags
    WHERE LOWER(schema_name) = LOWER('{{ schema }}')
      AND LOWER(tag_name) = LOWER('{{ pii_tag_key }}')
),
ai_reads AS (
    SELECT LOWER(source_table_name) AS table_name,
           array_sort(collect_set(concat(entity_type, ':', COALESCE(entity_id, target_table_full_name, '?')))) AS ai_consumers_30d
    FROM system.access.table_lineage
    WHERE LOWER(source_catalog) = LOWER('{{ catalog }}')
      AND LOWER(source_schema)  = LOWER('{{ schema }}')
      AND source_table_name IS NOT NULL
      AND event_date >= date_sub(current_date(), {{ lookback_days }})
      AND event_time >= current_timestamp() - INTERVAL {{ lookback_days }} DAYS
      AND REGEXP_LIKE(LOWER(concat_ws(' ', entity_id, target_table_full_name)),
                      'train|model|feature|embed|vector|rag|agent|llm|genie|fine_tun')
    GROUP BY LOWER(source_table_name)
),
parsed AS (
    SELECT t.table_name, t.table_owner, t.comment,
           g.ai_allowed_purposes, g.legal_basis,
           p.table_name IS NOT NULL AS has_pii_columns,
           r.ai_consumers_30d,
           CASE WHEN g.ai_allowed_purposes IS NOT NULL
                THEN filter(transform(split(LOWER(g.ai_allowed_purposes), '\\s*,\\s*'), x -> trim(x)),
                            x -> NOT array_contains(split('{{ purpose_vocabulary }}', ','), x))
           END AS invalid_tokens
    FROM tables_in_scope t
    LEFT JOIN tags     g USING (table_name)
    LEFT JOIN pii      p USING (table_name)
    LEFT JOIN ai_reads r USING (table_name)
)
SELECT
    table_name,
    table_owner,
    ai_allowed_purposes,
    invalid_tokens,
    has_pii_columns,
    legal_basis,
    ai_consumers_30d,
    comment,
    CASE
        WHEN ai_allowed_purposes IS NULL OR trim(ai_allowed_purposes) = '' THEN
             CASE WHEN has_pii_columns THEN 'UNDECLARED_PII' ELSE 'UNDECLARED' END
        WHEN size(invalid_tokens) > 0 THEN 'DECLARED_INVALID_VALUE'
        ELSE 'DECLARED'
    END AS status
FROM parsed
ORDER BY
    CASE status
        WHEN 'UNDECLARED_PII'         THEN 0
        WHEN 'UNDECLARED'             THEN 1
        WHEN 'DECLARED_INVALID_VALUE' THEN 2
        ELSE 3
    END,
    has_pii_columns DESC, table_name
```

If the caller cannot read `system.access.table_lineage`, remove the `ai_reads` CTE and the `ai_consumers_30d` column; the rest is `information_schema` only.

### Declarations contradicted by use

Tables tagged `none` (or without `training`) that an AI-looking entity read in the window. Same heuristic as above; a hit is a prompt for a conversation, not proof of a violation.

```sql
WITH declared AS (
    SELECT LOWER(table_name) AS table_name,
           transform(split(LOWER(tag_value), '\\s*,\\s*'), x -> trim(x)) AS purposes
    FROM {{ catalog }}.information_schema.table_tags
    WHERE LOWER(schema_name) = LOWER('{{ schema }}')
      AND LOWER(tag_name) = 'ai_allowed_purposes'
)
SELECT d.table_name, d.purposes,
       l.entity_type, l.entity_id, l.target_table_full_name,
       l.user_identity.email AS read_by, MAX(l.event_time) AS last_read
FROM declared d
JOIN system.access.table_lineage l
  ON  LOWER(l.source_catalog)    = LOWER('{{ catalog }}')
  AND LOWER(l.source_schema)     = LOWER('{{ schema }}')
  AND LOWER(l.source_table_name) = d.table_name
WHERE l.event_date >= date_sub(current_date(), {{ lookback_days }})
  AND l.event_time >= current_timestamp() - INTERVAL {{ lookback_days }} DAYS
  AND REGEXP_LIKE(LOWER(concat_ws(' ', l.entity_id, l.target_table_full_name)),
                  'train|model|fine_tun|embed|vector|rag|agent|llm')
  AND (array_contains(d.purposes, 'none') OR NOT array_contains(d.purposes, 'training'))
GROUP BY d.table_name, d.purposes, l.entity_type, l.entity_id, l.target_table_full_name, l.user_identity.email
ORDER BY last_read DESC
```
