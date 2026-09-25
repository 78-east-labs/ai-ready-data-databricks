# Diagnostic: schema_conformity

Shows what was rescued and why, which values fail the cast and what they look like, which declared types disagree with the column's name, and when the drift started.

## Context

Four queries:

1. **Rescued fields.** Parses `_rescued_data` (a JSON object keyed by field name, plus `_file_path`) and counts rescues per field with a sample value. This is the exact list of source fields that do not fit the declared schema: new columns the producer added, type changes, case differences in field names.
2. **Uncastable values.** Up to 100 distinct values of `{{ column }}` that fail `TRY_CAST` to `{{ target_type }}`, with counts and a guessed cause (empty string, thousands separator, alternative date format, boolean word, free text).
3. **Declared type versus name role.** For every column of the table, whether the declared type is looser than the name implies (`_id` as STRING, `_at` as STRING, `_count` as DOUBLE), with the proposed target type. This is the metadata-only view the upstream framework uses and it needs no scan.
4. **Drift onset by source file.** Rescued rows and uncastable values per `_metadata.file_path` and modification time, newest first, to find the load where the producer changed.

Placeholders as in the check. `{{ rescue_column }}` default `_rescued_data`.

## SQL

### Rescued fields

```sql
WITH declared AS (
    SELECT column_name
    FROM {{ catalog }}.information_schema.columns
    WHERE LOWER(table_schema) = LOWER('{{ schema }}')
      AND LOWER(table_name)   = LOWER('{{ asset }}')
),
rescued AS (
    SELECT from_json({{ rescue_column }}, 'MAP<STRING, STRING>') AS fields
    FROM {{ catalog }}.{{ schema }}.{{ asset }}
    WHERE {{ rescue_column }} IS NOT NULL
),
per_field AS (
    SELECT
        kv.key                                              AS rescued_field,
        COUNT(*)                                            AS rows_rescued,
        COUNT(DISTINCT kv.value)                            AS distinct_values,
        MIN(kv.value)                                       AS sample_value
    FROM rescued
    LATERAL VIEW explode(fields) AS kv
    GROUP BY kv.key
)
SELECT
    p.rescued_field,
    p.rows_rescued,
    p.distinct_values,
    p.sample_value,
    CASE
        WHEN p.rescued_field = '_file_path'                 THEN 'source file marker'
        WHEN exact.column_name IS NOT NULL                  THEN 'declared column: value did not match its type'
        WHEN ci.column_name IS NOT NULL                     THEN 'declared column with different case in source'
        ELSE 'field not in declared schema'
    END                                                     AS reason
FROM per_field p
LEFT JOIN declared exact ON exact.column_name = p.rescued_field
LEFT JOIN declared ci    ON LOWER(ci.column_name) = LOWER(p.rescued_field)
ORDER BY p.rows_rescued DESC
```

`from_json` to `MAP<STRING, STRING>` works because rescued values are serialized as strings. Nested rescues (a struct field that did not fit) appear under the top-level field name with a JSON string value.

### Uncastable values

```sql
SELECT
    {{ column }}                                            AS value,
    COUNT(*)                                                AS rows,
    length({{ column }})                                    AS char_length,
    CASE
        WHEN TRIM({{ column }}) = ''                                              THEN 'EMPTY_STRING'
        WHEN LOWER(TRIM({{ column }})) IN ('null', 'none', 'n/a', 'na', 'nan', '-')  THEN 'NULL_TOKEN'
        WHEN TRY_CAST(regexp_replace({{ column }}, '[,_ ]', '') AS {{ target_type }}) IS NOT NULL THEN 'THOUSANDS_SEPARATOR_OR_SPACES'
        WHEN TRY_CAST(regexp_replace({{ column }}, '[$€£%]', '') AS {{ target_type }}) IS NOT NULL THEN 'CURRENCY_OR_PERCENT_SYMBOL'
        WHEN TRY_CAST(TRY_TO_TIMESTAMP({{ column }}, 'dd/MM/yyyy') AS {{ target_type }}) IS NOT NULL
          OR TRY_CAST(TRY_TO_TIMESTAMP({{ column }}, 'MM/dd/yyyy') AS {{ target_type }}) IS NOT NULL
          OR TRY_CAST(TRY_TO_TIMESTAMP({{ column }}, 'yyyyMMdd') AS {{ target_type }}) IS NOT NULL THEN 'ALTERNATIVE_DATE_FORMAT'
        WHEN LOWER(TRIM({{ column }})) IN ('yes', 'no', 'y', 'n', 'on', 'off')     THEN 'BOOLEAN_WORD'
        WHEN REGEXP_LIKE({{ column }}, '^-?[0-9]+\\.[0-9]+$')                     THEN 'DECIMAL_INTO_INTEGER'
        ELSE 'FREE_TEXT_OR_OTHER'
    END                                                     AS likely_cause
FROM {{ catalog }}.{{ schema }}.{{ asset }}
WHERE {{ column }} IS NOT NULL
  AND TRY_CAST({{ column }} AS {{ target_type }}) IS NULL
GROUP BY {{ column }}
ORDER BY rows DESC
LIMIT 100
```

`TRY_TO_TIMESTAMP` needs DBR 11.3+ (every current warehouse). The `likely_cause` label tells the fix which normalization to apply before the cast; `FREE_TEXT_OR_OTHER` values are genuinely not of the target type.

### Declared type versus name role

```sql
SELECT
    c.column_name,
    c.full_data_type                                        AS declared_type,
    CASE
        WHEN c.data_type = 'STRING' AND REGEXP_LIKE(LOWER(c.column_name), '(_at$|_ts$|_time$|_timestamp$)') AND NOT REGEXP_LIKE(LOWER(c.column_name), 'format|zone') THEN 'TIMESTAMP'
        WHEN c.data_type = 'STRING' AND REGEXP_LIKE(LOWER(c.column_name), '(_date$|^date_|_dt$)')            THEN 'DATE'
        WHEN c.data_type = 'STRING' AND REGEXP_LIKE(LOWER(c.column_name), '(^is_|^has_|_flag$)')             THEN 'BOOLEAN'
        WHEN c.data_type = 'STRING' AND REGEXP_LIKE(LOWER(c.column_name), '(_amount$|_amt$|_price$|_total$|_cost$|_revenue$)') THEN 'DECIMAL(18,4)'
        WHEN c.data_type = 'STRING' AND REGEXP_LIKE(LOWER(c.column_name), '(_count$|_cnt$|_qty$|_quantity$)') THEN 'BIGINT'
        WHEN c.data_type = 'STRING' AND REGEXP_LIKE(LOWER(c.column_name), '(^id$|_id$)') AND NOT REGEXP_LIKE(LOWER(c.column_name), '(uuid|guid|external|ext_)') THEN 'BIGINT (or keep STRING if alphanumeric)'
        WHEN c.data_type IN ('FLOAT', 'DOUBLE') AND REGEXP_LIKE(LOWER(c.column_name), '(_count$|_cnt$|_qty$|_quantity$)') THEN 'BIGINT'
        WHEN c.data_type IN ('FLOAT', 'DOUBLE') AND REGEXP_LIKE(LOWER(c.column_name), '(_amount$|_amt$|_price$|_total$|_cost$|_revenue$)') THEN 'DECIMAL(18,4) (exact arithmetic)'
        WHEN c.data_type = 'VARIANT' AND NOT REGEXP_LIKE(LOWER(c.column_name), '(json|payload|raw|body|properties|attributes)') THEN 'STRUCT with declared fields'
    END                                                     AS proposed_type,
    c.is_nullable,
    c.comment
FROM {{ catalog }}.information_schema.columns c
WHERE LOWER(c.table_schema) = LOWER('{{ schema }}')
  AND LOWER(c.table_name)   = LOWER('{{ asset }}')
ORDER BY proposed_type IS NULL, c.ordinal_position
```

Rows with a `proposed_type` come first. This is a naming heuristic, not a finding; confirm with the uncastable-values query before changing anything.

### Drift onset by source file

```sql
SELECT
    _metadata.file_path                                     AS data_file,
    _metadata.file_modification_time                        AS written_at,
    COUNT(*)                                                AS rows_in_file,
    COUNT_IF({{ rescue_column }} IS NOT NULL)               AS rescued_rows,
    COUNT_IF({{ column }} IS NOT NULL AND TRY_CAST({{ column }} AS {{ target_type }}) IS NULL) AS uncastable_rows,
    MIN(get_json_object({{ rescue_column }}, '$._file_path')) AS sample_source_file
FROM {{ catalog }}.{{ schema }}.{{ asset }}
GROUP BY _metadata.file_path, _metadata.file_modification_time
HAVING rescued_rows > 0 OR uncastable_rows > 0
ORDER BY written_at DESC
LIMIT 100
```

`$._file_path` inside `_rescued_data` names the original raw file (Auto Loader writes it); `_metadata.file_path` is the Delta data file. Together they tell you which producer run introduced the change and whether it persisted.
