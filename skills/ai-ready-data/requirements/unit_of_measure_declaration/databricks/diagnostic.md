# Diagnostic: unit_of_measure_declaration

Lists every measured numeric column on base tables in the schema with its `unit` tag, the unit implied by its name or comment, an inferred unit category, and a status.

## Context

One row per column in the check's population (numeric, minus identifiers, counts, flags and ordinals). Fields:

- `unit_tag`: the value of the `{{ unit_tag_key }}` tag (default `unit`), if any.
- `unit_from_name`: the suffix the name carries (`usd`, `ms`, `pct`, ...), if any. This is what the bulk fix turns into a tag.
- `comment_unit_hit`: the first unit word found in the comment, if any.
- `inferred_category`: `MONETARY`, `PERCENTAGE`, `DURATION`, `WEIGHT`, `LENGTH`, `TEMPERATURE`, `STORAGE`, `ENERGY`, or `UNKNOWN`, from name substrings. It tells you what kind of unit to ask for, not which one.
- `declaration_status`: `TAGGED`, `NAME_SUFFIX`, `COMMENT`, or `UNDECLARED`.
- `suggested_unit`: a value for the tag when the name suffix is unambiguous (`_usd` -> `usd`); NULL otherwise. `_m` and `_g` are ambiguous (meters or millions, grams or a generic suffix) and are not suggested.

Sorted so undeclared columns come first, monetary ones ahead of the rest, then by table and ordinal position. A monetary column without a currency is the most expensive kind of missing unit.

## SQL

```sql
WITH numeric_columns AS (
    SELECT LOWER(c.table_name)  AS table_name,
           c.column_name        AS column_name_cased,
           LOWER(c.column_name) AS column_name,
           c.full_data_type,
           c.ordinal_position,
           c.comment
    FROM {{ catalog }}.information_schema.columns c
    JOIN {{ catalog }}.information_schema.tables t
      ON c.table_schema = t.table_schema AND c.table_name = t.table_name
    WHERE LOWER(c.table_schema) = LOWER('{{ schema }}')
      AND t.table_type IN ('MANAGED', 'EXTERNAL')
      AND c.data_type IN ('INT', 'BIGINT', 'SMALLINT', 'TINYINT', 'DECIMAL', 'FLOAT', 'DOUBLE')
      AND NOT REGEXP_LIKE(LOWER(c.column_name),
          '(^id$|_id$|_key$|_sk$|_num$|_number$|_code$|count|^n_|_cnt$|qty|quantity'
       || '|^is_|^has_|_flag$|_rank$|_index$|_position$|^year$|^month$|^day$|^quarter$|^week$)')
),
role_identifiers AS (
    SELECT DISTINCT LOWER(table_name) AS table_name, LOWER(column_name) AS column_name
    FROM {{ catalog }}.information_schema.column_tags
    WHERE LOWER(schema_name) = LOWER('{{ schema }}')
      AND LOWER(tag_name) = 'semantic_role' AND LOWER(tag_value) IN ('identifier', 'flag')
),
population AS (
    SELECT n.* FROM numeric_columns n
    LEFT JOIN role_identifiers r USING (table_name, column_name)
    WHERE r.column_name IS NULL
),
unit_tagged AS (
    SELECT LOWER(table_name) AS table_name, LOWER(column_name) AS column_name, max(tag_value) AS unit_tag
    FROM {{ catalog }}.information_schema.column_tags
    WHERE LOWER(schema_name) = LOWER('{{ schema }}')
      AND LOWER(tag_name) = LOWER('{{ unit_tag_key }}')
      AND tag_value IS NOT NULL AND trim(tag_value) <> ''
    GROUP BY 1, 2
),
classified AS (
    SELECT p.*, ut.unit_tag,
           regexp_extract(p.column_name,
               '_(usd|eur|gbp|cents|pct|percent|bps|ms|secs?|seconds|mins?|minutes|hrs|hours|days|kg|g|lbs|km|m|mi|cm|mm|bytes|kb|mb|gb|kwh)$', 1) AS unit_from_name,
           regexp_extract(LOWER(COALESCE(p.comment, '')),
               '(^|[^a-z])(usd|eur|gbp|inr|jpy|cents|dollars?|euros?|ms|milliseconds?|seconds?|secs?|minutes?|mins?|hours?|days?|weeks?|months?|years?'
            || '|percent|percentage|pct|basis points|bps|ratio|kg|kilograms?|grams?|lbs?|pounds?|km|kilomet(?:er|re)s?|met(?:er|re)s?|miles?|cm|mm'
            || '|celsius|fahrenheit|kwh|bytes?|kb|mb|gb|unit:|in units of|measured in)([^a-z]|$)', 2) AS comment_unit_hit,
           contains(COALESCE(p.comment, ''), '%') AS comment_has_percent_sign,
           CASE
               WHEN REGEXP_LIKE(p.column_name, '(amount|price|cost|revenue|fee|charge|total|balance|salary|spend|budget|value|payment|tax|discount)') THEN 'MONETARY'
               WHEN REGEXP_LIKE(p.column_name, '(pct|percent|ratio|rate|share|margin|conversion)')                                 THEN 'PERCENTAGE'
               WHEN REGEXP_LIKE(p.column_name, '(duration|latency|elapsed|age|tenure|time_to|_ms$|_sec|_min|_hrs|_hours|_days|delay|uptime)') THEN 'DURATION'
               WHEN REGEXP_LIKE(p.column_name, '(weight|mass|_kg$|_lbs$|_g$)')                                                     THEN 'WEIGHT'
               WHEN REGEXP_LIKE(p.column_name, '(length|height|width|depth|distance|radius|_km$|_mi$|_cm$|_mm$)')                   THEN 'LENGTH'
               WHEN REGEXP_LIKE(p.column_name, '(temp|celsius|fahrenheit)')                                                        THEN 'TEMPERATURE'
               WHEN REGEXP_LIKE(p.column_name, '(bytes|_kb$|_mb$|_gb$|storage|size)')                                              THEN 'STORAGE'
               WHEN REGEXP_LIKE(p.column_name, '(kwh|energy|power|watt)')                                                          THEN 'ENERGY'
               ELSE 'UNKNOWN'
           END AS inferred_category
    FROM population p
    LEFT JOIN unit_tagged ut USING (table_name, column_name)
)
SELECT
    table_name,
    column_name_cased                                   AS column_name,
    full_data_type,
    inferred_category,
    unit_tag,
    NULLIF(unit_from_name, '')                          AS unit_from_name,
    COALESCE(NULLIF(comment_unit_hit, ''),
             CASE WHEN comment_has_percent_sign THEN '%' END) AS comment_unit_hit,
    CASE
        WHEN unit_tag IS NOT NULL                                        THEN 'TAGGED'
        WHEN unit_from_name <> ''                                        THEN 'NAME_SUFFIX'
        WHEN comment_unit_hit <> '' OR comment_has_percent_sign          THEN 'COMMENT'
        ELSE 'UNDECLARED'
    END                                                 AS declaration_status,
    CASE
        WHEN unit_tag IS NOT NULL THEN NULL
        WHEN unit_from_name IN ('usd','eur','gbp','cents','bps','ms','kg','lbs','km','mi','cm','mm','bytes','kb','mb','gb','kwh','days','hours','minutes','seconds') THEN unit_from_name
        WHEN unit_from_name IN ('pct','percent') THEN 'percent'
        WHEN unit_from_name IN ('sec','secs')    THEN 'seconds'
        WHEN unit_from_name IN ('min','mins')    THEN 'minutes'
        WHEN unit_from_name = 'hrs'              THEN 'hours'
        ELSE NULL
    END                                                 AS suggested_unit,
    left(comment, 120)                                  AS comment_preview
FROM classified
ORDER BY
    CASE WHEN unit_tag IS NOT NULL THEN 3
         WHEN unit_from_name <> '' THEN 2
         WHEN comment_unit_hit <> '' OR comment_has_percent_sign THEN 1
         ELSE 0 END,
    inferred_category = 'MONETARY' DESC,
    table_name, ordinal_position
```
