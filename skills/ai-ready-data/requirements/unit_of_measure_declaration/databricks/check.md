# Check: unit_of_measure_declaration

Fraction of measured numeric columns on base tables in the schema whose unit of measure is declared, through a `unit` column tag, a unit word in the comment, or a unit suffix in the name.

## Context

A `DOUBLE` named `duration` is useless to an agent until it knows seconds from milliseconds, and a `DECIMAL` named `amount` needs a currency. Databricks has no unit metadata on columns, so this is a **tag / proxy** check with three signals:

1. **Column tag** `{{ unit_tag_key }}` (default `unit`) in `information_schema.column_tags` with a non-empty value (`usd`, `cents`, `seconds`, `ms`, `kg`, `percent`, `count`). This is the deterministic signal and the one the fix applies.
2. **Comment** containing a unit word or symbol, matched as a whole word: currencies (`usd|eur|gbp|inr|jpy|cents|dollars?|euros?`), time (`ms|milliseconds?|seconds?|secs?|minutes?|mins?|hours?|days?|weeks?|months?|years?`), ratio (`percent|percentage|pct|%|basis points|bps|ratio`), physical (`kg|kilograms?|g|grams?|lbs?|pounds?|km|kilometers?|m|meters?|mi|miles?|cm|mm|celsius|fahrenheit|kwh|bytes?|kb|mb|gb`), or the phrase `unit:` / `in units of` / `measured in`.
3. **Name suffix** (proxy): `_usd`, `_eur`, `_gbp`, `_cents`, `_pct`, `_percent`, `_bps`, `_ms`, `_sec`, `_secs`, `_seconds`, `_min`, `_mins`, `_minutes`, `_hrs`, `_hours`, `_days`, `_kg`, `_g`, `_lbs`, `_km`, `_m`, `_mi`, `_cm`, `_mm`, `_bytes`, `_kb`, `_mb`, `_gb`, `_kwh`.

The population is numeric columns (`INT`, `BIGINT`, `SMALLINT`, `TINYINT`, `DECIMAL`, `FLOAT`, `DOUBLE`) minus columns that do not carry a unit by nature: identifiers and keys (`^id$`, `_id$`, `_key$`, `_sk$`, `_num$`, `_number$`, `_code$`), counts (`count`, `^n_`, `_cnt$`, `qty`, `quantity`), flags and ordinals (`^is_`, `^has_`, `_flag$`, `_rank$`, `_index$`, `_position$`, `^year$|^month$|^day$|^quarter$|^week$`), and anything already tagged `semantic_role = 'identifier'`. Counts are excluded because "count of orders" is its own unit; if the team prefers to tag those `unit = 'count'`, use the variant that keeps them in the population.

Comment and name matching are proxies: `total` in a comment does not make the unit explicit, and `_m` could be meters or millions. The strict variant counts only the tag.

Tags and comments appear in `information_schema` immediately. Rows are filtered to objects the caller can see.

Placeholders beyond the standard set: `{{ unit_tag_key }}` (default `unit`).

Returns NULL (N/A) when the schema contains no measured numeric columns on base tables.

## SQL

### Tag, comment or name suffix (primary)

```sql
WITH numeric_columns AS (
    SELECT LOWER(c.table_name)  AS table_name,
           LOWER(c.column_name) AS column_name,
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
    SELECT DISTINCT LOWER(table_name) AS table_name, LOWER(column_name) AS column_name
    FROM {{ catalog }}.information_schema.column_tags
    WHERE LOWER(schema_name) = LOWER('{{ schema }}')
      AND LOWER(tag_name) = LOWER('{{ unit_tag_key }}')
      AND tag_value IS NOT NULL AND trim(tag_value) <> ''
),
classified AS (
    SELECT p.table_name, p.column_name,
           (   ut.column_name IS NOT NULL
            OR REGEXP_LIKE(p.column_name,
                 '_(usd|eur|gbp|cents|pct|percent|bps|ms|secs?|seconds|mins?|minutes|hrs|hours|days|kg|g|lbs|km|m|mi|cm|mm|bytes|kb|mb|gb|kwh)$')
            OR (p.comment IS NOT NULL AND REGEXP_LIKE(LOWER(p.comment),
                 '(^|[^a-z])(usd|eur|gbp|inr|jpy|cents|dollars?|euros?'
              || '|ms|milliseconds?|seconds?|secs?|minutes?|mins?|hours?|days?|weeks?|months?|years?'
              || '|percent|percentage|pct|basis points|bps|ratio'
              || '|kg|kilograms?|grams?|lbs?|pounds?|km|kilomet(er|re)s?|met(er|re)s?|miles?|cm|mm|celsius|fahrenheit|kwh|bytes?|kb|mb|gb'
              || '|unit:|in units of|measured in)([^a-z]|$)'))
            OR (p.comment IS NOT NULL AND contains(p.comment, '%'))
           ) AS has_unit
    FROM population p
    LEFT JOIN unit_tagged ut USING (table_name, column_name)
)
SELECT
    COUNT_IF(has_unit)                              AS columns_with_unit,
    COUNT(*)                                        AS total_measured_columns,
    COUNT_IF(has_unit)::DOUBLE / NULLIF(COUNT(*), 0) AS value
FROM classified
```

### Tag only (variant)

Strict: only the `unit` tag counts. Use once the team has adopted the tag, or when the schema's naming makes the suffix proxy unreliable (`_m`, `_g`).

```sql
WITH population AS (
    SELECT LOWER(c.table_name)  AS table_name,
           LOWER(c.column_name) AS column_name
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
unit_tagged AS (
    SELECT DISTINCT LOWER(table_name) AS table_name, LOWER(column_name) AS column_name
    FROM {{ catalog }}.information_schema.column_tags
    WHERE LOWER(schema_name) = LOWER('{{ schema }}')
      AND LOWER(tag_name) = LOWER('{{ unit_tag_key }}')
      AND tag_value IS NOT NULL AND trim(tag_value) <> ''
)
SELECT
    COUNT_IF(ut.column_name IS NOT NULL)                              AS columns_with_unit,
    COUNT(*)                                                          AS total_measured_columns,
    COUNT_IF(ut.column_name IS NOT NULL)::DOUBLE / NULLIF(COUNT(*), 0) AS value
FROM population p
LEFT JOIN unit_tagged ut USING (table_name, column_name)
```

### All numeric columns, counts included (variant)

Keeps count and quantity columns in the population (identifiers, flags and ordinals still excluded) and accepts `count` as a unit word. Use when the convention is that every measure, including counts, is tagged.

```sql
WITH population AS (
    SELECT LOWER(c.table_name)  AS table_name,
           LOWER(c.column_name) AS column_name,
           c.comment
    FROM {{ catalog }}.information_schema.columns c
    JOIN {{ catalog }}.information_schema.tables t
      ON c.table_schema = t.table_schema AND c.table_name = t.table_name
    WHERE LOWER(c.table_schema) = LOWER('{{ schema }}')
      AND t.table_type IN ('MANAGED', 'EXTERNAL')
      AND c.data_type IN ('INT', 'BIGINT', 'SMALLINT', 'TINYINT', 'DECIMAL', 'FLOAT', 'DOUBLE')
      AND NOT REGEXP_LIKE(LOWER(c.column_name),
          '(^id$|_id$|_key$|_sk$|_num$|_number$|_code$|^is_|^has_|_flag$|_rank$|_index$|_position$|^year$|^month$|^day$|^quarter$|^week$)')
),
unit_tagged AS (
    SELECT DISTINCT LOWER(table_name) AS table_name, LOWER(column_name) AS column_name
    FROM {{ catalog }}.information_schema.column_tags
    WHERE LOWER(schema_name) = LOWER('{{ schema }}')
      AND LOWER(tag_name) = LOWER('{{ unit_tag_key }}')
      AND tag_value IS NOT NULL AND trim(tag_value) <> ''
)
SELECT
    COUNT_IF(ut.column_name IS NOT NULL
             OR REGEXP_LIKE(p.column_name, '_(usd|eur|gbp|cents|pct|percent|bps|ms|secs?|seconds|mins?|minutes|hrs|hours|days|kg|g|lbs|km|m|mi|cm|mm|bytes|kb|mb|gb|kwh|count|cnt)$')
             OR (p.comment IS NOT NULL AND REGEXP_LIKE(LOWER(p.comment),
                 '(^|[^a-z])(usd|eur|gbp|cents|dollars?|ms|seconds?|minutes?|hours?|days?|percent|pct|bps|kg|km|bytes?|count|number of|unit:|measured in)([^a-z]|$)')))
                                                                    AS columns_with_unit,
    COUNT(*)                                                        AS total_measured_columns,
    COUNT_IF(ut.column_name IS NOT NULL
             OR REGEXP_LIKE(p.column_name, '_(usd|eur|gbp|cents|pct|percent|bps|ms|secs?|seconds|mins?|minutes|hrs|hours|days|kg|g|lbs|km|m|mi|cm|mm|bytes|kb|mb|gb|kwh|count|cnt)$')
             OR (p.comment IS NOT NULL AND REGEXP_LIKE(LOWER(p.comment),
                 '(^|[^a-z])(usd|eur|gbp|cents|dollars?|ms|seconds?|minutes?|hours?|days?|percent|pct|bps|kg|km|bytes?|count|number of|unit:|measured in)([^a-z]|$)')))::DOUBLE
        / NULLIF(COUNT(*), 0)                                       AS value
FROM population p
LEFT JOIN unit_tagged ut USING (table_name, column_name)
```
