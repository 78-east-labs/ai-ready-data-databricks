# Fix: unit_of_measure_declaration

Declare the unit of each measured numeric column with the `unit` column tag, and state it in the comment.

## Context

The primary fix is the tag: `ALTER TABLE ... ALTER COLUMN ... SET TAGS ('{{ unit_tag_key }}' = '<unit>')` (default key `unit`). It is idempotent, needs `APPLY TAG` on the table or ownership, rewrites nothing, and gives agents a machine-readable unit without parsing prose. Use lowercase canonical values and keep the list short: ISO currency codes (`usd`, `eur`, `gbp`), `cents` when the column is an integer minor unit, `percent` for 0 to 100 and `ratio` for 0 to 1, SI or common time units (`ms`, `seconds`, `minutes`, `hours`, `days`), `kg`, `km`, `bytes`, `count`. Register `unit` as a governed tag with that allowed list if the account uses tag policies.

Put the unit in the comment as well ("Order total in USD, including tax"), since the comment is what humans and the Assistant read first. The comment path alone also satisfies the check.

Renaming a column to add a suffix (`revenue` to `revenue_usd`) makes the unit visible in every query and is the strongest signal, but it breaks downstream readers. Do it only for new tables or with a deprecation window; for existing tables use the tag.

The tag documents a fact a human must confirm: which unit the pipeline writes. A wrong `unit` tag is worse than none, because an agent will convert on it. The bulk variant therefore only derives the tag from an unambiguous name suffix (`_usd`, `_ms`, `_pct`) and stops there; columns with no suffix go through review, and `_m` / `_g` are excluded because they are ambiguous.

## Fix: Tag one column

Guard (skip if a row comes back with the desired value):

```sql
SELECT tag_value
FROM {{ catalog }}.information_schema.column_tags
WHERE LOWER(schema_name) = LOWER('{{ schema }}')
  AND LOWER(table_name)  = LOWER('{{ asset }}')
  AND LOWER(column_name) = LOWER('{{ column }}')
  AND LOWER(tag_name)    = LOWER('{{ unit_tag_key }}')
```

```sql
ALTER TABLE {{ catalog }}.{{ schema }}.{{ asset }}
ALTER COLUMN {{ column }} SET TAGS ('{{ unit_tag_key }}' = '{{ unit }}')
```

## Fix: State the unit in the comment

`COMMENT ON COLUMN` replaces the whole comment; read `information_schema.columns.comment` first and keep the existing text.

```sql
COMMENT ON COLUMN {{ catalog }}.{{ schema }}.{{ asset }}.{{ column }}
IS '{{ existing_comment }} Unit: {{ unit }}.'
```

## Fix: Bulk tag columns whose name suffix declares the unit

Emits one `SET TAGS` per untagged measured column whose name ends in an unambiguous unit suffix, mapping the suffix to the canonical tag value.

```sql
WITH population AS (
    SELECT c.table_name, c.column_name, c.ordinal_position,
           regexp_extract(LOWER(c.column_name),
               '_(usd|eur|gbp|cents|pct|percent|bps|ms|secs?|seconds|mins?|minutes|hrs|hours|days|kg|lbs|km|mi|cm|mm|bytes|kb|mb|gb|kwh)$', 1) AS suffix
    FROM {{ catalog }}.information_schema.columns c
    JOIN {{ catalog }}.information_schema.tables t
      ON c.table_schema = t.table_schema AND c.table_name = t.table_name
    LEFT JOIN {{ catalog }}.information_schema.column_tags ut
      ON LOWER(ut.schema_name) = LOWER(c.table_schema)
     AND LOWER(ut.table_name)  = LOWER(c.table_name)
     AND LOWER(ut.column_name) = LOWER(c.column_name)
     AND LOWER(ut.tag_name)    = LOWER('{{ unit_tag_key }}')
    WHERE LOWER(c.table_schema) = LOWER('{{ schema }}')
      AND t.table_type IN ('MANAGED', 'EXTERNAL')
      AND c.data_type IN ('INT', 'BIGINT', 'SMALLINT', 'TINYINT', 'DECIMAL', 'FLOAT', 'DOUBLE')
      AND ut.column_name IS NULL
),
mapped AS (
    SELECT *,
           CASE
               WHEN suffix IN ('pct', 'percent') THEN 'percent'
               WHEN suffix IN ('sec', 'secs')    THEN 'seconds'
               WHEN suffix IN ('min', 'mins')    THEN 'minutes'
               WHEN suffix = 'hrs'               THEN 'hours'
               WHEN suffix <> ''                 THEN suffix
           END AS unit
    FROM population
)
SELECT concat(
    'ALTER TABLE {{ catalog }}.{{ schema }}.`', table_name,
    '` ALTER COLUMN `', column_name,
    '` SET TAGS (''{{ unit_tag_key }}'' = ''', unit, ''');'
) AS stmt
FROM mapped
WHERE unit IS NOT NULL
ORDER BY table_name, ordinal_position
```

Show the generated statements to the user before executing them.

## Fix: Bulk tag from a unit mapping table

For the remaining columns, the unit has to come from a person or from the source system's data dictionary. Collect the answers in `{{ catalog }}.{{ schema }}.unit_map(table_name STRING, column_name STRING, unit STRING)` (the diagnostic output with a filled-in `suggested_unit` column is a good starting shape) and generate tags plus comment suffixes from it:

```sql
SELECT concat(
    'ALTER TABLE {{ catalog }}.{{ schema }}.`', c.table_name,
    '` ALTER COLUMN `', c.column_name,
    '` SET TAGS (''{{ unit_tag_key }}'' = ''', LOWER(trim(m.unit)), ''');'
) AS stmt
FROM {{ catalog }}.information_schema.columns c
JOIN {{ catalog }}.{{ schema }}.unit_map m
  ON LOWER(m.table_name) = LOWER(c.table_name) AND LOWER(m.column_name) = LOWER(c.column_name)
WHERE LOWER(c.table_schema) = LOWER('{{ schema }}')
  AND m.unit IS NOT NULL AND trim(m.unit) <> ''
UNION ALL
SELECT concat(
    'COMMENT ON COLUMN {{ catalog }}.{{ schema }}.`', c.table_name, '`.`', c.column_name,
    '` IS ''', replace(concat(COALESCE(trim(c.comment), ''), CASE WHEN c.comment IS NULL OR trim(c.comment) = '' THEN '' ELSE ' ' END,
                              'Unit: ', LOWER(trim(m.unit)), '.'), '''', ''''''), ''';'
) AS stmt
FROM {{ catalog }}.information_schema.columns c
JOIN {{ catalog }}.{{ schema }}.unit_map m
  ON LOWER(m.table_name) = LOWER(c.table_name) AND LOWER(m.column_name) = LOWER(c.column_name)
WHERE LOWER(c.table_schema) = LOWER('{{ schema }}')
  AND m.unit IS NOT NULL AND trim(m.unit) <> ''
  AND NOT REGEXP_LIKE(LOWER(COALESCE(c.comment, '')), 'unit:')
ORDER BY stmt
```

The comment branch appends `Unit: ...` to the existing comment rather than replacing it, and skips comments that already contain `Unit:`, so re-running is safe.

## Fix: Verify a suspected unit before tagging

When a monetary column has no suffix and the owner is unsure whether it holds dollars or cents, look at the data before tagging. Integer types with values in the tens of thousands for a retail order are usually cents; decimals with two-place scale are usually major units.

```sql
SELECT
    percentile_approx({{ column }}, 0.5)     AS median,
    percentile_approx({{ column }}, 0.99)    AS p99,
    max({{ column }})                        AS max_value,
    COUNT_IF({{ column }} <> round({{ column }})) AS non_integer_rows
FROM {{ catalog }}.{{ schema }}.{{ asset }} TABLESAMPLE ({{ sample_rows }} ROWS)
```

Default `sample_rows` is 1,000,000. This narrows the question; it does not answer it. Confirm with the pipeline owner.

## Organizational guidance

Units should be decided when a measure is defined and never inferred afterwards. Adopt suffix naming for new measure columns (`_usd`, `_cents`, `_ms`, `_pct`) so the unit travels with every query; put `unit` in dbt column `meta` and render it to `SET TAGS` in a post-hook; define measures once in a metric view with the unit in the measure comment so downstream tools read one definition. Make a numeric gold column without a `unit` tag a review comment, and make the Genie space or agent instructions read the tag so the declaration has a consumer.
