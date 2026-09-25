# Check: demographic_representation

Fraction of training datasets in the schema that carry a `demographic_profile` table tag documenting how the dataset's demographic distribution compares to the target population.

## Context

Whether a training set represents the population it will be used on is not something the catalog can compute: it needs a target population definition (a census cut, the customer base, the eligible applicant pool) and a human judgement about acceptable deviation. Databricks has no primitive for it, so the framework measures whether that judgement was recorded, through the table tag `demographic_profile` in `{{ catalog }}.information_schema.table_tags`. Typical values: `matches_us_census_2024`, `overweights_18_24_by_12pct_accepted`, `not_assessed_synthetic_data`. The check verifies presence and a non-empty value, not the content.

Strength: tag. The population is training datasets, found the same way as in `bias_testing_coverage`: a `training_set` table tag (any value) or a table name matching `{{ training_patterns }}` (default `(^|_)(train|training|trainset|training_set|training_data|labels?|dataset|feature_set|ml)($|_)`). Set `{{ training_patterns }}` to `.` to treat every base table as a training dataset when the schema is ML-only.

Demographic attributes are sensitive data in most jurisdictions. The check reads only metadata. The diagnostic lists columns whose names suggest protected attributes so a reviewer can build the profile; it does not read their values.

`information_schema` reflects tags immediately. Rows are limited to what the caller can see.

Returns NULL (N/A) when the schema contains no training datasets.

## SQL

### Training tables with a demographic_profile tag (primary)

```sql
WITH training_tables AS (
    SELECT DISTINCT LOWER(t.table_name) AS table_name
    FROM {{ catalog }}.information_schema.tables t
    LEFT JOIN {{ catalog }}.information_schema.table_tags tg
      ON  LOWER(tg.schema_name) = LOWER(t.table_schema)
      AND LOWER(tg.table_name)  = LOWER(t.table_name)
      AND LOWER(tg.tag_name)    = 'training_set'
    WHERE LOWER(t.table_schema) = LOWER('{{ schema }}')
      AND t.table_type IN ('MANAGED', 'EXTERNAL')
      AND (tg.tag_name IS NOT NULL
           OR REGEXP_LIKE(LOWER(t.table_name), '{{ training_patterns }}'))
),
profiled AS (
    SELECT DISTINCT LOWER(table_name) AS table_name
    FROM {{ catalog }}.information_schema.table_tags
    WHERE LOWER(schema_name) = LOWER('{{ schema }}')
      AND LOWER(tag_name) = 'demographic_profile'
      AND tag_value IS NOT NULL AND trim(tag_value) <> ''
)
SELECT
    COUNT_IF(p.table_name IS NOT NULL)            AS profiled_training_tables,
    COUNT(*)                                       AS training_tables,
    COUNT_IF(p.table_name IS NOT NULL)::DOUBLE
        / NULLIF(COUNT(*), 0)                      AS value
FROM training_tables tt
LEFT JOIN profiled p USING (table_name)
```

### Only training tables that contain demographic attribute columns (variant)

Narrows the denominator to training tables that have at least one column whose name suggests a protected attribute (`{{ demographic_patterns }}`, default below). A training set with no demographic columns cannot be profiled by group and arguably needs a different kind of representation statement; this variant leaves it out.

Default `{{ demographic_patterns }}`:

```
(^|_)(gender|sex|age|age_group|age_bucket|dob|birth_year|race|ethnicity|ethnic|religion|disability|disabled|nationality|citizenship|marital|marital_status|income|income_band|zip|zipcode|postcode|country|language)($|_)
```

```sql
WITH training_tables AS (
    SELECT DISTINCT LOWER(t.table_name) AS table_name
    FROM {{ catalog }}.information_schema.tables t
    LEFT JOIN {{ catalog }}.information_schema.table_tags tg
      ON  LOWER(tg.schema_name) = LOWER(t.table_schema)
      AND LOWER(tg.table_name)  = LOWER(t.table_name)
      AND LOWER(tg.tag_name)    = 'training_set'
    WHERE LOWER(t.table_schema) = LOWER('{{ schema }}')
      AND t.table_type IN ('MANAGED', 'EXTERNAL')
      AND (tg.tag_name IS NOT NULL
           OR REGEXP_LIKE(LOWER(t.table_name), '{{ training_patterns }}'))
),
with_attributes AS (
    SELECT DISTINCT LOWER(table_name) AS table_name
    FROM {{ catalog }}.information_schema.columns
    WHERE LOWER(table_schema) = LOWER('{{ schema }}')
      AND REGEXP_LIKE(LOWER(column_name), '{{ demographic_patterns }}')
),
profiled AS (
    SELECT DISTINCT LOWER(table_name) AS table_name
    FROM {{ catalog }}.information_schema.table_tags
    WHERE LOWER(schema_name) = LOWER('{{ schema }}')
      AND LOWER(tag_name) = 'demographic_profile'
      AND tag_value IS NOT NULL AND trim(tag_value) <> ''
)
SELECT
    COUNT_IF(p.table_name IS NOT NULL)            AS profiled_training_tables,
    COUNT(*)                                       AS training_tables_with_attributes,
    COUNT_IF(p.table_name IS NOT NULL)::DOUBLE
        / NULLIF(COUNT(*), 0)                      AS value
FROM training_tables tt
JOIN with_attributes a USING (table_name)
LEFT JOIN profiled p USING (table_name)
```
