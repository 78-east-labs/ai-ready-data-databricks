# Fix: categorical_validity

Normalize, remap, null or remove values outside the vocabulary, then lock the vocabulary in with a CHECK constraint.

## Context

Read the diagnostic first; the `status` column tells you which of these to use:

1. **Normalize case and whitespace** for `CASE_OR_WHITESPACE` rows. Mechanical, safe, and usually the bulk of the violations.
2. **Remap known variants** for `NEAR_MATCH` rows and legacy codes, using an explicit mapping you have confirmed with the owner. Never auto-apply the Levenshtein suggestion.
3. **Extend the vocabulary** when `UNKNOWN` values are legitimate new categories. This is a change to `{{ allowed_values }}` (or an `INSERT` into the reference table), not to the data.
4. **NULL the remaining unknowns.** Keeps the row; the column becomes a `data_completeness` matter.
5. **Quarantine and delete** only when the category is what makes the row meaningful (an event type nobody can interpret).
6. **Add a CHECK constraint** once the check returns 1.0. Databricks enforces it on every write, so the vocabulary cannot drift again without someone changing the constraint.

`{{ allowed_values }}` must not contain `NULL`; `NOT IN` with a NULL member returns UNKNOWN for every row and the statement silently does nothing.

## Fix: Blast radius

```sql
SELECT
    COUNT_IF({{ column }} NOT IN ({{ allowed_values }}))                                   AS invalid_rows,
    COUNT_IF({{ column }} NOT IN ({{ allowed_values }})
             AND LOWER(TRIM({{ column }})) IN (SELECT LOWER(TRIM(a)) FROM (SELECT explode(array({{ allowed_values }})) AS a))) AS fixable_by_normalization,
    COUNT(*)                                                                               AS non_null_rows
FROM {{ catalog }}.{{ schema }}.{{ asset }}
WHERE {{ column }} IS NOT NULL
```

## Fix: Normalize case and whitespace

Rewrites a value to the canonical allowed spelling when it matches one after `LOWER(TRIM())`. Written as a `MERGE` because Databricks `UPDATE ... SET` does not take a correlated subquery. Only rows that currently fail are matched, so it is idempotent.

```sql
MERGE INTO {{ catalog }}.{{ schema }}.{{ asset }} t
USING (SELECT explode(array({{ allowed_values }})) AS canonical) a
ON  t.{{ column }} IS NOT NULL
AND t.{{ column }} NOT IN ({{ allowed_values }})
AND LOWER(TRIM(t.{{ column }})) = LOWER(TRIM(a.canonical))
WHEN MATCHED THEN UPDATE SET t.{{ column }} = a.canonical
```

If two allowed values differ only by case (`'US'` and `'us'`), a target row matches two source rows and the `MERGE` fails with `DELTA_MULTIPLE_SOURCE_ROW_MATCHING_TARGET_ROW_IN_MERGE`. That is correct behaviour: the vocabulary itself is the defect; resolve that first.

## Fix: Remap legacy or misspelled codes

`{{ value_mapping }}` is a `MAP` literal you have confirmed, for example `map('activ', 'active', 'ACT', 'active', 'pendng', 'pending')`.

```sql
UPDATE {{ catalog }}.{{ schema }}.{{ asset }}
SET {{ column }} = element_at({{ value_mapping }}, {{ column }})
WHERE {{ column }} IN (SELECT explode(map_keys({{ value_mapping }})))
```

Idempotent: once a value is remapped it no longer appears in the map's keys.

## Fix: NULL unknown values

```sql
UPDATE {{ catalog }}.{{ schema }}.{{ asset }}
SET {{ column }} = NULL
WHERE {{ column }} IS NOT NULL
  AND {{ column }} NOT IN ({{ allowed_values }})
```

## Fix: Quarantine, then delete rows with unknown values

```sql
CREATE TABLE IF NOT EXISTS {{ catalog }}.{{ schema }}.{{ asset }}_invalid_categories
AS SELECT *, '' AS violated_column, current_timestamp() AS quarantined_at
   FROM {{ catalog }}.{{ schema }}.{{ asset }} WHERE 1 = 0;

INSERT INTO {{ catalog }}.{{ schema }}.{{ asset }}_invalid_categories
SELECT *, '{{ column }}', current_timestamp()
FROM {{ catalog }}.{{ schema }}.{{ asset }}
WHERE {{ column }} IS NOT NULL AND {{ column }} NOT IN ({{ allowed_values }});

DELETE FROM {{ catalog }}.{{ schema }}.{{ asset }}
WHERE {{ column }} IS NOT NULL AND {{ column }} NOT IN ({{ allowed_values }});
```

## Fix: Declare the vocabulary as a CHECK constraint

Guard:

```sql
SELECT 1
FROM {{ catalog }}.information_schema.table_constraints
WHERE LOWER(table_schema) = LOWER('{{ schema }}')
  AND LOWER(table_name)   = LOWER('{{ asset }}')
  AND constraint_name = '{{ column }}_allowed_values'
```

If no row, and the check returns 1.0:

```sql
ALTER TABLE {{ catalog }}.{{ schema }}.{{ asset }}
ADD CONSTRAINT {{ column }}_allowed_values
CHECK ({{ column }} IS NULL OR {{ column }} IN ({{ allowed_values }}))
```

`ADD CONSTRAINT` scans the table and fails if any row violates. Writers that later send an unknown code get a `DELTA_VIOLATE_CONSTRAINT_WITH_VALUES` error, so tell the pipeline owner; for raw landing tables a Lakeflow expectation that quarantines is usually the better place for the rule.

Vocabularies that change often (product lines, campaign codes) should not be CHECK constraints. Keep them in a reference table, use the reference variant of the check, and declare a `FOREIGN KEY ... NOT ENFORCED` to the reference so `referential_integrity` can discover the pair.

## Fix: Bulk generation of CHECK constraints for low-cardinality string columns

Emits, for each STRING column in the table with at most `{{ max_cardinality }}` (default 20) distinct values and no existing CHECK constraint, a query that prints the `ALTER TABLE` statement with the observed vocabulary. Two hops because the vocabulary needs a data scan. Review each vocabulary before running its statement: observed is not the same as allowed.

```sql
WITH string_cols AS (
    SELECT col.table_name, col.column_name
    FROM {{ catalog }}.information_schema.columns col
    WHERE LOWER(col.table_schema) = LOWER('{{ schema }}')
      AND LOWER(col.table_name)   = LOWER('{{ asset }}')
      AND col.data_type = 'STRING'
      AND NOT REGEXP_LIKE(LOWER(col.column_name), '(name|description|comment|text|note|email|url|address|_id$)')
),
constrained AS (
    SELECT DISTINCT LOWER(c.sql) AS s
    FROM {{ catalog }}.information_schema.table_constraints tc
    JOIN {{ catalog }}.information_schema.check_constraints c
      ON c.constraint_name = tc.constraint_name AND c.constraint_schema = tc.constraint_schema
    WHERE LOWER(tc.table_schema) = LOWER('{{ schema }}')
      AND LOWER(tc.table_name)   = LOWER('{{ asset }}')
      AND tc.constraint_type = 'CHECK'
)
SELECT concat(
    'SELECT CASE WHEN COUNT(DISTINCT `', s.column_name, '`) <= {{ max_cardinality }} THEN concat(',
    '''ALTER TABLE {{ catalog }}.{{ schema }}.`', s.table_name, '` ADD CONSTRAINT `', s.column_name,
    '_allowed_values` CHECK (`', s.column_name, '` IS NULL OR `', s.column_name, '` IN ('', ',
    'array_join(transform(array_sort(collect_set(`', s.column_name, '`)), x -> concat('''''''', replace(x, '''''''', ''''''''''''), '''''''')), '', ''), ',
    '''));'') END AS stmt FROM {{ catalog }}.{{ schema }}.`', s.table_name, '` WHERE `', s.column_name, '` IS NOT NULL;'
) AS proposal_query
FROM string_cols s
LEFT JOIN constrained c
  ON REGEXP_LIKE(c.s, concat('(^|[^a-z0-9_])`?', LOWER(s.column_name), '`?([^a-z0-9_]|$)'))
WHERE c.s IS NULL
ORDER BY s.column_name
```

The quoting is dense because the emitted query itself builds quoted literals. For a column `status` it prints:

```sql
SELECT CASE WHEN COUNT(DISTINCT `status`) <= 20 THEN concat(
  'ALTER TABLE cat.sch.`orders` ADD CONSTRAINT `status_allowed_values` CHECK (`status` IS NULL OR `status` IN (',
  array_join(transform(array_sort(collect_set(`status`)), x -> concat('''', replace(x, '''', ''''''), '''')), ', '),
  '));') END AS stmt
FROM cat.sch.`orders` WHERE `status` IS NOT NULL;
```

## Organizational guidance

A controlled vocabulary needs an owner and a single source. Keep each code set in one reference table in a shared schema, give it a `PRIMARY KEY`, and declare `FOREIGN KEY ... NOT ENFORCED` from every table that uses it. Validate at bronze-to-silver with a Lakeflow expectation (`EXPECT (status IN (...)) ON VIOLATION DROP ROW`, or a join to the reference table with `EXPECT (ref_code IS NOT NULL)`), so unknown codes are counted in the pipeline event log and quarantined rather than found downstream. For truly fixed sets (ISO country codes, boolean-like flags) a `CHECK` constraint on the silver and gold tables is the cheapest guarantee Databricks offers.
