# Fix: anonymization_effectiveness

Find the PII the name heuristic misses, then attach column masks to every PII column that lacks one.

## Context

Order of operations:

1. **Widen discovery before masking.** The check only sees well-named columns. Enable Databricks Data Classification on the catalog so the scanner tags PII by content, then re-run the diagnostic; the newly tagged columns join the worklist through `column_masking`.
2. **Create one mask function per data shape, not per column.** A mask is a SQL UDF that takes the column value and returns the same type. Keep them in a governance schema, gate on `is_account_group_member()` (account-level groups, evaluated with group nesting) rather than `current_user()` string comparisons, and make the redacted branch return something that keeps downstream joins working when possible (a stable hash rather than a constant).
3. **Attach the mask.** `ALTER TABLE ... ALTER COLUMN ... SET MASK` is metadata only. It needs table ownership plus `EXECUTE` on the function for the table owner. A column can carry exactly one mask, so run the guard first.
4. **Verify from a warehouse.** Masks are enforced on SQL warehouses and UC-enabled compute. Query the column as a non-member and confirm redaction; then re-run the check.

The check is also satisfied by ABAC policies that mask by tag, which scale better once PII is tagged. That path is in `column_masking`'s fix.

## Fix: Enable Data Classification to find untagged PII

```bash
databricks api patch /api/2.1/unity-catalog/catalogs/{{ catalog }} \
  --json '{"enable_auto_classification": true}'
```

Results arrive as column tags after the scan interval. Review them in Catalog Explorer or with the `classification` diagnostic before masking on their basis; the classifier is probabilistic.

## Fix: Create reusable mask functions

Guard: skip any function that already exists.

```sql
SELECT routine_name
FROM {{ catalog }}.information_schema.routines
WHERE LOWER(routine_schema) = LOWER('{{ governance_schema }}')
  AND LOWER(routine_name) IN ('mask_string_full', 'mask_email', 'mask_string_hash', 'mask_date_year')
```

Create only the ones missing. `{{ governance_schema }}` defaults to `governance`; `{{ privileged_group }}` defaults to `pii_readers`.

```sql
-- Full redaction for free-text identifiers (names, addresses, card numbers).
CREATE FUNCTION {{ catalog }}.{{ governance_schema }}.mask_string_full(x STRING)
RETURNS STRING
RETURN CASE WHEN is_account_group_member('{{ privileged_group }}') THEN x ELSE '***' END;

-- Keeps the domain so aggregate analysis by provider still works.
CREATE FUNCTION {{ catalog }}.{{ governance_schema }}.mask_email(x STRING)
RETURNS STRING
RETURN CASE
    WHEN is_account_group_member('{{ privileged_group }}') THEN x
    WHEN x IS NULL THEN NULL
    ELSE concat('***@', element_at(split(x, '@'), -1))
END;

-- Deterministic pseudonym: joins between masked tables still line up.
CREATE FUNCTION {{ catalog }}.{{ governance_schema }}.mask_string_hash(x STRING)
RETURNS STRING
RETURN CASE
    WHEN is_account_group_member('{{ privileged_group }}') THEN x
    WHEN x IS NULL THEN NULL
    ELSE sha2(concat('{{ hash_salt }}', x), 256)
END;

-- Dates of birth: keep the year, drop month and day.
CREATE FUNCTION {{ catalog }}.{{ governance_schema }}.mask_date_year(d DATE)
RETURNS DATE
RETURN CASE WHEN is_account_group_member('{{ privileged_group }}') THEN d ELSE trunc(d, 'YEAR') END;
```

`{{ hash_salt }}` must be a secret the organisation controls; a hash without a salt is reversible for low-entropy values like phone numbers. Use `CREATE OR REPLACE FUNCTION` only when the user confirms the body is unchanged, because replacing a function that is already attached changes behaviour on every masked column at once.

Grant execution to the owners of the tables that will attach the mask:

```sql
GRANT EXECUTE ON FUNCTION {{ catalog }}.{{ governance_schema }}.mask_string_full TO `{{ table_owner_group }}`;
```

## Fix: Attach a mask to one column

Guard (skip if a row comes back; a column holds one mask and a second `SET MASK` replaces it):

```sql
SELECT mask_name
FROM {{ catalog }}.information_schema.column_masks
WHERE LOWER(schema_name) = LOWER('{{ schema }}')
  AND LOWER(table_name)  = LOWER('{{ asset }}')
  AND LOWER(column_name) = LOWER('{{ column }}')
```

Then:

```sql
ALTER TABLE {{ catalog }}.{{ schema }}.{{ asset }}
ALTER COLUMN {{ column }}
SET MASK {{ catalog }}.{{ governance_schema }}.mask_string_full;
```

The mask function's parameter type must match the column type (STRING mask on a STRING column, DATE mask on a DATE column). For a mask that needs another column to decide (for example, only redact when `country = 'DE'`), add `USING COLUMNS (country)` after the function name and give the function a second parameter.

## Fix: Generate SET MASK statements for every unprotected candidate

Picks a function by column name and type. Review the output, remove false positives (surrogate keys, hashes, IDs that merely contain `address`), then run the remainder.

```sql
WITH pii_columns AS (
    SELECT c.table_name, c.column_name, c.data_type
    FROM {{ catalog }}.information_schema.columns c
    JOIN {{ catalog }}.information_schema.tables t
      ON c.table_schema = t.table_schema AND c.table_name = t.table_name
    WHERE LOWER(c.table_schema) = LOWER('{{ schema }}')
      AND t.table_type IN ('MANAGED', 'EXTERNAL')
      AND REGEXP_LIKE(LOWER(c.column_name), '{{ pii_patterns }}')
),
masked AS (
    SELECT DISTINCT LOWER(table_name) AS table_name, LOWER(column_name) AS column_name
    FROM {{ catalog }}.information_schema.column_masks
    WHERE LOWER(schema_name) = LOWER('{{ schema }}')
)
SELECT concat(
    'ALTER TABLE {{ catalog }}.{{ schema }}.`', p.table_name,
    '` ALTER COLUMN `', p.column_name, '` SET MASK {{ catalog }}.{{ governance_schema }}.',
    CASE
        WHEN p.data_type = 'DATE'                                  THEN 'mask_date_year'
        WHEN REGEXP_LIKE(LOWER(p.column_name), '(^|_)e?_?mail($|_)') THEN 'mask_email'
        WHEN REGEXP_LIKE(LOWER(p.column_name), '(^|_)(ssn|passport|national_id|tax_id|iban|card_number|credit_card)($|_)')
                                                                   THEN 'mask_string_hash'
        ELSE 'mask_string_full'
    END,
    ';'
) AS stmt,
p.data_type
FROM pii_columns p
LEFT JOIN masked m
  ON LOWER(p.table_name) = m.table_name AND LOWER(p.column_name) = m.column_name
WHERE m.column_name IS NULL
  AND p.data_type IN ('STRING', 'DATE')
ORDER BY p.table_name, p.column_name
```

Candidates whose type is not STRING or DATE (for example a BIGINT `phone`) are excluded; write a typed mask for those by hand.

## Fix: Verify a mask redacts for non-members

Run from a SQL warehouse as a user outside `{{ privileged_group }}`:

```sql
SELECT {{ column }} FROM {{ catalog }}.{{ schema }}.{{ asset }} LIMIT 5;
```

If plain values come back, the caller is a member of the group (check with `SELECT is_account_group_member('{{ privileged_group }}')`), or the compute does not enforce UC masks (single-user clusters on old runtimes), or the mask body is wrong.

## Organizational guidance

Name heuristics are the floor, not the policy. Make PII tagging the source of truth (Data Classification plus human review), express masking as ABAC policies bound to the `pii` tag so new columns inherit protection the moment they are tagged, and keep mask functions in one governance schema owned by the security team. Put the mask function library and the `SET MASK` statements in Terraform (`databricks_sql_table` column masks) or the table's Lakeflow pipeline definition so new tables ship masked instead of being back-filled after an assessment.
