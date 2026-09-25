# Fix: column_masking

Attach a column mask to every column tagged as PII, either per column with `SET MASK` or for the whole schema with an ABAC policy bound to the tag.

## Context

A column mask is a SQL UDF that receives the column value and returns a value of the same type; Unity Catalog calls it for every read on SQL warehouses and UC-enabled compute. Two ways to attach it:

- **Per column**: `ALTER TABLE ... ALTER COLUMN ... SET MASK fn`. Explicit, visible in `information_schema.column_masks`, one mask per column. Needs table ownership and `EXECUTE` on the function.
- **ABAC policy**: `CREATE POLICY ... COLUMN MASK fn ... MATCH COLUMNS hasTag('pii')` on the schema or catalog. Every column tagged `pii`, now or later, is masked without touching the table. This is the scalable end state once tagging is reliable. The syntax is newer and still evolving; treat the statement below as best-effort and check it against the current docs for your release.

Rules that keep masks correct:

- Gate on `is_account_group_member('group')`, which resolves nested account groups. Do not compare `current_user()` to literal emails; those lists rot.
- Match the parameter type to the column type. A STRING mask cannot be attached to a BIGINT column.
- A mask changes what every non-member sees, including pipelines that run as service principals. Add the pipeline principals to the reader group, or expect downstream tables to fill with `***`.
- Never `CREATE OR REPLACE` a function that is already attached unless the body is confirmed unchanged; the replacement takes effect on every column at once.

If the diagnostic's worklist is empty because nothing is tagged, fix tagging first (`classification`, or enable Data Classification below); masking untagged columns by name is the `anonymization_effectiveness` path.

## Fix: Enable Data Classification so PII gets tagged

```bash
databricks api patch /api/2.1/unity-catalog/catalogs/{{ catalog }} \
  --json '{"enable_auto_classification": true}'
```

After the scan, run the diagnostic's tag-key inventory and set `{{ pii_tag_key }}` to the key the classifier used.

## Fix: Create the mask function library

Guard first (skip functions that exist):

```sql
SELECT routine_name
FROM {{ catalog }}.information_schema.routines
WHERE LOWER(routine_schema) = LOWER('{{ governance_schema }}')
  AND LOWER(routine_name) IN ('mask_string_full', 'mask_email', 'mask_string_hash',
                              'mask_date_year', 'mask_number_null')
```

`{{ governance_schema }}` defaults to `governance`, `{{ privileged_group }}` to `pii_readers`.

```sql
CREATE FUNCTION {{ catalog }}.{{ governance_schema }}.mask_string_full(x STRING)
RETURNS STRING
RETURN CASE WHEN is_account_group_member('{{ privileged_group }}') THEN x ELSE '***' END;

CREATE FUNCTION {{ catalog }}.{{ governance_schema }}.mask_email(x STRING)
RETURNS STRING
RETURN CASE
    WHEN is_account_group_member('{{ privileged_group }}') THEN x
    WHEN x IS NULL THEN NULL
    ELSE concat('***@', element_at(split(x, '@'), -1))
END;

CREATE FUNCTION {{ catalog }}.{{ governance_schema }}.mask_string_hash(x STRING)
RETURNS STRING
RETURN CASE
    WHEN is_account_group_member('{{ privileged_group }}') THEN x
    WHEN x IS NULL THEN NULL
    ELSE sha2(concat('{{ hash_salt }}', x), 256)
END;

CREATE FUNCTION {{ catalog }}.{{ governance_schema }}.mask_date_year(d DATE)
RETURNS DATE
RETURN CASE WHEN is_account_group_member('{{ privileged_group }}') THEN d ELSE trunc(d, 'YEAR') END;

CREATE FUNCTION {{ catalog }}.{{ governance_schema }}.mask_number_null(n BIGINT)
RETURNS BIGINT
RETURN CASE WHEN is_account_group_member('{{ privileged_group }}') THEN n ELSE NULL END;
```

Grant `EXECUTE` to the principals that own the tables to be masked:

```sql
GRANT EXECUTE ON FUNCTION {{ catalog }}.{{ governance_schema }}.mask_string_full TO `{{ table_owner_group }}`;
```

## Fix: Attach a mask to one column

Guard:

```sql
SELECT mask_name
FROM {{ catalog }}.information_schema.column_masks
WHERE LOWER(schema_name) = LOWER('{{ schema }}')
  AND LOWER(table_name)  = LOWER('{{ asset }}')
  AND LOWER(column_name) = LOWER('{{ column }}')
```

Skip if a row comes back (or confirm the user wants to replace that mask). Then:

```sql
ALTER TABLE {{ catalog }}.{{ schema }}.{{ asset }}
ALTER COLUMN {{ column }}
SET MASK {{ catalog }}.{{ governance_schema }}.{{ mask_function }};
```

To remove a mask later: `ALTER TABLE ... ALTER COLUMN ... DROP MASK`.

## Fix: Generate SET MASK statements for every tagged, unmasked column

Chooses a function from the column type and the tag value or name. Review, then run.

```sql
WITH pii_columns AS (
    SELECT DISTINCT ct.table_name, ct.column_name, ct.tag_value
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
SELECT concat(
    'ALTER TABLE {{ catalog }}.{{ schema }}.`', p.table_name,
    '` ALTER COLUMN `', p.column_name, '` SET MASK {{ catalog }}.{{ governance_schema }}.',
    CASE
        WHEN c.data_type = 'DATE'                                          THEN 'mask_date_year'
        WHEN c.data_type IN ('BIGINT', 'INT', 'SMALLINT', 'TINYINT')       THEN 'mask_number_null'
        WHEN REGEXP_LIKE(LOWER(concat(p.column_name, ' ', p.tag_value)), 'e_?mail') THEN 'mask_email'
        WHEN REGEXP_LIKE(LOWER(concat(p.column_name, ' ', p.tag_value)),
                         'ssn|passport|national_id|tax_id|iban|card|account_number|identifier')
                                                                           THEN 'mask_string_hash'
        ELSE 'mask_string_full'
    END,
    ';'
) AS stmt,
c.data_type
FROM pii_columns p
JOIN {{ catalog }}.information_schema.columns c
  ON  LOWER(c.table_schema) = LOWER('{{ schema }}')
  AND LOWER(c.table_name)   = LOWER(p.table_name)
  AND LOWER(c.column_name)  = LOWER(p.column_name)
LEFT JOIN masked m
  ON LOWER(p.table_name) = m.table_name AND LOWER(p.column_name) = m.column_name
WHERE m.column_name IS NULL
  AND c.data_type IN ('STRING', 'DATE', 'BIGINT', 'INT', 'SMALLINT', 'TINYINT')
ORDER BY p.table_name, p.column_name
```

Columns of other types (DECIMAL, TIMESTAMP, STRUCT) are left out; write a typed mask for them by hand.

## Fix: Mask by tag with an ABAC policy (best-effort)

One policy on the schema masks every column tagged `{{ pii_tag_key }}` for everyone except the privileged group. Requires ABAC to be enabled for the metastore and `MANAGE` on the schema. Verify the exact clause order against the current `CREATE POLICY` reference before running.

Guard (view is new; if it does not exist, list policies in Catalog Explorer under the schema's Policies tab):

```sql
SELECT policy_name
FROM {{ catalog }}.information_schema.abac_policy_definitions
WHERE LOWER(policy_name) = LOWER('mask_{{ pii_tag_key }}_{{ schema }}')
```

```sql
CREATE POLICY mask_{{ pii_tag_key }}_{{ schema }}
ON SCHEMA {{ catalog }}.{{ schema }}
COLUMN MASK {{ catalog }}.{{ governance_schema }}.mask_string_full
TO `account users`
EXCEPT `{{ privileged_group }}`
FOR TABLES
MATCH COLUMNS hasTag('{{ pii_tag_key }}') AS pii_col
ON COLUMN pii_col;
```

The mask function's parameter type must match every column the policy can hit, so a STRING-only function needs the `MATCH COLUMNS` predicate to also filter on type (`hasTag('pii') AND columnType = 'STRING'`) or the policy fails on the first INT column. Because ABAC-masked columns may not appear in `information_schema.column_masks`, re-run the `anonymization_effectiveness` ABAC variant (or verify a masked query result) rather than expecting this check to move immediately.

## Fix: Verify enforcement

From a SQL warehouse as a non-member:

```sql
SELECT is_account_group_member('{{ privileged_group }}') AS am_reader,
       {{ column }}
FROM {{ catalog }}.{{ schema }}.{{ asset }}
LIMIT 5;
```

`am_reader = false` with plain values means the mask is not enforced on this compute or the function body is wrong.

## Organizational guidance

Tagging is the contract; masking should follow it automatically. Declare `pii` as a governed tag with an allowed value list (`email`, `phone`, `name`, `government_id`, `address`, `dob`, `financial`), have Data Classification plus reviewers apply it, and bind ABAC column-mask policies to the tag at catalog level so protection is inherited by every new table. Keep the mask function library in one governance schema under security-team ownership, deploy it with Terraform, and add the assessment principal to a read-only reviewer group so it can inspect function bodies without seeing data.
