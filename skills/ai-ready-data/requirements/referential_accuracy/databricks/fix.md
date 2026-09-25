# Fix: referential_accuracy

Bring the column into agreement with the authoritative source, or correct the source when the source is wrong, and make the reference the pipeline's input rather than a check.

## Context

Decide which side is right before touching either. The diagnostic breakdown drives the choice:

1. **Formatting differences** (`match_after_normalization`, `match_within_tolerance`): the values agree in substance. Normalize the source column to the reference's format, or change the check's comparison mode if the format difference is intentional.
2. **Outright mismatches where the reference is correct**: overwrite the source column from the reference with a `MERGE`. This is the standard "conform to the golden record" operation.
3. **Outright mismatches where the source is correct** (the reference is stale): the fix is in the reference system. Do not edit the reference copy in Databricks unless it is the system of record; open the ticket, or if the reference is a Delta table you own, `MERGE` the other way with the same guard.
4. **Unmatched keys**: the reference lacks the entity. Either the reference is incomplete (add the entity there) or the source key is in a different format (normalize keys, then re-check). Repointing to a sentinel is covered under `referential_integrity`.

Overwriting values from a reference is a data change with the deleted-file retention window (7 days by default) as the undo. Run the blast radius, and keep a before-image if the table has no Change Data Feed.

## Fix: Blast radius

```sql
WITH ref AS (
    SELECT {{ reference_key }} AS k, MAX({{ reference_column }}) AS v
    FROM {{ reference_table }}
    WHERE {{ reference_key }} IS NOT NULL
    GROUP BY {{ reference_key }}
)
SELECT
    COUNT_IF(r.k IS NOT NULL AND NOT (s.{{ column }} <=> r.v))     AS rows_to_overwrite,
    COUNT_IF(r.k IS NOT NULL AND NOT (s.{{ column }} <=> r.v) AND r.v IS NULL) AS rows_that_would_become_null,
    COUNT_IF(r.k IS NULL)                                          AS unmatched_rows,
    COUNT(*)                                                       AS rows_with_value
FROM {{ catalog }}.{{ schema }}.{{ asset }} s
LEFT JOIN ref r ON s.{{ join_key }} = r.k
WHERE s.{{ column }} IS NOT NULL
```

## Fix: Normalize the source column to the reference format

For text: trim and match the reference's case. Only rows that currently disagree but agree after normalization are touched.

```sql
MERGE INTO {{ catalog }}.{{ schema }}.{{ asset }} t
USING (
    SELECT {{ reference_key }} AS k, MAX({{ reference_column }}) AS v
    FROM {{ reference_table }}
    WHERE {{ reference_key }} IS NOT NULL
    GROUP BY {{ reference_key }}
) r
ON  t.{{ join_key }} = r.k
AND t.{{ column }} IS NOT NULL
AND NOT (t.{{ column }} <=> r.v)
AND LOWER(TRIM(CAST(t.{{ column }} AS STRING))) = LOWER(TRIM(CAST(r.v AS STRING)))
WHEN MATCHED THEN UPDATE SET t.{{ column }} = r.v
```

## Fix: Conform the source column to the reference

Overwrites every mismatched value with the reference's value. Rows whose reference value is NULL are skipped by default (`r.v IS NOT NULL`); remove that condition only if "the reference says nothing" should clear the source.

```sql
MERGE INTO {{ catalog }}.{{ schema }}.{{ asset }} t
USING (
    SELECT {{ reference_key }} AS k, MAX({{ reference_column }}) AS v
    FROM {{ reference_table }}
    WHERE {{ reference_key }} IS NOT NULL
    GROUP BY {{ reference_key }}
) r
ON  t.{{ join_key }} = r.k
AND t.{{ column }} IS NOT NULL
AND NOT (t.{{ column }} <=> r.v)
AND r.v IS NOT NULL
WHEN MATCHED THEN UPDATE SET t.{{ column }} = r.v
```

Idempotent: after one run nothing matches the `ON` clause. If `{{ reference_table }}` is federated, the `USING` subquery is pushed down to the remote system; for large references stage it into a Delta table first.

## Fix: Keep a before-image for rows about to change

If the table does not have Change Data Feed, capture the old values so the overwrite can be audited or reversed by key.

```sql
CREATE TABLE IF NOT EXISTS {{ catalog }}.{{ schema }}.{{ asset }}_accuracy_corrections (
    {{ join_key }}      STRING,
    column_name         STRING,
    old_value           STRING,
    new_value           STRING,
    reference_table     STRING,
    corrected_at        TIMESTAMP
);

INSERT INTO {{ catalog }}.{{ schema }}.{{ asset }}_accuracy_corrections
SELECT CAST(s.{{ join_key }} AS STRING), '{{ column }}', CAST(s.{{ column }} AS STRING), CAST(r.v AS STRING),
       '{{ reference_table }}', current_timestamp()
FROM {{ catalog }}.{{ schema }}.{{ asset }} s
JOIN (SELECT {{ reference_key }} AS k, MAX({{ reference_column }}) AS v
      FROM {{ reference_table }} WHERE {{ reference_key }} IS NOT NULL GROUP BY {{ reference_key }}) r
  ON s.{{ join_key }} = r.k
WHERE s.{{ column }} IS NOT NULL AND NOT (s.{{ column }} <=> r.v) AND r.v IS NOT NULL;
```

Run it immediately before the conform `MERGE`. Turning on Change Data Feed (`ALTER TABLE ... SET TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')`) makes this table unnecessary for future corrections; see `change_detection`.

## Fix: Normalize keys so unmatched rows can join

When `unmatched_rows` is high and the diagnostic's sample keys look like the reference keys with different padding, case or prefixes, normalize the source key rather than the value. `{{ key_expression }}` is the normalization you have verified, for example `LPAD(CAST(customer_id AS STRING), 10, '0')` or `UPPER(TRIM(country))`.

```sql
UPDATE {{ catalog }}.{{ schema }}.{{ asset }} s
SET {{ join_key }} = {{ key_expression }}
WHERE s.{{ join_key }} IS NOT NULL
  AND s.{{ join_key }} <> {{ key_expression }}
  AND NOT EXISTS (SELECT 1 FROM {{ reference_table }} r WHERE r.{{ reference_key }} = s.{{ join_key }})
  AND EXISTS     (SELECT 1 FROM {{ reference_table }} r WHERE r.{{ reference_key }} = {{ key_expression }})
```

Only keys that do not match now and would match after normalization are rewritten, so a correct key is never touched.

## Fix: Bulk generation of conform statements for every shared column

For a table that mirrors a reference record, emit one conform `MERGE` per column that exists in both tables with the same name (excluding the key). Review before running; a shared name is not proof of shared meaning.

```sql
WITH src AS (
    SELECT column_name
    FROM {{ catalog }}.information_schema.columns
    WHERE LOWER(table_schema) = LOWER('{{ schema }}') AND LOWER(table_name) = LOWER('{{ asset }}')
),
ref AS (
    SELECT column_name
    FROM {{ reference_catalog }}.information_schema.columns
    WHERE LOWER(table_schema) = LOWER('{{ reference_schema }}') AND LOWER(table_name) = LOWER('{{ reference_asset }}')
)
SELECT concat(
    'MERGE INTO {{ catalog }}.{{ schema }}.`{{ asset }}` t USING (SELECT `{{ reference_key }}` AS k, MAX(`', s.column_name,
    '`) AS v FROM {{ reference_catalog }}.{{ reference_schema }}.`{{ reference_asset }}` WHERE `{{ reference_key }}` IS NOT NULL GROUP BY `{{ reference_key }}`) r ',
    'ON t.`{{ join_key }}` = r.k AND t.`', s.column_name, '` IS NOT NULL AND NOT (t.`', s.column_name, '` <=> r.v) AND r.v IS NOT NULL ',
    'WHEN MATCHED THEN UPDATE SET t.`', s.column_name, '` = r.v;'
) AS stmt
FROM src s
JOIN ref r ON LOWER(r.column_name) = LOWER(s.column_name)
WHERE LOWER(s.column_name) NOT IN (LOWER('{{ join_key }}'), LOWER('{{ reference_key }}'))
ORDER BY s.column_name
```

`{{ reference_catalog }}.{{ reference_schema }}.{{ reference_asset }}` is `{{ reference_table }}` split into its three parts; a federated reference exposes `information_schema` through its foreign catalog as well.

## Organizational guidance

A column that has to be checked against a reference should be derived from that reference, not maintained in parallel. Rebuild the column in the pipeline as a join to the system of record (through Lakehouse Federation or a synced copy with a freshness SLA) so accuracy is structural. Where two systems are both edited, decide which one owns each attribute, write it down in the table comment, and route all edits of that attribute through the owner. Record every correction (the before-image table above, or Change Data Feed) so the source system's team can see what Databricks changed and fix the root cause on their side.
