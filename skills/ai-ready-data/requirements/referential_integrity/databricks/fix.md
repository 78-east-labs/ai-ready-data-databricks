# Fix: referential_integrity

Resolve orphan foreign-key rows and declare the relationship so future assessments can discover it.

## Context

Which fix is right depends on why the parent row is missing, so run the diagnostic first. In rough order of preference:

1. **Restore or backfill the parent.** If the parent was deleted, Delta time travel can bring the rows back without touching the child. If the parent load is late, the orphans clear on the next run; wait, and fix the scheduling (see `propagation_latency_compliance`).
2. **Point orphans at a sentinel parent row** (an `UNKNOWN` member in the dimension). Keeps the child rows and makes the join total. Standard warehouse practice; the sentinel key must exist in the parent first.
3. **NULL the orphan key.** Keeps the row, drops the broken link. Only when a NULL FK is meaningful to consumers.
4. **Quarantine or DELETE the orphan rows.** Irreversible after the deleted-file retention window (7 days by default). Prefer copying them to a quarantine table first.
5. **Declare the FOREIGN KEY constraint.** Informational only; it documents the relationship, enables discovery by the check, and with `RELY` lets the optimizer skip the join. The parent key must be declared `PRIMARY KEY` or `UNIQUE` first.

Databricks does not enforce foreign keys, so no fix here prevents new orphans. That is done in the pipeline (see Organizational guidance).

Every mutating option uses `NOT EXISTS` rather than `NOT IN (subquery)`; `NOT IN` returns UNKNOWN for every row when the parent key contains a NULL, and the statement silently does nothing.

## Fix: Blast radius

```sql
SELECT COUNT(*) AS orphan_rows
FROM {{ catalog }}.{{ schema }}.{{ asset }} s
WHERE s.{{ fk_column }} IS NOT NULL
  AND NOT EXISTS (
      SELECT 1 FROM {{ ref_table }} r WHERE r.{{ ref_key }} = s.{{ fk_column }}
  )
```

Compare with the diagnostic's `child_rows` sum before proceeding.

## Fix: Restore deleted parent rows from history

Find the last version where the parent held the missing keys, then re-insert only those rows. No child rows change.

```sql
-- 1. Find a version that still had the keys (adjust the version number from DESCRIBE HISTORY)
SELECT COUNT(*) AS recoverable
FROM {{ ref_table }} VERSION AS OF {{ parent_version }} h
WHERE h.{{ ref_key }} IN (
    SELECT DISTINCT s.{{ fk_column }}
    FROM {{ catalog }}.{{ schema }}.{{ asset }} s
    WHERE s.{{ fk_column }} IS NOT NULL
      AND NOT EXISTS (SELECT 1 FROM {{ ref_table }} r WHERE r.{{ ref_key }} = s.{{ fk_column }})
);

-- 2. Re-insert (idempotent: only keys still missing are inserted)
INSERT INTO {{ ref_table }}
SELECT h.*
FROM {{ ref_table }} VERSION AS OF {{ parent_version }} h
WHERE NOT EXISTS (SELECT 1 FROM {{ ref_table }} r WHERE r.{{ ref_key }} = h.{{ ref_key }})
  AND h.{{ ref_key }} IN (
      SELECT DISTINCT s.{{ fk_column }}
      FROM {{ catalog }}.{{ schema }}.{{ asset }} s
      WHERE s.{{ fk_column }} IS NOT NULL
  );
```

## Fix: Repoint orphans to a sentinel parent row

`{{ sentinel_key }}` is the key of the `UNKNOWN` member (for example `-1` or `'UNKNOWN'`). Insert it first if absent, then update the child. Both statements are idempotent.

```sql
INSERT INTO {{ ref_table }} ({{ ref_key }})
SELECT {{ sentinel_key }}
WHERE NOT EXISTS (SELECT 1 FROM {{ ref_table }} WHERE {{ ref_key }} = {{ sentinel_key }});

UPDATE {{ catalog }}.{{ schema }}.{{ asset }} s
SET {{ fk_column }} = {{ sentinel_key }}
WHERE s.{{ fk_column }} IS NOT NULL
  AND s.{{ fk_column }} <> {{ sentinel_key }}
  AND NOT EXISTS (
      SELECT 1 FROM {{ ref_table }} r WHERE r.{{ ref_key }} = s.{{ fk_column }}
  );
```

Fill the sentinel row's other required columns (`NOT NULL` ones) in the `INSERT`; the one-column form above only works when every other parent column is nullable or has a default.

## Fix: NULL the orphan key

```sql
UPDATE {{ catalog }}.{{ schema }}.{{ asset }} s
SET {{ fk_column }} = NULL
WHERE s.{{ fk_column }} IS NOT NULL
  AND NOT EXISTS (
      SELECT 1 FROM {{ ref_table }} r WHERE r.{{ ref_key }} = s.{{ fk_column }}
  )
```

This moves the problem to `data_completeness` on the same column. Say so in the change record.

## Fix: Quarantine, then delete orphan rows

Copy to a quarantine table (created once, appended thereafter), then delete. Re-running after a clean pass affects zero rows.

```sql
CREATE TABLE IF NOT EXISTS {{ catalog }}.{{ schema }}.{{ asset }}_orphans
AS SELECT *, current_timestamp() AS quarantined_at
   FROM {{ catalog }}.{{ schema }}.{{ asset }} WHERE 1 = 0;

INSERT INTO {{ catalog }}.{{ schema }}.{{ asset }}_orphans
SELECT s.*, current_timestamp()
FROM {{ catalog }}.{{ schema }}.{{ asset }} s
WHERE s.{{ fk_column }} IS NOT NULL
  AND NOT EXISTS (SELECT 1 FROM {{ ref_table }} r WHERE r.{{ ref_key }} = s.{{ fk_column }});

DELETE FROM {{ catalog }}.{{ schema }}.{{ asset }} s
WHERE s.{{ fk_column }} IS NOT NULL
  AND NOT EXISTS (SELECT 1 FROM {{ ref_table }} r WHERE r.{{ ref_key }} = s.{{ fk_column }});
```

## Fix: Declare the foreign key

Guard:

```sql
SELECT 1
FROM {{ catalog }}.information_schema.table_constraints
WHERE LOWER(table_schema) = LOWER('{{ schema }}')
  AND LOWER(table_name)   = LOWER('{{ asset }}')
  AND constraint_name = '{{ asset }}_{{ fk_column }}_fk'
```

If no row, and the parent key is already a `PRIMARY KEY` or `UNIQUE` constraint:

```sql
ALTER TABLE {{ catalog }}.{{ schema }}.{{ asset }}
ADD CONSTRAINT {{ asset }}_{{ fk_column }}_fk
FOREIGN KEY ({{ fk_column }}) REFERENCES {{ ref_table }} ({{ ref_key }}) NOT ENFORCED RELY
```

`NOT ENFORCED` is the only mode Unity Catalog supports for foreign keys and is stated explicitly so nobody reads the DDL as a guarantee. Drop `RELY` if the check does not score 1.0.

## Fix: Bulk generation of FOREIGN KEY statements from naming

For base tables in the schema with an `<x>_id` column that matches a sibling table `<x>` or `<x>s` whose primary key is a single column, emit the constraint. Name matching is a heuristic; run the check on each pair before executing.

```sql
WITH pk AS (
    SELECT LOWER(tc.table_name) AS parent_table, MAX(k.column_name) AS pk_column
    FROM {{ catalog }}.information_schema.table_constraints tc
    JOIN {{ catalog }}.information_schema.key_column_usage k
      ON k.constraint_name = tc.constraint_name AND k.table_schema = tc.table_schema AND k.table_name = tc.table_name
    WHERE LOWER(tc.table_schema) = LOWER('{{ schema }}') AND tc.constraint_type = 'PRIMARY KEY'
    GROUP BY LOWER(tc.table_name)
    HAVING COUNT(*) = 1
),
child AS (
    SELECT LOWER(c.table_name) AS child_table, c.column_name,
           regexp_replace(LOWER(c.column_name), '_id$', '') AS stem
    FROM {{ catalog }}.information_schema.columns c
    JOIN {{ catalog }}.information_schema.tables t
      ON t.table_schema = c.table_schema AND t.table_name = c.table_name
    WHERE LOWER(c.table_schema) = LOWER('{{ schema }}')
      AND t.table_type IN ('MANAGED', 'EXTERNAL')
      AND REGEXP_LIKE(LOWER(c.column_name), '_id$')
),
existing AS (
    SELECT DISTINCT LOWER(k.table_name) AS child_table, k.column_name
    FROM {{ catalog }}.information_schema.table_constraints tc
    JOIN {{ catalog }}.information_schema.key_column_usage k
      ON k.constraint_name = tc.constraint_name AND k.table_schema = tc.table_schema AND k.table_name = tc.table_name
    WHERE LOWER(tc.table_schema) = LOWER('{{ schema }}') AND tc.constraint_type = 'FOREIGN KEY'
)
SELECT concat(
    'ALTER TABLE {{ catalog }}.{{ schema }}.`', ch.child_table, '` ADD CONSTRAINT `',
    ch.child_table, '_', ch.column_name, '_fk` FOREIGN KEY (`', ch.column_name,
    '`) REFERENCES {{ catalog }}.{{ schema }}.`', pk.parent_table, '` (`', pk.pk_column, '`) NOT ENFORCED;'
) AS stmt
FROM child ch
JOIN pk ON pk.parent_table IN (ch.stem, concat(ch.stem, 's'), concat('dim_', ch.stem))
LEFT JOIN existing e ON e.child_table = ch.child_table AND e.column_name = ch.column_name
WHERE e.child_table IS NULL
  AND ch.child_table <> pk.parent_table
ORDER BY ch.child_table, ch.column_name
```

## Organizational guidance

Orphans are a pipeline ordering problem before they are a data problem. Load parents before children in the same job (Lakeflow pipelines can express this as a dependency; in Databricks Workflows use task dependencies, not two independent schedules). In Lakeflow, left-join the parent inside the flow and add a `CONSTRAINT fk_resolves EXPECT (parent_key IS NOT NULL) ON VIOLATION DROP ROW` expectation (expectations cannot contain subqueries, so the join has to be in the query); in dbt use the `relationships` test. Either way orphans are counted or quarantined at write time rather than found at assessment time. Declare `FOREIGN KEY ... RELY` in the DDL template so the relationship graph is discoverable in `information_schema` and in Catalog Explorer's entity-relationship view.
