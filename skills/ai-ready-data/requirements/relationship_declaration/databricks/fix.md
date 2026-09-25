# Fix: relationship_declaration

Declare FOREIGN KEY constraints between tables that join but do not say so.

## Context

`ALTER TABLE child ADD CONSTRAINT name FOREIGN KEY (cols) REFERENCES parent (cols)` is informational on Databricks: it neither scans nor rewrites data and does not fail on orphan rows. Three preconditions do apply:

- The parent must already have a PRIMARY KEY on exactly the referenced columns. If it does not, fix `entity_identifier_declaration` on the parent first.
- The child column types must match the parent key types.
- The constraint name must not already exist in the schema.

Because nothing is enforced, a wrong FK misleads every consumer that reads it (Genie will join on it). Run the orphan count first; a high orphan rate usually means the column is not really a reference to that table (or the parent is incomplete, which is a `referential_integrity` problem). Add `RELY` only when the orphan count is zero and the pipeline keeps it that way; `RELY` lets the optimizer eliminate the join entirely.

Requires ownership of (or `MODIFY` on) the child table and `SELECT` on the parent. Naming: `fk_{{ asset }}_{{ column }}`.

## Fix: Declare one FOREIGN KEY

Orphan count (`{{ parent_asset }}` and `{{ parent_column }}` are the referenced table and its PK column):

```sql
SELECT
    COUNT_IF(p.{{ parent_column }} IS NULL AND c.{{ column }} IS NOT NULL)  AS orphan_rows,
    COUNT_IF(c.{{ column }} IS NULL)                                         AS null_fk_rows,
    COUNT(*)                                                                  AS total_rows
FROM {{ catalog }}.{{ schema }}.{{ asset }} c
LEFT JOIN {{ catalog }}.{{ schema }}.{{ parent_asset }} p
  ON c.{{ column }} = p.{{ parent_column }}
```

Guard (skip if a row comes back):

```sql
SELECT 1
FROM {{ catalog }}.information_schema.table_constraints
WHERE LOWER(table_schema)    = LOWER('{{ schema }}')
  AND LOWER(table_name)      = LOWER('{{ asset }}')
  AND LOWER(constraint_name) = LOWER('fk_{{ asset }}_{{ column }}')
```

Confirm the parent PK exists on that column (must return a row):

```sql
SELECT tc.constraint_name
FROM {{ catalog }}.information_schema.table_constraints tc
JOIN {{ catalog }}.information_schema.key_column_usage k
  ON k.constraint_schema = tc.constraint_schema AND k.constraint_name = tc.constraint_name
 AND k.table_schema = tc.table_schema AND k.table_name = tc.table_name
WHERE LOWER(tc.table_schema) = LOWER('{{ schema }}')
  AND LOWER(tc.table_name)   = LOWER('{{ parent_asset }}')
  AND tc.constraint_type = 'PRIMARY KEY'
  AND LOWER(k.column_name)   = LOWER('{{ parent_column }}')
```

Apply:

```sql
ALTER TABLE {{ catalog }}.{{ schema }}.{{ asset }}
ADD CONSTRAINT fk_{{ asset }}_{{ column }}
FOREIGN KEY ({{ column }}) REFERENCES {{ catalog }}.{{ schema }}.{{ parent_asset }} ({{ parent_column }})
```

Composite keys: list the columns in PK order on both sides, `FOREIGN KEY (a, b) REFERENCES parent (a, b)`.

## Fix: Bulk declare FKs inferred from column names

Emits `ADD CONSTRAINT` for every column in the schema that has the same name as a single-column PRIMARY KEY on a different table and is not already covered by an FK. This is the same inference the diagnostic shows as `suggested_fks`. Step 1 generates orphan counts, step 2 the ALTERs for the pairs you keep.

Step 1:

```sql
WITH single_col_pk AS (
    SELECT LOWER(tc.table_name) AS parent_table, max(k.column_name) AS pk_column,
           max(c.data_type) AS pk_type
    FROM {{ catalog }}.information_schema.table_constraints tc
    JOIN {{ catalog }}.information_schema.key_column_usage k
      ON k.constraint_schema = tc.constraint_schema AND k.constraint_name = tc.constraint_name
     AND k.table_schema = tc.table_schema AND k.table_name = tc.table_name
    JOIN {{ catalog }}.information_schema.columns c
      ON c.table_schema = k.table_schema AND c.table_name = k.table_name AND c.column_name = k.column_name
    WHERE LOWER(tc.table_schema) = LOWER('{{ schema }}') AND tc.constraint_type = 'PRIMARY KEY'
    GROUP BY LOWER(tc.table_name)
    HAVING COUNT(*) = 1
),
declared AS (
    SELECT LOWER(k.table_name) AS table_name, LOWER(k.column_name) AS column_name
    FROM {{ catalog }}.information_schema.table_constraints tc
    JOIN {{ catalog }}.information_schema.key_column_usage k
      ON k.constraint_schema = tc.constraint_schema AND k.constraint_name = tc.constraint_name
     AND k.table_schema = tc.table_schema AND k.table_name = tc.table_name
    WHERE LOWER(tc.table_schema) = LOWER('{{ schema }}') AND tc.constraint_type = 'FOREIGN KEY'
),
candidates AS (
    SELECT c.table_name AS child_table, c.column_name AS child_column,
           p.parent_table, p.pk_column
    FROM {{ catalog }}.information_schema.columns c
    JOIN {{ catalog }}.information_schema.tables t
      ON c.table_schema = t.table_schema AND c.table_name = t.table_name
    JOIN single_col_pk p
      ON LOWER(c.column_name) = LOWER(p.pk_column)
     AND LOWER(c.table_name) <> p.parent_table
     AND c.data_type = p.pk_type
    LEFT JOIN declared d
      ON d.table_name = LOWER(c.table_name) AND d.column_name = LOWER(c.column_name)
    WHERE LOWER(c.table_schema) = LOWER('{{ schema }}')
      AND t.table_type IN ('MANAGED', 'EXTERNAL')
      AND d.column_name IS NULL
)
SELECT concat(
    'SELECT ''', child_table, ''' AS child_table, ''', child_column, ''' AS child_column, ''',
    parent_table, ''' AS parent_table, ',
    'COUNT_IF(p.`', pk_column, '` IS NULL AND c.`', child_column, '` IS NOT NULL) AS orphan_rows, COUNT(*) AS total_rows ',
    'FROM {{ catalog }}.{{ schema }}.`', child_table, '` c LEFT JOIN {{ catalog }}.{{ schema }}.`', parent_table,
    '` p ON c.`', child_column, '` = p.`', pk_column, '`;'
) AS stmt
FROM candidates
ORDER BY child_table, child_column
```

Step 2, for the pairs you accept (`{{ accepted_triples }}` written as `('orders','customer_id','customers'), ('order_items','order_id','orders')`):

```sql
WITH single_col_pk AS (
    SELECT LOWER(tc.table_name) AS parent_table, max(k.column_name) AS pk_column
    FROM {{ catalog }}.information_schema.table_constraints tc
    JOIN {{ catalog }}.information_schema.key_column_usage k
      ON k.constraint_schema = tc.constraint_schema AND k.constraint_name = tc.constraint_name
     AND k.table_schema = tc.table_schema AND k.table_name = tc.table_name
    WHERE LOWER(tc.table_schema) = LOWER('{{ schema }}') AND tc.constraint_type = 'PRIMARY KEY'
    GROUP BY LOWER(tc.table_name)
    HAVING COUNT(*) = 1
)
SELECT concat(
    'ALTER TABLE {{ catalog }}.{{ schema }}.`', c.table_name,
    '` ADD CONSTRAINT fk_', LOWER(c.table_name), '_', LOWER(c.column_name),
    ' FOREIGN KEY (`', c.column_name, '`) REFERENCES {{ catalog }}.{{ schema }}.`', p.parent_table,
    '` (`', p.pk_column, '`);'
) AS stmt
FROM {{ catalog }}.information_schema.columns c
JOIN single_col_pk p ON LOWER(c.column_name) = LOWER(p.pk_column)
WHERE LOWER(c.table_schema) = LOWER('{{ schema }}')
  AND (LOWER(c.table_name), LOWER(c.column_name), p.parent_table) IN ( {{ accepted_triples }} )
ORDER BY c.table_name, c.column_name
```

Show the generated statements to the user before executing them. Typical false positives: a `status_id` that matches a PK named `status_id` on an unrelated lookup table, or two tables that both carry `region_id` from different source systems. Drop those lines rather than declare them.

## Fix: Declare a FK to a parent in another schema

Same statement with a qualified parent. The parent's PK is resolved catalog-wide, so the check counts it.

```sql
ALTER TABLE {{ catalog }}.{{ schema }}.{{ asset }}
ADD CONSTRAINT fk_{{ asset }}_{{ column }}
FOREIGN KEY ({{ column }}) REFERENCES {{ catalog }}.{{ parent_schema }}.{{ parent_asset }} ({{ parent_column }})
```

## Organizational guidance

Join paths belong in the model definition. dbt on Databricks renders `constraints: [{type: foreign_key, expression: "catalog.schema.parent (col)"}]` from the model contract into UC FOREIGN KEYs; Lakeflow declarative pipelines accept `CONSTRAINT ... FOREIGN KEY` clauses in `CREATE STREAMING TABLE`; Terraform's `databricks_sql_table` has a `constraint` block. Once FKs are declared, build the Genie space or semantic model from them rather than hand-writing join instructions, so the constraints are the single source of truth and drift is visible. Pair each FK with a `referential_integrity` check in the pipeline so `RELY` can be set truthfully.
