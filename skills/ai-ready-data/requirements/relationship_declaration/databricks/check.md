# Check: relationship_declaration

Fraction of base tables in the schema that participate in at least one declared FOREIGN KEY relationship, as the referencing (child) table or the referenced (parent) table.

## Context

This is a **native** check. Unity Catalog stores FOREIGN KEY constraints in `information_schema.table_constraints` (`constraint_type = 'FOREIGN KEY'`, `table_name` = child) and links each to the parent's PRIMARY KEY through `information_schema.referential_constraints` (`unique_constraint_schema`, `unique_constraint_name`). The parent table is found by joining that PK constraint name back to `table_constraints`. A table passes if it is the child of any FK or the parent of any FK.

FOREIGN KEY constraints on Databricks are **informational**: Delta does not reject orphan rows. They still count, and count more than any other signal, because they are the only machine-readable statement of how tables join. Genie spaces, the Assistant, dbt semantic models and BI tools read them to build join paths; without them a text-to-SQL agent has to guess joins from column names. `referential_integrity` measures whether the declared FKs actually hold in the data.

Scoping detail: FKs can cross schemas. A dimension table in `{{ schema }}` referenced by fact tables in another schema of the same catalog should pass, so the FK side of the query is read catalog-wide and only the tables in scope are restricted to `{{ schema }}`. FKs that reference a table in a different catalog are not seen by `{{ catalog }}.information_schema`; swap in `system.information_schema` if that matters.

A schema of one table can never score above 0 on the parent side and only passes if that table references a table elsewhere. That is expected; the requirement is about the join graph, and a graph with one node has no edges.

`information_schema` reflects `ADD CONSTRAINT` immediately. Rows are filtered to objects the caller can see; if the caller cannot see the parent table, the parent-side match still works because it is resolved by constraint name, not by reading the parent.

Returns NULL (N/A) when the schema contains no base tables.

## SQL

### Child or parent in any FOREIGN KEY (primary)

```sql
WITH tables_in_scope AS (
    SELECT LOWER(table_name) AS table_name
    FROM {{ catalog }}.information_schema.tables
    WHERE LOWER(table_schema) = LOWER('{{ schema }}')
      AND table_type IN ('MANAGED', 'EXTERNAL')
),
fk AS (
    SELECT LOWER(tc.table_schema) AS child_schema,
           LOWER(tc.table_name)   AS child_table,
           LOWER(pk.table_schema) AS parent_schema,
           LOWER(pk.table_name)   AS parent_table
    FROM {{ catalog }}.information_schema.table_constraints tc
    JOIN {{ catalog }}.information_schema.referential_constraints rc
      ON rc.constraint_schema = tc.constraint_schema
     AND rc.constraint_name   = tc.constraint_name
    JOIN {{ catalog }}.information_schema.table_constraints pk
      ON pk.constraint_schema = rc.unique_constraint_schema
     AND pk.constraint_name   = rc.unique_constraint_name
     AND pk.constraint_type   = 'PRIMARY KEY'
    WHERE tc.constraint_type = 'FOREIGN KEY'
),
related AS (
    SELECT child_table AS table_name FROM fk WHERE child_schema = LOWER('{{ schema }}')
    UNION
    SELECT parent_table FROM fk WHERE parent_schema = LOWER('{{ schema }}')
)
SELECT
    COUNT_IF(r.table_name IS NOT NULL)                              AS related_tables,
    COUNT(*)                                                        AS total_tables,
    COUNT_IF(r.table_name IS NOT NULL)::DOUBLE / NULLIF(COUNT(*), 0) AS value
FROM tables_in_scope t
LEFT JOIN related r USING (table_name)
```

### Child side only (variant)

Stricter: only tables that declare an outgoing FK pass. Use when the schema is a fact/event layer whose dimensions live elsewhere, and the question is "does every fact table say what it joins to".

```sql
WITH tables_in_scope AS (
    SELECT LOWER(table_name) AS table_name
    FROM {{ catalog }}.information_schema.tables
    WHERE LOWER(table_schema) = LOWER('{{ schema }}')
      AND table_type IN ('MANAGED', 'EXTERNAL')
),
children AS (
    SELECT DISTINCT LOWER(table_name) AS table_name
    FROM {{ catalog }}.information_schema.table_constraints
    WHERE LOWER(table_schema) = LOWER('{{ schema }}')
      AND constraint_type = 'FOREIGN KEY'
)
SELECT
    COUNT_IF(c.table_name IS NOT NULL)                              AS tables_with_fk,
    COUNT(*)                                                        AS total_tables,
    COUNT_IF(c.table_name IS NOT NULL)::DOUBLE / NULLIF(COUNT(*), 0) AS value
FROM tables_in_scope t
LEFT JOIN children c USING (table_name)
```

### Inferred relationships (variant, proxy)

Not a substitute for the primary. Counts a table as related if it has a column whose name equals the single-column PRIMARY KEY column of another table in the schema (for example `orders.customer_id` matching `customers` PK `customer_id`), whether or not an FK is declared. Use it only to show how much of the join graph could be declared today; the gap between this and the primary is the bulk fix's target.

```sql
WITH tables_in_scope AS (
    SELECT LOWER(table_name) AS table_name
    FROM {{ catalog }}.information_schema.tables
    WHERE LOWER(table_schema) = LOWER('{{ schema }}')
      AND table_type IN ('MANAGED', 'EXTERNAL')
),
single_col_pk AS (
    SELECT LOWER(tc.table_name) AS parent_table, LOWER(max(k.column_name)) AS pk_column
    FROM {{ catalog }}.information_schema.table_constraints tc
    JOIN {{ catalog }}.information_schema.key_column_usage k
      ON k.constraint_schema = tc.constraint_schema AND k.constraint_name = tc.constraint_name
     AND k.table_schema = tc.table_schema AND k.table_name = tc.table_name
    WHERE LOWER(tc.table_schema) = LOWER('{{ schema }}') AND tc.constraint_type = 'PRIMARY KEY'
    GROUP BY LOWER(tc.table_name)
    HAVING COUNT(*) = 1
),
inferred AS (
    SELECT LOWER(c.table_name) AS table_name
    FROM {{ catalog }}.information_schema.columns c
    JOIN single_col_pk p ON LOWER(c.column_name) = p.pk_column AND LOWER(c.table_name) <> p.parent_table
    WHERE LOWER(c.table_schema) = LOWER('{{ schema }}')
    UNION
    SELECT p.parent_table
    FROM single_col_pk p
    JOIN {{ catalog }}.information_schema.columns c
      ON LOWER(c.column_name) = p.pk_column AND LOWER(c.table_name) <> p.parent_table
    WHERE LOWER(c.table_schema) = LOWER('{{ schema }}')
)
SELECT
    COUNT_IF(i.table_name IS NOT NULL)                              AS inferred_related_tables,
    COUNT(*)                                                        AS total_tables,
    COUNT_IF(i.table_name IS NOT NULL)::DOUBLE / NULLIF(COUNT(*), 0) AS value
FROM tables_in_scope t
LEFT JOIN inferred i USING (table_name)
```
