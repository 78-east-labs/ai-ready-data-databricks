# Check: entity_identifier_declaration

Fraction of base tables in the schema that declare a PRIMARY KEY constraint.

## Context

This is a **native** check. Unity Catalog records PRIMARY KEY constraints in `{{ catalog }}.information_schema.table_constraints` with `constraint_type = 'PRIMARY KEY'`. A table passes when at least one such row exists for it. PRIMARY KEY on Databricks is **informational**: Delta does not reject duplicate keys. It still counts here because it is the only place the table's grain is declared in a machine-readable way, and it is what Genie, the Assistant, Feature Engineering (feature tables require a PK) and BI semantic layers read to know what one row means. The PK's columns must be NOT NULL, so a declared PK also guarantees the identifier is never missing.

Databricks does not support UNIQUE constraints (the keyword is reserved), so unlike other platforms there is no weaker identifier declaration to fall back on. A table whose uniqueness is documented only in a comment scores 0.

The check does not verify that the key is actually unique in the data; `uniqueness` does that and uses the declared PK as its default key. A PK declared with `RELY` tells the optimizer to trust uniqueness for join elimination; declare `RELY` only after `uniqueness` passes.

`information_schema` reflects `ADD CONSTRAINT` immediately. Rows are filtered to objects the caller can see. Tables in `hive_metastore` are not assessable (no constraints, no `information_schema`).

Returns NULL (N/A) when the schema contains no base tables.

## SQL

### Tables with a PRIMARY KEY (primary)

```sql
WITH tables_in_scope AS (
    SELECT LOWER(table_name) AS table_name
    FROM {{ catalog }}.information_schema.tables
    WHERE LOWER(table_schema) = LOWER('{{ schema }}')
      AND table_type IN ('MANAGED', 'EXTERNAL')
),
with_pk AS (
    SELECT DISTINCT LOWER(table_name) AS table_name
    FROM {{ catalog }}.information_schema.table_constraints
    WHERE LOWER(table_schema) = LOWER('{{ schema }}')
      AND constraint_type = 'PRIMARY KEY'
)
SELECT
    COUNT_IF(pk.table_name IS NOT NULL)                              AS tables_with_pk,
    COUNT(*)                                                         AS total_tables,
    COUNT_IF(pk.table_name IS NOT NULL)::DOUBLE / NULLIF(COUNT(*), 0) AS value
FROM tables_in_scope t
LEFT JOIN with_pk pk USING (table_name)
```

### Including streaming tables and materialized views (variant)

Lakeflow-managed objects can also carry PRIMARY KEY constraints (declared in the pipeline's `CREATE STREAMING TABLE ... CONSTRAINT` clause or on the source). Use this when the schema is mostly pipeline output and the base-table view would be near-empty.

```sql
WITH tables_in_scope AS (
    SELECT LOWER(table_name) AS table_name
    FROM {{ catalog }}.information_schema.tables
    WHERE LOWER(table_schema) = LOWER('{{ schema }}')
      AND table_type IN ('MANAGED', 'EXTERNAL', 'STREAMING_TABLE', 'MATERIALIZED_VIEW')
),
with_pk AS (
    SELECT DISTINCT LOWER(table_name) AS table_name
    FROM {{ catalog }}.information_schema.table_constraints
    WHERE LOWER(table_schema) = LOWER('{{ schema }}')
      AND constraint_type = 'PRIMARY KEY'
)
SELECT
    COUNT_IF(pk.table_name IS NOT NULL)                              AS tables_with_pk,
    COUNT(*)                                                         AS total_tables,
    COUNT_IF(pk.table_name IS NOT NULL)::DOUBLE / NULLIF(COUNT(*), 0) AS value
FROM tables_in_scope t
LEFT JOIN with_pk pk USING (table_name)
```
