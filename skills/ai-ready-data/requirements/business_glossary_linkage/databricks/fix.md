# Fix: business_glossary_linkage

Link columns to business terms with the `glossary_term` column tag, or expose them through a metric view.

## Context

Two paths:

- **Tag the column** with `{{ glossary_tag_key }} = '<term>'` (default key `glossary_term`). This is the direct fix for the check and the one that scales: `ALTER TABLE ... ALTER COLUMN ... SET TAGS` is idempotent, needs `APPLY TAG` on the table (or ownership), rewrites no data, and the tag becomes searchable in Catalog Explorer and readable from `information_schema.column_tags`. The tag value should be the canonical term identifier from your glossary (`net_revenue`, `active_customer`), not a sentence.
- **Define a metric view** over the table so the column participates in a governed measure or dimension. This is the stronger semantic signal (it gives agents and Genie a definition, not just a label), but it is more work and the check only detects it best-effort. Do it for the fact tables that matter; tag everything else.

The tag documents a decision a human must make: which business term this column means. Applying `glossary_term = 'unknown'` to every column makes the score 1.0 and the metadata useless. The bulk variant below therefore only emits statements for columns where the term can be read from existing evidence (a comment that already names it, or a matching column already tagged elsewhere in the schema), and asks for review before running.

If the account uses governed tag policies, `glossary_term` may need to be created as a governed tag with an allowed-values list first (account console > Tag policies). Check before applying.

## Fix: Tag a single column

```sql
ALTER TABLE {{ catalog }}.{{ schema }}.{{ asset }}
ALTER COLUMN {{ column }} SET TAGS ('{{ glossary_tag_key }}' = '{{ glossary_term }}')
```

Guard (skip if a row comes back with the same value; re-setting is otherwise harmless):

```sql
SELECT tag_value
FROM {{ catalog }}.information_schema.column_tags
WHERE LOWER(schema_name) = LOWER('{{ schema }}')
  AND LOWER(table_name)  = LOWER('{{ asset }}')
  AND LOWER(column_name) = LOWER('{{ column }}')
  AND LOWER(tag_name)    = LOWER('{{ glossary_tag_key }}')
```

## Fix: Propagate terms already used in the schema

When a column named `customer_id` is tagged `glossary_term = 'customer'` on one table, every other untagged `customer_id` in the schema almost certainly means the same thing. This emits one `ALTER` per untagged column whose name matches an already-tagged column name with exactly one distinct term in the schema.

```sql
WITH term_by_column_name AS (
    SELECT LOWER(column_name) AS column_name,
           max(tag_value)     AS glossary_term,
           COUNT(DISTINCT tag_value) AS n_terms
    FROM {{ catalog }}.information_schema.column_tags
    WHERE LOWER(schema_name) = LOWER('{{ schema }}')
      AND LOWER(tag_name) = LOWER('{{ glossary_tag_key }}')
      AND tag_value IS NOT NULL AND tag_value <> ''
    GROUP BY LOWER(column_name)
    HAVING COUNT(DISTINCT tag_value) = 1
),
untagged AS (
    SELECT c.table_name, c.column_name
    FROM {{ catalog }}.information_schema.columns c
    JOIN {{ catalog }}.information_schema.tables t
      ON c.table_schema = t.table_schema AND c.table_name = t.table_name
    LEFT JOIN {{ catalog }}.information_schema.column_tags ct
      ON LOWER(ct.schema_name) = LOWER(c.table_schema)
     AND LOWER(ct.table_name)  = LOWER(c.table_name)
     AND LOWER(ct.column_name) = LOWER(c.column_name)
     AND LOWER(ct.tag_name)    = LOWER('{{ glossary_tag_key }}')
    WHERE LOWER(c.table_schema) = LOWER('{{ schema }}')
      AND t.table_type IN ('MANAGED', 'EXTERNAL')
      AND ct.column_name IS NULL
)
SELECT concat(
    'ALTER TABLE {{ catalog }}.{{ schema }}.`', u.table_name,
    '` ALTER COLUMN `', u.column_name,
    '` SET TAGS (''{{ glossary_tag_key }}'' = ''', replace(tb.glossary_term, '''', ''''''), ''');'
) AS stmt
FROM untagged u
JOIN term_by_column_name tb ON LOWER(u.column_name) = tb.column_name
ORDER BY u.table_name, u.column_name
```

Show the generated statements to the user before executing them. A column name shared across tables with different meanings (`status`, `type`, `value`) is the usual false positive; drop those lines.

## Fix: Seed terms from a term list

If a glossary already exists outside Databricks (Collibra, Alation, a spreadsheet), load it as a two-column table `term_map(column_name STRING, glossary_term STRING)` and generate the tag statements from it:

```sql
SELECT concat(
    'ALTER TABLE {{ catalog }}.{{ schema }}.`', c.table_name,
    '` ALTER COLUMN `', c.column_name,
    '` SET TAGS (''{{ glossary_tag_key }}'' = ''', replace(m.glossary_term, '''', ''''''), ''');'
) AS stmt
FROM {{ catalog }}.information_schema.columns c
JOIN {{ catalog }}.information_schema.tables t
  ON c.table_schema = t.table_schema AND c.table_name = t.table_name
JOIN {{ catalog }}.{{ schema }}.term_map m
  ON LOWER(m.column_name) = LOWER(c.column_name)
WHERE LOWER(c.table_schema) = LOWER('{{ schema }}')
  AND t.table_type IN ('MANAGED', 'EXTERNAL')
ORDER BY c.table_name, c.ordinal_position
```

## Fix: Expose the column through a metric view

For fact tables, define the business measures once in a metric view. Columns referenced as `dimensions` or `measures` sources count as linked. `CREATE VIEW ... WITH METRICS` requires a SQL warehouse or DBR 16.4+; check the metric views release note for your region.

```sql
CREATE VIEW IF NOT EXISTS {{ catalog }}.{{ schema }}.{{ asset }}_metrics
WITH METRICS
LANGUAGE YAML
AS $$
version: 0.1
source: {{ catalog }}.{{ schema }}.{{ asset }}
comment: Governed measures for {{ asset }}
dimensions:
  - name: order_date
    expr: order_date
  - name: customer_id
    expr: customer_id
measures:
  - name: net_revenue
    expr: SUM(amount_usd - discount_usd)
    comment: Revenue after discounts, USD
  - name: order_count
    expr: COUNT(1)
$$
```

Replace the dimension and measure lists with the table's real columns. `CREATE VIEW IF NOT EXISTS` is safe to re-run; to change an existing metric view use `CREATE OR REPLACE VIEW`, which is fine for views (it is only tables that must never be replaced).

## Organizational guidance

A glossary only works when the term list is owned. Keep the canonical term identifiers in one place (a governed tag with allowed values, or a `term_map` table under change control), and put the `glossary_term` tag into the table creation path: dbt `meta: {glossary_term: ...}` rendered to `SET TAGS` in a post-hook, Lakeflow pipeline table properties, or Terraform `databricks_sql_table` column tags. Add a check in CI that every new column on a gold-layer table carries a term, so linkage is enforced at creation rather than back-filled.
