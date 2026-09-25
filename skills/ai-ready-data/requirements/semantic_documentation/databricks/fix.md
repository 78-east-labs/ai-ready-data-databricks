# Fix: semantic_documentation

Add table and column comments, by hand, in bulk from existing metadata, or with Unity Catalog AI-generated comments.

## Context

Comments are metadata only: `COMMENT ON TABLE` / `COMMENT ON COLUMN` rewrite nothing, create no Delta history and need `MODIFY` on the table (or ownership). They are visible immediately in `information_schema`, Catalog Explorer, Genie and the Assistant.

Three paths, in the order to try them:

1. **Delegate the first draft to AI-generated comments.** This is the framework's delegation for this requirement. In Catalog Explorer, open the table, click "AI generate" on the description and on the columns, review and accept. In SQL, `ai_gen()` produces the same kind of draft and the bulk variant below turns it into `COMMENT ON` statements. Drafts describe what a column looks like from its name and type; they do not know business meaning. Accept them as a floor and correct the ones that matter (money, dates, statuses, keys).
2. **Copy comments that already exist.** If a column named `customer_id` is commented on one table, the same text almost certainly fits `customer_id` elsewhere. If the schema is a dbt project with `description:` fields, `persist_docs` writes them for free. If a source system carries descriptions, load them as a mapping table and generate the statements.
3. **Write them.** For the table comment, one or two sentences: what one row is, where the data comes from, how often it refreshes, and who owns it. For columns: meaning, unit, allowed values, and grain if it differs from the table.

Guard for every option: `COMMENT ON` replaces the existing text. Read `information_schema.tables.comment` / `columns.comment` first and prompt before overwriting a non-empty comment. All bulk variants below filter to empty comments, so they never overwrite.

`COMMENT ON COLUMN` is available on current SQL warehouses and DBR 16.1+. On older runtimes use `ALTER TABLE t ALTER COLUMN c COMMENT '...'`, which does the same thing.

## Fix: Comment one table

```sql
COMMENT ON TABLE {{ catalog }}.{{ schema }}.{{ asset }} IS '{{ table_comment }}'
```

## Fix: Comment one column

```sql
COMMENT ON COLUMN {{ catalog }}.{{ schema }}.{{ asset }}.{{ column }} IS '{{ column_comment }}'
```

Fallback syntax:

```sql
ALTER TABLE {{ catalog }}.{{ schema }}.{{ asset }} ALTER COLUMN {{ column }} COMMENT '{{ column_comment }}'
```

## Fix: Enable AI-generated comments in Catalog Explorer

Workspace admins turn the feature on under Settings > Advanced > "AI-assisted features" (it is on by default in most workspaces). Then, per table: Catalog Explorer > table > "AI generate" next to the description, review, accept; and "AI generate" on the Columns tab to draft all column comments at once. Accepted comments are written with the same `COMMENT ON` semantics and count in the check immediately. There is no bulk-accept across tables in the UI; use the `ai_gen()` variant below for that.

## Fix: Bulk propagate existing column comments by column name

Emits one `COMMENT ON COLUMN` per uncommented column whose name is commented identically elsewhere in the schema (exactly one distinct comment text for that name).

```sql
WITH comment_by_name AS (
    SELECT LOWER(c.column_name) AS column_name,
           max(c.comment)       AS comment,
           COUNT(DISTINCT c.comment) AS n_texts
    FROM {{ catalog }}.information_schema.columns c
    JOIN {{ catalog }}.information_schema.tables t
      ON c.table_schema = t.table_schema AND c.table_name = t.table_name
    WHERE LOWER(c.table_schema) = LOWER('{{ schema }}')
      AND t.table_type IN ('MANAGED', 'EXTERNAL')
      AND c.comment IS NOT NULL AND trim(c.comment) <> ''
    GROUP BY LOWER(c.column_name)
    HAVING COUNT(DISTINCT c.comment) = 1
)
SELECT concat(
    'COMMENT ON COLUMN {{ catalog }}.{{ schema }}.`', c.table_name, '`.`', c.column_name,
    '` IS ''', replace(cb.comment, '''', ''''''), ''';'
) AS stmt
FROM {{ catalog }}.information_schema.columns c
JOIN {{ catalog }}.information_schema.tables t
  ON c.table_schema = t.table_schema AND c.table_name = t.table_name
JOIN comment_by_name cb ON LOWER(c.column_name) = cb.column_name
WHERE LOWER(c.table_schema) = LOWER('{{ schema }}')
  AND t.table_type IN ('MANAGED', 'EXTERNAL')
  AND (c.comment IS NULL OR trim(c.comment) = '')
ORDER BY c.table_name, c.ordinal_position
```

Show the generated statements to the user before executing them. Generic names (`value`, `type`, `status`, `name`) are the false positives; drop those lines.

## Fix: Bulk draft comments with ai_gen()

Emits `COMMENT ON TABLE` and `COMMENT ON COLUMN` statements for one table, with the text drafted by `ai_gen()` from the object names, types and sibling columns. Requires a serverless or Pro SQL warehouse in a region where `ai_gen` is available; elsewhere use Catalog Explorer. One model call per object; run per table.

```sql
WITH cols AS (
    SELECT c.table_name, c.column_name, c.full_data_type, c.ordinal_position, c.comment,
           t.comment AS table_comment
    FROM {{ catalog }}.information_schema.columns c
    JOIN {{ catalog }}.information_schema.tables t
      ON c.table_schema = t.table_schema AND c.table_name = t.table_name
    WHERE LOWER(c.table_schema) = LOWER('{{ schema }}')
      AND LOWER(c.table_name)   = LOWER('{{ asset }}')
      AND t.table_type IN ('MANAGED', 'EXTERNAL')
),
schema_text AS (
    SELECT table_name, table_comment,
           array_join(transform(array_sort(collect_list(struct(ordinal_position, column_name, full_data_type))),
                                s -> concat(s.column_name, ' ', s.full_data_type)), ', ') AS column_list
    FROM cols GROUP BY table_name, table_comment
),
table_stmt AS (
    SELECT 0 AS ord, concat(
        'COMMENT ON TABLE {{ catalog }}.{{ schema }}.`', table_name, '` IS ''',
        replace(trim(ai_gen(concat(
            'Write a two-sentence data catalog description (no preamble) for a table named `', table_name,
            '` in schema `{{ schema }}` with columns: ', column_list,
            '. First sentence: what one row represents. Second: likely source and grain. Do not invent refresh cadence or owners.'
        ))), '''', ''''''), ''';') AS stmt
    FROM schema_text
    WHERE table_comment IS NULL OR trim(table_comment) = ''
),
column_stmts AS (
    SELECT c.ordinal_position AS ord, concat(
        'COMMENT ON COLUMN {{ catalog }}.{{ schema }}.`', c.table_name, '`.`', c.column_name, '` IS ''',
        replace(trim(ai_gen(concat(
            'Write a one-sentence data catalog description (under 25 words, no preamble) for column `', c.column_name,
            '` of type ', c.full_data_type, ' in table `', c.table_name, '` whose columns are: ', s.column_list,
            '. Mention the unit if it is a measure and the allowed values if it is obviously a status or flag.'
        ))), '''', ''''''), ''';') AS stmt
    FROM cols c
    JOIN schema_text s USING (table_name)
    WHERE c.comment IS NULL OR trim(c.comment) = ''
)
SELECT stmt FROM table_stmt
UNION ALL
SELECT stmt FROM column_stmts
ORDER BY ord
```

Review every draft before running it. Correct anything about money, time zones, keys and statuses; those are where a plausible but wrong sentence causes a wrong query later.

## Fix: Bulk apply comments from a mapping table

If descriptions exist elsewhere (a source system's data dictionary, a spreadsheet), load them as `{{ catalog }}.{{ schema }}.column_descriptions(table_name STRING, column_name STRING, description STRING)` and generate the statements:

```sql
SELECT concat(
    'COMMENT ON COLUMN {{ catalog }}.{{ schema }}.`', c.table_name, '`.`', c.column_name,
    '` IS ''', replace(d.description, '''', ''''''), ''';'
) AS stmt
FROM {{ catalog }}.information_schema.columns c
JOIN {{ catalog }}.{{ schema }}.column_descriptions d
  ON LOWER(d.table_name) = LOWER(c.table_name) AND LOWER(d.column_name) = LOWER(c.column_name)
WHERE LOWER(c.table_schema) = LOWER('{{ schema }}')
  AND (c.comment IS NULL OR trim(c.comment) = '')
  AND d.description IS NOT NULL AND trim(d.description) <> ''
ORDER BY c.table_name, c.ordinal_position
```

## Organizational guidance

Comments only stay accurate when they live with the code that builds the table. In dbt, put `description:` on models and columns and set `persist_docs: {relation: true, columns: true}` so every run writes them to Unity Catalog. In Lakeflow declarative pipelines, use the `COMMENT` clause on `CREATE STREAMING TABLE` / `CREATE MATERIALIZED VIEW` and on each column. In Terraform, `databricks_sql_table` takes `comment` at table and column level. Make an empty description on a gold-layer model a failing CI check. Use AI-generated comments to clear the backlog once, not as the ongoing process.
