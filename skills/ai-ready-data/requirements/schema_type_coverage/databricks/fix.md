# Fix: schema_type_coverage

Make each column's semantic role explicit with a comment or a `semantic_role` tag.

## Context

Two paths, and they combine well:

- **Comment the column.** `COMMENT ON COLUMN` (or `ALTER TABLE ... ALTER COLUMN ... COMMENT`) is metadata only, needs `MODIFY` on the table or ownership, and is what Catalog Explorer, Genie and the Assistant show. Write the role, the unit if any, and the allowed values if few: `Order status at last update. One of: open, paid, shipped, cancelled.` A comment is the fix that also moves `semantic_documentation`.
- **Tag the role.** `ALTER TABLE ... ALTER COLUMN ... SET TAGS ('semantic_role' = 'identifier')` gives a machine-readable role with a closed vocabulary (`identifier`, `temporal`, `measure`, `flag`, `categorical`, `attribute`, `text_content`, `embedding`). `semantic_role` is not in the framework's default tag table; if the account uses governed tag policies, create it there first with those allowed values. Tags need `APPLY TAG` on the table.

For a schema with hundreds of uncommented columns, delegate the first pass to Unity Catalog's AI-generated comments: either accept them per table in Catalog Explorer (table page > AI generate), or generate them in SQL with `ai_gen()` as shown below (serverless or Pro SQL warehouse in a supported region). Generated comments describe what the column looks like, not what it means to the business; review them before applying, and treat them as a floor, not a finish.

The `semantic_role` tag value is a decision a human should confirm: the diagnostic's `inferred_role` is a name-based guess. The bulk variant only emits tags for roles that are unambiguous from type and name (identifiers by `_id` suffix, temporal by DATE/TIMESTAMP type, flags by BOOLEAN type, embeddings by `ARRAY<FLOAT>`); everything else goes through the comment path with review.

## Fix: Comment one column

Guard: read the current comment first and prompt before overwriting a non-empty one.

```sql
SELECT comment
FROM {{ catalog }}.information_schema.columns
WHERE LOWER(table_schema) = LOWER('{{ schema }}')
  AND LOWER(table_name)   = LOWER('{{ asset }}')
  AND LOWER(column_name)  = LOWER('{{ column }}')
```

```sql
COMMENT ON COLUMN {{ catalog }}.{{ schema }}.{{ asset }}.{{ column }} IS '{{ comment }}'
```

Fallback for runtimes without `COMMENT ON COLUMN`:

```sql
ALTER TABLE {{ catalog }}.{{ schema }}.{{ asset }} ALTER COLUMN {{ column }} COMMENT '{{ comment }}'
```

## Fix: Tag one column with its role

```sql
ALTER TABLE {{ catalog }}.{{ schema }}.{{ asset }}
ALTER COLUMN {{ column }} SET TAGS ('semantic_role' = '{{ semantic_role }}')
```

## Fix: Bulk tag roles that are unambiguous from type or name

Emits one `SET TAGS` per untagged, uncommented column whose role is certain enough to apply without review: BOOLEAN columns are flags, DATE/TIMESTAMP columns are temporal, `ARRAY<FLOAT>`/`ARRAY<DOUBLE>` are embeddings, and `_id`/`_key`/`_sk`/`_uuid` suffixes are identifiers.

```sql
WITH columns_in_scope AS (
    SELECT c.table_name, c.column_name, c.data_type, c.full_data_type, c.ordinal_position
    FROM {{ catalog }}.information_schema.columns c
    JOIN {{ catalog }}.information_schema.tables t
      ON c.table_schema = t.table_schema AND c.table_name = t.table_name
    LEFT JOIN {{ catalog }}.information_schema.column_tags ct
      ON LOWER(ct.schema_name) = LOWER(c.table_schema)
     AND LOWER(ct.table_name)  = LOWER(c.table_name)
     AND LOWER(ct.column_name) = LOWER(c.column_name)
    WHERE LOWER(c.table_schema) = LOWER('{{ schema }}')
      AND t.table_type IN ('MANAGED', 'EXTERNAL')
      AND (c.comment IS NULL OR trim(c.comment) = '')
      AND ct.column_name IS NULL
),
roles AS (
    SELECT *,
           CASE
               WHEN LOWER(full_data_type) RLIKE '^array<(float|double)>'                    THEN 'embedding'
               WHEN data_type = 'BOOLEAN'                                                    THEN 'flag'
               WHEN data_type IN ('DATE', 'TIMESTAMP', 'TIMESTAMP_NTZ')                      THEN 'temporal'
               WHEN REGEXP_LIKE(LOWER(column_name), '(^id$|_id$|_key$|_sk$|_uuid$|_guid$)') THEN 'identifier'
           END AS semantic_role
    FROM columns_in_scope
)
SELECT concat(
    'ALTER TABLE {{ catalog }}.{{ schema }}.`', table_name,
    '` ALTER COLUMN `', column_name,
    '` SET TAGS (''semantic_role'' = ''', semantic_role, ''');'
) AS stmt
FROM roles
WHERE semantic_role IS NOT NULL
ORDER BY table_name, ordinal_position
```

Show the generated statements to the user before executing them.

## Fix: Bulk generate comments with ai_gen()

Emits a `COMMENT ON COLUMN` per uncommented column with a draft description produced by `ai_gen()` from the table name, column name, type and (optionally) the table comment. Requires a serverless or Pro SQL warehouse in a region where `ai_gen` is available; on other compute, use Catalog Explorer's AI-generated comments instead. Costs one model call per column, so run it per table (`{{ asset }}`) rather than per schema on the first pass.

```sql
WITH uncommented AS (
    SELECT c.table_name, c.column_name, c.full_data_type, c.ordinal_position, t.comment AS table_comment
    FROM {{ catalog }}.information_schema.columns c
    JOIN {{ catalog }}.information_schema.tables t
      ON c.table_schema = t.table_schema AND c.table_name = t.table_name
    WHERE LOWER(c.table_schema) = LOWER('{{ schema }}')
      AND LOWER(c.table_name)   = LOWER('{{ asset }}')
      AND t.table_type IN ('MANAGED', 'EXTERNAL')
      AND (c.comment IS NULL OR trim(c.comment) = '')
),
drafted AS (
    SELECT table_name, column_name, ordinal_position,
           ai_gen(concat(
               'Write a one-sentence data catalog description (under 25 words, no preamble) for the column `',
               column_name, '` of type ', full_data_type, ' in the table `', table_name, '`',
               CASE WHEN table_comment IS NOT NULL THEN concat(' described as: ', table_comment) ELSE '' END,
               '. State the semantic role (identifier, timestamp, amount, category, flag, free text) and the unit if it is a measure.'
           )) AS draft
    FROM uncommented
)
SELECT concat(
    'COMMENT ON COLUMN {{ catalog }}.{{ schema }}.`', table_name, '`.`', column_name,
    '` IS ''', replace(trim(draft), '''', ''''''), ''';'
) AS stmt
FROM drafted
ORDER BY ordinal_position
```

Review every draft. The model has seen the column name and type, not the data or the business; it will write "Unique identifier for the record" for `id` and guess at anything ambiguous. Edit the ambiguous ones before running the statements.

## Organizational guidance

Roles should be declared where columns are born. In dbt, `columns: - name: ... description: ... meta: {semantic_role: ...}` with `persist_docs: {columns: true}` writes the comment to Unity Catalog on every run, and a post-hook can render `meta` into `SET TAGS`. In Lakeflow declarative pipelines, `COMMENT` clauses on `CREATE STREAMING TABLE` columns persist to UC. Adopt a naming convention (`_id`, `_at`, `_usd`, `is_`) and enforce it in review or with a linter such as SQLFluff, so the name-pattern proxy is reliable, then use the strict variant of the check as the target.
