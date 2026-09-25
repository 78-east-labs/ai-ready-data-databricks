# Fix: embedding_dimension_consistency

Re-embed the rows that carry the wrong dimension, then stop it recurring with a CHECK constraint on `size()`.

## Context

There is no in-place conversion between dimensions; a 768-vector cannot become a 1024-vector. The fix is to regenerate the minority rows with the model that produced the majority, and the only durable prevention is a constraint the writer cannot bypass.

Steps:

1. **Pick the target dimension.** The diagnostic's `schema_common_dimension`, or the dimension of the model the serving index was built with. A Vector Search index has one `embedding_dimension`; the table must match it.
2. **Find the model.** The column comment should name the endpoint (see `embedding_coverage` fix). If it does not, ask the owner; guessing produces vectors from a different model that pass the size check and fail retrieval silently.
3. **Null out the wrong-size vectors, re-embed them.** Data-mutating, so blast radius first. Re-embedding uses `ai_query()` and bills per token.
4. **Add a CHECK constraint** so a future writer with a different model gets an error instead of a silent mix.
5. **Re-sync any Vector Search index** built on the table (`TRIGGERED` pipelines do not pick up the change on their own).

Placeholders: `{{ target_dimension }}` (from the diagnostic), `{{ embedding_endpoint }}`, `{{ column }}` (the embedding column), `{{ text_column }}` (the source text).

Permissions: ownership or `MODIFY` on the table; `CAN QUERY` on the endpoint.

## Fix: Blast radius

```sql
SELECT
    COUNT_IF(size({{ column }}) <> {{ target_dimension }})   AS rows_to_reembed,
    COUNT_IF({{ column }} IS NOT NULL)                       AS embedded_rows,
    SUM(CASE WHEN size({{ column }}) <> {{ target_dimension }}
             THEN length({{ text_column }}) / 4 ELSE 0 END)  AS approx_tokens
FROM {{ catalog }}.{{ schema }}.{{ asset }}
```

## Fix: Re-embed the off-dimension rows

Two statements so the second is idempotent and resumable. The first clears only vectors of the wrong size; the second embeds only NULLs, so re-running either is safe.

```sql
UPDATE {{ catalog }}.{{ schema }}.{{ asset }}
SET {{ column }} = NULL
WHERE {{ column }} IS NOT NULL
  AND size({{ column }}) <> {{ target_dimension }};

UPDATE {{ catalog }}.{{ schema }}.{{ asset }}
SET {{ column }} = ai_query('{{ embedding_endpoint }}', {{ text_column }})
WHERE {{ column }} IS NULL
  AND {{ text_column }} IS NOT NULL
  AND length({{ text_column }}) > 0;
```

If the endpoint's dimension does not equal `{{ target_dimension }}`, stop: you are about to create the same problem in the other direction. `SELECT size(ai_query('{{ embedding_endpoint }}', 'probe'))` tells you the endpoint's dimension in one call.

## Fix: Enforce the dimension with a CHECK constraint

Delta CHECK constraints are enforced on write. Existing rows are validated when the constraint is added, so run this after the re-embed; it fails if any row still violates it. Guard: skip if `information_schema.table_constraints` already has `{{ asset }}_{{ column }}_dim`.

```sql
ALTER TABLE {{ catalog }}.{{ schema }}.{{ asset }}
ADD CONSTRAINT {{ asset }}_{{ column }}_dim
CHECK ({{ column }} IS NULL OR size({{ column }}) = {{ target_dimension }});
```

Also record the model in the column comment so the next person knows what produced the vectors (guard: `information_schema.columns.comment` empty, else prompt before overwriting):

```sql
COMMENT ON COLUMN {{ catalog }}.{{ schema }}.{{ asset }}.{{ column }}
IS 'Embedding of {{ text_column }} via {{ embedding_endpoint }} ({{ target_dimension }} dims)';
```

## Fix: Re-sync dependent Vector Search indexes

For every Delta Sync index whose `delta_sync_index_spec.source_table` is this table (see `vector_index_coverage` for how to list them):

```python
from databricks.sdk import WorkspaceClient
w = WorkspaceClient()
w.vector_search_indexes.sync_index(index_name="{{ catalog }}.{{ schema }}.{{ asset }}_index")
```

CLI: `databricks vector-search-indexes sync-index {{ catalog }}.{{ schema }}.{{ asset }}_index`. Continuous indexes pick the change up from the Change Data Feed on their own.

## Fix: Bulk-generate CHECK constraints for every consistent column

Once every column is clean, lock them all. Feed the diagnostic's `CONSISTENT` rows in as a temp view `consistent_columns(table_name, column_name, dimension)`.

```sql
SELECT concat(
    'ALTER TABLE {{ catalog }}.{{ schema }}.`', table_name,
    '` ADD CONSTRAINT `', table_name, '_', column_name, '_dim` ',
    'CHECK (`', column_name, '` IS NULL OR size(`', column_name, '`) = ', dimension, ');'
) AS stmt
FROM consistent_columns
ORDER BY table_name, column_name
```

Show the generated statements to the user before executing them.

## Organizational guidance

Mixed dimensions come from model changes that were rolled out to the writer before the backfill, or from two pipelines writing the same column with different endpoints. Put the endpoint name and dimension in the table-creation template alongside the CHECK constraint, and treat an embedding model change as a schema migration: new column (`embedding_v2`), full backfill, switch the index, drop the old column via the normal deprecation process. Never rewrite in place.
