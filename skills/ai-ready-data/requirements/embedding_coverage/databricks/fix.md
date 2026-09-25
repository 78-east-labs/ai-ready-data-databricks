# Fix: embedding_coverage

Add an embedding column to a text table and populate it with a Databricks-hosted embedding model, or let a Vector Search Delta Sync index compute the embeddings.

## Context

Two honest paths, and the choice is a design decision (model, dimension, refresh) that belongs to the team owning the table:

- **Store embeddings in the table.** `ALTER TABLE ... ADD COLUMN embedding ARRAY<FLOAT>` then `UPDATE ... SET embedding = ai_query('<embedding endpoint>', text_col)`. The vectors are then visible to SQL, reusable across indexes, and this check passes. `ai_query()` against a Foundation Model API embedding endpoint (`databricks-gte-large-en`, 1024 dimensions, or `databricks-bge-large-en`, 1024 dimensions) returns `ARRAY<FLOAT>`. It runs on serverless or Pro SQL warehouses in regions where Foundation Model APIs are available; on a classic warehouse run the same `UPDATE` from a notebook.
- **Let Vector Search compute them.** Create a Delta Sync index with `embedding_source_columns` pointing at the text column. Databricks embeds and re-embeds on sync. The vectors live in the index, not the table, so this check stays at 0 for that table while `vector_index_coverage` passes. Cheaper to operate; worse when several consumers need the raw vectors.

Pick one dimension per schema and reuse it. `embedding_dimension_consistency` measures convergence, and Vector Search indexes are single-dimension.

Cost note: `ai_query()` bills per token through the endpoint. Run the blast-radius query first, then populate in bounded batches (`WHERE embedding IS NULL LIMIT` is not valid in `UPDATE`; bound by key range or by a `date` column instead as shown below).

Guard for `ADD COLUMN`:

```sql
SELECT 1
FROM {{ catalog }}.information_schema.columns
WHERE LOWER(table_schema) = LOWER('{{ schema }}')
  AND LOWER(table_name)   = LOWER('{{ asset }}')
  AND LOWER(column_name)  = LOWER('{{ embedding_column }}')
```

Skip the `ALTER` if it returns a row. Placeholders: `{{ embedding_column }}` (default `embedding`), `{{ embedding_endpoint }}` (default `databricks-gte-large-en`), `{{ column }}` (the text column), `{{ key_column }}`.

Permissions: ownership or `MODIFY` on the table; `CAN QUERY` on the serving endpoint; for the index path, `CREATE TABLE` on the schema and `CAN USE` on the Vector Search endpoint.

## Fix: Add the embedding column

Metadata-only; no rewrite.

```sql
ALTER TABLE {{ catalog }}.{{ schema }}.{{ asset }}
ADD COLUMN {{ embedding_column }} ARRAY<FLOAT>
COMMENT 'Embedding of {{ column }} via {{ embedding_endpoint }} (1024 dims)';
```

## Fix: Populate embeddings with ai_query()

Blast radius:

```sql
SELECT COUNT(*) AS rows_to_embed,
       SUM(length({{ column }})) / 4 AS approx_tokens
FROM {{ catalog }}.{{ schema }}.{{ asset }}
WHERE {{ column }} IS NOT NULL
  AND length({{ column }}) > 0
  AND {{ embedding_column }} IS NULL
```

Populate. The `WHERE ... IS NULL` makes it idempotent; re-running embeds only rows that still need it. Text longer than the model's window (512 tokens for the `gte` and `bge` endpoints) is truncated by the endpoint, so chunk first (`chunk_readiness`) when the text is long.

```sql
UPDATE {{ catalog }}.{{ schema }}.{{ asset }}
SET {{ embedding_column }} = ai_query('{{ embedding_endpoint }}', {{ column }})
WHERE {{ column }} IS NOT NULL
  AND length({{ column }}) > 0
  AND {{ embedding_column }} IS NULL;
```

Bounded batch form for large tables (repeat with the next range):

```sql
UPDATE {{ catalog }}.{{ schema }}.{{ asset }}
SET {{ embedding_column }} = ai_query('{{ embedding_endpoint }}', {{ column }})
WHERE {{ column }} IS NOT NULL
  AND length({{ column }}) > 0
  AND {{ embedding_column }} IS NULL
  AND {{ key_column }} BETWEEN {{ batch_start }} AND {{ batch_end }};
```

## Fix: Keep embeddings current

Rows updated after the backfill have stale or NULL vectors. Either reset the vector on text change in the writing pipeline (`MERGE ... WHEN MATCHED AND src.text <> tgt.text THEN UPDATE SET text = src.text, embedding = NULL`) and rerun the idempotent `UPDATE` above on a schedule, or move to the index path where sync handles it. A scheduled job that runs the `UPDATE` hourly is enough for most tables.

## Fix: Vector Search Delta Sync index with managed embeddings

No column is added to the table. The source table needs Change Data Feed; enabling it does not rewrite data.

```sql
ALTER TABLE {{ catalog }}.{{ schema }}.{{ asset }}
SET TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true');
```

Then create the index (Python, Databricks SDK). Guard: `w.vector_search_indexes.get_index(index_name=...)` raises `NotFound` when it does not exist.

```python
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.vectorsearch import (
    DeltaSyncVectorIndexSpecRequest, EmbeddingSourceColumn, PipelineType, VectorIndexType)

w = WorkspaceClient()
w.vector_search_indexes.create_index(
    name="{{ catalog }}.{{ schema }}.{{ asset }}_index",
    endpoint_name="{{ vector_search_endpoint }}",
    primary_key="{{ key_column }}",
    index_type=VectorIndexType.DELTA_SYNC,
    delta_sync_index_spec=DeltaSyncVectorIndexSpecRequest(
        source_table="{{ catalog }}.{{ schema }}.{{ asset }}",
        pipeline_type=PipelineType.TRIGGERED,
        embedding_source_columns=[EmbeddingSourceColumn(
            name="{{ column }}", embedding_model_endpoint_name="{{ embedding_endpoint }}")],
    ),
)
```

CLI equivalent: `databricks vector-search-indexes create-index --json '{...}'` with the same fields. Triggered indexes sync on `w.vector_search_indexes.sync_index(index_name=...)`; continuous ones follow the CDF automatically.

## Fix: Bulk-generate ADD COLUMN for every NO_EMBEDDING table

Feed the diagnostic's `NO_EMBEDDING` rows in as a temp view `no_embedding(table_name)`.

```sql
SELECT concat(
    'ALTER TABLE {{ catalog }}.{{ schema }}.`', table_name,
    '` ADD COLUMN {{ embedding_column }} ARRAY<FLOAT> ',
    'COMMENT ''Embedding via {{ embedding_endpoint }}'';'
) AS stmt
FROM no_embedding
ORDER BY table_name
```

Show the generated statements to the user before executing them. Population statements are generated the same way from the `UPDATE` template once the text column per table is confirmed.

## Organizational guidance

Decide once per schema: which embedding endpoint, which dimension, table-stored or index-managed, and who pays for the tokens. Write it into the schema comment and into the pipeline template so new text tables arrive with an `embedding` column and a scheduled `ai_query()` backfill, or with an index definition in the same repo as the table. Re-embedding after a model change is a full-table operation; version the endpoint name in the column comment so the change is visible.
