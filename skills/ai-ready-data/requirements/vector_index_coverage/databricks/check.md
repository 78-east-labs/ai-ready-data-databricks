# Check: vector_index_coverage

Fraction of embedding-bearing tables in the schema that are the source table of at least one Mosaic AI Vector Search index.

## Context

Vector Search is the Databricks primitive for approximate nearest neighbour retrieval. Without an index, a similarity query is a full scan with a UDF; with a Delta Sync index, the table is served from an endpoint with HNSW-style retrieval and filters. Index metadata is not in `information_schema` or any system table, so this is an **SDK-mode** check.

Population (denominator): base tables with an `ARRAY<FLOAT>` or `ARRAY<DOUBLE>` column, found from `information_schema.columns.full_data_type` (same scoping as `embedding_coverage`). Numerator: those tables whose full name equals `delta_sync_index_spec.source_table` of at least one index on any Vector Search endpoint in the workspace. Direct Access indexes have no source table (rows are pushed through the API) and cannot credit a table; the SDK snippet reports them separately.

Managed-embedding shops (Delta Sync index with `embedding_source_columns`, Databricks computes vectors) keep no embedding column in the table, so those tables are absent from the primary denominator even though they are indexed. The text-table variant swaps the denominator to text-bearing tables (chunk tables, document tables) so that layout is scored: fraction of text tables with an index, whichever way the embeddings are produced. Pick the variant that matches how the schema stores embeddings; the diagnostic shows both.

What the signal proves: an index object exists whose declared source is this table. It does not prove the index is online or caught up; `retrieval_recall_compliance` checks `status.ready`, the pipeline type and the indexed row count. An index in `PROVISIONING` or `FAILED` state still counts here.

SDK notes. `w.vector_search_endpoints.list_endpoints()` lists endpoints; `w.vector_search_indexes.list_indexes(endpoint_name=...)` returns lightweight index objects (`name`, `endpoint_name`, `primary_key`, `index_type`, `creator`); `w.vector_search_indexes.get_index(index_name=...)` returns the full object with `delta_sync_index_spec.source_table`, `.pipeline_type`, `.embedding_source_columns`, `.embedding_vector_columns`, `direct_access_index_spec`, `status.ready`, `status.indexed_row_count`. On some SDK releases the list objects already include the spec; the snippet calls `get_index` only when `delta_sync_index_spec` is missing. Index names are three-level UC names and `source_table` is returned as written at creation, so both sides are lowercased before comparing.

Permissions: the caller needs access to each endpoint to list its indexes (workspace access is enough for listing endpoints; `CAN USE` on the endpoint or ownership of the index for `get_index`), and `USE CATALOG` / `USE SCHEMA` / `SELECT` on the index object in Unity Catalog. Indexes the caller cannot see are missing from the numerator; the report should say how many endpoints were readable.

No lag; the API returns current state.

Returns NULL (N/A) when the schema has no embedding-bearing tables (primary) or no text-bearing tables (variant).

## SQL

### Embedding-bearing tables (denominator)

```sql
SELECT DISTINCT concat(LOWER('{{ catalog }}'), '.', LOWER('{{ schema }}'), '.', LOWER(c.table_name)) AS full_name
FROM {{ catalog }}.information_schema.columns c
JOIN {{ catalog }}.information_schema.tables t
  ON c.table_schema = t.table_schema AND c.table_name = t.table_name
WHERE LOWER(c.table_schema) = LOWER('{{ schema }}')
  AND t.table_type IN ('MANAGED', 'EXTERNAL')
  AND LOWER(c.full_data_type) IN ('array<float>', 'array<double>')
ORDER BY full_name
```

### Text-bearing tables (variant denominator)

```sql
SELECT DISTINCT concat(LOWER('{{ catalog }}'), '.', LOWER('{{ schema }}'), '.', LOWER(c.table_name)) AS full_name
FROM {{ catalog }}.information_schema.columns c
JOIN {{ catalog }}.information_schema.tables t
  ON c.table_schema = t.table_schema AND c.table_name = t.table_name
WHERE LOWER(c.table_schema) = LOWER('{{ schema }}')
  AND t.table_type IN ('MANAGED', 'EXTERNAL')
  AND c.data_type = 'STRING'
  AND REGEXP_LIKE(LOWER(c.column_name),
      '(text|content|description|body|message|comment|summary|abstract|document|article|note|transcript|review|chunk)')
ORDER BY full_name
```

### Indexed source tables (numerator, SDK)

Python, `databricks-sdk>=0.20`.

```python
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound, PermissionDenied

w = WorkspaceClient()
indexed_sources, direct_access, unreadable = {}, [], []
for ep in w.vector_search_endpoints.list_endpoints():
    try:
        for ix in w.vector_search_indexes.list_indexes(endpoint_name=ep.name):
            full = ix if getattr(ix, "delta_sync_index_spec", None) else \
                   w.vector_search_indexes.get_index(index_name=ix.name)
            spec = full.delta_sync_index_spec
            if spec and spec.source_table:
                indexed_sources.setdefault(spec.source_table.lower(), []).append(ix.name)
            else:
                direct_access.append(ix.name)
    except (NotFound, PermissionDenied) as e:
        unreadable.append((ep.name, type(e).__name__))

tables = [...]  # full_name rows from the SQL above, already lowercase
covered = [t for t in tables if t in indexed_sources]
value = len(covered) / len(tables) if tables else None
print(len(covered), len(tables), value, "unreadable endpoints:", unreadable)
```

CLI equivalent:

```bash
databricks vector-search-endpoints list-endpoints --output json | jq -r '.[].name' \
| while read ep; do
    databricks vector-search-indexes list-indexes "$ep" --output json | jq -r '.[].name' \
    | while read ix; do
        databricks vector-search-indexes get-index "$ix" --output json \
        | jq -r '[.name, .index_type, (.delta_sync_index_spec.source_table // "")] | @tsv'
      done
  done
```

Aggregate: `value = tables whose full name appears as a source_table / tables`, NULL when the table list is empty. Report `tables_with_index`, `embedding_tables` (or `text_tables` for the variant), `value`, and the count of unreadable endpoints.
