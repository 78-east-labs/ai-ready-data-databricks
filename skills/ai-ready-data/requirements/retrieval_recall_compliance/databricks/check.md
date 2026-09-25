# Check: retrieval_recall_compliance

Fraction of embedding-bearing tables in the schema that are served by a Vector Search index which is online (`status.ready = true`), is a Delta Sync index with a pipeline type set, and is caught up with its source.

## Context

Recall is a property of a retriever measured against labeled queries; no platform metadata can produce it. What the platform can show is whether the conditions for good recall hold: the index is up, it is fed from the table (not a stale one-off upload), and it contains the rows the table contains. That is what this **proxy** check measures, and it should be read as "nothing in the serving path is known to be broken", not as a recall number. Real recall comes from an evaluation set (`eval_coverage`) run through `mlflow.genai.evaluate()` with retrieval metrics.

Per table, at least one index must satisfy all of:

- `delta_sync_index_spec.source_table` equals the table (case-insensitive). Direct Access indexes are excluded: nothing ties their content to the table.
- `index_type == DELTA_SYNC` and `delta_sync_index_spec.pipeline_type` is `TRIGGERED` or `CONTINUOUS` (present on every Delta Sync index; a missing value indicates an object the API could not fully describe).
- `status.ready == true`.
- **Caught up:** `status.indexed_row_count >= (1 - {{ sync_tolerance }}) * source_rows`, where `source_rows` is `SELECT COUNT(*)` on the table with the same null filter the index applies (rows with a NULL embedding or NULL text are not indexed). `{{ sync_tolerance }}` defaults to `0.05`. A `TRIGGERED` index that has not been synced since a large load fails this leg and that is the intended finding.

Denominator: base tables with an `ARRAY<FLOAT>` / `ARRAY<DOUBLE>` column (self-managed embeddings). For managed-embedding schemas swap in the text-table denominator from `vector_index_coverage`; the SDK snippet takes either list.

Caveats. `indexed_row_count` reflects the last completed sync; a `CONTINUOUS` index mid-catch-up reads low for minutes. Counting the source is a table scan; on very large tables use the `DESCRIBE HISTORY` row-count approximation in the variant. An index can be ready, synced and still return poor neighbours because the embedding model is wrong for the domain; this check cannot see that.

SDK objects and fields are as in `vector_index_coverage`: `list_endpoints`, `list_indexes(endpoint_name)`, `get_index(index_name)` with `delta_sync_index_spec`, `status.ready`, `status.indexed_row_count`, `status.message`. Permissions: same as that check, plus `SELECT` on each source table for the count.

No lag on the API; the table count is live.

Returns NULL (N/A) when the schema has no embedding-bearing tables.

## SQL

### Embedding-bearing tables with their embedding column (denominator)

```sql
SELECT concat(LOWER('{{ catalog }}'), '.', LOWER('{{ schema }}'), '.', LOWER(c.table_name)) AS full_name,
       min(c.column_name) AS embedding_column
FROM {{ catalog }}.information_schema.columns c
JOIN {{ catalog }}.information_schema.tables t
  ON c.table_schema = t.table_schema AND c.table_name = t.table_name
WHERE LOWER(c.table_schema) = LOWER('{{ schema }}')
  AND t.table_type IN ('MANAGED', 'EXTERNAL')
  AND LOWER(c.full_data_type) IN ('array<float>', 'array<double>')
GROUP BY 1
ORDER BY 1
```

### Indexable row count for one table (run per table, feeds the caught-up test)

```sql
SELECT COUNT(*) AS source_rows
FROM {{ catalog }}.{{ schema }}.{{ asset }}
WHERE {{ column }} IS NOT NULL
```

`{{ column }}` is the embedding column for a self-managed index, or the text column for a managed one.

### Compliance per table (SDK)

```python
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound, PermissionDenied
from databricks.sdk.service.vectorsearch import VectorIndexType

w = WorkspaceClient()
TOL = {{ sync_tolerance }}
indexes = {}  # source_table -> list of (name, ready, pipeline_type, indexed_rows)
for ep in w.vector_search_endpoints.list_endpoints():
    try:
        for ix in w.vector_search_indexes.list_indexes(endpoint_name=ep.name):
            f = w.vector_search_indexes.get_index(index_name=ix.name)
            s = f.delta_sync_index_spec
            if f.index_type == VectorIndexType.DELTA_SYNC and s and s.source_table:
                indexes.setdefault(s.source_table.lower(), []).append(
                    (f.name, bool(f.status and f.status.ready), s.pipeline_type,
                     (f.status.indexed_row_count if f.status else None) or 0))
    except (NotFound, PermissionDenied):
        pass

source_rows = {...}  # full_name -> source_rows from the per-table SQL
tables = list(source_rows)
def compliant(t):
    return any(ready and ptype is not None and rows >= (1 - TOL) * source_rows[t]
               for _, ready, ptype, rows in indexes.get(t, []))
passing = [t for t in tables if compliant(t)]
print(len(passing), len(tables), len(passing) / len(tables) if tables else None)
```

CLI equivalent for the index side:

```bash
databricks vector-search-endpoints list-endpoints --output json | jq -r '.[].name' \
| while read ep; do
    databricks vector-search-indexes list-indexes "$ep" --output json | jq -r '.[].name' \
    | while read ix; do
        databricks vector-search-indexes get-index "$ix" --output json | jq -r \
          '[.name, .index_type, (.delta_sync_index_spec.source_table // ""),
            (.delta_sync_index_spec.pipeline_type // ""), (.status.ready // false),
            (.status.indexed_row_count // 0)] | @tsv'
      done
  done
```

Aggregate: `value = compliant tables / embedding tables`, NULL when there are none. Report `compliant_tables`, `embedding_tables`, `value`.

### Ready and fed only, no row-count test (variant)

Drop the caught-up leg when counting source rows is too expensive or the tables are known to be small and static. In the snippet, replace the `compliant` predicate with `ready and ptype is not None`. This misses stale `TRIGGERED` indexes, which is the most common real failure, so prefer the primary when scans are affordable.

### Row count from history instead of a scan (variant)

For very large tables, approximate `source_rows` from the last `OPTIMIZE` or `WRITE` commit's `operationMetrics['numOutputRows']` is unreliable (it counts rows in touched files only). The cheap exact option is the Delta statistics on a warehouse: `SELECT COUNT(*)` on a Delta table without a `WHERE` reads file-level counts from the log and does not scan data. Use the null-filtered count only for tables where the embedding column is known to be sparsely populated; otherwise:

```sql
SELECT COUNT(*) AS source_rows
FROM {{ catalog }}.{{ schema }}.{{ asset }}
```
