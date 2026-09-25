# Diagnostic: vector_index_coverage

One row per embedding-bearing or text-bearing table with the indexes built on it (name, endpoint, type, pipeline type, embedding mode, ready flag, indexed rows), Change Data Feed status, and a status label; plus an inventory of every index in the workspace and what it points at.

## Context

The SQL side lists the candidate tables (both denominators from the check, tagged by which one they belong to) with the two facts an index creation needs: the primary key columns and whether `delta.enableChangeDataFeed` is on (Delta Sync indexes require it). CDF is a table property, so it comes from a `DESCRIBE DETAIL` probe per table; the SQL emits the probe list.

The SDK side produces the index inventory. Join the two on lowercase table full name. Status per table:

- `INDEXED`: at least one Delta Sync index names the table as source.
- `INDEXED_NOT_READY`: indexed but every index reports `status.ready = false` (provisioning or failed). Counts for this check, fails `retrieval_recall_compliance`.
- `NOT_INDEXED_READY`: no index, but the table has a PK and CDF, so an index can be created now.
- `NOT_INDEXED_BLOCKED`: no index and missing PK or CDF; prerequisites first.

`embedding_mode` on each index is `managed` (`embedding_source_columns` set, Databricks computes vectors) or `self_managed` (`embedding_vector_columns` set, the table holds vectors). A self-managed index on a table whose embedding column is mostly NULL indexes nothing useful; cross-check with the `embedding_coverage` population probe.

Sorted `NOT_INDEXED_READY` first (cheapest wins), then blocked, then indexed.

## SQL

### Candidate tables with primary keys

```sql
WITH embedding_tables AS (
    SELECT DISTINCT LOWER(c.table_name) AS table_name, 'embedding' AS population
    FROM {{ catalog }}.information_schema.columns c
    JOIN {{ catalog }}.information_schema.tables t
      ON c.table_schema = t.table_schema AND c.table_name = t.table_name
    WHERE LOWER(c.table_schema) = LOWER('{{ schema }}')
      AND t.table_type IN ('MANAGED', 'EXTERNAL')
      AND LOWER(c.full_data_type) IN ('array<float>', 'array<double>')
),
text_tables AS (
    SELECT DISTINCT LOWER(c.table_name) AS table_name, 'text' AS population
    FROM {{ catalog }}.information_schema.columns c
    JOIN {{ catalog }}.information_schema.tables t
      ON c.table_schema = t.table_schema AND c.table_name = t.table_name
    WHERE LOWER(c.table_schema) = LOWER('{{ schema }}')
      AND t.table_type IN ('MANAGED', 'EXTERNAL')
      AND c.data_type = 'STRING'
      AND REGEXP_LIKE(LOWER(c.column_name),
          '(text|content|description|body|message|comment|summary|abstract|document|article|note|transcript|review|chunk)')
),
candidates AS (
    SELECT table_name, array_sort(collect_set(population)) AS populations
    FROM (SELECT * FROM embedding_tables UNION ALL SELECT * FROM text_tables)
    GROUP BY table_name
),
pk_columns AS (
    SELECT LOWER(k.table_name) AS table_name,
           array_sort(collect_list(struct(k.ordinal_position, k.column_name))).column_name AS pk_columns
    FROM {{ catalog }}.information_schema.key_column_usage k
    JOIN {{ catalog }}.information_schema.table_constraints c
      ON k.constraint_name = c.constraint_name
     AND k.table_schema = c.table_schema AND k.table_name = c.table_name
    WHERE LOWER(k.table_schema) = LOWER('{{ schema }}')
      AND c.constraint_type = 'PRIMARY KEY'
    GROUP BY LOWER(k.table_name)
),
embedding_columns AS (
    SELECT LOWER(table_name) AS table_name,
           array_sort(collect_set(column_name)) AS embedding_columns
    FROM {{ catalog }}.information_schema.columns
    WHERE LOWER(table_schema) = LOWER('{{ schema }}')
      AND LOWER(full_data_type) IN ('array<float>', 'array<double>')
    GROUP BY LOWER(table_name)
)
SELECT
    concat(LOWER('{{ catalog }}'), '.', LOWER('{{ schema }}'), '.', c.table_name) AS full_name,
    c.populations,
    p.pk_columns,
    e.embedding_columns,
    concat('DESCRIBE DETAIL {{ catalog }}.{{ schema }}.`', c.table_name, '`') AS cdf_probe
FROM candidates c
LEFT JOIN pk_columns        p USING (table_name)
LEFT JOIN embedding_columns e USING (table_name)
ORDER BY full_name
```

Run each `cdf_probe` and read `properties['delta.enableChangeDataFeed']`.

### Index inventory (SDK)

```python
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound, PermissionDenied

w = WorkspaceClient()
rows = []
for ep in w.vector_search_endpoints.list_endpoints():
    try:
        for ix in w.vector_search_indexes.list_indexes(endpoint_name=ep.name):
            f = w.vector_search_indexes.get_index(index_name=ix.name)
            s, st = f.delta_sync_index_spec, f.status
            rows.append(dict(
                index=f.name, endpoint=ep.name, index_type=str(f.index_type),
                source=(s.source_table.lower() if s and s.source_table else None),
                pipeline_type=(str(s.pipeline_type) if s else None),
                embedding_mode=("managed" if s and s.embedding_source_columns else
                                "self_managed" if s and s.embedding_vector_columns else None),
                ready=(st.ready if st else None),
                indexed_rows=(st.indexed_row_count if st else None),
                message=(st.message if st else None)))
    except (NotFound, PermissionDenied) as e:
        rows.append(dict(index=None, endpoint=ep.name, message=f"unreadable: {e}"))
for r in sorted(rows, key=lambda r: (r.get("source") or "", r.get("index") or "")):
    print(r)
```

CLI: the `get-index` loop in the check file prints the same fields; add `.status.ready`, `.status.indexed_row_count`, `.delta_sync_index_spec.pipeline_type` to the `jq` projection.

### Assembled status (join rule)

For each candidate `full_name`: collect the inventory rows whose `source` equals it. Status is `INDEXED` if any row has `ready = true`, `INDEXED_NOT_READY` if rows exist but none is ready, else `NOT_INDEXED_READY` when `pk_columns` is non-null and CDF is `true`, else `NOT_INDEXED_BLOCKED` with the missing prerequisite named (`no_pk`, `no_cdf`, or both).

### Index history on one source table

Delta Sync creation and syncs are visible on the source table's history as reads by the index pipeline; the index's own state is only in the API. The source table's write cadence tells you how far a `TRIGGERED` index can drift between syncs:

```sql
SELECT version, timestamp, operation, operationMetrics['numOutputRows'] AS rows_written
FROM (DESCRIBE HISTORY {{ catalog }}.{{ schema }}.{{ asset }})
WHERE operation IN ('WRITE', 'MERGE', 'UPDATE', 'DELETE', 'STREAMING UPDATE')
ORDER BY version DESC
LIMIT 20
```
