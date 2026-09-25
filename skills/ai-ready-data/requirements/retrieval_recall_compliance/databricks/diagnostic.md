# Diagnostic: retrieval_recall_compliance

One row per index on a table in the schema with its readiness, pipeline type, indexed rows versus source rows, sync lag, endpoint state and status message, and a status label; plus a recall probe you can run against an evaluation set.

## Context

The check condenses four conditions into pass or fail; the diagnostic shows which one fails and by how much. For each Delta Sync index whose source is a table in the schema:

- `ready`, `pipeline_type`, `status.message` (the API's own explanation when not ready: quota, failed pipeline, missing CDF, deleted source).
- `indexed_rows`, `source_rows`, `sync_gap_pct` = `(source_rows - indexed_rows) / source_rows`.
- `endpoint_state` from `w.vector_search_endpoints.get_endpoint(name).endpoint_status.state` (`ONLINE`, `PROVISIONING`, `OFFLINE`).
- `source_last_write` from `DESCRIBE HISTORY` on the table (most recent write commit timestamp) so a stale `TRIGGERED` index shows how long it has been behind.

Status:

- `COMPLIANT`: ready, pipeline type set, gap within `{{ sync_tolerance }}`.
- `STALE`: ready but gap above tolerance. Fix: sync (`TRIGGERED`) or investigate the pipeline (`CONTINUOUS`).
- `NOT_READY`: `ready = false`. Read `message`.
- `ENDPOINT_DOWN`: endpoint not `ONLINE`; every index on it is affected.
- `NO_INDEX`: the table has no Delta Sync index at all (from the denominator join).

Sorted `NO_INDEX`, `ENDPOINT_DOWN`, `NOT_READY`, `STALE`, then `COMPLIANT`.

The last section is the honest recall measurement: given an evaluation table with `(query_text, expected_chunk_id)` rows, run the index and compute recall@k. It is a small Python loop, not a metadata query, and it is the only way to put a number on recall.

## SQL

### Source tables, embedding columns and last write

```sql
SELECT concat(LOWER('{{ catalog }}'), '.', LOWER('{{ schema }}'), '.', LOWER(c.table_name)) AS full_name,
       array_sort(collect_set(c.column_name))                                   AS embedding_columns,
       concat('SELECT COUNT(*) AS source_rows FROM {{ catalog }}.{{ schema }}.`', c.table_name,
              '` WHERE `', min(c.column_name), '` IS NOT NULL')                  AS count_probe,
       concat('SELECT MAX(timestamp) AS source_last_write FROM (DESCRIBE HISTORY {{ catalog }}.{{ schema }}.`',
              c.table_name, '`) WHERE operation IN (''WRITE'',''MERGE'',''UPDATE'',''DELETE'',''STREAMING UPDATE'')')
                                                                                 AS last_write_probe
FROM {{ catalog }}.information_schema.columns c
JOIN {{ catalog }}.information_schema.tables t
  ON c.table_schema = t.table_schema AND c.table_name = t.table_name
WHERE LOWER(c.table_schema) = LOWER('{{ schema }}')
  AND t.table_type IN ('MANAGED', 'EXTERNAL')
  AND LOWER(c.full_data_type) IN ('array<float>', 'array<double>')
GROUP BY c.table_name
ORDER BY full_name
```

Run the two probes per table and hand the results to the SDK step.

### Index state (SDK)

```python
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound, PermissionDenied
from databricks.sdk.service.vectorsearch import VectorIndexType

w = WorkspaceClient()
SCHEMA_PREFIX = "{{ catalog }}.{{ schema }}.".lower()
source_rows, last_write = {...}, {...}   # from the SQL probes, keyed by full_name
TOL = {{ sync_tolerance }}
rows = []
for ep in w.vector_search_endpoints.list_endpoints():
    ep_state = str(ep.endpoint_status.state) if ep.endpoint_status else None
    try:
        for ix in w.vector_search_indexes.list_indexes(endpoint_name=ep.name):
            f = w.vector_search_indexes.get_index(index_name=ix.name)
            s, st = f.delta_sync_index_spec, f.status
            src = (s.source_table or "").lower() if s else ""
            if f.index_type != VectorIndexType.DELTA_SYNC or not src.startswith(SCHEMA_PREFIX):
                continue
            ready = bool(st and st.ready); idx_rows = (st.indexed_row_count if st else 0) or 0
            src_rows = source_rows.get(src); gap = None if not src_rows else (src_rows - idx_rows) / src_rows
            status = ("ENDPOINT_DOWN" if ep_state != "EndpointStatusState.ONLINE" and ep_state != "ONLINE"
                      else "NOT_READY" if not ready
                      else "STALE" if gap is not None and gap > TOL else "COMPLIANT")
            rows.append((status, src, f.name, ep.name, ep_state, str(s.pipeline_type), ready,
                         idx_rows, src_rows, None if gap is None else round(gap * 100, 2),
                         last_write.get(src), st.message if st else None))
    except (NotFound, PermissionDenied) as e:
        rows.append(("UNREADABLE", None, None, ep.name, ep_state, None, None, None, None, None, None, str(e)))
covered = {r[1] for r in rows}
rows += [("NO_INDEX", t, None, None, None, None, None, 0, source_rows[t], 100.0, last_write.get(t), None)
         for t in source_rows if t not in covered]
order = {"NO_INDEX": 0, "ENDPOINT_DOWN": 1, "NOT_READY": 2, "STALE": 3, "UNREADABLE": 4, "COMPLIANT": 5}
for r in sorted(rows, key=lambda r: (order[r[0]], r[1] or "")):
    print(r)  # status, source, index, endpoint, endpoint_state, pipeline_type, ready, indexed_rows,
              # source_rows, sync_gap_pct, source_last_write, message
```

The endpoint state enum's string form has differed between SDK releases (`ONLINE` versus `EndpointStatusState.ONLINE`); the comparison above accepts both. Print `ep_state` raw if in doubt.

### Recall@k against an evaluation set

Requires an eval table with `query_text` and `expected_id` (the primary key of the chunk that should be retrieved). Uses the index's own query API, so the embedding endpoint is applied consistently for managed indexes; for self-managed indexes pass `query_vector` computed with the same model instead of `query_text`.

```python
from databricks.sdk import WorkspaceClient
w = WorkspaceClient()
INDEX, K, PK = "{{ catalog }}.{{ schema }}.{{ asset }}_index", 10, "{{ key_column }}"
evals = spark.table("{{ catalog }}.{{ schema }}.{{ eval_asset }}").select("query_text", "expected_id").collect()
hits = 0
for r in evals:
    res = w.vector_search_indexes.query_index(index_name=INDEX, columns=[PK],
                                              query_text=r.query_text, num_results=K)
    ids = {row[0] for row in (res.result.data_array or [])}
    hits += str(r.expected_id) in {str(i) for i in ids}
print(f"recall@{K} = {hits / len(evals):.3f} over {len(evals)} queries")
```

A recall@10 under 0.8 on a curated eval set is a retrieval problem (chunking, model, or missing filters) that no amount of index syncing fixes; take it to the `chunk_readiness` and `embedding_coverage` fixes.
