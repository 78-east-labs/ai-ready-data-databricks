# Fix: retrieval_recall_compliance

Bring each index back to ready and caught up: sync stale triggered indexes, repair not-ready ones from their status message, restore the endpoint, and then measure recall against an evaluation set.

## Context

Match the fix to the diagnostic status:

- **`NO_INDEX`**: create one; that is the `vector_index_coverage` fix.
- **`STALE`** on a `TRIGGERED` index: nobody called `sync_index` after the last load. Sync now and wire the sync into the writing job.
- **`STALE`** on a `CONTINUOUS` index: the sync pipeline is behind or failed. Look at `status.message` and the index's pipeline in the Lakeflow UI; a failed pipeline usually means a schema change on the source (a dropped or retyped column that the index syncs) or CDF disabled after creation.
- **`NOT_READY`**: read `status.message`. Common causes and their fixes are below.
- **`ENDPOINT_DOWN`**: the endpoint is provisioning or was scaled to zero on a workspace where that is possible; wait or recreate. Nothing on the table side helps.
- **`COMPLIANT` but recall is poor** in the eval probe: the serving path is fine and the retrieval design is not. Re-chunk (`chunk_readiness`), change the embedding model, add metadata filters, or switch to hybrid search (`query_type="HYBRID"` combines keyword and vector scores) and re-measure.

All SDK calls below are idempotent (sync on a current index is a no-op; property sets are no-ops when already set). Never delete an index a live agent queries; if a rebuild is unavoidable, create `{{ asset }}_index_v2`, switch the retriever, then retire the old one.

Permissions: `CAN USE` on the endpoint and ownership (or `MODIFY`) of the index for sync; ownership or `MODIFY` on the source table for `ALTER TABLE`.

## Fix: Sync a stale TRIGGERED index

```python
from databricks.sdk import WorkspaceClient
w = WorkspaceClient()
w.vector_search_indexes.sync_index(index_name="{{ catalog }}.{{ schema }}.{{ asset }}_index")
```

CLI: `databricks vector-search-indexes sync-index {{ catalog }}.{{ schema }}.{{ asset }}_index`. Then poll `get_index(...).status.indexed_row_count` until it matches the source count within tolerance.

Make it durable by appending the same call as the last task of the job that writes the source table (a notebook task with the two lines above, or a Python wheel task). For Lakeflow Declarative Pipelines, add it as a downstream job task triggered on pipeline completion.

## Fix: Repair a NOT_READY index from its message

`status.message` names the cause. The frequent ones:

**Change Data Feed disabled on the source.** Re-enable it; the index resumes on the next sync. Enabling CDF does not rewrite data, but the index cannot recover the changes made while it was off, so trigger a sync afterwards and check the row count.

```sql
ALTER TABLE {{ catalog }}.{{ schema }}.{{ asset }}
SET TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true');
```

**Source table overwritten or replaced** (`CREATE OR REPLACE TABLE`, `INSERT OVERWRITE` on a `CONTINUOUS` index). Delta Sync cannot follow a table whose history was reset. Move the writer to `MERGE` or append semantics (see `incremental_update_coverage`), then create a new index version and switch the retriever.

**Embedding dimension mismatch** (self-managed). Vectors of the wrong length were written to the source. Fix with the `embedding_dimension_consistency` fix, then sync.

**Embedding model endpoint unavailable** (managed). The serving endpoint named in `embedding_source_columns` is down or the caller lost `CAN QUERY` on it. Check `w.serving_endpoints.get(name="{{ embedding_endpoint }}").state`; restore, then sync.

**Primary key no longer unique.** A load introduced duplicate keys; the sync fails on upsert. Find them:

```sql
SELECT {{ key_column }}, COUNT(*) AS n
FROM {{ catalog }}.{{ schema }}.{{ asset }}
GROUP BY {{ key_column }}
HAVING COUNT(*) > 1
ORDER BY n DESC
LIMIT 50
```

Deduplicate at the source (a `MERGE` from a deduplicated staging query, blast radius = the count above), then sync.

## Fix: Move a hot table from TRIGGERED to CONTINUOUS

Pipeline type cannot be changed in place. Create a continuous index beside the triggered one and switch the retriever when it is ready.

```python
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound
from databricks.sdk.service.vectorsearch import (
    DeltaSyncVectorIndexSpecRequest, EmbeddingSourceColumn, PipelineType, VectorIndexType)

w = WorkspaceClient()
name = "{{ catalog }}.{{ schema }}.{{ asset }}_index_cont"
try:
    w.vector_search_indexes.get_index(index_name=name); print("exists, skipping")
except NotFound:
    old = w.vector_search_indexes.get_index(index_name="{{ catalog }}.{{ schema }}.{{ asset }}_index")
    spec = old.delta_sync_index_spec
    w.vector_search_indexes.create_index(
        name=name, endpoint_name=old.endpoint_name, primary_key=old.primary_key,
        index_type=VectorIndexType.DELTA_SYNC,
        delta_sync_index_spec=DeltaSyncVectorIndexSpecRequest(
            source_table=spec.source_table, pipeline_type=PipelineType.CONTINUOUS,
            embedding_source_columns=spec.embedding_source_columns,
            embedding_vector_columns=spec.embedding_vector_columns,
            columns_to_sync=spec.columns_to_sync))
```

Continuous sync holds compute continuously; only do this for tables whose staleness costs more than the pipeline.

## Fix: Restore an endpoint

```python
from databricks.sdk import WorkspaceClient
w = WorkspaceClient()
ep = w.vector_search_endpoints.get_endpoint(endpoint_name="{{ vector_search_endpoint }}")
print(ep.endpoint_status.state, ep.endpoint_status.message)
```

`PROVISIONING` resolves on its own. `OFFLINE` with a message about quota or capacity is an account-level limit; open a support case or move indexes to another endpoint (create the index anew on the other endpoint; indexes cannot be moved).

## Fix: Bulk-sync every STALE triggered index

Feed the diagnostic's `STALE` rows with `pipeline_type` `TRIGGERED` into the loop.

```python
from databricks.sdk import WorkspaceClient
w = WorkspaceClient()
for name in stale_triggered_indexes:  # from the diagnostic
    w.vector_search_indexes.sync_index(index_name=name)
    print("synced", name)
```

## Organizational guidance

Treat the index as part of the table's pipeline: the job that writes the table syncs the index, an alert fires when `indexed_row_count` lags the source by more than tolerance for an hour, and every retriever change is gated on recall@k against the eval set (`eval_coverage`). Record the target recall and the eval set name in the index's description so the number this check cannot compute is still written down somewhere the next engineer will find it.
