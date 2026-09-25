# Fix: vector_index_coverage

Create a Delta Sync Vector Search index on each embedding-bearing (or text-bearing) table that lacks one, after enabling Change Data Feed and declaring a primary key.

## Context

A Delta Sync index needs three things from the source table: a primary key column (unique, non-null; it does not have to be a declared constraint, but declaring one keeps everyone honest), `delta.enableChangeDataFeed = true`, and either an embedding column (self-managed) or a text column plus an embedding model endpoint (managed). It also needs a Vector Search endpoint to live on; endpoints are workspace objects, billed while they exist, and one endpoint hosts many indexes.

Self-managed versus managed embeddings:

- **Self-managed** (`embedding_vector_columns`): the table already has `ARRAY<FLOAT>` vectors, produced by whatever pipeline the team runs. Use this when the vectors are also needed in SQL, or when the model is not a Databricks endpoint. The `embedding_dimension` given at creation must equal the array length (`embedding_dimension_consistency`).
- **Managed** (`embedding_source_columns`): the index computes embeddings from a text column using a serving endpoint (`databricks-gte-large-en`, or a custom endpoint). Simplest to operate; Databricks re-embeds changed rows on sync. Query-time embedding of the user's question uses the same endpoint.

Pipeline type: `TRIGGERED` syncs on demand (call `sync_index` after the writing job); `CONTINUOUS` streams from CDF with seconds of lag at higher cost. Start `TRIGGERED` unless the table changes more than hourly and staleness matters.

Idempotency: `create_index` fails if the name exists. Guard with `get_index` and skip. Never delete and recreate an index that a running agent queries; create a new one with a version suffix and switch the retriever.

Permissions: `CREATE TABLE` on the schema where the index lives (indexes are UC objects), `CAN USE` on the endpoint, `SELECT` on the source table, and ownership or `MODIFY` on the source for the `ALTER TABLE` prerequisites. Creating an endpoint needs workspace admin or the Vector Search endpoint creation entitlement.

## Fix: Prerequisites on the source table

CDF (guard: `SHOW TBLPROPERTIES {{ catalog }}.{{ schema }}.{{ asset }} ('delta.enableChangeDataFeed')` already `true`). Enabling it does not rewrite data.

```sql
ALTER TABLE {{ catalog }}.{{ schema }}.{{ asset }}
SET TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true');
```

Primary key (guard: `table_constraints` has no `PRIMARY KEY` for the table). Check uniqueness first: `SELECT COUNT(*) - COUNT(DISTINCT {{ key_column }}), COUNT_IF({{ key_column }} IS NULL) FROM {{ catalog }}.{{ schema }}.{{ asset }}` must return `0, 0`.

```sql
ALTER TABLE {{ catalog }}.{{ schema }}.{{ asset }} ALTER COLUMN {{ key_column }} SET NOT NULL;
ALTER TABLE {{ catalog }}.{{ schema }}.{{ asset }} ADD CONSTRAINT {{ asset }}_pk PRIMARY KEY ({{ key_column }});
```

## Fix: Ensure a Vector Search endpoint exists

```python
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound
from databricks.sdk.service.vectorsearch import EndpointType

w = WorkspaceClient()
try:
    w.vector_search_endpoints.get_endpoint(endpoint_name="{{ vector_search_endpoint }}")
except NotFound:
    w.vector_search_endpoints.create_endpoint_and_wait(
        name="{{ vector_search_endpoint }}", endpoint_type=EndpointType.STANDARD)
```

CLI: `databricks vector-search-endpoints create-endpoint {{ vector_search_endpoint }} STANDARD`. Provisioning takes several minutes.

## Fix: Create a self-managed Delta Sync index

```python
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound
from databricks.sdk.service.vectorsearch import (
    DeltaSyncVectorIndexSpecRequest, EmbeddingVectorColumn, PipelineType, VectorIndexType)

w = WorkspaceClient()
name = "{{ catalog }}.{{ schema }}.{{ asset }}_index"
try:
    w.vector_search_indexes.get_index(index_name=name); print("exists, skipping")
except NotFound:
    w.vector_search_indexes.create_index(
        name=name, endpoint_name="{{ vector_search_endpoint }}",
        primary_key="{{ key_column }}", index_type=VectorIndexType.DELTA_SYNC,
        delta_sync_index_spec=DeltaSyncVectorIndexSpecRequest(
            source_table="{{ catalog }}.{{ schema }}.{{ asset }}",
            pipeline_type=PipelineType.TRIGGERED,
            embedding_vector_columns=[EmbeddingVectorColumn(
                name="{{ embedding_column }}", embedding_dimension={{ embedding_dimension }})],
            columns_to_sync=["{{ key_column }}", "{{ text_column }}"]))
```

`columns_to_sync` lists the columns returned with search hits; leave it out to sync all columns. `{{ embedding_dimension }}` comes from `SELECT size({{ embedding_column }}) FROM ... LIMIT 1`.

## Fix: Create a managed-embedding Delta Sync index

```python
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound
from databricks.sdk.service.vectorsearch import (
    DeltaSyncVectorIndexSpecRequest, EmbeddingSourceColumn, PipelineType, VectorIndexType)

w = WorkspaceClient()
name = "{{ catalog }}.{{ schema }}.{{ asset }}_index"
try:
    w.vector_search_indexes.get_index(index_name=name); print("exists, skipping")
except NotFound:
    w.vector_search_indexes.create_index(
        name=name, endpoint_name="{{ vector_search_endpoint }}",
        primary_key="{{ key_column }}", index_type=VectorIndexType.DELTA_SYNC,
        delta_sync_index_spec=DeltaSyncVectorIndexSpecRequest(
            source_table="{{ catalog }}.{{ schema }}.{{ asset }}",
            pipeline_type=PipelineType.TRIGGERED,
            embedding_source_columns=[EmbeddingSourceColumn(
                name="{{ text_column }}",
                embedding_model_endpoint_name="{{ embedding_endpoint }}")]))
```

CLI for either form:

```bash
databricks vector-search-indexes create-index --json '{
  "name": "{{ catalog }}.{{ schema }}.{{ asset }}_index",
  "endpoint_name": "{{ vector_search_endpoint }}",
  "primary_key": "{{ key_column }}",
  "index_type": "DELTA_SYNC",
  "delta_sync_index_spec": {
    "source_table": "{{ catalog }}.{{ schema }}.{{ asset }}",
    "pipeline_type": "TRIGGERED",
    "embedding_source_columns": [{"name": "{{ text_column }}",
                                  "embedding_model_endpoint_name": "{{ embedding_endpoint }}"}]
  }
}'
```

The first sync starts automatically. Poll `get_index(...).status.ready`; the initial build of a large table can take tens of minutes to hours.

## Fix: Schedule syncs for TRIGGERED indexes

Add a final task to the job that writes the source table:

```python
from databricks.sdk import WorkspaceClient
WorkspaceClient().vector_search_indexes.sync_index(index_name="{{ catalog }}.{{ schema }}.{{ asset }}_index")
```

CLI: `databricks vector-search-indexes sync-index {{ catalog }}.{{ schema }}.{{ asset }}_index`. A sync on an index that is already current is a cheap no-op.

## Fix: Bulk-generate prerequisites for every NOT_INDEXED table

Feed the diagnostic's `NOT_INDEXED_*` rows in as a temp view `not_indexed(table_name)`. Index creation itself runs through the SDK loop with the same list.

```sql
SELECT concat(
    'ALTER TABLE {{ catalog }}.{{ schema }}.`', table_name,
    '` SET TBLPROPERTIES (''delta.enableChangeDataFeed'' = ''true'');'
) AS stmt
FROM not_indexed
ORDER BY table_name
```

Show the generated statements to the user before executing them.

## Organizational guidance

Keep the index definition in the same repository and deployment bundle as the table that feeds it (Databricks Asset Bundles support `vector_search_index` resources), so a new chunk table cannot ship without its index. Standardize on one endpoint per environment and one embedding endpoint per schema, name indexes `{table}_index`, and make the writing job's last task the `sync_index` call. Review the inventory quarterly for indexes whose source table no longer exists.
