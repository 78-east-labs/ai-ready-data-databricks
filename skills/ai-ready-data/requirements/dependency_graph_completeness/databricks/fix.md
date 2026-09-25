# Fix: dependency_graph_completeness

Bring writers and readers onto compute that Unity Catalog can observe, and mark tables that legitimately have no other side.

## Context

There is no statement that inserts a lineage edge. Lineage appears when a read or write runs on Unity Catalog-enabled compute (SQL warehouses, clusters in shared or single-user access mode on a supported runtime, serverless, Lakeflow pipelines) against a Unity Catalog table. Missing edges have a short list of causes, and the diagnostic's `status` column points at which one applies:

- `ISOLATED` with a recent `last_altered`: an external writer (Delta written from Spark outside Databricks, Trino, a vendor tool writing the storage path directly) or a cluster without Unity Catalog access mode. Move the write onto UC compute, or accept that the table is a boundary and tag it.
- `NO_DOWNSTREAM`: nobody reads it on the platform. Either it is consumed by exporting files (route the consumer through a warehouse or Delta Sharing instead), it is a dead table (candidate for archival, not a fix here), or it is read only by classic clusters on a runtime below the lineage minimum.
- `NO_UPSTREAM` with an old `last_altered`: static reference data loaded once, before the window. Widen `{{ lookback_days }}` (retention is one year) before treating this as a gap.
- Everything missing: `system.access` is not enabled or not granted. See the first fix.
- Recent runs missing: lineage lags by up to a few hours. Wait, then re-run.

No fix here rewrites data.

## Fix: Enable and grant the lineage system tables

Run as a metastore admin. Enabling is idempotent; granting twice is harmless.

```bash
databricks system-schemas enable {{ metastore_id }} access
```

```sql
GRANT USE SCHEMA ON SCHEMA system.access TO `{{ assessment_principal }}`;
GRANT SELECT ON TABLE system.access.table_lineage  TO `{{ assessment_principal }}`;
GRANT SELECT ON TABLE system.access.column_lineage TO `{{ assessment_principal }}`;
```

Lineage rows are also filtered to tables the caller can see, so the assessment principal needs `SELECT` (or at least `USE SCHEMA` plus `BROWSE`) on the schema being assessed.

## Fix: Route an external writer through Unity Catalog

For a table currently written by a process outside the platform, land the files in an external location and load them with a job that Unity Catalog observes. `COPY INTO` skips files it has already loaded, so the statement is safe to schedule.

```sql
COPY INTO {{ catalog }}.{{ schema }}.{{ asset }}
FROM '{{ source_location }}'
FILEFORMAT = PARQUET
```

Run it from a Lakeflow job so the edge also carries `entity_type = 'JOB'` (see `agent_attribution`).

## Fix: Give a dead-end table an observed consumer

If a table is genuinely consumed only outside the platform (an exported extract), create the extract through a view on a warehouse so the read is recorded, and point the external process at the view or at a Delta Share instead of at raw files. `CREATE VIEW IF NOT EXISTS` is idempotent.

```sql
CREATE VIEW IF NOT EXISTS {{ catalog }}.{{ schema }}.{{ asset }}_export
COMMENT 'Export surface for {{ consumer_name }}; reads are recorded in system.access.table_lineage'
AS SELECT * FROM {{ catalog }}.{{ schema }}.{{ asset }}
```

## Fix: Declare a boundary table

Some tables really are graph edges of the organization: a landing table fed by a partner's SFTP drop, or a table whose only reader is a regulator's extract. Lineage will never see the other side. Record that so the next assessor does not chase it. This tag documents a decision; do not apply it to tables whose missing edge is just an unmigrated job.

```sql
ALTER TABLE {{ catalog }}.{{ schema }}.{{ asset }}
SET TAGS ('lineage_boundary' = '{{ upstream_or_downstream }}',
          'lineage_boundary_reason' = '{{ short_reason }}')
```

Bulk form, for every `ISOLATED` table from the diagnostic, emitting statements with a marker the operator must replace:

```sql
WITH tables_in_scope AS (
    SELECT LOWER(table_name) AS table_name
    FROM {{ catalog }}.information_schema.tables
    WHERE LOWER(table_schema) = LOWER('{{ schema }}')
      AND table_type IN ('MANAGED', 'EXTERNAL')
),
touched AS (
    SELECT DISTINCT LOWER(target_table_name) AS table_name
    FROM system.access.table_lineage
    WHERE LOWER(target_catalog) = LOWER('{{ catalog }}') AND LOWER(target_schema) = LOWER('{{ schema }}')
      AND target_table_full_name IS NOT NULL
      AND event_time >= current_timestamp() - INTERVAL {{ lookback_days }} DAYS
    UNION
    SELECT DISTINCT LOWER(source_table_name)
    FROM system.access.table_lineage
    WHERE LOWER(source_catalog) = LOWER('{{ catalog }}') AND LOWER(source_schema) = LOWER('{{ schema }}')
      AND source_table_full_name IS NOT NULL
      AND event_time >= current_timestamp() - INTERVAL {{ lookback_days }} DAYS
)
SELECT concat(
    'ALTER TABLE {{ catalog }}.{{ schema }}.`', t.table_name,
    '` SET TAGS (''lineage_boundary'' = ''<fill_in>'', ''lineage_boundary_reason'' = ''<fill_in>'');'
) AS stmt
FROM tables_in_scope t
LEFT JOIN touched x USING (table_name)
WHERE x.table_name IS NULL
ORDER BY t.table_name
```

Show the statements to the user and do not execute any that still contain `<fill_in>`.

## Organizational guidance

Lineage coverage tracks compute policy. Require Unity Catalog access mode on every cluster policy, retire runtimes below the lineage minimum, and make Delta Sharing (not file export) the sanctioned way to hand data to other platforms. Treat a table that stays `ISOLATED` for two consecutive assessments as either a boundary to tag or a table to archive.
