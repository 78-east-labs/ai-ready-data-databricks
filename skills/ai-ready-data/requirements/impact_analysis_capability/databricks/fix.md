# Fix: impact_analysis_capability

Make every consumer of the schema visible to Unity Catalog lineage, and use lineage before changing a table.

## Context

Nothing can be added to a table to give it consumers. The score rises when the reads that already happen start being recorded, or when the assessment looks far enough back to see them. Causes of a `NO_CONSUMERS` table, from the diagnostic:

- **Consumers read outside Unity Catalog.** BI tools pointed at exported files, Spark jobs on non-UC clusters, or other engines reading the storage location. Point them at a SQL warehouse (JDBC/ODBC reads are recorded with `entity_type = 'QUERY'` or NULL entity but still produce the source row) or share the table through Delta Sharing, whose reads also appear in lineage.
- **Window too short.** Monthly or quarterly jobs fall outside 30 days. Re-run with a larger `{{ lookback_days }}`; lineage keeps one year.
- **Runtime below the lineage minimum** or clusters without UC access mode. Upgrade the cluster policy.
- **Lag.** Reads from the last few hours are not in the table yet.
- **Genuinely unused.** A table nobody reads is a candidate for archival, which is out of scope here but worth listing in the report.

Permissions: the caller sees lineage only for tables they can access. A low score across the whole schema, with `information_schema` populated, usually means the assessment principal lacks `SELECT` on the tables or on `system.access.table_lineage`.

## Fix: Grant lineage visibility to the assessment principal

```sql
GRANT USE SCHEMA ON SCHEMA system.access TO `{{ assessment_principal }}`;
GRANT SELECT ON TABLE system.access.table_lineage TO `{{ assessment_principal }}`;
GRANT SELECT ON SCHEMA {{ catalog }}.{{ schema }} TO `{{ assessment_principal }}`;
```

Re-granting is harmless. If `system.access` does not exist, a metastore admin enables it with `databricks system-schemas enable {{ metastore_id }} access`.

## Fix: Replace file exports with an observed read path

For a consumer that pulls files from the table's storage location, expose the same data through a view on a warehouse or through a share. Reads of the view are attributed to the underlying table in lineage.

```sql
CREATE VIEW IF NOT EXISTS {{ catalog }}.{{ schema }}.{{ asset }}_serving
COMMENT 'Read surface for {{ consumer_name }}; replaces direct file access so reads appear in lineage'
AS SELECT * FROM {{ catalog }}.{{ schema }}.{{ asset }}
```

For cross-organization consumers, create a Delta Share once (`databricks shares create {{ share_name }}` fails if it exists, so check `databricks shares get {{ share_name }}` first) and add the table:

```bash
databricks shares get {{ share_name }} >/dev/null 2>&1 || databricks shares create {{ share_name }}
databricks shares update {{ share_name }} --json '{"updates": [{"action": "ADD", "data_object": {"name": "{{ catalog }}.{{ schema }}.{{ asset }}", "data_object_type": "TABLE"}}]}'
```

The `update` with `ADD` fails if the table is already in the share; treat that error as success.

## Fix: Run the impact query before a schema change

This is the capability the requirement is named for. Before altering `{{ asset }}`, enumerate everything that read it in the last `{{ lookback_days }}` days, with the consuming entity and the derived table, so owners can be notified:

```sql
SELECT
    UPPER(entity_type)        AS entity_type,
    entity_id,
    target_table_full_name    AS derived_table,
    user_identity.email       AS run_as,
    COUNT(*)                  AS reads,
    MAX(event_time)           AS last_read
FROM system.access.table_lineage
WHERE LOWER(source_table_full_name) = LOWER('{{ catalog }}.{{ schema }}.{{ asset }}')
  AND event_time >= current_timestamp() - INTERVAL {{ lookback_days }} DAYS
GROUP BY 1, 2, 3, 4
ORDER BY last_read DESC
```

For column drops or type changes, use `system.access.column_lineage` with `source_column_name = '{{ column }}'` in the same shape to find only the consumers that touch that column.

## Organizational guidance

Make the impact query above a required step in the change process: a pull request that alters a governed table attaches its output. Keep consumers on the platform by policy (warehouses and Delta Sharing for external access, no direct storage credentials for BI tools), and run the assessment with a window that matches the longest reporting cycle the schema serves.
