# Diagnostic: access_audit_coverage

Per-table view of reads seen by lineage versus reads seen by the audit log over the lookback window, with a status that says which side is missing.

## Context

One row per base table in the schema. `lineage_reads` counts `table_lineage` rows where the table is a source; `audit_events` counts `system.access.audit` rows that resolved the table (`getTable` or `generateTemporaryTableCredential`); `denied_events` counts audit rows whose `response.status_code` is not 200, which usually means a permission failure and is itself useful audit evidence. `distinct_readers` and `last_audit_event` come from the audit side.

Status values:

- `AUDITED`: read in lineage and present in the audit log.
- `READ_NO_AUDIT`: lineage shows reads but no audit row. For a UC table this is almost always lag (audit lands in minutes to hours) or a `request_params` key mismatch; re-run later and run the key probe from the check.
- `AUDIT_ONLY`: audit rows exist but lineage has none. Common for metadata-only access (Catalog Explorer, `DESCRIBE`) or when lineage is lagging behind audit.
- `NOT_READ`: neither source has anything. Dormant table; no audit gap.

`{{ lookback_days }}` defaults to 30. Sorted so `READ_NO_AUDIT` comes first.

## SQL

```sql
WITH tables_in_scope AS (
    SELECT LOWER(table_name) AS table_name, table_owner
    FROM {{ catalog }}.information_schema.tables
    WHERE LOWER(table_schema) = LOWER('{{ schema }}')
      AND table_type IN ('MANAGED', 'EXTERNAL')
),
lineage AS (
    SELECT LOWER(source_table_name) AS table_name,
           COUNT(*)                 AS lineage_reads,
           MAX(event_time)          AS last_lineage_read
    FROM system.access.table_lineage
    WHERE LOWER(source_catalog) = LOWER('{{ catalog }}')
      AND LOWER(source_schema)  = LOWER('{{ schema }}')
      AND source_table_name IS NOT NULL
      AND event_date >= date_sub(current_date(), {{ lookback_days }})
      AND event_time >= current_timestamp() - INTERVAL {{ lookback_days }} DAYS
    GROUP BY LOWER(source_table_name)
),
audit AS (
    SELECT LOWER(element_at(split(full_name, '\\.'), 3)) AS table_name,
           COUNT(*)                                       AS audit_events,
           COUNT_IF(status_code <> 200)                   AS denied_events,
           COUNT(DISTINCT reader)                         AS distinct_readers,
           MAX(event_time)                                AS last_audit_event
    FROM (
        SELECT COALESCE(request_params['full_name_arg'],
                        request_params['table_full_name']) AS full_name,
               response.status_code                        AS status_code,
               user_identity.email                         AS reader,
               event_time
        FROM system.access.audit
        WHERE service_name = 'unityCatalog'
          AND action_name IN ('getTable', 'generateTemporaryTableCredential')
          AND event_date >= date_sub(current_date(), {{ lookback_days }})
          AND event_time >= current_timestamp() - INTERVAL {{ lookback_days }} DAYS
    )
    WHERE full_name IS NOT NULL
      AND LOWER(element_at(split(full_name, '\\.'), 1)) = LOWER('{{ catalog }}')
      AND LOWER(element_at(split(full_name, '\\.'), 2)) = LOWER('{{ schema }}')
    GROUP BY LOWER(element_at(split(full_name, '\\.'), 3))
)
SELECT
    t.table_name,
    t.table_owner,
    COALESCE(l.lineage_reads, 0)     AS lineage_reads,
    l.last_lineage_read,
    COALESCE(a.audit_events, 0)      AS audit_events,
    COALESCE(a.denied_events, 0)     AS denied_events,
    COALESCE(a.distinct_readers, 0)  AS distinct_readers,
    a.last_audit_event,
    CASE
        WHEN l.table_name IS NOT NULL AND a.table_name IS NOT NULL THEN 'AUDITED'
        WHEN l.table_name IS NOT NULL AND a.table_name IS NULL     THEN 'READ_NO_AUDIT'
        WHEN l.table_name IS NULL     AND a.table_name IS NOT NULL THEN 'AUDIT_ONLY'
        ELSE 'NOT_READ'
    END AS status
FROM tables_in_scope t
LEFT JOIN lineage l USING (table_name)
LEFT JOIN audit   a USING (table_name)
ORDER BY
    CASE status
        WHEN 'READ_NO_AUDIT' THEN 0
        WHEN 'AUDIT_ONLY'    THEN 1
        WHEN 'NOT_READ'      THEN 2
        ELSE 3
    END,
    t.table_name
```
