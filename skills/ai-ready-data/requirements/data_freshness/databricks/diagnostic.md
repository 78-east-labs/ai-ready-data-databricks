# Diagnostic: data_freshness

One row per base table with its SLA, last write, lag, who or what wrote it last, and a status that separates stale tables from tables lineage cannot see.

## Context

Reuses the check's scoping. Adds the writer identity from the same lineage row (`entity_type` and `entity_id` tell you whether a JOB, PIPELINE, NOTEBOOK or ad-hoc QUERY did the last write; `user_identity.email` is the principal), the number of writes in the window (a table written once a month has a different problem than one written hourly that stopped), and `information_schema.tables.last_altered` for comparison.

Status values:

- `FRESH`: lag within SLA.
- `STALE`: a write exists in the last 30 days but it is older than the SLA.
- `NO_UC_WRITE_30D`: no lineage write in 30 days. Either the table is abandoned or it is written by something lineage cannot see (legacy cluster, external engine, direct storage writes). Run the `DESCRIBE HISTORY` probe on these before concluding.
- `SLA_TAG_INVALID`: tag present but not numeric.

Sorted worst-first: unseen tables, then stalest by lag ratio.

## SQL

```sql
WITH tables_in_scope AS (
    SELECT LOWER(table_name) AS table_name,
           CONCAT(LOWER('{{ catalog }}'), '.', LOWER('{{ schema }}'), '.', LOWER(table_name)) AS full_name,
           table_owner,
           last_altered
    FROM {{ catalog }}.information_schema.tables
    WHERE LOWER(table_schema) = LOWER('{{ schema }}')
      AND table_type IN ('MANAGED', 'EXTERNAL')
),
sla AS (
    SELECT LOWER(table_name) AS table_name,
           tag_value                     AS sla_tag_raw,
           TRY_CAST(tag_value AS DOUBLE) AS sla_hours
    FROM {{ catalog }}.information_schema.table_tags
    WHERE LOWER(schema_name) = LOWER('{{ schema }}')
      AND tag_name = 'freshness_sla_hours'
),
writes AS (
    SELECT LOWER(target_table_full_name) AS full_name,
           event_time,
           entity_type,
           entity_id,
           user_identity.email AS writer_email,
           ROW_NUMBER() OVER (PARTITION BY LOWER(target_table_full_name) ORDER BY event_time DESC) AS rn,
           COUNT(*)     OVER (PARTITION BY LOWER(target_table_full_name)) AS writes_30d
    FROM system.access.table_lineage
    WHERE LOWER(target_catalog) = LOWER('{{ catalog }}')
      AND LOWER(target_schema)  = LOWER('{{ schema }}')
      AND target_table_full_name IS NOT NULL
      AND event_time >= current_timestamp() - INTERVAL 30 DAYS
),
last_write AS (
    SELECT full_name, event_time AS last_write_at, entity_type, entity_id, writer_email, writes_30d
    FROM writes WHERE rn = 1
)
SELECT
    t.table_name,
    t.table_owner,
    s.sla_tag_raw,
    COALESCE(s.sla_hours, {{ default_sla_hours }})                    AS effective_sla_hours,
    s.sla_hours IS NOT NULL                                             AS sla_declared,
    lw.last_write_at,
    timestampdiff(HOUR, lw.last_write_at, current_timestamp())          AS lag_hours,
    ROUND(timestampdiff(HOUR, lw.last_write_at, current_timestamp())
          / COALESCE(s.sla_hours, {{ default_sla_hours }}), 2)          AS lag_over_sla,
    lw.writes_30d,
    lw.entity_type                                                      AS last_writer_type,
    lw.entity_id                                                        AS last_writer_id,
    lw.writer_email                                                     AS last_writer,
    t.last_altered                                                      AS info_schema_last_altered,
    CASE
        WHEN s.sla_tag_raw IS NOT NULL AND s.sla_hours IS NULL THEN 'SLA_TAG_INVALID'
        WHEN lw.last_write_at IS NULL                          THEN 'NO_UC_WRITE_30D'
        WHEN timestampdiff(HOUR, lw.last_write_at, current_timestamp())
             <= COALESCE(s.sla_hours, {{ default_sla_hours }}) THEN 'FRESH'
        ELSE 'STALE'
    END                                                                 AS status
FROM tables_in_scope t
LEFT JOIN sla        s  USING (table_name)
LEFT JOIN last_write lw USING (full_name)
ORDER BY
    CASE status WHEN 'NO_UC_WRITE_30D' THEN 0 WHEN 'SLA_TAG_INVALID' THEN 1 WHEN 'STALE' THEN 2 ELSE 3 END,
    lag_over_sla DESC NULLS FIRST,
    t.table_name
```

### Confirm a `NO_UC_WRITE_30D` table from its own log (per table)

```sql
SELECT version, timestamp, operation, userName, job.jobId AS job_id, operationMetrics['numOutputRows'] AS rows_written
FROM (DESCRIBE HISTORY {{ catalog }}.{{ schema }}.{{ asset }})
WHERE operation NOT IN ('OPTIMIZE', 'VACUUM START', 'VACUUM END', 'SET TBLPROPERTIES',
                        'ADD COLUMNS', 'CHANGE COLUMN', 'CLUSTER BY', 'COMMENT ON', 'ADD CONSTRAINT')
ORDER BY version DESC
LIMIT 5
```

If this shows recent commits that lineage did not, the writer is outside UC compute; freshness is fine but `agent_attribution` and `lineage_completeness` will flag the same table. If the warehouse rejects `FROM (DESCRIBE HISTORY ...)`, run `DESCRIBE HISTORY ... LIMIT 50` alone and filter the rows client-side.
