# Check: data_freshness

Fraction of base tables whose most recent write falls inside their freshness SLA.

## Context

Two inputs per table: the SLA and the last write time.

The SLA comes from the Unity Catalog table tag `freshness_sla_hours` (a number of hours as text, for example `24`). Tables without the tag fall back to `{{ default_sla_hours }}` (default 24). Declaring the SLA is a human decision; the tag is how the framework reads it. The variant below scores only tables that carry the tag, which is the stricter reading of "declared freshness SLA".

The last write comes from `system.access.table_lineage`. Every write executed on Unity Catalog compute produces a lineage row whose `target_table_full_name` is the written table (lowercase `catalog.schema.table`) and whose `event_time` is when the statement ran. Taking `MAX(event_time)` per target gives the last write. This is preferred over `information_schema.tables.last_altered`, which moves on DDL and some metadata operations and is not a reliable DML signal, and over `DESCRIBE DETAIL.lastModified`, which also moves on `OPTIMIZE` and `VACUUM`.

Caveats. Lineage lags by up to a few hours, so a table written five minutes ago can look stale; do not fail a table whose computed lag is inside the lineage lag budget without confirming with the probe variant. Lineage only records writes from UC-enabled compute (warehouses, UC clusters, serverless, Lakeflow pipelines); writes from legacy clusters or external engines writing straight to storage are invisible and make the table look unwritten. Retention is one year. The scan is bounded to the last 30 days for cost: a table with no write in 30 days fails any SLA up to 720 hours, and SLAs longer than that should widen the window. `event_time` is documented as a lineage column; if the query errors on it, confirm with `DESCRIBE TABLE system.access.table_lineage`.

Strength is **native + tag**: the write signal is real platform telemetry; the SLA is a convention.

Returns NULL (N/A) when the schema has no base tables (primary) or no tables with the SLA tag (variant).

## SQL

### Lineage last write vs SLA (primary)

```sql
WITH tables_in_scope AS (
    SELECT LOWER(table_name) AS table_name,
           CONCAT(LOWER('{{ catalog }}'), '.', LOWER('{{ schema }}'), '.', LOWER(table_name)) AS full_name
    FROM {{ catalog }}.information_schema.tables
    WHERE LOWER(table_schema) = LOWER('{{ schema }}')
      AND table_type IN ('MANAGED', 'EXTERNAL')
),
sla AS (
    SELECT LOWER(table_name) AS table_name,
           TRY_CAST(tag_value AS DOUBLE) AS sla_hours
    FROM {{ catalog }}.information_schema.table_tags
    WHERE LOWER(schema_name) = LOWER('{{ schema }}')
      AND tag_name = 'freshness_sla_hours'
),
last_write AS (
    SELECT LOWER(target_table_full_name) AS full_name,
           MAX(event_time)               AS last_write_at
    FROM system.access.table_lineage
    WHERE LOWER(target_catalog) = LOWER('{{ catalog }}')
      AND LOWER(target_schema)  = LOWER('{{ schema }}')
      AND target_table_full_name IS NOT NULL
      AND event_time >= current_timestamp() - INTERVAL 30 DAYS
    GROUP BY LOWER(target_table_full_name)
),
scored AS (
    SELECT
        t.table_name,
        COALESCE(s.sla_hours, {{ default_sla_hours }})          AS sla_hours,
        lw.last_write_at,
        timestampdiff(HOUR, lw.last_write_at, current_timestamp()) AS lag_hours
    FROM tables_in_scope t
    LEFT JOIN sla        s  USING (table_name)
    LEFT JOIN last_write lw USING (full_name)
)
SELECT
    COUNT_IF(lag_hours IS NOT NULL AND lag_hours <= sla_hours)          AS fresh_tables,
    COUNT(*)                                                             AS total_tables,
    COUNT_IF(lag_hours IS NOT NULL AND lag_hours <= sla_hours)::DOUBLE
        / NULLIF(COUNT(*), 0)                                            AS value
FROM scored
```

A table with no lineage write in the window has `lag_hours IS NULL` and fails; the diagnostic shows those separately so the operator can tell "never written on UC compute" from "stale".

### Declared SLA only (variant)

Same logic, denominator restricted to tables that carry a parseable `freshness_sla_hours` tag.

```sql
WITH tables_in_scope AS (
    SELECT LOWER(table_name) AS table_name,
           CONCAT(LOWER('{{ catalog }}'), '.', LOWER('{{ schema }}'), '.', LOWER(table_name)) AS full_name
    FROM {{ catalog }}.information_schema.tables
    WHERE LOWER(table_schema) = LOWER('{{ schema }}')
      AND table_type IN ('MANAGED', 'EXTERNAL')
),
sla AS (
    SELECT LOWER(table_name) AS table_name,
           TRY_CAST(tag_value AS DOUBLE) AS sla_hours
    FROM {{ catalog }}.information_schema.table_tags
    WHERE LOWER(schema_name) = LOWER('{{ schema }}')
      AND tag_name = 'freshness_sla_hours'
),
last_write AS (
    SELECT LOWER(target_table_full_name) AS full_name,
           MAX(event_time)               AS last_write_at
    FROM system.access.table_lineage
    WHERE LOWER(target_catalog) = LOWER('{{ catalog }}')
      AND LOWER(target_schema)  = LOWER('{{ schema }}')
      AND target_table_full_name IS NOT NULL
      AND event_time >= current_timestamp() - INTERVAL 30 DAYS
    GROUP BY LOWER(target_table_full_name)
)
SELECT
    COUNT_IF(timestampdiff(HOUR, lw.last_write_at, current_timestamp()) <= s.sla_hours) AS fresh_tables,
    COUNT(*)                                                                             AS declared_tables,
    COUNT_IF(timestampdiff(HOUR, lw.last_write_at, current_timestamp()) <= s.sla_hours)::DOUBLE
        / NULLIF(COUNT(*), 0)                                                            AS value
FROM tables_in_scope t
JOIN sla s USING (table_name)
LEFT JOIN last_write lw USING (full_name)
WHERE s.sla_hours IS NOT NULL
```

### Delta history last write (variant, probe mode)

Use when lineage is disabled, lagging, or when writers bypass UC compute. It reads the table's own transaction log, so it sees every commit regardless of who made it, with no lag. Cost is one metadata call per table.

**(a) Enumerate:** the `tables_in_scope` CTE above, plus the `sla` CTE to carry each table's SLA (or `{{ default_sla_hours }}`) alongside its name.

**(b) Probe:**

```sql
DESCRIBE HISTORY {{ catalog }}.{{ schema }}.{{ asset }} LIMIT 50
```

**(c) Predicate.** In words: take the newest commit whose `operation` changes data (not `OPTIMIZE`, `VACUUM START`, `VACUUM END`, `SET TBLPROPERTIES`, `ADD COLUMNS`, `CHANGE COLUMN`, `CLUSTER BY`, `COMMENT ON`, `ADD CONSTRAINT`), and pass when it is no older than the SLA. Over the probe's rows:

```sql
timestampdiff(HOUR,
    MAX(CASE WHEN operation IN ('WRITE', 'MERGE', 'UPDATE', 'DELETE', 'STREAMING UPDATE',
                                'COPY INTO', 'CREATE TABLE AS SELECT',
                                'CREATE OR REPLACE TABLE AS SELECT', 'REPLACE TABLE AS SELECT',
                                'RESTORE', 'TRUNCATE')
             THEN timestamp END),
    current_timestamp()) <= <sla_hours for this table>
```

If none of the 50 most recent commits changed data, the table fails (50 maintenance-only commits means no data has arrived for a long time).

**(d) Aggregation:** `value = passing / probed`, NULL when nothing was probed.
