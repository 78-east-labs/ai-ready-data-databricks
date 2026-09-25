# Databricks

Platform rules for SQL generation and execution on Databricks (Unity Catalog + Delta Lake). Requirement-specific context lives in each requirement's markdown files. This file covers only cross-cutting rules.

## Object Hierarchy

Databricks uses a three-level namespace: `catalog.schema.table`. The framework's generic `database` level maps to a Unity Catalog **catalog**. Placeholders used across all requirement files:

| Placeholder | Meaning | Example |
|---|---|---|
| `{{ catalog }}` | Unity Catalog catalog | `prod_analytics` |
| `{{ schema }}` | Schema inside the catalog | `customer_360` |
| `{{ asset }}` | Table, view, streaming table or materialized view | `orders` |
| `{{ column }}` | Column name | `order_total` |

Only Unity Catalog objects are assessable. `hive_metastore.*` tables have no `information_schema`, no tags, no masks, no lineage and no audit coverage. If the user points at `hive_metastore`, say so and recommend migrating (`SYNC SCHEMA` / `UCX`) before assessing.

## SQL Rules

- Dialect is Databricks SQL (Spark SQL). `COUNT_IF()`, `NULLIF()`, `TRY_CAST()`, `REGEXP_LIKE()` / `RLIKE`, `TABLESAMPLE (n ROWS)` and `::` casts all work. Cast to double with `::DOUBLE` (`::FLOAT` is single precision).
- **Metadata catalog.** Use `{{ catalog }}.information_schema.*` for objects inside one catalog and `system.information_schema.*` when you need the whole metastore. Both carry the same views. Names in `information_schema` are stored lowercase for catalogs, schemas and tables; column names preserve case. Filter with `LOWER(x) = LOWER('{{ y }}')` on both sides so callers can pass any casing.
- **Base tables** are `table_type IN ('MANAGED', 'EXTERNAL')`. Streaming tables and materialized views are `'STREAMING_TABLE'` and `'MATERIALIZED_VIEW'`. Views are `'VIEW'`. Delta Sharing / Lakehouse Federation objects are `'FOREIGN'`. Unless a check says otherwise, "table" means base table.
- **No `RESULT_SCAN`.** `SHOW` and `DESCRIBE` output cannot be joined in SQL. When a check needs per-table properties (`DESCRIBE DETAIL`, `DESCRIBE HISTORY`, `SHOW TBLPROPERTIES`), it uses the **probe-and-aggregate** pattern below.
- **System tables are metastore-wide** and lag. Budget for delay and warn the user when recently created objects or recent runs are missing:
  - `system.access.audit`: minutes to a few hours
  - `system.access.table_lineage` / `column_lineage`: up to a few hours; 1-year retention
  - `system.query.history`: minutes; covers SQL warehouses, serverless notebooks/jobs and Lakeflow pipelines only. Classic all-purpose cluster queries do not appear.
  - `system.lakeflow.*`: minutes to hours
  - `system.storage.predictive_optimization_operations_history`: hours
- **Determinism.** Any `LIMIT` on a system table scan is paired with a stable `ORDER BY` (`event_time DESC`, `start_time DESC`).
- **N/A.** When the denominator is zero the query returns `NULL` via `NULLIF(denominator, 0)`. Never a hard-coded `1.0` or `0.0`.
- **Name pattern matching.** Use `REGEXP_LIKE(LOWER(col), '...')`. Avoid `LIKE '%_x'` (`_` is a wildcard).
- **Timestamps.** `current_timestamp()`, `date_sub()`, `timestampadd(HOUR, -n, ts)` and `timestampdiff(HOUR, a, b)` are the portable choices.

## Probe-and-Aggregate Pattern

Several signals (Change Data Feed, liquid clustering, row tracking, retention, last write operation) live in Delta table properties or history, not in `information_schema`. For those checks:

1. Enumerate tables in scope from `{{ catalog }}.information_schema.tables`.
2. For each table run the probe statement given in the check file (`DESCRIBE DETAIL`, `DESCRIBE HISTORY ... LIMIT n`, or `SHOW TBLPROPERTIES`).
3. Evaluate the per-table predicate given in the check file.
4. `value = tables_passing / tables_probed`, `NULL` if nothing was probed.

Cost is one metadata call per table; no data is scanned. For schemas with hundreds of tables, run the probes through the Databricks SDK or CLI rather than one SQL round trip each. Reference helper (Python, Databricks SDK):

```python
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementState

w = WorkspaceClient()

def sql(stmt, warehouse_id):
    r = w.statement_execution.execute_statement(
        statement=stmt, warehouse_id=warehouse_id, wait_timeout="50s")
    assert r.status.state == StatementState.SUCCEEDED, r.status
    cols = [c.name for c in r.manifest.schema.columns]
    return [dict(zip(cols, row)) for row in (r.result.data_array or [])]

def describe_detail(catalog, schema, warehouse_id):
    tables = sql(f"""
        SELECT table_name FROM {catalog}.information_schema.tables
        WHERE LOWER(table_schema) = LOWER('{schema}')
          AND table_type IN ('MANAGED','EXTERNAL')""", warehouse_id)
    return {t["table_name"]: sql(
        f"DESCRIBE DETAIL {catalog}.{schema}.`{t['table_name']}`", warehouse_id)[0]
        for t in tables}
```

`DESCRIBE DETAIL` returns `format`, `partitionColumns`, `clusteringColumns`, `numFiles`, `sizeInBytes`, `properties` (map), `tableFeatures`, `lastModified`. `DESCRIBE HISTORY` returns one row per commit with `timestamp`, `operation`, `operationParameters`, `operationMetrics`, `job`, `notebook`, `userMetadata`.

## Tag Conventions

Several requirements have no native Databricks primitive (consent, purpose, retention, license, glossary, freshness SLA). The framework measures them through Unity Catalog **tags** on tables and columns, read from `information_schema.table_tags` / `column_tags`. The default tag keys are below; users can override the key name through a profile placeholder.

| Signal | Default tag key | Applied to | Example value |
|---|---|---|---|
| Sensitivity / PII class | `pii` or `sensitivity` | column | `email`, `high` |
| Freshness SLA | `freshness_sla_hours` | table | `24` |
| Retention | `retention_days` | table | `730` |
| Legal basis / consent | `legal_basis` | table | `consent`, `contract` |
| Permitted AI purposes | `ai_allowed_purposes` | table | `rag,analytics` |
| Source system | `source_system` | table | `salesforce` |
| Collection method | `collection_method` | table | `api_extract` |
| License | `license` | table | `CC-BY-4.0` |
| Glossary term | `glossary_term` | column | `net_revenue` |
| Unit of measure | `unit` | column | `usd`, `seconds` |
| Temporal validity | `temporal_scope` | table | `daily_snapshot` |
| Evaluation set | `eval_set_for` | table | `orders_agent` |
| Bias testing | `bias_tested_at` | table | `2026-08-30` |
| Demographic profile | `demographic_profile` | table | `matches_us_census_2024` |
| Feature table | `feature_table` | table | `true` |

Tags are set with `ALTER TABLE t SET TAGS ('k' = 'v')` and `ALTER TABLE t ALTER COLUMN c SET TAGS ('k' = 'v')`. Governed tag policies (account console) can restrict allowed keys and values; check them before inventing new keys.

## Idempotency Guards

Before non-idempotent fix operations, run the guard. Skip if the desired state already exists.

| Operation | Guard | Skip If |
|---|---|---|
| `ALTER TABLE ... SET TAGS` | `SELECT 1 FROM {{ catalog }}.information_schema.table_tags WHERE ... AND tag_name = '{k}'` | Has rows with the same value (re-setting is otherwise harmless) |
| `ALTER TABLE ... ADD CONSTRAINT` | `SELECT 1 FROM {{ catalog }}.information_schema.table_constraints WHERE table_name = '{asset}' AND constraint_name = '{name}'` | Has rows |
| `ALTER TABLE ... ALTER COLUMN ... SET MASK` | `SELECT 1 FROM {{ catalog }}.information_schema.column_masks WHERE ... AND column_name = '{column}'` | Has rows (a column can carry one mask) |
| `ALTER TABLE ... SET ROW FILTER` | `SELECT 1 FROM {{ catalog }}.information_schema.row_filters WHERE table_name = '{asset}'` | Has rows (one filter per table) |
| `CREATE FUNCTION` (mask/filter UDF) | `SELECT 1 FROM {{ catalog }}.information_schema.routines WHERE routine_name = '{name}'` | Has rows. Use `CREATE OR REPLACE FUNCTION` only if the user confirms the body is unchanged |
| `ALTER TABLE ... SET TBLPROPERTIES` | `SHOW TBLPROPERTIES {{ asset }}` | Property already at the desired value |
| `ALTER TABLE ... CLUSTER BY` | `DESCRIBE DETAIL` → `clusteringColumns` | Already set to the same keys (warn and prompt if different) |
| `ALTER TABLE ... ALTER COLUMN ... SET NOT NULL` | `information_schema.columns.is_nullable` | Already `NO` |
| `COMMENT ON` | `information_schema.tables.comment` / `columns.comment` | Non-empty (prompt before overwriting) |
| `OPTIMIZE` / `VACUUM` | None needed | Re-running is safe |
| `ALTER TABLE ... ENABLE PREDICTIVE OPTIMIZATION` | `DESCRIBE EXTENDED` shows `Predictive Optimization` | Already `ENABLE` |

Never use `CREATE OR REPLACE TABLE` in a fix. It rewrites history and breaks time travel, streaming readers and Delta Sync indexes.

## Delegations

| Requirement | Delegate To | When |
|---|---|---|
| `semantic_documentation` | Unity Catalog AI-generated comments (Catalog Explorer, or `ai_gen()` in SQL) | Tables and columns lack comments |
| `classification` / `column_masking` | Databricks Data Classification (auto-tagging, Preview) if enabled on the catalog | Before hand-tagging PII columns |
| `vector_index_coverage` / `retrieval_recall_compliance` | Databricks Vector Search endpoint via SDK or CLI | Index metadata is not in SQL system tables |
| `feature_materialization_coverage` / `training_serving_parity` | Feature Engineering in Unity Catalog (`databricks-feature-engineering`) | Feature tables need online serving |
| `distribution_conformity` / `outlier_prevalence` / `bias_testing_coverage` | Lakehouse Monitoring (`quality.*` profile and drift metrics tables) | A monitor exists on the table |

## Permissions

When a check or fix fails with an access error, verify these grants.

| Access | Grant |
|---|---|
| `{{ catalog }}.information_schema.*` | `USE CATALOG` + `USE SCHEMA` on the objects (rows are filtered to what the caller can see) |
| `system.access.*`, `system.query.*`, `system.lakeflow.*`, `system.storage.*` | `GRANT USE SCHEMA ON SCHEMA system.<schema>`; `GRANT SELECT ON TABLE system.<schema>.<table>`; the schema must be enabled by a metastore admin (`system schemas enable`) |
| `DESCRIBE DETAIL` / `DESCRIBE HISTORY` | `SELECT` on the table |
| Setting tags | `APPLY TAG` on the object (or ownership) |
| Masks and row filters | Ownership of the table, `EXECUTE` on the function |
| Vector Search index listing | `USE CATALOG`/`USE SCHEMA` on the index's location plus access to the endpoint |

## Compute Notes

- Run checks on a **SQL warehouse** (serverless preferred). Warehouses expose `system.query.history` entries for the assessment itself and honor masks and row filters, which matters when verifying fixes.
- Some functions need a minimum runtime: `is_valid_utf8()` (DBR 16.1+ / current warehouses), `try_parse_json()` / VARIANT (DBR 15.3+), `ai_gen()` and other `ai_*` functions (serverless or Pro warehouses in supported regions). Check files call this out where it applies and give a fallback.
- Delta Sync vector indexes require `delta.enableChangeDataFeed = true` on the source table. Enabling it does not rewrite data.
