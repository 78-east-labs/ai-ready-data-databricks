# Authoring Requirement Files (Databricks)

Every requirement lives at `skills/ai-ready-data/requirements/{key}/databricks/` with exactly three files: `check.md`, `diagnostic.md`, `fix.md`. This is the contract they follow.

## Before writing

Read, in this order:

1. `skills/ai-ready-data/platforms/DATABRICKS.md`: dialect rules, system tables and their lag, probe-and-aggregate pattern, tag conventions, idempotency guards, permissions.
2. `docs/DATABRICKS-MAPPING.md`: the signal, strength and execution mode chosen for the requirement you are writing, plus placeholder defaults.
3. `skills/ai-ready-data/requirements/requirements.yaml`: the requirement's description, scope and extra placeholders.
4. The exemplar: `skills/ai-ready-data/requirements/classification/databricks/*.md`.

## File format

```
# {Check|Diagnostic|Fix}: {requirement_key}

{One-line description.}

## Context

{Prose. What it measures, which Databricks signal, why that signal, its
 strength (native / proxy / tag / data), lag and permission caveats,
 variant selection guidance, when it returns NULL.}

## SQL

### {Variant name} (primary)

```sql
...
```

### {Variant name} (variant)

```sql
...
```
```

Fix files use `## Fix: {name}` sections instead of `## SQL`, one per remediation option, each with prose then a fenced block (SQL or bash), and end with `## Organizational guidance` when the real fix is a process change.

## Rules for check.md

- The final SELECT returns a column named `value` (DOUBLE, 0.0 to 1.0, 1.0 is perfect) plus the numerator and denominator as named columns so the report can show counts.
- Denominator zero returns NULL via `NULLIF(denominator, 0)`. Never a literal 1.0 or 0.0 fallback.
- Placeholders: `{{ catalog }}`, `{{ schema }}`, `{{ asset }}`, `{{ column }}`, plus any extra placeholder from the manifest. State every extra placeholder's default in the Context section.
- `information_schema` filters: `LOWER(x) = LOWER('{{ y }}')` on both sides. Read from `{{ catalog }}.information_schema.*`, not `system.information_schema.*`, unless the check is metastore-wide by nature.
- Base tables are `table_type IN ('MANAGED','EXTERNAL')` unless the requirement is about views, MVs or streaming tables.
- Casts: `::DOUBLE`. Conditional counts: `COUNT_IF(...)`. Regex: `REGEXP_LIKE(LOWER(col), '...')`.
- System table scans over `system.access.*`, `system.query.history`, `system.lakeflow.*` use a time window (`event_time >= current_timestamp() - INTERVAL {{ lookback_days }} DAYS`) and, if capped, `ORDER BY ... DESC LIMIT n`.
- **Probe mode** checks (Delta properties / history): give (a) the enumeration SQL, (b) the per-table probe statement, (c) the per-table predicate in plain words and as a SQL expression over the probe's output columns, (d) the aggregation rule. Follow the pattern in `platforms/DATABRICKS.md`. Where a pure-SQL approximation exists, offer it as a variant and say what it misses.
- **SDK mode** checks (Vector Search, online tables): give a Python snippet using `databricks.sdk.WorkspaceClient` and the equivalent `databricks` CLI command. Keep the snippet under 30 lines. State the required permission.
- Table-scoped checks run per table and are aggregated by the orchestrator; write them for one `{{ asset }}`. Column-scoped checks are per `{{ asset }}.{{ column }}`. Offer a `TABLESAMPLE ({{ sample_rows }} ROWS)` variant whenever the primary variant scans rows.
- Name the system tables and functions you use exactly. Columns you may rely on:
  - `information_schema.tables`: table_catalog, table_schema, table_name, table_type, table_owner, comment, created, last_altered, data_source_format
  - `information_schema.columns`: table_schema, table_name, column_name, ordinal_position, data_type, full_data_type, is_nullable, comment, partition_index
  - `information_schema.table_tags` / `column_tags`: catalog_name, schema_name, table_name, (column_name), tag_name, tag_value
  - `information_schema.column_masks`: catalog_name, schema_name, table_name, column_name, mask_catalog, mask_schema, mask_name, mask_col_usage
  - `information_schema.row_filters`: catalog_name, schema_name, table_name, filter_catalog, filter_schema, filter_name, filter_col_usage
  - `information_schema.table_constraints`: constraint_catalog, constraint_schema, constraint_name, table_catalog, table_schema, table_name, constraint_type ('PRIMARY KEY','FOREIGN KEY','CHECK','UNIQUE'), enforced
  - `information_schema.key_column_usage`: constraint_name, table_schema, table_name, column_name, ordinal_position, position_in_unique_constraint
  - `information_schema.referential_constraints`: constraint_name, unique_constraint_catalog, unique_constraint_schema, unique_constraint_name
  - `information_schema.constraint_column_usage`: table_schema, table_name, column_name, constraint_name
  - `information_schema.check_constraints`: constraint_name, sql (the expression)
  - `information_schema.views`: table_schema, table_name, view_definition
  - `information_schema.routines`: routine_schema, routine_name, routine_definition
  - `system.access.table_lineage`: entity_type, entity_id, entity_run_id, source_table_full_name, source_catalog, source_schema, source_table_name, source_path, source_type, target_table_full_name, target_catalog, target_schema, target_table_name, target_type, user_identity (struct, `.email`), event_time, event_date, query_statement_id, is_direct_lineage
  - `system.access.column_lineage`: same plus source_column_name, target_column_name
  - `system.access.audit`: event_time, event_date, service_name, action_name, request_params (map<string,string>), response (struct: status_code, error_code, result), user_identity (struct: email), user_agent
  - `system.query.history`: statement_id, executed_by, statement_text, statement_type, execution_status ('FINISHED','FAILED','CANCELED'), start_time, end_time, total_duration_ms, execution_duration_ms, read_rows, produced_rows, written_rows, written_bytes, compute (struct: type, warehouse_id), query_source (struct: job_info.job_id, job_info.job_run_id, notebook_id, dashboard_id, sql_query_id, alert_id, genie_space_id), query_tags (map<string,string>), client_application
  - `system.lakeflow.jobs`: job_id, name, description, tags, change_time, delete_time, run_as_user_id, trigger_type
  - `system.lakeflow.job_run_timeline`: job_id, run_id, period_start_time, period_end_time, trigger_type, run_type, run_name, result_state ('SUCCESS','FAILED','SKIPPED','CANCELED','TIMEDOUT','UPSTREAM_FAILED'), termination_code
  - `system.lakeflow.pipelines`: pipeline_id, name, created_by, run_as, tags, settings, change_time, delete_time
  - `system.lakeflow.pipeline_update_timeline`: pipeline_id, update_id, period_start_time, period_end_time, update_type, result_state
  - `system.storage.predictive_optimization_operations_history`: catalog_name, schema_name, table_name, operation_type ('COMPACTION','VACUUM','ANALYZE'), operation_status, start_time, end_time, operation_metrics
  - `DESCRIBE DETAIL t`: format, name, description, location, createdAt, lastModified, partitionColumns, clusteringColumns, numFiles, sizeInBytes, properties (map), minReaderVersion, minWriterVersion, tableFeatures
  - `DESCRIBE HISTORY t`: version, timestamp, userId, userName, operation, operationParameters (map), job (struct), notebook (struct), clusterId, readVersion, isBlindAppend, operationMetrics (map), userMetadata
  - Lakehouse Monitoring output tables: `{table}_profile_metrics` (window, slice_key, slice_value, column_name, count, num_nulls, avg, stddev, min, max, distinct_count, ...) and `{table}_drift_metrics` (window, window_cmp, drift_type, column_name, js_distance, ks_test (struct), chi_squared_test (struct), wasserstein_distance)
- If you are not sure a column exists, say so in Context and give the user a one-line probe to confirm. Do not invent columns silently.

## Rules for diagnostic.md

- Read-only. Returns one row per table (or per column / per event) with the fields a human needs to decide what to fix. Sort worst-first.
- Reuse the check's scoping CTE so the diagnostic and the check agree on the population.

## Rules for fix.md

- Every executable option is idempotent or preceded by the guard from `platforms/DATABRICKS.md`.
- Never `CREATE OR REPLACE TABLE`. Never `DROP`. Never `VACUUM` with a retention under 7 days. Prefer `ALTER TABLE ... SET TBLPROPERTIES`, `SET TAGS`, `ADD CONSTRAINT`, `SET MASK`, `SET ROW FILTER`, `CLUSTER BY`, `COMMENT ON`, `OPTIMIZE`.
- Data-mutating fixes (UPDATE / DELETE / MERGE) come with a blast-radius query first (`SELECT COUNT(*) ... WHERE <same predicate>`).
- Where the honest fix is a tag, say plainly that the tag documents a decision a human must make (legal basis, license, retention) and that applying it without that decision is worse than leaving it empty.
- Include a bulk-generation variant (a SELECT that emits the ALTER statements) whenever the fix applies per table or per column.
- End with `## Organizational guidance` when the durable fix is process: pipeline templates, governed tags, Lakeflow settings, dbt meta, Terraform.

## Tone

Plain, specific, no marketing. Say what a signal proves and what it does not. Short paragraphs. No em dashes; use commas, periods or parentheses.
