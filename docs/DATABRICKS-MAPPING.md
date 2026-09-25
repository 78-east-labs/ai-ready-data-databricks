# Requirement → Databricks Signal Mapping

How each of the 62 framework requirements is measured on Databricks, and how strong that measurement is.

**Strength legend**

- **native**: a first-class Unity Catalog / Delta / system-table signal measures the requirement directly
- **proxy**: a real platform signal that correlates with the requirement but does not prove it
- **tag**: no platform primitive exists; measured through a Unity Catalog tag convention (see `platforms/DATABRICKS.md`)
- **data**: computed by scanning the table's rows (table- or column-scoped)

**Execution mode**

- **sql**: one SQL statement against `information_schema` or `system.*`
- **probe**: probe-and-aggregate (one `DESCRIBE DETAIL` / `DESCRIBE HISTORY` / `SHOW TBLPROPERTIES` per table)
- **sdk**: Databricks SDK or CLI call (no SQL surface)

## Clean

| Requirement | Signal | Strength | Mode |
|---|---|---|---|
| categorical_validity | values ∈ `{{ allowed_values }}`; fallback: values ∈ distinct set of a reference/dimension table | data | sql |
| cross_column_consistency | rows satisfying `{{ consistency_predicate }}`; discover candidates from `check_constraints` | data | sql |
| data_completeness | `1 - nulls/rows`, sampled variant with `TABLESAMPLE` | data | sql |
| distribution_conformity | Lakehouse Monitoring `*_drift_metrics` (`js_distance`, `ks_test`, `chi_squared_test`) within `{{ drift_tolerance }}`; fallback: compare mean/stddev vs a baseline snapshot table | native (with monitor) / data | sql |
| encoding_validity | `is_valid_utf8()` and no `�`; fallback regex when the function is unavailable | data | sql |
| outlier_prevalence | rows with |z| ≤ `{{ z_threshold }}` across numeric columns; Lakehouse Monitoring profile metrics variant | data | sql |
| referential_accuracy | join to `{{ reference_table }}` on `{{ reference_key }}`, compare `{{ reference_column }}` | data | sql |
| referential_integrity | anti-join along declared FKs from `referential_constraints` + `key_column_usage`; manual FK variant | native + data | sql |
| schema_conformity | `_rescued_data IS NULL` (Auto Loader / `from_json` rescue column) and `TRY_CAST(col AS {{ target_type }}) IS NOT NULL` | data | sql |
| syntactic_validity | `try_parse_json()` / `from_json` non-null for string payloads; `_rescued_data IS NULL` for ingested rows | data | sql |
| uniqueness | distinct `{{ key_columns }}` / rows; default keys from PRIMARY KEY constraint | native + data | sql |
| value_range_validity | values in `[{{ min_value }}, {{ max_value }}]`; defaults parsed from a CHECK constraint on the column when present | data | sql |

## Contextual

| Requirement | Signal | Strength | Mode |
|---|---|---|---|
| business_glossary_linkage | column tag `glossary_term`, or column referenced by a metric view (`tables.table_type = 'METRIC_VIEW'`, views definition) | tag / proxy | sql |
| constraint_declaration | `columns.is_nullable = 'NO'` OR in `key_column_usage` (PK/FK) OR referenced in `check_constraints` OR comment matches a range pattern | native | sql |
| entity_identifier_declaration | `table_constraints.constraint_type = 'PRIMARY KEY'` | native (informational constraint) | sql |
| relationship_declaration | table appears in `referential_constraints` as child or parent | native (informational constraint) | sql |
| schema_type_coverage | column has non-empty comment OR any column tag OR name matches a role pattern (`_id$`, `_at$`, `_date$`, `amount`, `count`, `is_`, `flag`, …) | native / proxy | sql |
| semantic_documentation | non-empty `tables.comment` and `columns.comment` (both weighted) | native | sql |
| temporal_scope_declaration | table tag `temporal_scope` OR columns matching `valid_from|valid_to|effective_|as_of|snapshot_date` | tag / proxy | sql |
| unit_of_measure_declaration | numeric column has tag `unit` OR comment mentions a unit token (`usd|eur|ms|seconds|hours|kg|km|percent|%`) OR name suffix (`_usd`, `_ms`, `_pct`, `_cents`) | tag / proxy | sql |

## Consumable

| Requirement | Signal | Strength | Mode |
|---|---|---|---|
| access_optimization | `sizeInBytes ≥ {{ large_table_bytes }}` AND (`clusteringColumns` non-empty OR `partitionColumns` non-empty OR predictive optimization ops in last 30d) | native | probe |
| batch_throughput_sufficiency | `system.query.history` write statements (INSERT/MERGE/COPY/CTAS) touching schema: `written_rows / (total_duration_ms/1000) ≥ {{ min_rows_per_second }}` | native | sql |
| chunk_readiness | text tables: p95 `length(text_col) ≤ {{ max_chunk_chars }}`; presence of `chunk_id`/`chunk_index` column as a stronger signal | data / proxy | sql |
| embedding_coverage | text-bearing tables with an `ARRAY<FLOAT>` / `ARRAY<DOUBLE>` column (`columns.full_data_type`) | native | sql |
| embedding_dimension_consistency | per embedding column `COUNT(DISTINCT size(col)) = 1` | data | sql |
| eval_coverage | table tag `eval_set_for`, sibling table named `{table}_eval*`/`eval_*`, or MLflow evaluation dataset in UC | tag / proxy | sql |
| feature_materialization_coverage | feature tables (PK constraint or `feature_table` tag) that have an online table / synced table (`tables.table_type` for online tables; SDK `online_tables.list` / synced tables) | native | sql + sdk |
| native_format_availability | `tables.data_source_format IN ('DELTA','ICEBERG')` (views and MVs excluded, FOREIGN scored by federation format) | native | sql |
| point_lookup_availability | `clusteringColumns` OR `partitionColumns` OR `delta.bloomFilter.*` property OR liquid + deletion vectors | native | probe |
| retrieval_recall_compliance | Vector Search indexes on schema tables with `status.ready = true` and `index_type = DELTA_SYNC` with `pipeline_type` set; ratio over embedding-bearing tables | proxy | sdk |
| search_optimization | predictive optimization operation in `system.storage.predictive_optimization_operations_history` in `{{ lookback_days }}` OR (`clusteringColumns` non-empty AND an `OPTIMIZE` commit in history within window) | native | sql + probe |
| serving_latency_compliance | `system.query.history` SELECTs whose `statement_id` joins `table_lineage.query_statement_id` for a schema table (fallback: `statement_text` match) with `total_duration_ms ≤ {{ latency_threshold_ms }}` | native | sql |
| vector_index_coverage | embedding-bearing tables that are the `source_table` of at least one Vector Search index (`vector_search_indexes.list_indexes` per endpoint) | native | sdk |

## Current

| Requirement | Signal | Strength | Mode |
|---|---|---|---|
| change_detection | `delta.enableChangeDataFeed = true` in `DESCRIBE DETAIL` properties | native | probe |
| data_freshness | table tag `freshness_sla_hours` (default `{{ default_sla_hours }}`); last write = max `table_lineage.event_time` where `target_table_full_name` = table (fallback `DESCRIBE HISTORY` latest write); pass when lag ≤ SLA | native + tag | sql |
| feature_refresh_compliance | `STREAMING_TABLE` / `MATERIALIZED_VIEW` rows: last successful `pipeline_update_timeline` update (or `DESCRIBE HISTORY`) within `{{ staleness_hours }}` | native | sql / probe |
| incremental_update_coverage | last `{{ history_commits }}` commits: `operation IN ('STREAMING UPDATE','MERGE','UPDATE','DELETE')` or `WRITE` with `mode = 'Append'`, not `Overwrite` / `CREATE OR REPLACE TABLE AS SELECT` / `REPLACE TABLE` | native | probe |
| point_in_time_correctness | feature tables whose `SHOW CREATE TABLE` PK clause includes `TIMESERIES` | native | probe |
| propagation_latency_compliance | for tables with upstream lineage: `max(target write time) ≥ max(upstream write time) - {{ sla_hours }}` using `table_lineage` | native | sql |
| schema_evolution_tracking | `delta.logRetentionDuration ≥ {{ min_history_days }}` (default 30d applies when unset) AND at least one commit visible in history | native | probe |
| temporal_referential_integrity | `{{ timestamp_column }}` non-null, ≤ now, ≥ `{{ min_valid_timestamp }}` | data | sql |
| training_serving_parity | feature tables with an online/synced table whose source is the same UC table (SDK: online table `spec.source_table_full_name`) | proxy | sdk |

## Correlated

| Requirement | Signal | Strength | Mode |
|---|---|---|---|
| agent_attribution | `table_lineage` write events (target in schema) with non-null `entity_type` (JOB / PIPELINE / NOTEBOOK / DASHBOARD / QUERY) OR matching `query.history.query_tags` non-empty; anonymous = null entity | native | sql |
| data_provenance | tags `source_system` AND `collection_method` present, OR at least one upstream lineage edge whose source is a path/external location | tag / native | sql |
| data_version_coverage | `delta.deletedFileRetentionDuration ≥ {{ min_retention_days }}` AND `delta.logRetentionDuration ≥ {{ min_retention_days }}` (defaults 7d / 30d when unset) | native | probe |
| dependency_graph_completeness | table has ≥1 upstream edge AND ≥1 downstream edge in `table_lineage` within window | native | sql |
| impact_analysis_capability | table has ≥1 downstream edge (`source_table_full_name` = table) in `table_lineage` | native | sql |
| lineage_completeness | table has upstream edges in both `table_lineage` and `column_lineage` | native | sql |
| pipeline_execution_audit | distinct writer entities (JOB / PIPELINE ids from `table_lineage`) that have run records in `job_run_timeline` / `pipeline_update_timeline` within window | native | sql |
| record_level_traceability | `delta.enableRowTracking = true` OR a column matching `source_record_id|_source_id|correlation_id|_ingest_id|_metadata_file_path` | native / proxy | probe + sql |
| transformation_documentation | derived assets (VIEW / MATERIALIZED_VIEW / STREAMING_TABLE, or tables written by a job/pipeline) whose comment is non-empty OR whose writing job/pipeline has a description | native / proxy | sql |

## Compliant

| Requirement | Signal | Strength | Mode |
|---|---|---|---|
| access_audit_coverage | tables read in window (`table_lineage`) that also appear in `system.access.audit` (`service_name = 'unityCatalog'`, `action_name IN ('getTable','generateTemporaryTableCredential')`, `request_params.full_name_arg`) | native | sql |
| anonymization_effectiveness | PII-candidate columns by name regex (`email|phone|ssn|passport|dob|birth|address|first_name|last_name|full_name|ip_address|credit_card`) that have a `column_masks` row | native / proxy | sql |
| bias_testing_coverage | training tables (tag `training_set` or name pattern) with tag `bias_tested_at` OR a Lakehouse Monitoring profile table with `slice_key IS NOT NULL` | tag / proxy | sql |
| classification | tables with ≥1 `table_tags` row; column variant with `column_tags` | native | sql |
| column_masking | columns tagged `{{ pii_tag_key }}` (default `pii`, or `sensitivity`) that have a `column_masks` row | native | sql |
| consent_coverage | tables with any PII-tagged column that carry tag `legal_basis` | tag | sql |
| demographic_representation | training tables with tag `demographic_profile` | tag | sql |
| license_compliance | external datasets (`table_type = 'FOREIGN'`, tables in a Delta Sharing catalog, or tag `source_type = 'external'`) with tag `license` | tag / native | sql |
| purpose_limitation | tables with tag `ai_allowed_purposes` | tag | sql |
| retention_policy | tables with tag `retention_days` AND `delta.deletedFileRetentionDuration` not longer than that value | tag + native | sql + probe |
| row_access_policy | tables with a `row_filters` row | native | sql |

## Placeholder defaults

| Placeholder | Default |
|---|---|
| `lookback_days` | 7 (30 for lineage-based checks) |
| `sample_rows` | 1,000,000 |
| `large_table_bytes` | 10 GB (10737418240) |
| `latency_threshold_ms` | 1000 |
| `min_rows_per_second` | 10000 |
| `max_chunk_chars` | 4000 |
| `default_sla_hours` | 24 |
| `staleness_hours` | 24 |
| `sla_hours` | 24 |
| `history_commits` | 20 |
| `min_retention_days` | 30 |
| `min_history_days` | 30 |
| `z_threshold` | 4 |
| `drift_tolerance` | 0.1 (JS distance) |
| `pii_tag_key` | `pii` |
