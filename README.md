# AI-Ready Data for Databricks

A Databricks implementation of the [AI-Ready Data Framework](https://github.com/Snowflake-Labs/ai-ready-data): six factors, 62 measurable requirements, five workload profiles, and an installable skill that lets a coding agent score a Unity Catalog schema, explain the gaps, and fix them with your approval.

The framework defines what to measure. This repo measures it on Databricks, using Unity Catalog `information_schema`, Delta table metadata, and the `system.*` tables (lineage, audit, query history, Lakeflow, predictive optimization), plus the Databricks SDK where a signal has no SQL surface (Vector Search indexes, online tables).

Maintained by [78 East Labs](https://78eastlabs.com).

## Who this is for

- **Data engineers** on Databricks building pipelines that feed RAG, agents, feature stores or training.
- **Platform teams** running Unity Catalog for many workspaces who want one scorecard per schema.
- **Architects** deciding whether a catalog is ready for Genie, Agent Bricks, Vector Search or Feature Serving.
- **Coding agents** (Claude Code, Cursor, Codex, Databricks Assistant) that need a repeatable protocol for assessing and remediating data.

## The six factors

1. **[Clean](factors/0-clean.md)**: accurate, complete, valid, free of errors that compromise consumption.
2. **[Contextual](factors/1-contextual.md)**: meaning is explicit and colocated with the data.
3. **[Consumable](factors/2-consumable.md)**: right format, right latency for AI workloads.
4. **[Current](factors/3-current.md)**: freshness enforced by infrastructure, not convention.
5. **[Correlated](factors/4-correlated.md)**: traceable from source to every decision it informs.
6. **[Compliant](factors/5-compliant.md)**: explicit ownership, enforced access boundaries, AI-specific safeguards.

The factor documents are the upstream framework's, unchanged. Requirement keys, factors, scopes and profile thresholds are also kept identical to upstream so reports are comparable across platforms and this repo can be contributed back as the `databricks` platform.

## What's Databricks-specific

| Framework concept | Databricks signal |
|---|---|
| Object hierarchy | `catalog.schema.table` (the framework's `database` = a Unity Catalog catalog) |
| Metadata | `{catalog}.information_schema.*` (tables, columns, constraints, tags, column masks, row filters) |
| Change tracking | Delta Change Data Feed (`delta.enableChangeDataFeed`) |
| Time travel / versioning | `delta.logRetentionDuration`, `delta.deletedFileRetentionDuration` |
| Physical layout | Liquid clustering, partitioning, predictive optimization, `OPTIMIZE` |
| Lineage | `system.access.table_lineage`, `system.access.column_lineage` |
| Audit | `system.access.audit` (always on for Unity Catalog) |
| Query attribution and latency | `system.query.history` (`query_source`, `query_tags`, `total_duration_ms`) |
| Pipeline execution | `system.lakeflow.jobs`, `job_run_timeline`, `pipelines`, `pipeline_update_timeline` |
| Embeddings and retrieval | `ARRAY<FLOAT>` columns, Mosaic AI Vector Search (Delta Sync / Direct Access indexes) |
| Features | Unity Catalog feature tables (PRIMARY KEY, `TIMESERIES`), Lakebase synced tables / online tables |
| Masking and row security | Column mask functions, row filter functions, ABAC policies |
| Policy signals with no primitive | Unity Catalog tags with documented default keys (`pii`, `freshness_sla_hours`, `retention_days`, `legal_basis`, `ai_allowed_purposes`, `license`, ...) |

Each requirement's implementation states how strong its signal is: **native** (the platform proves it), **proxy** (correlates with it), **tag** (a documented human decision), or **data** (computed from rows). See [`docs/DATABRICKS-MAPPING.md`](docs/DATABRICKS-MAPPING.md) for the full table.

## Quick start

### Install as a skill

```bash
npx skills add 78eastlabs/ai-ready-data-databricks
```

Or clone the repo into your workspace; the agent reads `skills/ai-ready-data/SKILL.md`.

### Give the agent a way to run SQL

Any one of these works. Credentials stay in the environment, never in the conversation.

- Databricks CLI (`databricks auth login`, then `databricks api post /api/2.0/sql/statements ...`)
- Databricks SDK for Python (`WorkspaceClient().statement_execution`)
- `databricks-sql-connector`
- A Databricks MCP server with a SQL tool

Set `DATABRICKS_WAREHOUSE_ID` to a SQL warehouse (serverless preferred). For system-table checks, a metastore admin must have enabled the `access`, `query`, `lakeflow` and `storage` system schemas and granted `SELECT` on them.

### Run an assessment

```
Assess prod_analytics.customer_360 for agent readiness.
```

The agent confirms the catalog, schema and warehouse, loads the `agents` profile, shows what is runnable, executes the checks, and presents a scored report by factor. From there: `tell-me-more` runs diagnostics, `remediate` walks the fixes stage by stage with approval at each step.

For a portfolio view:

```
Scan the prod_analytics catalog for AI readiness.
```

## How it works

Three phases, light to deep: **Scan**, **Assess**, **Remediate**.

1. Confirm Unity Catalog (the legacy `hive_metastore` cannot be assessed)
2. Discovery: name the catalog, schema and tables, explore first, or scan the whole catalog
3. Profile: `rag`, `agents`, `feature-serving`, `training`, full, or hand-picked requirements
4. Adjust: `skip`, `set`, `add`
5. Coverage: see what is runnable in this workspace (system schemas enabled? Vector Search present?)
6. Assess: each check returns a 0 to 1 score; pass when score >= threshold
7. Remediate: Databricks-specific fixes (tags, constraints, masks, properties, clustering, MERGE patterns) with idempotency guards, executed only on approval

### Built-in profiles

| Profile | Requirements | Best for |
|---|---|---|
| `scan` | 8 | Catalog-wide sweep: fast proxies for prioritization |
| `rag` | 28 | Chunking, embeddings, Vector Search sync, document governance |
| `agents` | 38 | Genie / Text-to-SQL / tool use: documentation, constraints, latency, audit |
| `feature-serving` | 39 | Online features: point lookups, materialization, refresh SLAs |
| `training` | 50 | Fine-tuning and ML training: temporal integrity, versioning, bias, licensing |

Thresholds are the upstream defaults. Override any of them on the fly or in a custom profile:

```yaml
name: hp-dataos-agents
extends: agents
overrides:
  skip:
    - business_glossary_linkage
  set:
    semantic_documentation: { min: 0.80 }
  add:
    row_access_policy: { min: 0.80 }
```

### Execution modes

Most checks are one SQL statement. Two other modes exist because Databricks keeps some signals outside SQL:

- **Probe-and-aggregate**: Delta properties and history (`DESCRIBE DETAIL`, `DESCRIBE HISTORY`, `SHOW TBLPROPERTIES`) are read once per table and folded into a schema score. The platform reference ships a small SDK helper for schemas with hundreds of tables.
- **SDK**: Vector Search indexes and online tables are listed through `databricks.sdk` (with CLI equivalents in each file).

Every check file says which mode it uses, what permission it needs, how much the underlying system table lags, and when it returns N/A instead of a number.

## Key concepts

- **Requirement**: a platform-agnostic criterion (`requirements/requirements.yaml`). 62 total.
- **Check**: `check.md`, context plus SQL (or probe / SDK recipe) returning `value` in 0 to 1.
- **Diagnostic**: `diagnostic.md`, read-only detail, worst-first.
- **Fix**: `fix.md`, remediation options with guards, blast-radius queries and organizational guidance. Runs only on approval.
- **Profile**: a curated set of requirements with thresholds in six stages.
- **Platform reference**: `platforms/DATABRICKS.md`, the dialect, system tables, tag conventions, idempotency guards and permissions everything else assumes.

## Structure

```
factors/                              # The six factors (upstream framework, CC BY 4.0)
docs/
  DATABRICKS-MAPPING.md               # Requirement -> Databricks signal, strength, mode
  AUTHORING.md                        # Contract for writing requirement files
skills/
  ai-ready-data/
    SKILL.md                          # Orchestration protocol (Scan, Assess, Remediate)
    platforms/
      DATABRICKS.md                   # Dialect, system tables, tags, guards, permissions
    requirements/
      requirements.yaml               # Manifest (62 requirements)
      {requirement_key}/
        databricks/
          check.md
          diagnostic.md
          fix.md
    profiles/
      scan.yaml  rag.yaml  agents.yaml  feature-serving.yaml  training.yaml
demos/                                # Seed a demo schema with deliberate gaps
tools/
  validate.py                         # Manifest / profile / file consistency checks
```

## Extending

**Add a requirement.** Add an entry to `requirements.yaml`, create `requirements/{key}/databricks/` with the three files per [`docs/AUTHORING.md`](docs/AUTHORING.md), add it to the relevant profiles, run `python3 tools/validate.py`.

**Add a profile.** Create `profiles/{name}.yaml` with the six stages, or `extends` an existing one.

**Add a platform.** The layout is the upstream framework's. Add `platforms/{PLATFORM}.md` and `requirements/{key}/{platform}/` files, and list the platform under `implementations`.

## Relationship to the upstream framework

This repo is a derivative of [Snowflake-Labs/ai-ready-data](https://github.com/Snowflake-Labs/ai-ready-data). The factor prose, definitions, requirement keys and profile thresholds are theirs and are reused under CC BY 4.0. The orchestration protocol is adapted. All Databricks implementations, the platform reference, the mapping document and the tooling are new. If the upstream project accepts platform contributions, the `requirements/*/databricks/` directories and `platforms/DATABRICKS.md` are laid out to drop in directly.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md). Issues welcome for wrong column names, runtime-version gotchas, and better signals for the tag-based requirements.

## License

Documentation and framework content: [CC BY 4.0](LICENSE-DOC.md), with attribution to the AI-Ready Data Framework contributors and 78 East Labs.

Code, SQL and skill files: [Apache 2.0](LICENSE.md).
