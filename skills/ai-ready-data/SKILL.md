---
name: ai-ready-data
description: Assess and optimize Databricks (Unity Catalog + Delta Lake) data for AI workloads. Scan catalogs for prioritization, assess schemas against RAG / agents / feature-serving / training profiles, and guide remediation.
---

# AI-Ready Data

Assess Databricks data products for AI-readiness and remediate gaps. Scope is always a Unity Catalog `catalog.schema` (plus optional tables). Each requirement is a self-contained directory with three markdown files per platform: `check.md` (context + SQL returning a 0 to 1 score), `diagnostic.md` (context + detail SQL), and `fix.md` (context + remediation SQL and/or organizational guidance). Each file co-locates all relevant context, constraints, gotchas, variant selection guidance, and platform-specific notes, directly above the SQL it applies to. The manifest (`requirements/requirements.yaml`) provides lightweight metadata for profile-load time. Every assessment has exactly six stages named after the six factors of AI-ready data, use these exact names everywhere (reports, plans, tasks): **Clean**, **Contextual**, **Consumable**, **Current**, **Correlated**, **Compliant**.

## What This Skill Does

Three phases, light to deep:

1. **Scan**: Lightweight estate-level sweep across many schemas. Produces a comparative readiness view for prioritization. Uses the `scan` profile.
2. **Assess**: Deep evaluation of specific assets against a profile (RAG, agents, training, feature-serving, or custom). Scores each requirement, reports pass/fail.
3. **Remediate**: For failing requirements, present platform-specific fixes, get approval, execute, verify.

## Conversation Flow

```
1. Platform        → user selects platform
2. Discovery       → guided, explore, or scan (estate-level)
3. Profile         → user picks a profile or selects individual requirements
4. Adjustments     → apply overrides (skip/set/add)
5. Coverage        → show what's runnable vs N/A before executing
6. Assess          → execute checks, score, report
7. Report          → present results, offer to save standardized report to filesystem
8. Remediate       → platform-specific fixes for failures
```

### Step 1: Platform

This skill ships one platform. Supported platforms:

- `databricks` (Unity Catalog + Delta Lake; SQL warehouse recommended)

Confirm the user is on Unity Catalog. Objects under `hive_metastore` cannot be assessed (no `information_schema`, tags, masks, lineage or audit). If they are on the legacy metastore, stop and recommend a UC migration first.

Load the platform reference from `platforms/`: either `platforms/{PLATFORM}.md` or `platforms/{platform}/{PLATFORM}.md`. This is your reference for all platform-specific behavior during this session.

### Step 2: Discovery

Discovery has three modes: **guided** (user already knows what to assess), **explore** (user wants to understand the landscape first), and **scan** (estate-level sweep for prioritization). Ask the user which fits:

```
How would you like to scope this assessment?
  1. I know which catalog/schema/tables to assess
  2. Help me explore what's available first
  3. Scan my data estate for AI readiness
```

#### Mode 1: Guided Discovery

Ask the user:

1. **What catalog?**
2. **What schema?**
3. **What tables?** All tables in the schema, or specific ones?
4. **Which SQL warehouse** should checks run on? (serverless preferred)

This establishes the scope for the assessment. No SQL is executed during guided discovery.

#### Mode 2: Explore-First Discovery

Run read-only reconnaissance queries against the platform to help the user understand their data landscape before choosing targets. Present findings progressively, do not dump everything at once.

**Step 2a: Catalog landscape.** List accessible catalogs with schema and table counts from `system.information_schema.tables` (skip `system`, `samples`, `__databricks_internal` and `hive_metastore`). Present a summary:

```
Available catalogs:
  prod_analytics      42 schemas    1,204 tables
  raw_ingestion       12 schemas      387 tables
  ml_features          5 schemas       89 tables
```

Ask the user to pick a catalog (or multiple).

**Step 2b: Schema inventory.** For the selected catalog(s), enumerate schemas with structural signals:

```
prod_analytics schemas:
  Schema             Tables  Views  MVs/STs  Tagged  Masked cols  Row filters
  product_metrics        34     12        3     yes            6            2
  user_behavior          28      4        0      no            0            0
  raw_events            112      0        0      no            0            0
```

Structural signals to surface (all from `{{ catalog }}.information_schema`):
- Table and view counts (`tables.table_type`)
- Materialized views and streaming tables (indicates Lakeflow / DLT pipelines)
- Tags on tables or columns (`table_tags`, `column_tags`; indicates governance investment)
- Column masks and row filters (`column_masks`, `row_filters`; indicates compliance posture)
- Non-Delta formats (`tables.data_source_format`)
- Storage size via `DESCRIBE DETAIL` only when the user asks; it is one call per table

**Step 2c: Narrow scope.** Based on what the user sees, help them select the catalog, schema, and tables for assessment. Users may choose all tables in a schema or pick specific ones.

All reconnaissance queries are read-only. No mutating operations during discovery.

#### Mode 3: Scan (Estate-Level)

Scan produces a comparative readiness view across many schemas within a catalog (or across catalogs). It uses the `scan` profile, a lightweight subset of requirements that are all schema-scoped and fast to execute.

**Step 2s-a: Scope.** Ask the user for the catalog (or the whole metastore via `system.information_schema`). No schema or table selection, the scan covers all schemas in the catalog.

**Step 2s-b: Execute scan profile.** Load `profiles/scan.yaml`. For each schema in the catalog, run the scan profile's requirements. All checks in the scan profile must be schema-scoped to allow batch execution across many schemas.

**Step 2s-c: Present portfolio view.** Score each schema and present a comparative ranking:

```
Estate Scan | databricks | {CATALOG}

Schema                  Readiness   Contextual  Consumable  Current  Compliant
────────                ─────────   ──────────  ──────────  ───────  ─────────
product_metrics              4.2         4.5         3.0      5.0        4.5
user_behavior                3.1         2.0         3.5      4.0        3.0
raw_events                   1.4         1.0         1.0      2.5        1.0

{N} schemas scanned. {H} above readiness threshold (≥ 3.5).
```

**Step 2s-d: Drill down.** After presenting the scan, offer the user a choice:

```
Options:
  1. Assess a specific schema in depth (pick from the list)
  2. Export scan results
  3. Done
```

If the user picks a schema, transition to the full Assess flow (Step 3 onward) with that schema pre-scoped. The scan phase flows naturally into the assess phase.

### Step 3: Profile

Ask the user what they want to assess:

Load each profile YAML from `profiles/` and count its requirements to present accurate numbers:

```
What would you like to assess?
  1. RAG readiness ({N} requirements)
  2. Feature serving readiness ({N} requirements)
  3. Training readiness ({N} requirements)
  4. Agent readiness ({N} requirements)
  5. Full assessment (all {N} requirements)
  6. Let me pick specific requirements
```

If the user picks a built-in profile, load `profiles/{name}.yaml`.

If the user picks "full assessment," include all requirements from `requirements/requirements.yaml` with default thresholds of `0.80`.

#### Option 6: Fine-Grained Requirement Picker

If the user wants to pick specific requirements, **dynamically generate the catalog from `requirements/requirements.yaml`** at runtime. Read every entry, group by `factor`, number sequentially, and present in this format:

```
Select the requirements you want to assess (comma-separated numbers, or "all" within a factor):

─── {Factor Name} ───
 {N}. {requirement_key}    {description}
 ...

─── {Next Factor} ───
 ...
```

Use the factor order: Clean, Contextual, Consumable, Current, Correlated, Compliant. Number requirements sequentially across all factors starting at 1.

Users can respond with:
- Specific numbers: `1, 2, 13, 34, 52` (picks those)
- Factor groups: `all Clean, all Compliant` (picks every requirement in those factors)
- Ranges: `1-12, 52-62`
- Mix: `all Clean, 13, 34-36, 52`

For each selected requirement, apply a default threshold of `0.80`. The user can adjust thresholds in Step 4 (Adjustments).

### Step 4: Adjustments

After loading the profile, offer three adjustment verbs:

- **`skip <requirement>`**: Exclude a requirement entirely.
- **`set <requirement> <threshold>`**: Override a threshold (e.g., `set chunk_readiness 0.70`).
- **`add <requirement> <threshold>`**: Include a requirement not in the profile.

### Step 5: Coverage Summary

Before executing, intersect the selected requirements with what the platform can actually run. For each requirement, check if `requirements/{key}/{platform}/check.md` exists.

Present the coverage summary:

```
{Profile} Assessment | databricks | {CATALOG}.{SCHEMA}

Selected: {N} requirements
Runnable: {R}
Not available: {N-R} (no implementation for this platform)
  - {requirement_key}: no {platform} implementation
  - ...

Proceed?
```

**Checkpoint:** User confirms before execution begins.

### Step 6: Assess

Load `requirements/requirements.yaml` once at session start. This manifest provides lightweight metadata: description, factor, scope, and implementations.

For each stage in order (Clean, Contextual, Consumable, Current, Correlated, Compliant), for each requirement:

1. Look up the requirement entry in `requirements/requirements.yaml` for metadata (scope).
2. Read `requirements/{requirement_name}/{platform}/check.md`. This file contains all context (constraints, gotchas, variant guidance) and one or more SQL blocks.
3. Use the context in the file to determine which SQL block to execute (e.g., sampled vs full scan based on row count, primary vs variant based on available platform features).
4. Substitute `{{ placeholder }}` values from the user's scope context and the SQL block itself (catalog, schema, asset, column, plus any requirement-specific values documented in the check file's context section and listed under `placeholders` in the manifest).
5. Execute the SQL on the chosen warehouse. Read the `value` result (float 0.0 to 1.0, where 1.0 is perfect). Some checks use the **probe-and-aggregate** pattern from `platforms/DATABRICKS.md` (one `DESCRIBE DETAIL` / `DESCRIBE HISTORY` per table, then a predicate). A few (Vector Search indexes, online tables) have no SQL surface and use the Databricks SDK or CLI; the check file says so.
6. Compare `value >= threshold` to determine pass/fail.
7. If no implementation exists for this platform, report `N/A`.

SQL blocks within markdown files use `{{ placeholder }}` syntax for variable substitution. The `scope` field in the manifest tells you whether the check is schema-scoped, table-scoped, or column-scoped:

- **Schema-scoped** (only `catalog`, `schema`): run once per schema.
- **Table-scoped** (includes `asset`): run per table, aggregate results.
- **Column-scoped** (includes `column`): run per column, aggregate results.

### Scoring conventions

1. **Range.** `value` is a float in `[0.0, 1.0]` where `1.0` is perfect. All requirements are `gte`-direction: pass when `value >= threshold`.
2. **N/A (empty denominator).** When the check has nothing to measure (no in-scope tables, no in-scope columns, no events in the window, etc.), the SQL must return `NULL` via `NULLIF(denominator, 0)`. The orchestrator renders `NULL` as `N/A` in reports and treats it as neither pass nor fail. **Never** emit a hard-coded `1.0` or `0.0` fallback; they silently inflate or deflate dashboards.
3. **Casing.** All `information_schema` filters wrap both sides in `LOWER(...)` (e.g. `LOWER(table_schema) = LOWER('{{ schema }}')`) so callers can pass identifiers in any case.
4. **Determinism.** `LIMIT` on system table scans (`system.access.*`, `system.query.history`, `system.lakeflow.*`) must be paired with a stable `ORDER BY` (`event_time DESC`, `start_time DESC`) so repeated runs return the same score.
5. **LIKE vs REGEXP_LIKE.** Prefer `REGEXP_LIKE(col, pattern)` for name pattern matching. `LIKE '%_x'` is unsafe because `_` is a single-character wildcard.
6. **Tag-based signals.** Where a requirement has no native primitive, the check reads Unity Catalog tags with the default keys listed in `platforms/DATABRICKS.md`. Tell the user which tag key was assumed whenever such a check scores 0 or N/A; the fix is often "adopt the tag", not "change the data".

### Step 7: Report

Present results in conversation first, then offer to save:

```
{Profile} Assessment | databricks | {CATALOG}.{SCHEMA}

{Stage Name}                                              {PASS/FAIL}
  "{why}"
  {requirement}    {value}  (need >= {threshold})         {PASS/FAIL}

Summary: {N} of {total} stages passing ({M} of {R} requirements passing)
```

#### Save Report to Filesystem

After presenting results in conversation, offer to write a standardized report file:

```
Would you like me to save this report to your filesystem?
  Default path: ./ai-ready-report-{CATALOG}-{SCHEMA}-{YYYY-MM-DD}.md
  (You can specify a different path)
```

If the user confirms, write the report in the following standardized format. The report must be self-contained, a reader who wasn't present for the conversation should understand the full context, results, and next steps.

**Report format:**

```markdown
# AI-Ready Data Assessment

| Field         | Value                              |
|---------------|------------------------------------|
| Date          | {YYYY-MM-DD}                       |
| Platform      | databricks                         |
| Catalog       | {CATALOG}                          |
| Schema        | {SCHEMA}                           |
| Tables        | {table count} ({list or "all"})    |
| Warehouse     | {warehouse name or id}             |
| Profile       | {profile name or "custom"}         |
| Requirements  | {selected count} of {manifest count} |
| Runnable      | {runnable count}                   |

## Summary

{N} of {total} stages passing. {M} of {R} requirements passing.

{1-3 sentence narrative: what's strong, what's the biggest gap, what's the highest-ROI fix.}

## Results by Stage

### {Stage Name}: {PASS/FAIL}

> {why}

| Requirement | Score | Threshold | Status |
|-------------|-------|-----------|--------|
| {key}       | {val} | {thresh}  | {P/F}  |

{Repeat for each stage}

## Failing Requirements

{For each failing requirement, one subsection:}

### {requirement_key}: {value} (need >= {threshold})

- **Factor:** {factor}
- **What it measures:** {description from manifest}
- **Scope:** {schema/table/column}
- **Constraints:** {any constraints from manifest, or "None"}

## Recommended Next Steps

{Prioritized list of remediation actions, ordered by impact.
 Group by effort level: quick wins first, then medium effort, then larger investments.
 Reference specific requirements and their current scores.}

---

*Generated by ai-ready-data assessment. Re-run to track progress.*
```

If the assessment was run against multiple tables, include per-table breakdowns under each stage where relevant (table-scoped and column-scoped checks).

If the user ran a custom selection (option 6), note which requirements were included and which were excluded under the metadata table.

**Checkpoint:** Options: `remediate` (fix gaps), `tell-me-more` (run diagnostics), `done` (stop).

### Diagnostics

When the user wants detail on a failing requirement, read `requirements/{requirement_name}/{platform}/diagnostic.md`. The file contains context explaining what the diagnostic measures and one or more SQL blocks. Use the context to select the appropriate SQL, substitute placeholders, execute, and present the results. If the file doesn't exist, explain that diagnostics aren't available for this requirement on this platform.

---

## Remediation Workflow

Process failing stages in order. For each stage:

### Present Stage Context

```
Stage: {Stage Name}
Why:   {why}

Failing requirements:
  {requirement}: {value} (need >= {threshold})
```

### Load Fix Operations

For each failing requirement:

1. Read `requirements/{requirement_name}/{platform}/fix.md`. This file contains all context (constraints, preconditions, delegation notes) and one or more remediation options, each with its own SQL block and/or organizational guidance.
2. Use the context to determine which remediation option(s) to present. Some fixes are executable SQL; others are organizational process guidance for humans.
3. Substitute `{{ placeholder }}` values in the SQL blocks.
4. Check the platform reference for delegation targets. If the fix references a delegated workflow, follow it.

### Present Remediation Plan

Show the substituted implementation, affected objects, and any constraints.

**Checkpoint:** Options: `approve` (execute), `skip` (next stage), `modify` (edit SQL), `tell-me-more` (diagnostics), `abort` (stop).

### Execute with Idempotency Guards

Before executing non-idempotent operations, check the platform reference for idempotency guards. Run the guard first; skip the operation if the desired state already exists.

Skipped guards are not failures, the desired state already exists. Never use `CREATE OR REPLACE TABLE` in a fix; it destroys Delta history and breaks streaming readers and Delta Sync indexes. `CREATE OR REPLACE FUNCTION` / `VIEW` are acceptable only where the fix file says so.

### Verify

Re-run the platform check implementation for each requirement in the stage. Show before/after:

```
{Stage Name}: remediation complete

  {requirement}:
    Before: {old_value}
    After:  {new_value}
    Status: {PASS/FAIL}
```

### Proceed or Finish

Move to the next failing stage. After all stages:

```
Remediation Complete

Stage                    Before    After
─────                    ──────    ─────
{Stage Name}             FAIL      PASS
{Stage Name}             FAIL      PASS

What changed:
  {Stage}: {one-line summary}
```

---

## Overrides

Overrides are applied in memory for the current run. For repeatability, overrides can be saved as a custom profile using `extends`:

```yaml
name: my-rag-profile
extends: rag
overrides:
  skip:
    - embedding_coverage
  set:
    chunk_readiness: { min: 0.70 }
  add:
    row_access_policy: { min: 0.50 }
```

When loading a profile with `extends`, first load the base profile, then apply overrides.

---

## Constraints

1. **Read-only during assessment.** Never execute mutating operations during scan or assess phases.
2. **Fix operations require approval.** Execute only with explicit user consent per stage.
3. **Never batch without consent.** Present the plan first, execute stage-by-stage with approval.
4. **Surface all constraints.** Show constraints from the fix file's context section before executing fix operations.
5. **No credentials in output.** Host, token and warehouse id come from the Databricks CLI profile or environment (`DATABRICKS_HOST`, `DATABRICKS_TOKEN`, `DATABRICKS_WAREHOUSE_ID`). Never print them.
6. **Read platform docs first.** Load `platforms/DATABRICKS.md` before executing any operations.
7. **Use capability gating.** If a workspace lacks a feature (system schema not enabled, Vector Search not provisioned, Lakehouse Monitoring absent), return `N/A` with the reason instead of failing the requirement.
8. **Stay inside Unity Catalog.** Refuse to assess or mutate `hive_metastore` objects.

---

## Requirement Directory Convention

Each requirement has a directory under `requirements/` containing platform-specific implementations as markdown files. The manifest (`requirements/requirements.yaml`) provides lightweight metadata needed at profile-load time. All detailed context, constraints, gotchas, variant selection guidance, platform-specific notes, lives in the markdown files themselves, co-located directly above the SQL they apply to.

| File | Purpose |
|---|---|
| `requirements.yaml` | Single manifest: lightweight metadata (description, factor, scope, implementations) |
| `{requirement_key}/{platform}/check.md` | Context + check SQL (read-only, returns normalized 0 to 1 score) |
| `{requirement_key}/{platform}/diagnostic.md` | Context + diagnostic SQL (read-only detail drill-down) |
| `{requirement_key}/{platform}/fix.md` | Context + remediation SQL and/or organizational guidance (mutating, requires approval) |

### Markdown file format

Each file follows this structure:

```
# {Type}: {requirement_key}

{One-line description}

## Context

{Prose: what it measures, constraints, gotchas, platform-specific notes,
 variant selection guidance, preconditions. Everything the agent needs
 to understand before executing the SQL.}

## SQL

{One or more fenced SQL blocks. If multiple variants or options exist,
 use ### subheadings with prose explaining when to use each one.}
```

A single file can contain multiple SQL implementations. For example, `check.md` can contain both a full-scan and a sampled variant; `fix.md` can contain multiple remediation options with different tradeoffs. The agent reads the context to determine which SQL block to use.

Fix files may contain organizational process guidance (not just SQL), for example, governance decisions, ownership assignments, or data model restructuring advice.

## File Layout

```
skills/ai-ready-data/
  SKILL.md                              ← You are here
  platforms/                            ← Platform references
    DATABRICKS.md                       ← Dialect, system tables, tag conventions, guards, permissions
  profiles/                             ← Assessment profiles
    scan.yaml                           ← Estate-level scan profile (lightweight)
    rag.yaml                            ← RAG readiness profile
    feature-serving.yaml                ← Feature serving readiness profile
    training.yaml                       ← Training readiness profile
    agents.yaml                         ← Agents readiness profile
  requirements/                         ← Requirement manifest + implementation directories
    requirements.yaml                   ← Single manifest (all requirement metadata)
    {requirement_key}/
      databricks/
        check.md                        ← Context + check SQL (or probe pattern / SDK call)
        diagnostic.md                   ← Context + diagnostic SQL
        fix.md                          ← Context + remediation SQL/guidance
```

## Adding a New Requirement

1. Add an entry to `requirements/requirements.yaml` with: description, factor, scope, implementations.
2. Create `requirements/{name}/{platform}/` directory with three markdown files:
   - `check.md` (required), context + SQL returning a `value` score 0 to 1
   - `diagnostic.md` (required), context + SQL for detail drill-down
   - `fix.md` (required), remediation SQL and/or organizational guidance
3. Add the requirement to the relevant profile YAML(s) under the matching factor stage.

## Adding a New Profile

1. Create `profiles/{name}.yaml` with six stages (Clean, Contextual, Consumable, Current, Correlated, Compliant).
2. Select requirements for each stage and set thresholds appropriate for the use case.
3. Alternatively, use `extends` to derive from an existing profile and apply overrides.

## Adding a New Platform

This repository is Databricks-first, but the layout is the upstream framework's, so another platform can be added the same way:

1. Create `platforms/{PLATFORM}.md` covering capabilities, dialect, permissions, nuances, idempotency guards, and delegation targets.
2. Add requirement files under `requirements/{key}/{platform}/` and list the platform under `implementations` in the manifest.

## Execution Setup

The agent needs a way to run SQL against a warehouse. Any of these work:

- **Databricks CLI:** `databricks api post /api/2.0/sql/statements --json '{"warehouse_id": "...", "statement": "..."}'`, or the `databricks sql` helpers where available.
- **Databricks SDK (Python):** `WorkspaceClient().statement_execution.execute_statement(...)`; see the helper in `platforms/DATABRICKS.md`.
- **Databricks SQL Connector:** `databricks-sql-connector` with `server_hostname`, `http_path` and a token from the environment.
- **A Databricks MCP server** exposing a SQL tool, if one is configured for the coding agent.

Ask once which one is available, then reuse it for the whole session.
