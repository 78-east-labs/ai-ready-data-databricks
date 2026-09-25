# Demo: Scan + Agents

About 20 minutes. Seeds a catalog with three schemas at different readiness levels, scans them, drills into the worst, assesses it against the `agents` profile, and remediates a few stages.

## Setup

1. Pick a SQL warehouse and a catalog you can create schemas in. The script uses `airdemo`; change the `CREATE CATALOG` and `USE CATALOG` lines if you need another name.
2. Run `setup.sql` in the SQL editor or with the CLI:

   ```bash
   databricks api post /api/2.0/sql/statements --json "$(jq -n \
     --arg wh "$DATABRICKS_WAREHOUSE_ID" \
     --arg sql "$(cat demos/scan-agents/setup.sql)" \
     '{warehouse_id: $wh, statement: $sql, wait_timeout: "50s"}')"
   ```

   The statement API runs one statement at a time; the SQL editor runs the whole file. If you use the API, split the file on `;` first or paste it into the editor.

3. Make sure the coding agent can run SQL against the same warehouse (CLI profile, SDK, or connector) and that the `system.access`, `system.query` and `system.lakeflow` schemas are enabled if you want lineage, audit and attribution checks to return numbers rather than N/A. On a fresh demo they will mostly be N/A anyway because nothing has read or written the tables yet; run a couple of `SELECT`s against `airdemo.good_orders.orders` and wait an hour if you want to see them populate.

## Walkthrough

**1. Scan the catalog.**

```
Scan the airdemo catalog for AI readiness.
```

Expect `good_orders` on top (tags, comments, keys, CDF, masks), `meh_events` in the middle (CDF and a few comments, no keys or tags), `raw_dump` at the bottom (nothing declared).

**2. Drill into the worst schema with the agents profile.**

```
Assess airdemo.raw_dump for agent readiness.
```

The coverage step lists what is runnable. Things to watch for in the report:

- Clean: `uniqueness` fails on `contacts` (duplicate id 1), `data_completeness` fails on `email`, `encoding_validity` catches the replacement character in `notes`, `syntactic_validity` catches the broken JSON in `payload`.
- Contextual: `semantic_documentation`, `entity_identifier_declaration`, `constraint_declaration` all 0.
- Compliant: `anonymization_effectiveness` flags `email`, `phone`, `ssn` as unmasked PII candidates; `classification` and `column_masking` are 0 or N/A because nothing is tagged.

**3. Ask for detail.**

```
tell-me-more on anonymization_effectiveness
```

**4. Remediate a stage.**

```
remediate
```

Approve the Contextual stage first (comments, a primary key on `contacts.id` once duplicates are removed, tags). Then Compliant (a mask function on `ssn` and `email`). Watch the agent run the guard before each `ALTER`, and re-run the check to show before/after.

**5. Compare with the good schema.**

```
Assess airdemo.good_orders for agent readiness.
```

Most Contextual and Compliant requirements pass. The remaining gaps are the honest ones: no lineage or query history yet (N/A), no Vector Search (N/A for RAG-only requirements), and `business_glossary_linkage` only partly covered.

## Teardown

Run `teardown.sql`. It drops the three schemas and the catalog with `CASCADE`.
