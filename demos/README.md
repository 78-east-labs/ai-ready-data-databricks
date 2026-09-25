# Demos

Pre-built demo schemas for the AI-Ready Data skill on Databricks. Each demo seeds a Unity Catalog catalog with deliberate quality and governance gaps, then walks through scan, assess and remediate.

| Demo | Directory | What it shows |
|---|---|---|
| **Scan + Agents** | `scan-agents/` | Catalog scan across 3 schemas, drill into the worst, agents assessment, remediation |

## Prerequisites

- A Unity Catalog workspace and a SQL warehouse (serverless preferred)
- Permission to create a catalog, or edit the script to use an existing one
- The coding agent configured to run SQL on that warehouse (see the README's Quick start)
- Optional: `system.access`, `system.query`, `system.lakeflow` schemas enabled, so lineage, audit and attribution checks return numbers instead of N/A

Open the `DEMO.md` in the demo directory for the walkthrough. Each demo has a `teardown.sql`.
