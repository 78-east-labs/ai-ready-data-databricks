---
name: ai-ready-data
description: Assess and optimize Databricks (Unity Catalog + Delta Lake) data for AI workloads. Scan catalogs for prioritization, assess schemas against RAG / agents / feature-serving / training profiles, and guide remediation.
---

# AI-Ready Data Agent (Databricks)

A skill for assessing and optimizing Unity Catalog data for AI workloads.

## Entry Point

Read `skills/ai-ready-data/SKILL.md` for the full protocol, then `skills/ai-ready-data/platforms/DATABRICKS.md` before running anything.

## Triggers

This skill activates when the user mentions:

- "assess my data", "is my data AI-ready", "check my schema", "check my catalog"
- "scan my catalog", "scan my data estate", "which schemas are ready", "prioritize my data"
- "data quality check", "data quality assessment", "Unity Catalog readiness"
- "optimize for AI", "make data AI-ready", "ready for Genie", "ready for Vector Search"
- "assess for RAG", "assess for agents", "assess for feature serving", "assess for training"

## Ground Rules

- Read-only during scan and assess. Fixes run only after explicit approval, one stage at a time.
- Unity Catalog only. Refuse `hive_metastore`.
- Credentials (host, token, warehouse id) come from the environment or the Databricks CLI profile. Never print them.
- When a workspace lacks a feature (system schema not enabled, no Vector Search endpoint, no Lakehouse Monitoring), report N/A with the reason. Do not fail the requirement.
- When a tag-based check scores 0, say which tag key was assumed; the fix is often "adopt the convention".

## Structure

```
skills/
  ai-ready-data/
    SKILL.md                    ← Orchestration protocol
    platforms/
      DATABRICKS.md             ← Dialect, system tables, tag conventions, guards, permissions
    requirements/
      requirements.yaml         ← Single manifest (all requirement metadata)
      {name}/
        databricks/
          check.md              ← Context + check SQL / probe / SDK recipe (read-only, 0–1 score)
          diagnostic.md         ← Context + diagnostic SQL (read-only detail)
          fix.md                ← Context + remediation SQL/guidance (mutating, needs approval)
    profiles/
      scan.yaml  rag.yaml  agents.yaml  feature-serving.yaml  training.yaml
```
