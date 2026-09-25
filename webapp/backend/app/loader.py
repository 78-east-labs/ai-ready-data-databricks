"""Reads requirements.yaml / profiles / check.md straight from the skill source of truth
(skills/ai-ready-data/) so the web app never forks the framework content."""
from __future__ import annotations

import re
from functools import lru_cache
from pathlib import Path

import yaml

REPO_ROOT = Path(__file__).resolve().parents[3]
SKILL = REPO_ROOT / "skills" / "ai-ready-data"
REQ_DIR = SKILL / "requirements"
PROFILES_DIR = SKILL / "profiles"

SQL_BLOCK_RE = re.compile(r"```sql\n(.*?)```", re.DOTALL | re.IGNORECASE)
PLACEHOLDER_RE = re.compile(r"{{\s*(\w+)\s*}}")

# Sensible platform-reference defaults for placeholders that don't need a human
# business decision (SLA windows, lookback windows, size thresholds). Anything
# else (allowed_values, consistency_predicate, ...) is left for manual targeting.
DEFAULT_PLACEHOLDERS = {
    "default_sla_hours": "24",
    "pii_tag_key": "pii",
    "pii_tag_pattern": "pii|sensitiv|personal|confidential|classification",
    "large_table_bytes": "10737418240",
    "lookback_days": "30",
    "sample_rows": "1000000",
    "staleness_hours": "24",
    "history_commits": "20",
    "min_retention_days": "7",
    "min_rows_per_second": "1000",
    "latency_threshold_ms": "2000",
    "table_weight": "0.3",
    "glossary_tag_key": "glossary_term",
    "source_system_tag": "source_system",
    "collection_method_tag": "collection_method",
    "training_patterns": r"(^|_)(train|training|trainset|training_set|training_data|labels?|dataset|feature_set|ml)($|_)",
    "purpose_vocabulary": "training,fine_tuning,evaluation,rag,analytics,feature_engineering,agent_tools,none",
    "comparison": "exact",
    "tolerance": "0.01",
    "tenancy_patterns": r"(^|_)(tenant|tenant_id|org_id|organization_id|account_id|customer_id|region|country|country_code|business_unit|department|legal_entity)($|_)",
    "temporal_tag_key": "temporal_scope",
    "unit_tag_key": "unit",
}


@lru_cache
def load_requirements() -> dict:
    data = yaml.safe_load((REQ_DIR / "requirements.yaml").read_text())
    return data.get("requirements") or {}


@lru_cache
def load_profile(name: str) -> dict:
    path = PROFILES_DIR / f"{name}.yaml"
    if not path.exists():
        raise FileNotFoundError(f"unknown profile: {name}")
    return yaml.safe_load(path.read_text())


def list_profiles() -> list[dict]:
    out = []
    for p in sorted(PROFILES_DIR.glob("*.yaml")):
        data = yaml.safe_load(p.read_text())
        count = sum(len(stage.get("requirements") or {}) for stage in data.get("stages", []))
        out.append({"name": data.get("name", p.stem), "description": data.get("description", ""), "count": count})
    return out


def profile_requirement_keys(profile: dict) -> list[tuple[str, str, float]]:
    """Return (requirement_key, stage_name, min_threshold) in declared order."""
    out = []
    for stage in profile.get("stages", []):
        for key, cfg in (stage.get("requirements") or {}).items():
            out.append((key, stage["name"], float(cfg["min"])))
    return out


def primary_sql(key: str, platform: str = "databricks") -> str | None:
    f = REQ_DIR / key / platform / "check.md"
    if not f.exists():
        return None
    blocks = SQL_BLOCK_RE.findall(f.read_text())
    return blocks[0].strip() if blocks else None


def render_sql(sql: str, catalog: str, schema: str) -> tuple[str, list[str]]:
    """Substitute {{ catalog }}/{{ schema }} and known-default placeholders.
    Returns (rendered_sql, unresolved_placeholder_names)."""
    missing: list[str] = []

    def repl(m: re.Match) -> str:
        name = m.group(1)
        if name == "catalog":
            return catalog
        if name == "schema":
            return schema
        if name in DEFAULT_PLACEHOLDERS:
            return DEFAULT_PLACEHOLDERS[name]
        missing.append(name)
        return m.group(0)

    rendered = PLACEHOLDER_RE.sub(repl, sql)
    return rendered, sorted(set(missing))
