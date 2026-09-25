#!/usr/bin/env python3
"""Consistency checks for the ai-ready-data-databricks repository.

Run from the repo root:  python3 tools/validate.py

Checks:
  * requirements.yaml parses and every entry has description / factor / scope / implementations
  * every requirement directory has check.md, diagnostic.md, fix.md for each listed platform
  * no orphan requirement directories (present on disk, missing from manifest)
  * every profile parses, uses the six stage names, and references only manifest keys
  * every check.md returns a column named `value` and uses NULLIF
  * no em dashes, no leftover Snowflake constructs, no TODO markers
"""
from __future__ import annotations

import re
import sys
from pathlib import Path

try:
    import yaml
except ImportError:  # pragma: no cover
    sys.exit("pip install pyyaml")

ROOT = Path(__file__).resolve().parents[1]
SKILL = ROOT / "skills" / "ai-ready-data"
REQ_DIR = SKILL / "requirements"
PROFILES = SKILL / "profiles"

STAGES = ["Clean", "Contextual", "Consumable", "Current", "Correlated", "Compliant"]
FACTORS = {s.lower() for s in STAGES}
SCOPES = {"schema", "table", "column"}
SNOWFLAKE_MARKERS = [  # scanned inside SQL blocks only
    r"account_usage", r"RESULT_SCAN", r"LAST_QUERY_ID", r"::FLOAT\b",
    r"\bQUERY_TAG\b", r"tag_references", r"MASKING POLICY", r"SYSTEM\$CLASSIFY",
    r"\bDYNAMIC TABLE\b", r"CREATE STREAM\b", r"\bSHOW STREAMS\b", r"\bDATEADD\(",
]

errors: list[str] = []
warnings: list[str] = []


def err(msg: str) -> None:
    errors.append(msg)


def warn(msg: str) -> None:
    warnings.append(msg)


def load_manifest() -> dict:
    data = yaml.safe_load((REQ_DIR / "requirements.yaml").read_text())
    reqs = data.get("requirements") or {}
    for key, entry in reqs.items():
        for field in ("description", "factor", "scope", "implementations"):
            if field not in entry:
                err(f"manifest: {key} missing '{field}'")
        if entry.get("factor") not in FACTORS:
            err(f"manifest: {key} has unknown factor {entry.get('factor')!r}")
        if entry.get("scope") not in SCOPES:
            err(f"manifest: {key} has unknown scope {entry.get('scope')!r}")
        if not re.fullmatch(r"[a-z][a-z0-9_]*", key):
            err(f"manifest: key {key!r} is not snake_case")
    return reqs


def check_files(reqs: dict) -> None:
    on_disk = {p.name for p in REQ_DIR.iterdir() if p.is_dir()}
    for key, entry in reqs.items():
        for platform in entry.get("implementations", []):
            d = REQ_DIR / key / platform
            for fname in ("check.md", "diagnostic.md", "fix.md"):
                f = d / fname
                if not f.exists():
                    err(f"missing {f.relative_to(ROOT)}")
                    continue
                text = f.read_text()
                if "—" in text:
                    err(f"em dash in {f.relative_to(ROOT)}")
                if re.search(r"\bTODO\b|\bTBD\b|\bFIXME\b", text):
                    err(f"TODO marker in {f.relative_to(ROOT)}")
                for block in re.findall(r"```sql\n(.*?)```", text, re.DOTALL | re.IGNORECASE):
                    for marker in SNOWFLAKE_MARKERS:
                        if re.search(marker, block):
                            warn(f"possible Snowflake construct /{marker}/ in {f.relative_to(ROOT)}")
                head = text.splitlines()[0] if text else ""
                expected = {"check.md": "Check", "diagnostic.md": "Diagnostic", "fix.md": "Fix"}[fname]
                if head.strip() != f"# {expected}: {key}":
                    err(f"bad title in {f.relative_to(ROOT)}: {head!r}")
                if fname == "check.md":
                    sql_value = re.search(r"\bAS\s+value\b", text, re.IGNORECASE)
                    prose_value = re.search(r"value\s*=\s*", text)  # probe / SDK modes define it in prose or Python
                    if not (sql_value or prose_value):
                        err(f"check.md for {key} never defines value")
                    if "NULLIF" not in text.upper() and not re.search(r"NULL when|None", text):
                        err(f"check.md for {key} has no N/A guard (NULLIF or 'NULL when')")
                    if "## Context" not in text:
                        err(f"check.md for {key} has no Context section")
                if fname == "fix.md" and "## Fix" not in text:
                    err(f"fix.md for {key} has no '## Fix:' section")
                if fname == "fix.md":
                    for block in re.findall(r"```sql\n(.*?)```", text, re.DOTALL | re.IGNORECASE):
                        if re.search(r"CREATE\s+OR\s+REPLACE\s+TABLE", block, re.IGNORECASE):
                            err(f"fix.md for {key} uses CREATE OR REPLACE TABLE in a SQL block")
    for name in sorted(on_disk - set(reqs)):
        err(f"orphan requirement directory not in manifest: {name}")


def check_profiles(reqs: dict) -> dict[str, int]:
    counts = {}
    for pf in sorted(PROFILES.glob("*.yaml")):
        data = yaml.safe_load(pf.read_text())
        if "extends" in data:
            continue
        seen = set()
        for stage in data.get("stages", []):
            if stage.get("name") not in STAGES:
                err(f"{pf.name}: unknown stage {stage.get('name')!r}")
            for key, cfg in (stage.get("requirements") or {}).items():
                if key not in reqs:
                    err(f"{pf.name}: unknown requirement {key}")
                elif reqs[key]["factor"] != stage["name"].lower():
                    err(f"{pf.name}: {key} listed under {stage['name']} but manifest says {reqs[key]['factor']}")
                if not isinstance(cfg, dict) or "min" not in cfg:
                    err(f"{pf.name}: {key} needs a {{ min: x }} threshold")
                elif not (0.0 <= float(cfg["min"]) <= 1.0):
                    err(f"{pf.name}: {key} threshold out of range")
                if key in seen:
                    err(f"{pf.name}: {key} listed twice")
                seen.add(key)
        counts[data.get("name", pf.stem)] = len(seen)
    return counts


def main() -> int:
    reqs = load_manifest()
    check_files(reqs)
    counts = check_profiles(reqs)
    by_factor: dict[str, int] = {}
    for entry in reqs.values():
        by_factor[entry["factor"]] = by_factor.get(entry["factor"], 0) + 1

    print(f"requirements: {len(reqs)}  " + "  ".join(f"{k}={v}" for k, v in sorted(by_factor.items())))
    print("profiles:     " + "  ".join(f"{k}={v}" for k, v in counts.items()))
    for w in warnings:
        print(f"WARN  {w}")
    for e in errors:
        print(f"ERROR {e}")
    print(f"{len(errors)} errors, {len(warnings)} warnings")
    return 1 if errors else 0


if __name__ == "__main__":
    sys.exit(main())
