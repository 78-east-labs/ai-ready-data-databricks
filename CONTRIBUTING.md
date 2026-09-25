# Contributing

Thanks for helping make Databricks data AI-ready. Pull requests, issues and corrections are welcome.

## What is most useful

- **Wrong column names or syntax.** Databricks system tables and SDK names change across releases. Several files flag a column or method as "confirm on your runtime". If you have confirmed it (or found it wrong), a one-line PR with the runtime version in the description is the most valuable contribution there is.
- **Stronger signals for tag-based requirements.** Consent, purpose, license, retention, glossary and bias testing are measured through tags today. If Unity Catalog grows a primitive for one of them, or you have a better proxy, open an issue first so we can agree on the signal before the SQL.
- **New requirements or profile changes.** Use the requirement proposal issue template. Keep keys, factors and scopes compatible with the upstream framework unless there is a strong reason not to.
- **Demo schemas.** Setup SQL that seeds a schema with deliberate gaps for a given profile.

## Before opening a PR

1. Read `docs/AUTHORING.md`. It is the contract every requirement file follows.
2. Run `python3 tools/validate.py` from the repo root. It must report 0 errors.
3. If you changed SQL, say which runtime and warehouse type you ran it on.
4. One topic per PR. Separate unrelated changes.

## Style

Plain language. Say what a signal proves and what it does not. No marketing adjectives. No em dashes (use commas, periods or parentheses). Short paragraphs.

## Licensing

By contributing you agree that documentation and framework content is licensed under CC BY 4.0 and code, SQL and skill files under Apache 2.0, matching the repo. No CLA is required.

## Upstream

This repo tracks the [AI-Ready Data Framework](https://github.com/Snowflake-Labs/ai-ready-data). Changes to factor definitions, requirement keys or default thresholds should go upstream first; we mirror them here.
