# Fix: access_audit_coverage

Remediation when tables read in the window do not show up in the Unity Catalog audit log.

## Context

There is no per-table switch for Unity Catalog auditing. Every metadata resolution and credential grant on a UC table is written to `system.access.audit`, and it cannot be turned off. So a low score is never fixed by altering the table. The causes, in order of likelihood:

1. **The `system.access` schema is not enabled or not granted.** If the check errored or returned zero audit rows for every table, the caller cannot see the audit table. Enable the schema (metastore admin) and grant it.
2. **Lag.** Audit rows land within minutes to a few hours; lineage rows can take a few hours too. A table read an hour ago may be in one table and not the other. Re-run after the lag window.
3. **The tables are in `hive_metastore`.** Legacy tables have no UC audit trail, no lineage and no `information_schema`. The fix is migration, not configuration.
4. **The `request_params` key differs.** The check reads `full_name_arg` and `table_full_name`. If the key probe in the check shows a different key, adjust the check rather than the platform.

None of the fixes below touch data.

## Fix: Enable and grant the system.access schema

Run as a metastore admin. Enabling is idempotent (an already-enabled schema returns an error you can ignore, or use the `list` call first).

```bash
METASTORE_ID=$(databricks metastores current --output json | jq -r .metastore_id)
databricks system-schemas list "$METASTORE_ID"
databricks system-schemas enable "$METASTORE_ID" access
```

Then grant the assessment principal read access:

```sql
GRANT USE SCHEMA ON SCHEMA system.access TO `{{ assessment_principal }}`;
GRANT SELECT ON TABLE system.access.audit         TO `{{ assessment_principal }}`;
GRANT SELECT ON TABLE system.access.table_lineage TO `{{ assessment_principal }}`;
```

`GRANT` is idempotent; re-running it is harmless.

## Fix: Confirm the audit table is populated for this catalog

Before assuming a table-level gap, confirm the audit log carries anything at all for the catalog in the last day. An empty result here points at enablement or a workspace-level issue, not at the tables.

```sql
SELECT action_name, COUNT(*) AS events, MAX(event_time) AS latest
FROM system.access.audit
WHERE service_name = 'unityCatalog'
  AND action_name IN ('getTable', 'generateTemporaryTableCredential')
  AND event_date >= date_sub(current_date(), 1)
  AND LOWER(element_at(split(COALESCE(request_params['full_name_arg'],
                                      request_params['table_full_name']), '\\.'), 1))
      = LOWER('{{ catalog }}')
GROUP BY action_name
ORDER BY action_name
```

## Fix: Migrate hive_metastore tables to Unity Catalog

If the user pointed at a legacy schema, upgrade it so reads become auditable. `SYNC` creates external UC tables that point at the same files; it does not copy data. For managed Hive tables or large estates, use UCX instead.

```sql
SYNC SCHEMA {{ catalog }}.{{ schema }}
FROM hive_metastore.{{ schema }}
DRY RUN;
```

Review the dry-run output, then run the same statement without `DRY RUN`. Consumers must then read `{{ catalog }}.{{ schema }}.*` instead of `hive_metastore.{{ schema }}.*`, otherwise the reads keep bypassing UC.

## Organizational guidance

Treat `system.access.audit` as the record of truth and keep it queryable: enable the `access` system schema in every metastore, grant it to the governance principal, and land a scheduled job that copies the rows you care about (`service_name = 'unityCatalog'`, table-resolving actions) into a governed table with your own retention, since the system table's retention is finite (365 days). Retire `hive_metastore` reads on a deadline; any pipeline still reading legacy tables is invisible to both audit and lineage.
