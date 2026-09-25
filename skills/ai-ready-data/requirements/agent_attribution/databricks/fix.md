# Fix: agent_attribution

Make every writer to the schema identifiable, either by running it as a workspace entity (job, pipeline, notebook) or by setting query tags on the session.

## Context

There is no table property that fixes attribution. The write has to arrive through a path Unity Catalog can name. Two levers, strongest first:

- **Run the writer as a Lakeflow job or pipeline.** Lineage then carries `entity_type = 'JOB'` or `'PIPELINE'`, an `entity_id` and an `entity_run_id`, which also unlocks `pipeline_execution_audit`. This is the durable fix for anything scheduled.
- **Set query tags on the session.** `SET query_tags = 'pipeline=x,run=y'` attaches a `map<string,string>` to every statement the session runs afterwards, visible in `system.query.history.query_tags`. This is the Databricks analogue of Snowflake's `QUERY_TAG`. It only helps where the statement lands in `system.query.history` (SQL warehouses, serverless notebooks and jobs, Lakeflow pipelines) and support depends on the warehouse type and runtime version; classic all-purpose clusters do not populate query history at all. Confirm on your warehouse with `SET query_tags = 'probe=1'; SELECT 1;` and look for the statement in `system.query.history` a few minutes later.

Neither lever rewrites data. The diagnostic's `anonymous_run_as` and `client_application` columns tell you which identities and tools to go after first: an anonymous writer with `client_application = 'Databricks SQL Driver for Go'` is an external service, one with a notebook-style name is a person.

## Fix: Set query tags at session start

Run this before the first write in every session that cannot be turned into a job (external orchestrators over JDBC/ODBC, ad hoc SQL editor work, agents calling the Statement Execution API). Use stable keys so reports can group on them.

```sql
SET query_tags = 'pipeline={{ pipeline_name }},run={{ run_id }},agent={{ agent_name }}';
```

For the Statement Execution API and the SQL connectors, issue the `SET` as the first statement on the same session (the Python connector's `session_configuration` argument also accepts it). For dbt on Databricks, put the statement in `on-run-start` or the model's `pre-hook` so every model run carries the invocation id.

## Fix: Wrap an ad hoc notebook in a job

A notebook run through a job carries `entity_type = 'JOB'` in lineage. Create a job once with the CLI; re-running the command with the same name creates a second job, so check `databricks jobs list --name` first.

```bash
databricks jobs list --name "{{ job_name }}" --output json
# only if the list above is empty:
databricks jobs create --json '{
  "name": "{{ job_name }}",
  "tasks": [{
    "task_key": "load",
    "notebook_task": {"notebook_path": "{{ notebook_path }}"},
    "environment_key": "default"
  }],
  "environments": [{"environment_key": "default", "spec": {"client": "2"}}]
}'
```

Serverless is used above so the run also appears in `system.query.history`. Replace `environment_key` with `job_cluster_key` and a cluster spec if the workload needs classic compute; lineage attribution still works, only query tags are lost.

## Fix: Verify

Allow a few hours for `system.access.table_lineage` to catch up, then re-run the check. To confirm a single fix landed, look for the statement directly:

```sql
SELECT statement_id, start_time, executed_by, to_json(query_tags) AS query_tags,
       query_source.job_info.job_id AS job_id
FROM system.query.history
WHERE start_time >= current_timestamp() - INTERVAL 1 DAYS
  AND (query_tags['pipeline'] = '{{ pipeline_name }}'
       OR query_source.job_info.job_id = '{{ job_id }}')
ORDER BY start_time DESC
LIMIT 20
```

## Organizational guidance

Attribution holds only when it is the default path. Give each pipeline its own service principal (so `run_as` alone narrows the search), forbid direct writes to governed schemas from personal identities with `GRANT MODIFY` limited to the service principals, and put the `SET query_tags` line into the shared connection helper used by external tools so nobody has to remember it. For agents, include the agent name and a conversation or run id in the tags; that is what lets an incident be traced back to a decision rather than to a service account.
