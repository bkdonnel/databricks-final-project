# CLAUDE.md

Guidance for Claude Code when working in this repo.

## What this is

Bootcamp capstone: an AI Job Hunting Copilot. A Databricks App (Flask) +
Lakebase Postgres (with `pgvector` for semantic search) + an AI agent with
tools. Full concept and architecture are in README.md; the phased build plan
(source of truth for "what's done / what's next") is in IMPLEMENTATION.md.

## Status

Repo is public at https://github.com/bkdonnel/databricks-final-project.
Phase 0 (API keys, Databricks App, Lakebase project `job-search-agent-db`)
and Phase 1 (`schema.sql`, run against the live instance) are complete;
`setup_secrets.py` (loads Adzuna/USAJobs keys into the `job-copilot`
Databricks secret scope) is written and run. Phase 2 (Spark ingestion
pipeline) is implemented and deployed — see IMPLEMENTATION.md's Phase 2
section for real deviations discovered along the way (this workspace is
**Databricks Free Edition**: serverless-only compute, no `run_as` service
principal for Jobs, Lakebase uses the newer "Postgres Autoscaling"
credential API with full endpoint-path resource names, and `psycopg` must
be installed without the `[binary]` extra). Phase 3 (embeddings/semantic
retrieval) is implemented and deployed — `pgvector` (not the standalone
Databricks Vector Search product) on the same Lakebase instance; see
IMPLEMENTATION.md's Phase 3 section, including a Free-Edition-specific
embeddings-endpoint batch-size limit discovered there. Before writing
code, check IMPLEMENTATION.md for which phase is active and update its
checkboxes as steps complete.

## Required architecture (bootcamp rubric — do not drop any of these)

1. A data pipeline in Spark (ingests job postings from third-party APIs).
2. Integration with at least one third-party API (Adzuna, USAJobs, and/or
   RemoteOK — see README.md for links).
3. Processing of unstructured data (job description text) via embeddings
   for semantic retrieval.
4. A Databricks App with a frontend.
5. An AI agent with tools that both read/retrieve AND write against
   Lakebase (not just a read-only chatbot).

## Conventions to follow

This project mirrors the structure of a prior bootcamp exercise
(`lakebase-support-app`): Flask app in `app.py`, Databricks Apps config in
`app.yaml`, schema + grants + sample data in `schema.sql`, `templates/` +
`static/` for the UI, `requirements.txt` for deps. For scheduled Databricks
Jobs (the Phase 2 Spark pipeline), mirror the Databricks Asset Bundle
scaffolding from another prior exercise (`databricks-lakebase-app-day-2`):
`databricks.yml` + `resources/*.yml` job definitions — but not that repo's
Lakebase auth (static password) or its notebook (runs on a Spark cluster
but never actually uses PySpark DataFrames — don't copy that part).

- **Lakebase auth**: connect using the OAuth-token-rotation pattern (a
  `psycopg.Connection` subclass that calls
  `WorkspaceClient().postgres.generate_database_credential()` per
  connection) — not a static password. Define this class inline in each
  component that needs it (Flask app, ingestion notebook) rather than
  factoring it into a shared importable module — Databricks Repos
  cross-file imports from notebooks are path-fragile, and the class is
  only ~15 lines.
- **Secrets**: never commit real `PGHOST` / `PGUSER` / `ENDPOINT_NAME` /
  API keys / `FLASK_SECRET_KEY` values or service-principal UUIDs. Use
  placeholders in `schema.sql` and `app.yaml` (e.g. `<DATABRICKS_CLIENT_ID>`)
  and real values only in local, gitignored env vars. Double check
  `schema.sql` specifically — it's the easiest place for a real UUID to
  slip in. Actual credentials (third-party API keys) go in a Databricks
  secret scope via `setup_secrets.py`, read back with `dbutils.secrets.get`
  in job/notebook contexts. Non-secret connection identifiers for
  Databricks Jobs (`PGHOST`/`PGUSER`/`PGDATABASE`/`ENDPOINT_NAME`) are NOT
  secrets — pass them as Databricks Asset Bundle `variables:` (no committed
  defaults) supplied via local `BUNDLE_VAR_*` env vars, the Job-context
  equivalent of `app.yaml`'s env vars for the Flask app.
- **Validation**: validate user input server-side in Flask routes
  (required fields, length limits, enum whitelisting for pipeline
  stage/priority-style fields), flashing errors back to the UI.
- **Agent tools**: keep read tools (search/retrieve) and write tools
  (save/update/log) clearly separated in the agent's tool definitions, and
  make destructive-ish writes (e.g. rejecting/removing an application)
  require an explicit confirmation param, matching the `DELETE`-to-confirm
  pattern used in the reference app.

## Don't

- Don't scaffold Spark/embedding/agent code speculatively ahead of the
  phase in IMPLEMENTATION.md that calls for it.
- Don't hardcode sample job postings as a substitute for the real API
  integration — the rubric requires a live third-party API pipeline.
