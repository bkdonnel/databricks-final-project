# CLAUDE.md

Guidance for Claude Code when working in this repo.

## What this is

Bootcamp capstone: an AI Job Hunting Copilot. A Databricks App (Flask) +
Lakebase Postgres + Databricks Vector Search + an AI agent with tools. Full
concept and architecture are in README.md; the phased build plan (source of
truth for "what's done / what's next") is in IMPLEMENTATION.md.

## Status

Planning phase. No application code exists yet — only planning docs
(README.md, CLAUDE.md, IMPLEMENTATION.md), committed and pushed to
https://github.com/bkdonnel/databricks-final-project (public). Before
writing code, check IMPLEMENTATION.md for which phase is active and update
its checkboxes as steps complete.

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
`static/` for the UI, `requirements.txt` for deps.

- **Lakebase auth**: connect using the OAuth-token-rotation pattern (a
  `psycopg.Connection` subclass that calls
  `WorkspaceClient().postgres.generate_database_credential()` per
  connection) — not a static password.
- **Secrets**: never commit real `PGHOST` / `PGUSER` / `ENDPOINT_NAME` /
  API keys / `FLASK_SECRET_KEY` values or service-principal UUIDs. Use
  placeholders in `schema.sql` and `app.yaml` (e.g. `<DATABRICKS_CLIENT_ID>`)
  and real values only in local, gitignored env vars. Double check
  `schema.sql` specifically — it's the easiest place for a real UUID to
  slip in.
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
