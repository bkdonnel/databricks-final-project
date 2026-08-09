# CLAUDE.md

Guidance for Claude Code when working in this repo.

## What this is

Bootcamp capstone: an AI Job Hunting Copilot. A Databricks App (Flask) +
Lakebase Postgres (with `pgvector` for semantic search) + an AI agent with
tools. Full concept and architecture are in README.md; the phased build plan
(source of truth for "what's done / what's next") is in IMPLEMENTATION.md.

## Status

Repo is public at https://github.com/bkdonnel/databricks-final-project.
**All phases (0-6) are complete and deployed live** — the bootcamp capstone
has been submitted and graded (90/100; see "Post-submission — grading
feedback response" at the end of IMPLEMENTATION.md for what that feedback
was and how it was handled). The live app:
`https://bootcamp-job-search-agent-7474658268863295.aws.databricksapps.com`
(requires Databricks workspace SSO — not reachable by an anonymous
visitor, including grading environments, which is why `evidence/` exists —
see below).

Phase 0/1: `setup_secrets.py` (Adzuna/USAJobs keys in the `job-copilot`
Databricks secret scope), `schema.sql` run against the live instance.
Phase 2 (Spark ingestion): scheduled Databricks Job, real PySpark
DataFrame dedupe — see IMPLEMENTATION.md's Phase 2 section for Free
Edition deviations (serverless-only compute, no `run_as` service
principal for Jobs, Lakebase's "Postgres Autoscaling" credential API with
full endpoint-path resource names, plain `psycopg` — no `[binary]` — for
notebooks) plus a post-submission `posted_at` normalization fix. Phase 3
(embeddings/semantic retrieval): `pgvector` on the same Lakebase instance
(not standalone Vector Search) — see IMPLEMENTATION.md's Phase 3 section,
including an embeddings-endpoint batch-size limit. Phase 4: Flask app
(`app.py`, `templates/`, `static/style.css`, `app.yaml`), single-user (no
login). Phase 5: the agent (`agent.py` + tool implementations in
`app.py`), wired into `/chat` — no Claude model available in this
workspace's Foundation Model API, so it uses
`databricks-meta-llama-3-3-70b-instruct` via direct REST (the typed SDK
has no tool-calling support at all). Phase 6: deployed via
`databricks apps deploy`, with two deploy-only fixes (the Apps container
needs `psycopg[binary]`, opposite of the notebook's plain `psycopg`; a
FMAPI QPS burst limit needed retry-with-backoff). Before writing code,
check IMPLEMENTATION.md for full details and update its checkboxes as
steps change.

**`evidence/`** holds supplementary material for the grading resubmission
(repo-structure confirmation, a job-run cross-validated two ways, app
screenshots) — not application code, don't treat it as part of the app.

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
- **Submission zip**: built via `git ls-files -z | xargs -0 zip <out>.zip`
  (tracked files only — `.env`/secrets stay excluded automatically because
  they're gitignored, not because of anything zip-specific). This means
  the zip goes stale the instant anything new is committed — always
  rebuild it fresh (clean `git status`, matching `origin/main`) right
  before an actual resubmission rather than reusing an earlier one. Any
  new file added for a submission (including images) needs the same
  secrets scan as code before it's committed — a screenshot can leak
  `PGHOST`/`PGUSER`/UUIDs just as easily as a config file.

## Don't

- Don't scaffold Spark/embedding/agent code speculatively ahead of the
  phase in IMPLEMENTATION.md that calls for it.
- Don't hardcode sample job postings as a substitute for the real API
  integration — the rubric requires a live third-party API pipeline.
