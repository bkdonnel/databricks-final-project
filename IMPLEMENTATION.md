# Implementation Plan — AI Job Hunting Copilot

Phased build plan. Check items off as they're completed; keep this file
current so it reflects real progress, not the original plan.

## Phase 0 — Setup

- [x] Sign up for API keys: [Adzuna](https://developer.adzuna.com/)
      (app ID + key) and [USAJobs](https://developer.usajobs.gov/)
      (key + registered email). RemoteOK needs no key. Stored in local
      gitignored `.env`.
- [x] Create the Databricks App from the workspace UI (Apps → Create app)
      to provision a service principal, per the pattern used in
      `lakebase-support-app`. Copy the `DATABRICKS_CLIENT_ID`.
- [x] Create a Lakebase Postgres project (e.g. `job-copilot-db`).
- [x] Confirm Databricks Vector Search is available in the workspace/edition
      being used.
- [x] `setup_secrets.py` written and run — Adzuna/USAJobs API keys are
      loaded into the Databricks secret scope (`job-copilot`) for use by
      the Phase 2 Spark job.

## Phase 1 — Lakebase schema

- [x] `schema.sql` written (tables, grants with `<DATABRICKS_CLIENT_ID>`
      placeholder, sample data) and run against the live
      `job-search-agent-db` Lakebase instance via the SQL Editor.

Design and run `schema.sql` covering:

- `users` — id, email, display name.
- `profiles` — id, user_id, target roles, location preference, remote
  preference, resume text/summary, notes.
- `skills` — id, user_id, skill name, proficiency/years (or a simple
  `profile_id`-linked skills list).
- `job_postings` — id, external_source, external_id, title, company,
  location, remote flag, salary range, description (raw text), url,
  posted_at, fetched_at.
- `applications` — id, user_id, job_posting_id, stage (`saved` /
  `applied` / `interviewing` / `rejected` / `offer`), applied_at,
  last_updated_at.
- `saved_jobs` — **decision: folded into `applications`** with
  `stage='saved'` as the default/initial stage, rather than a separate
  table. Keeps one uniqueness constraint (`user_id`, `job_posting_id`)
  and one set of stage-transition logic instead of duplicating it across
  two tables.
- `interview_notes` — id, application_id, note_text, follow_up_date,
  author, created_at.
- `contacts` — id, application_id, name, role, email, linkedin_url, notes.

Grant the app's service principal access the same way
`lakebase-support-app/schema.sql` does. Load a handful of sample rows for
local testing before the real pipeline is wired up.

## Phase 2 — Spark ingestion pipeline

- [x] Write a Spark job that calls Adzuna / USAJobs / RemoteOK, normalizes
      each source's response into the `job_postings` schema, dedupes on
      `(external_source, external_id)`, and upserts into Lakebase.
- [x] Run it as a Databricks Job (scheduled every 6h) rather than a
      one-off script.
- [x] Log row counts fetched/deduped/inserted/updated per run.
- [x] **Implemented and verified against the live instance** —
      `notebooks/ingest_job_postings.py` + `databricks.yml` +
      `resources/ingest_job_postings_job.yml`. Two manual runs confirmed:
      first run inserted 340 rows (151 Adzuna, 88 USAJobs, 101 RemoteOK,
      zero duplicate `(external_source, external_id)` pairs); second run
      advanced `fetched_at` on an existing row without creating a
      duplicate, proving the `DO UPDATE` path. Schedule is `UNPAUSED`.

  Real deviations discovered from the original design (all forced by
  this workspace being a **Databricks Free Edition** account — read
  before touching this pipeline again):
    - **Serverless only, no job clusters.** Free Edition rejects
      `new_cluster`/`job_clusters` job specs ("Only serverless compute is
      supported"). The task has no cluster reference at all — omitting it
      runs the notebook on serverless compute, which still provides a
      real Spark session for the dedupe step.
    - **No `run_as` service principal.** Binding a job to run as a
      service principal requires the account-level "Service Principal
      User" role, and Free Edition has no account-console/account-API
      access to grant it — confirmed via a real `terraform apply` error,
      not just docs. The job instead runs as its owner (the deploying
      user), and `schema.sql`'s service-principal grants are unused by
      this job; the deploying user already has full read/write on
      `job_postings` as the Lakebase instance owner. This SP/run_as
      limitation is specific to Jobs — it does **not** apply to Phase 4's
      Databricks App, which runs as its own service principal natively
      (no `run_as` binding involved).
    - **Lakebase project is on the newer "Postgres Autoscaling" API, not
      "Database Instances."** `w.database.generate_database_credential()`
      (instance_names=[...]) fails with "not found" for this project;
      the correct call is `w.postgres.generate_database_credential(
      endpoint=...)`, and `endpoint` must be the **full resource path**
      (`projects/<project>/branches/<branch>/endpoints/<endpoint>`, e.g.
      `projects/job-search-agent-db/branches/production/endpoints/primary`)
      — the short project name is rejected. Same likely applies when
      Phase 4's Flask app connects.
    - **`psycopg[binary]`'s compiled extension crashes the serverless
      Python kernel** (`SIGABRT` on `import psycopg` inside
      `psycopg/pq/__init__.py`). Fixed by installing plain `psycopg` (no
      `[binary]`/`[c]` extra — falls back to the ctypes/system-libpq
      backend) instead.
    - **`psycopg_pool.ConnectionPool` swallows the real connection
      error** behind a generic `PoolTimeout` after 30s. Since this
      notebook only needs one connection for a batch upsert (not
      concurrent-request pooling like a Flask app), it connects directly
      via `OAuthConnection.connect(...)` instead of a pool.
    - **`%pip install "databricks-sdk>=0.40.0"` silently no-ops** if the
      runtime's pre-installed version already satisfies the bound (it
      did, and that version predates the `.postgres` API) — needed
      `--upgrade` to actually get a version with `w.postgres`.
    - Local smoke-test (`scripts/verify_fetchers.py`) confirmed all 3
      fetchers return correctly-shaped data. RemoteOK did *not*
      reproduce the documented 403-without-User-Agent behavior on this
      run, but a descriptive `User-Agent` is still sent defensively.

## Phase 3 — Embeddings / semantic retrieval

- [x] Pick an embedding model available in the workspace (e.g. a
      Databricks-hosted embedding endpoint).
- [x] Embed `job_postings.description` (and optionally title + company)
      into a vector index (Databricks Vector Search, synced from the
      Lakebase table or a Delta table mirroring it).
- [x] Embed each user's `profiles` skills/resume summary the same way, so
      queries like "remote backend roles that don't require 5+ years of
      Kubernetes" can match against both the query text and the profile
      context.
- [x] Write a retrieval function: given a query (+ optional profile_id),
      return the top-N matching postings with scores.

  **Design decision: `pgvector` on Lakebase instead of the standalone
  Databricks Vector Search product.** The user had already run
  `CREATE EXTENSION vector;` on the live Lakebase instance before this
  phase started, confirming pgvector (not the separate Vector Search
  service) was the intended approach. This is a better fit anyway: the
  workspace's only Unity Catalog catalogs are `workspace`/`samples`/`system`
  (no project catalog), and a standalone Vector Search index would have
  needed either a Delta-table mirror of `job_postings` (extra moving parts,
  a second copy of the data, Change Data Feed) or a Direct Vector Access
  index registered under a UC schema anyway. With pgvector, Lakebase stays
  the single source of truth: `embedding vector(1024)` + `embedded_at`
  columns live directly on `job_postings`, with an HNSW cosine-distance
  index (`idx_job_postings_embedding`; pgvector 0.8.0 confirmed on this
  instance, which supports HNSW). No profile-embedding column was added —
  the retrieval function instead concatenates the query text with the
  profile's `target_roles`/`resume_summary`/`notes`/skills before embedding
  a single combined vector, which is simpler than storing and combining
  two separate embeddings and satisfies the same requirement.

  **Implementation:**
  - `notebooks/embed_job_postings.py` — new task in the existing
    `ingest_job_postings_job` (runs after `ingest_job_postings`, same 6h
    schedule). Finds rows where `embedding IS NULL OR embedded_at <
    fetched_at`, embeds `title + description` via the
    `databricks-gte-large-en` Foundation Model API endpoint (1024 dims;
    `databricks-bge-large-en` and `databricks-qwen3-embedding-0-6b` are
    also available at the same dimension if this one is ever unavailable),
    and writes vectors back via the same OAuth-token-rotation
    `psycopg.Connection` pattern used by `ingest_job_postings.py`.
  - `scripts/search_postings.py` — local verification script (same
    local-harness style as `scripts/verify_fetchers.py`): given a query and
    optional `--profile-id`, builds the combined query text, embeds it, and
    ranks `job_postings` by cosine similarity (`embedding <=> ...`) via
    plain SQL. Verified against live data: a query alone surfaces relevant
    Python/backend and DC-federal postings; the same query combined with a
    Platform-Engineer/Kubernetes profile re-ranks a "Platform Engineer"
    posting to #1 that didn't appear in the query-only top 5 — confirming
    profile context actually changes ranking, not just query text.

  **Real deviation discovered (Free Edition-specific, same spirit as
  Phase 2's list below):**
  - **The embeddings endpoint hangs (not errors) on batches of ~20+
    inputs.** First backfill run: every batch of 20 texts sent to
    `w.serving_endpoints.query()` printed
    `Timed out after 0:05:00` — the SDK's ~5-minute default timeout, not a
    fast failure. Local diagnostic (bypassing the notebook, calling the
    same endpoint directly) isolated it precisely: batches of 6/8/10/12/15
    items all returned in under a second regardless of text length (even
    the shortest possible text), but a batch of 20 — including 20
    *identical* short strings — hung past 30s every time. This is a hard
    per-request item-count limit on this Free Edition Foundation Model API
    endpoint, not a payload-size or text-length issue. Fixed by dropping
    `batch_size` to 10 (safe margin below the observed 15-works/20-hangs
    boundary) and wrapping each `query()` call in a
    `concurrent.futures` 30-second timeout so a future recurrence fails one
    batch in seconds instead of stalling the whole job for up to 5 minutes
    per batch. Also switched from committing once at the very end to
    committing after every batch, so a partial run leaves real progress
    instead of an opaque zero-rows-updated state. Backfill of all 388
    existing rows completed in ~27 seconds after the fix.

## Phase 4 — Databricks App (Flask frontend)

Mirror the structure of `lakebase-support-app`:

- [x] `app.py` — routes for: profile setup, job search/browse (calls the
      retrieval function), pipeline board (kanban-style view by stage),
      posting detail with notes/contacts.
- [x] `templates/` + `static/style.css` — UI, consistent with the
      dark-mode styling used previously.
- [x] `app.yaml` — runtime config (PGHOST, PGUSER, PGDATABASE,
      ENDPOINT_NAME, FLASK_SECRET_KEY, any vector-search endpoint env
      vars).
- [x] Reuse the OAuth-token-rotation connection pattern from
      `lakebase-support-app/app.py` — don't use a static Postgres
      password.

  **Scope decisions:**
  - **No login/multi-user auth.** The schema supports multiple `users`,
    but only one is seeded (`user_id=1`, Jane Doe) and the reference app
    has no auth either. Built as a single-user personal copilot
    (`DEFAULT_USER_ID = 1` in `app.py`) rather than adding a login system.
  - **Chat-with-agent view deferred to Phase 5**, not built here despite
    being in this phase's original list above. Phase 5 is explicitly
    "wire the agent into the Flask app (a chat endpoint/view)" — building
    a chat page now with no agent behind it would be throwaway UI once
    Phase 5 adds real tool-calling, and CLAUDE.md's "Don't" section rules
    out scaffolding agent code ahead of its phase.

  **Implementation:** connection pool + `OAuthConnection` class mirror
  `lakebase-support-app/app.py` exactly, except `psycopg[pool]` (no
  `[binary]` extra) per this workspace's Free Edition psycopg gotcha.
  Retrieval reuses Phase 3's pgvector approach directly in `app.py`
  (`embed()`/`profile_context()`/cosine-similarity SQL), not imported from
  `scripts/search_postings.py` — matches this repo's existing convention
  of each component defining its own small pieces inline rather than
  cross-file imports. `app.yaml` has placeholder values only; filling in
  real values and actually deploying the App is Phase 6's job, not this
  one. Verified locally: ran `flask run` with real (uncommitted) env vars
  against the live Lakebase instance and exercised every route/write path
  with curl — profile edit + validation, skill add/remove, search (plain
  and profile-combined), save-to-pipeline, stage transitions, notes,
  contacts, and the kanban board all confirmed against real data. No
  browser tool was available this session, so CSS/layout rendering was
  not visually confirmed, only functional behavior via HTTP requests.

## Phase 5 — AI agent

- [x] Define read tools: `search_postings(query, remote_only)`,
      `get_posting(posting_id)`, `get_pipeline()`, `get_notes(posting_id)`.
- [x] Define write tools: `save_posting(posting_id)`,
      `update_stage(posting_id, stage, confirm)`,
      `add_interview_note(posting_id, note_text, follow_up_date)`,
      `draft_cover_letter_snippet(posting_id)` (generation
      only — doesn't write unless the user explicitly saves the draft).
- [x] Wire the agent into the Flask app (a chat endpoint/view) so it can
      call these tools against Lakebase in response to natural-language
      requests.
- [x] Add a "surface stale applications" capability — e.g. a scheduled
      check or an on-demand agent query for applications with no
      `last_updated_at` change in N days.
- [x] Sanity-check: every write tool validates its inputs the same way
      the Flask routes do (stage whitelisting, required fields).

  **Real deviation discovered: `databricks-sdk`'s typed
  `serving_endpoints.query()` has no tool-calling support at all.**
  Inspecting `serving.py` in the installed SDK (0.125.0) turned up zero
  references to "tool" anywhere — the method's only escape hatch,
  `extra_params`, is typed `Dict[str, str]`, which can't carry a JSON tool
  schema. Foundation Model API chat endpoints are OpenAI-compatible over
  plain REST (`POST {host}/serving-endpoints/{name}/invocations`), so
  `agent.py`'s `query_chat()` calls that endpoint directly via `requests`
  (already a dependency), authenticated with
  `WorkspaceClient().config.authenticate()` headers — the same pattern
  every other component in this repo uses for its own `WorkspaceClient()`
  instance, just applied to a raw HTTP call instead of a typed SDK method.

  This workspace's Foundation Model API has no `databricks-claude-*`
  endpoint — only GPT-OSS-120b/20b, Qwen3-Next-80B, Qwen3.5-122B,
  Llama-4-Maverick, Llama-3.1-8B, Llama-3.3-70B, and Gemma-3-12B chat
  models. `scripts/verify_agent_tools.py` sent a trivial dummy tool to four
  candidates before committing to one; all four (Llama-3.3-70B,
  Llama-4-Maverick, GPT-OSS-120b, Qwen3-Next-80B) returned real
  `tool_calls` rather than answering in prose. Picked
  `databricks-meta-llama-3-3-70b-instruct` as `AGENT_MODEL` — Databricks'
  best-documented FMAPI function-calling model, and the cleanest/most
  consistent JSON tool-call arguments of the four in the smoke test.

  **Implementation:** `agent.py` holds the model-facing, DB-agnostic half —
  `TOOL_SCHEMAS` (OpenAI-format tool definitions), `SYSTEM_PROMPT`,
  `query_chat()` (the direct REST call, wrapped in a 30s timeout per the
  Phase 3 gotcha that FMAPI endpoints hang rather than error), and
  `run_agent_turn()` (the tool-calling loop, capped at 6 iterations). Tool
  *implementations* live in `app.py` next to the Lakebase helpers they
  reuse (`get_conn`, `embed`, `profile_context`, `search_postings`,
  `fetch_posting`, etc.) — putting them in `agent.py` instead would have
  created a circular import, since `app.py` already owns all Lakebase
  access. `update_stage_tool` requires `confirm="REJECT"` before moving a
  stage to `rejected`, matching CLAUDE.md's requirement that destructive-ish
  writes mirror the reference app's DELETE-to-confirm pattern; the model is
  instructed via `SYSTEM_PROMPT` to ask the user in plain language before
  ever passing that value. `draft_cover_letter_snippet` is read-only by
  design — it hands the model full posting + profile context and lets the
  model compose the draft in its reply; if the user wants it kept, the
  model calls `add_interview_note` instead of a new drafts table (avoids a
  schema change for something the existing tables already cover).
  Chat history is a simple **module-level in-memory list** of OpenAI-format
  messages, matching the rest of the app's no-JS, plain-form-POST style
  (`GET /chat`, `POST /chat`, `POST /chat/clear`) — acceptable for a
  single-user app with no login, with the noted tradeoff that history is
  lost on process restart and not safe across multiple worker processes.
  `get_stale_applications` satisfies the "surface stale applications" item
  as an on-demand agent query (applications not in `rejected`/`offer` with
  `last_updated_at` older than N days, default 14) rather than new
  scheduling infrastructure.

  **Verified against the live instance** via `flask run` + curl against
  `/chat`: a profile-matched search, `get_posting` lookup, `save_posting`,
  a `update_stage` reject attempt correctly refused without confirmation
  and then correctly applied after confirming, `add_interview_note`
  chained with `get_stale_applications` in a single turn, and
  `draft_cover_letter_snippet` — all confirmed against real data via
  `/pipeline` and `/postings/<id>` afterward (posting #1 now `rejected`,
  posting #2 has the test interview note). No browser tool was available
  this session either, so `templates/chat.html`'s CSS/layout wasn't
  visually confirmed, only functional behavior via HTTP requests (same
  caveat as Phase 4).

## Phase 6 — Deploy & verify

- [ ] Fill in real `app.yaml` values locally only; keep placeholders in
      anything committed.
- [ ] Deploy via the Apps UI (Git-connected) or `databricks apps deploy`,
      following whichever path matches the workspace being used.
- [ ] Verify against the bootcamp rubric: Spark pipeline runs and
      populates Lakebase, third-party API integration works end to end,
      embeddings + semantic search return sensible results, the app
      frontend is usable, and the agent can both retrieve and write.
- [ ] Before submitting: scan the repo for committed secrets (API keys,
      `PGHOST`/`PGUSER`/`ENDPOINT_NAME`, service-principal UUIDs) and
      scrub them — this tripped up the `lakebase-support-app` submission
      previously.
