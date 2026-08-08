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

- [ ] Write a Spark job that calls Adzuna / USAJobs / RemoteOK, normalizes
      each source's response into the `job_postings` schema, dedupes on
      `(external_source, external_id)`, and upserts into Lakebase.
- [ ] Run it as a Databricks Job (scheduled, e.g. every few hours) rather
      than a one-off script — this is the "data pipeline in Spark"
      requirement, so it needs to be a repeatable job, not a notebook run
      once by hand.
- [ ] Log row counts fetched/inserted/updated per run for a sanity check.
- [ ] **Design finalized (not yet implemented)** — see full plan at
      `/Users/bryandonnelly/.claude/plans/fizzy-giggling-peacock.md`.
      Summary:
    - Fetch raw JSON from the 3 APIs on the driver, build a Spark
      DataFrame (explicit `StructType`) from the normalized records, use
      real `.dropDuplicates(["external_source","external_id"])` +
      `.count()` — genuine Spark processing, not just code that happens to
      run on a cluster.
    - `.collect()` the deduped DataFrame and upsert via a single
      OAuth-rotated `psycopg` (v3) connection:
      `INSERT ... ON CONFLICT (external_source, external_id) DO UPDATE
      ... RETURNING (xmax = 0) AS inserted` to count inserted vs. updated
      in one pass; `DO UPDATE` explicitly refreshes `fetched_at`.
    - OAuth-token-rotation auth (per CLAUDE.md, pattern from
      `lakebase-support-app/app.py:16-46`), defined inline in the
      notebook — no shared `ingestion/` package (Databricks Repos
      cross-file imports are path-fragile; the class is ~15 lines, fine
      to duplicate later in Phase 4's Flask app).
    - Everything self-contained in `notebooks/ingest_job_postings.py`
      (widgets, secret reads via `dbutils.secrets.get`, 3 fetch/normalize
      functions, OAuthConnection, Spark dedupe, upsert+logging).
    - Deployed via a Databricks Asset Bundle: `databricks.yml` +
      `resources/ingest_job_postings_job.yml` — cron every 6h
      (`"0 0 0/6 * * ?"`), `pause_status: PAUSED` until a manual run is
      validated, single-node job cluster (`num_workers: 0`),
      `email_notifications.on_failure`.
    - `PGHOST`/`PGUSER`/`PGDATABASE`/`ENDPOINT_NAME` supplied as DAB
      `variables:` (no committed defaults) via local `BUNDLE_VAR_*` env
      vars — never committed. Secret scope `job-copilot` (already set up)
      covers the Adzuna/USAJobs API keys.
    - Each fetch function wrapped in its own try/except (return `[]` on
      failure, log a warning) so one flaky free API doesn't kill the run;
      raise if *all three* return zero rows so failure alerting fires.
    - RemoteOK needs a descriptive `User-Agent` header (bare UA → 403)
      and skips response element 0 (legal notice, not a posting).
      USAJobs needs `Host`/`User-Agent`/`Authorization-Key` headers (not
      a bearer token) — exact `PositionLocation`/`PositionRemuneration`
      nesting still needs confirming against a live response.
    - New files planned: `databricks.yml`, `resources/
      ingest_job_postings_job.yml`, `notebooks/ingest_job_postings.py`,
      `requirements.txt` (local-dev only, just `requests`),
      `scripts/verify_fetchers.py` (local smoke-test harness for the 3
      fetch/normalize functions against real `.env` keys, before wiring
      into the notebook). Also add `.databricks/` to `.gitignore`.
    - Full field-by-field normalization mapping (Adzuna/USAJobs/RemoteOK
      → `job_postings` columns) and the verification plan (local
      smoke-test → deploy → manual run → Lakebase sanity queries
      including a second run to prove the upsert path) are in the plan
      file above — read it before resuming implementation.

## Phase 3 — Embeddings / semantic retrieval

- [ ] Pick an embedding model available in the workspace (e.g. a
      Databricks-hosted embedding endpoint).
- [ ] Embed `job_postings.description` (and optionally title + company)
      into a vector index (Databricks Vector Search, synced from the
      Lakebase table or a Delta table mirroring it).
- [ ] Embed each user's `profiles` skills/resume summary the same way, so
      queries like "remote backend roles that don't require 5+ years of
      Kubernetes" can match against both the query text and the profile
      context.
- [ ] Write a retrieval function: given a query (+ optional profile_id),
      return the top-N matching postings with scores.

## Phase 4 — Databricks App (Flask frontend)

Mirror the structure of `lakebase-support-app`:

- [ ] `app.py` — routes for: profile setup, job search/browse (calls the
      retrieval function), pipeline board (kanban-style view by stage),
      posting detail with notes/contacts, chat-with-agent view.
- [ ] `templates/` + `static/style.css` — UI, consistent with the
      dark-mode styling used previously.
- [ ] `app.yaml` — runtime config (PGHOST, PGUSER, PGDATABASE,
      ENDPOINT_NAME, FLASK_SECRET_KEY, any vector-search endpoint env
      vars).
- [ ] Reuse the OAuth-token-rotation connection pattern from
      `lakebase-support-app/app.py` — don't use a static Postgres
      password.

## Phase 5 — AI agent

- [ ] Define read tools: `search_postings(query, profile_id)`,
      `get_posting(id)`, `get_pipeline(user_id)`, `get_notes(application_id)`.
- [ ] Define write tools: `save_posting(user_id, posting_id)`,
      `update_stage(application_id, stage)`,
      `add_interview_note(application_id, text, follow_up_date)`,
      `draft_cover_letter_snippet(posting_id, profile_id)` (generation
      only — doesn't write unless the user explicitly saves the draft).
- [ ] Wire the agent into the Flask app (a chat endpoint/view) so it can
      call these tools against Lakebase in response to natural-language
      requests.
- [ ] Add a "surface stale applications" capability — e.g. a scheduled
      check or an on-demand agent query for applications with no
      `last_updated_at` change in N days.
- [ ] Sanity-check: every write tool validates its inputs the same way
      the Flask routes do (stage whitelisting, required fields).

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
