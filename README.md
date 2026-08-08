# AI Job Hunting Copilot


### Link to App:
https://bootcamp-job-search-agent-7474658268863295.aws.databricksapps.com/postings

Capstone project for the "Rise of the AI Data Engineer" bootcamp. A Databricks
App (Flask) backed by Lakebase Postgres (with `pgvector` for semantic search),
with an AI agent that helps a job seeker find matching postings, tailor
application materials, and track their pipeline end to end.

Users describe their skills, target roles, and preferences. The agent
searches live job postings pulled in via a Spark pipeline, explains why a
posting is (or isn't) a good match using semantic retrieval over the job
description text, and can take real actions — saving postings, moving them
through pipeline stages, drafting tailored materials, and logging interview
notes — directly against Lakebase.

## Architecture

- **Spark pipeline** — pulls job postings from third-party APIs on a
  schedule, normalizes them, and writes them into Lakebase.
- **Lakebase Postgres** — relational store for users, profiles, postings,
  applications, and notes.
- **Embeddings / vector search** — job description text is embedded (via a
  Databricks Foundation Model API endpoint) into a `pgvector` column on
  `job_postings` in Lakebase itself, with an HNSW index for cosine-distance
  search — no separate Vector Search service or Delta table mirror needed.
  A search query is combined with the user's skills/resume profile text
  before embedding, so retrieval matches on both, e.g. "remote backend
  roles that don't require 5+ years of Kubernetes experience."
- **Databricks App (Flask)** — frontend for browsing postings, managing the
  pipeline board, and chatting with the agent.
- **AI agent** — tools for search/retrieval (read) and for saving postings,
  updating pipeline stage, drafting materials, and logging notes (write).

See [IMPLEMENTATION.md](IMPLEMENTATION.md) for the build plan and
[CLAUDE.md](CLAUDE.md) for repo conventions.

## Third-party APIs

- [Adzuna API](https://developer.adzuna.com/) — live job listings by
  keyword, location, category, salary. Free tier, requires an API key.
- [USAJobs Search API](https://developer.usajobs.gov/) — official U.S.
  federal job openings. Free, requires a key + registered email.
- [RemoteOK API](https://remoteok.com/api) — remote job listings. Free, no
  key required.

## Lakebase tables

`users`, `profiles`, `skills`, `job_postings`, `applications`, `saved_jobs`,
`interview_notes`, `contacts`

## Agent capabilities

- Search and rank job postings against a user's profile and preferences.
- Explain why a posting is or isn't a good match.
- Save a posting to a pipeline stage (saved, applied, interviewing,
  rejected, offer).
- Draft a tailored cover-letter snippet or resume bullet for a posting.
- Track interview notes and follow-up dates for saved applications.
- Surface stale applications that haven't been updated in a while.

## Status

All phases complete (setup, schema, Spark ingestion pipeline, semantic
embeddings/retrieval, Flask frontend, AI agent with tools, deployment) —
see [IMPLEMENTATION.md](IMPLEMENTATION.md) for full details, including
real deviations discovered along the way. The Flask app is a single-user
personal copilot for now (no login system). The agent is wired into a
`/chat` view and can both retrieve and write against Lakebase. Deployed
and verified live as a Databricks App.

## Bootcamp requirements checklist

- [x] Spark data pipeline
- [x] Third-party API integration (Adzuna / USAJobs / RemoteOK)
- [x] Unstructured data processing (job description text embeddings)
- [x] Databricks App with a frontend
- [x] AI agent with read + write tools
