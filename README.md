# AI Job Hunting Copilot

Capstone project for the "Rise of the AI Data Engineer" bootcamp. A Databricks
App (Flask) backed by Lakebase Postgres and Databricks Vector Search, with an
AI agent that helps a job seeker find matching postings, tailor application
materials, and track their pipeline end to end.

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
- **Embeddings / vector search** — job descriptions (and the user's
  skills/resume profile) are embedded for semantic matching, e.g. "remote
  backend roles that don't require 5+ years of Kubernetes experience."
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

Planning phase — see [IMPLEMENTATION.md](IMPLEMENTATION.md) for current
progress and next steps. No app code has been written yet.

## Bootcamp requirements checklist

- [ ] Spark data pipeline
- [ ] Third-party API integration (Adzuna / USAJobs / RemoteOK)
- [ ] Unstructured data processing (job description text embeddings)
- [ ] Databricks App with a frontend
- [ ] AI agent with read + write tools
