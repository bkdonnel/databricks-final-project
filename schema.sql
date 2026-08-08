-- =========================================================
-- AI Job Hunting Copilot: schema + auth + sample data
-- Run this in the Lakebase SQL Editor (against databricks_postgres)
-- =========================================================

-- Enable OAuth auth extension (lets Databricks tokens act as Postgres passwords)
CREATE EXTENSION IF NOT EXISTS databricks_auth;

-- Enable pgvector for semantic retrieval (Phase 3). Chosen over the
-- standalone Databricks Vector Search product to avoid mirroring
-- job_postings into a separate Unity Catalog Delta table just to feed an
-- index -- Lakebase Postgres stays the single source of truth, and
-- similarity search is plain SQL via the <=> operator.
CREATE EXTENSION IF NOT EXISTS vector;

-- Create a Postgres role for the app's service principal
-- (from the Databricks App's Environment tab -> DATABRICKS_CLIENT_ID).
SELECT databricks_create_role('<DATABRICKS_CLIENT_ID>', 'service_principal');

GRANT CONNECT ON DATABASE databricks_postgres TO "<DATABRICKS_CLIENT_ID>";
GRANT CREATE, USAGE ON SCHEMA public TO "<DATABRICKS_CLIENT_ID>";

-- =========================================================
-- Tables
-- =========================================================

CREATE TABLE IF NOT EXISTS users (
    user_id       SERIAL PRIMARY KEY,
    email         TEXT NOT NULL UNIQUE,
    display_name  TEXT NOT NULL,
    created_at    TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- One profile per user for now (target roles / preferences / resume
-- summary used both for display and as the text embedded for semantic
-- matching in Phase 3).
CREATE TABLE IF NOT EXISTS profiles (
    profile_id           SERIAL PRIMARY KEY,
    user_id               INTEGER NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
    target_roles          TEXT,
    location_preference   TEXT,
    remote_preference     TEXT NOT NULL DEFAULT 'any'
                              CHECK (remote_preference IN ('remote', 'hybrid', 'onsite', 'any')),
    resume_summary        TEXT,
    notes                 TEXT,
    created_at             TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at             TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (user_id)
);

CREATE TABLE IF NOT EXISTS skills (
    skill_id      SERIAL PRIMARY KEY,
    profile_id    INTEGER NOT NULL REFERENCES profiles(profile_id) ON DELETE CASCADE,
    skill_name    TEXT NOT NULL,
    proficiency   TEXT NOT NULL DEFAULT 'intermediate'
                      CHECK (proficiency IN ('beginner', 'intermediate', 'advanced', 'expert')),
    years_experience  NUMERIC(4,1)
);

-- Job postings ingested by the Spark pipeline (Phase 2). external_source +
-- external_id together dedupe across pipeline runs.
CREATE TABLE IF NOT EXISTS job_postings (
    posting_id        SERIAL PRIMARY KEY,
    external_source   TEXT NOT NULL,
    external_id       TEXT NOT NULL,
    title             TEXT NOT NULL,
    company           TEXT,
    location          TEXT,
    remote_flag       BOOLEAN NOT NULL DEFAULT FALSE,
    salary_min        INTEGER,
    salary_max        INTEGER,
    description       TEXT,
    url               TEXT,
    posted_at         TIMESTAMP,
    fetched_at        TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    -- Embedding of title + description (Phase 2's embed_job_postings task),
    -- 1024 dims to match the databricks-gte-large-en endpoint. embedded_at
    -- lets that task find rows that are new or have been re-fetched since
    -- their last embedding (embedded_at IS NULL OR embedded_at < fetched_at).
    embedding         vector(1024),
    embedded_at       TIMESTAMP,
    UNIQUE (external_source, external_id)
);

CREATE INDEX IF NOT EXISTS idx_job_postings_posted_at ON job_postings(posted_at DESC);

-- HNSW cosine-distance index for nearest-neighbor search over embeddings
-- (pgvector >=0.5.0; confirmed 0.8.0 on this Lakebase instance).
CREATE INDEX IF NOT EXISTS idx_job_postings_embedding
    ON job_postings USING hnsw (embedding vector_cosine_ops);

-- Tracks a user's pipeline for a posting. There is no separate
-- "saved_jobs" table -- saving a posting is just creating an
-- application row with stage='saved' (decided during Phase 1: keeping
-- one table avoids duplicating (user_id, job_posting_id) uniqueness and
-- stage-transition logic across two tables).
CREATE TABLE IF NOT EXISTS applications (
    application_id    SERIAL PRIMARY KEY,
    user_id            INTEGER NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
    job_posting_id     INTEGER NOT NULL REFERENCES job_postings(posting_id) ON DELETE CASCADE,
    stage              TEXT NOT NULL DEFAULT 'saved'
                           CHECK (stage IN ('saved', 'applied', 'interviewing', 'rejected', 'offer')),
    applied_at         TIMESTAMP,
    last_updated_at    TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    created_at         TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (user_id, job_posting_id)
);

CREATE INDEX IF NOT EXISTS idx_applications_user_id ON applications(user_id);
CREATE INDEX IF NOT EXISTS idx_applications_stage ON applications(stage);
CREATE INDEX IF NOT EXISTS idx_applications_last_updated_at ON applications(last_updated_at);

CREATE TABLE IF NOT EXISTS interview_notes (
    note_id           SERIAL PRIMARY KEY,
    application_id    INTEGER NOT NULL REFERENCES applications(application_id) ON DELETE CASCADE,
    note_text         TEXT NOT NULL,
    follow_up_date    DATE,
    author            TEXT NOT NULL,
    created_at        TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_interview_notes_application_id ON interview_notes(application_id);

CREATE TABLE IF NOT EXISTS contacts (
    contact_id        SERIAL PRIMARY KEY,
    application_id    INTEGER NOT NULL REFERENCES applications(application_id) ON DELETE CASCADE,
    name              TEXT NOT NULL,
    role              TEXT,
    email             TEXT,
    linkedin_url      TEXT,
    notes             TEXT,
    created_at        TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_contacts_application_id ON contacts(application_id);

-- Service principals don't inherit default schema perms - grant explicitly.
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE users TO "<DATABRICKS_CLIENT_ID>";
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE profiles TO "<DATABRICKS_CLIENT_ID>";
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE skills TO "<DATABRICKS_CLIENT_ID>";
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE job_postings TO "<DATABRICKS_CLIENT_ID>";
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE applications TO "<DATABRICKS_CLIENT_ID>";
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE interview_notes TO "<DATABRICKS_CLIENT_ID>";
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE contacts TO "<DATABRICKS_CLIENT_ID>";
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO "<DATABRICKS_CLIENT_ID>";

-- =========================================================
-- Sample data (for local testing before the Phase 2 pipeline is wired up)
-- =========================================================

INSERT INTO users (email, display_name) VALUES
    ('jane.doe@example.com', 'Jane Doe');

INSERT INTO profiles (user_id, target_roles, location_preference, remote_preference, resume_summary, notes) VALUES
    (1, 'Backend Engineer, Platform Engineer', 'Seattle, WA', 'remote',
     'Backend engineer with 5 years building Python/Go services on AWS. Interested in data-heavy platform roles.',
     'Prefer teams that use async work over meeting-heavy cultures.');

INSERT INTO skills (profile_id, skill_name, proficiency, years_experience) VALUES
    (1, 'Python', 'expert', 5.0),
    (1, 'PostgreSQL', 'advanced', 4.0),
    (1, 'Kubernetes', 'intermediate', 2.0),
    (1, 'Go', 'intermediate', 1.5);

INSERT INTO job_postings (external_source, external_id, title, company, location, remote_flag, salary_min, salary_max, description, url, posted_at) VALUES
    ('adzuna', 'sample-1', 'Senior Backend Engineer', 'Northwind Data',
     'Remote (US)', TRUE, 140000, 175000,
     'We are looking for a senior backend engineer to build data pipelines and APIs in Python. Experience with PostgreSQL and cloud infrastructure required. No Kubernetes expertise necessary, we use managed services.',
     'https://example.com/jobs/sample-1', CURRENT_TIMESTAMP - INTERVAL '2 days'),
    ('remoteok', 'sample-2', 'Platform Engineer', 'Fictional Robotics',
     'Remote (Worldwide)', TRUE, 130000, 160000,
     'Platform team seeks an engineer comfortable with Go, Kubernetes, and building internal developer tooling. 3+ years Kubernetes experience strongly preferred.',
     'https://example.com/jobs/sample-2', CURRENT_TIMESTAMP - INTERVAL '5 days'),
    ('usajobs', 'sample-3', 'IT Specialist (Applications)', 'Dept. of Example Affairs',
     'Washington, DC', FALSE, 95000, 120000,
     'Federal IT Specialist role supporting internal applications built on Python and PostgreSQL. On-site required, security clearance eligibility needed.',
     'https://example.com/jobs/sample-3', CURRENT_TIMESTAMP - INTERVAL '10 days');

INSERT INTO applications (user_id, job_posting_id, stage, applied_at) VALUES
    (1, 1, 'applied', CURRENT_TIMESTAMP - INTERVAL '1 day'),
    (1, 2, 'saved', NULL);

INSERT INTO interview_notes (application_id, note_text, follow_up_date, author) VALUES
    (1, 'Phone screen scheduled with recruiter for next week.', CURRENT_DATE + INTERVAL '7 days', 'Jane Doe');

INSERT INTO contacts (application_id, name, role, email, linkedin_url) VALUES
    (1, 'Alex Rivera', 'Technical Recruiter', 'alex.rivera@example.com', 'https://linkedin.com/in/example');
