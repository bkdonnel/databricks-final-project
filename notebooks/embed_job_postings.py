# Databricks notebook source
# MAGIC %md
# MAGIC # Embed Job Postings
# MAGIC
# MAGIC Finds `job_postings` rows that are new or have been re-fetched since
# MAGIC their last embedding (`embedding IS NULL OR embedded_at < fetched_at`),
# MAGIC embeds `title + description` via a Databricks Foundation Model API
# MAGIC embeddings endpoint, and writes the vectors back into the `embedding`
# MAGIC (pgvector) column for semantic retrieval (Phase 3).
# MAGIC
# MAGIC Runs as the `embed_job_postings` task in `ingest_job_postings_job`,
# MAGIC after the `ingest_job_postings` task, so every 6h cycle both ingests new
# MAGIC postings and embeds anything that needs it. Self-contained — no imports
# MAGIC from other repo files (Databricks Repos cross-file notebook imports are
# MAGIC path-fragile).

# COMMAND ----------

# MAGIC %pip uninstall -y psycopg2 psycopg2-binary
# MAGIC %pip install -q --upgrade "psycopg" "databricks-sdk>=0.40.0"
dbutils.library.restartPython()

# COMMAND ----------

dbutils.widgets.text("pg_host", "")
dbutils.widgets.text("pg_user", "")
dbutils.widgets.text("pg_database", "")
dbutils.widgets.text("endpoint_name", "")
dbutils.widgets.text("embedding_model", "databricks-gte-large-en")
# 20+ items per query() call hangs against this Free Edition endpoint --
# confirmed via local diagnostic: batches up to 15 (even 20 identical short
# strings) return in <1s, but 20 hangs past 30s regardless of text length.
# 10 keeps a safe margin below that threshold.
dbutils.widgets.text("batch_size", "10")
dbutils.widgets.text("description_max_chars", "2000")

PG_HOST = dbutils.widgets.get("pg_host")
PG_USER = dbutils.widgets.get("pg_user")
PG_DATABASE = dbutils.widgets.get("pg_database")
ENDPOINT_NAME = dbutils.widgets.get("endpoint_name")
EMBEDDING_MODEL = dbutils.widgets.get("embedding_model")
BATCH_SIZE = int(dbutils.widgets.get("batch_size"))
DESCRIPTION_MAX_CHARS = int(dbutils.widgets.get("description_max_chars"))

PG_PORT = "5432"
PG_SSLMODE = "require"

assert PG_HOST and PG_USER and PG_DATABASE and ENDPOINT_NAME, (
    "pg_host, pg_user, pg_database, and endpoint_name widgets are required"
)

# COMMAND ----------

import concurrent.futures

import psycopg
from databricks.sdk import WorkspaceClient

# Bounds a single query() call so a hang (see batch_size note above) fails
# in seconds instead of the SDK's ~5-minute default, leaving the row a
# candidate for the next run instead of stalling the whole job.
QUERY_TIMEOUT_SECONDS = 30

# ---------------------------------------------------------------------------
# Lakebase connection (OAuth token rotation pattern per CLAUDE.md, matching
# lakebase-support-app/app.py:16-46 and notebooks/ingest_job_postings.py —
# never a static Postgres password).
# ---------------------------------------------------------------------------
w = WorkspaceClient()


class OAuthConnection(psycopg.Connection):
    """psycopg connection that fetches a fresh Databricks OAuth token on
    every new connection instead of using a static password."""

    @classmethod
    def connect(cls, conninfo="", **kwargs):
        credential = w.postgres.generate_database_credential(endpoint=ENDPOINT_NAME)
        kwargs["password"] = credential.token
        return super().connect(conninfo, **kwargs)


PG_CONNINFO = (
    f"dbname={PG_DATABASE} user={PG_USER} host={PG_HOST} "
    f"port={PG_PORT} sslmode={PG_SSLMODE}"
)

# COMMAND ----------

# ---------------------------------------------------------------------------
# Fetch candidates: never embedded, or fetched again since the last embed.
# ---------------------------------------------------------------------------
SELECT_CANDIDATES_SQL = """
    SELECT posting_id, title, description
    FROM job_postings
    WHERE embedding IS NULL OR embedded_at < fetched_at
    ORDER BY posting_id
"""

with OAuthConnection.connect(PG_CONNINFO) as conn:
    with conn.cursor() as cur:
        cur.execute(SELECT_CANDIDATES_SQL)
        candidates = cur.fetchall()

candidate_count = len(candidates)
print(f"candidates={candidate_count} rows need embedding")

if candidate_count == 0:
    dbutils.notebook.exit("no candidates to embed")

# COMMAND ----------


# ---------------------------------------------------------------------------
# Embed in batches via the Foundation Model API embeddings endpoint. Each
# batch is wrapped in its own try/except so one bad batch doesn't drop
# everything else -- failed rows simply stay candidates for the next run
# (embedded_at is left unset).
# ---------------------------------------------------------------------------
def embedding_text(title, description):
    title = title or ""
    description = (description or "")[:DESCRIPTION_MAX_CHARS]
    return f"{title}. {description}".strip()


def to_pgvector_literal(vector):
    return "[" + ",".join(repr(float(x)) for x in vector) + "]"


UPDATE_SQL = """
    UPDATE job_postings
    SET embedding = %(embedding)s::vector, embedded_at = now()
    WHERE posting_id = %(posting_id)s
"""

embedded_count = 0
failed_count = 0

# One connection for the whole run, but commit after every batch (not once
# at the end) -- gives real incremental progress in the DB and means a
# hang/failure partway through doesn't lose already-embedded rows.
with OAuthConnection.connect(PG_CONNINFO) as conn:
    with conn.cursor() as cur:
        for batch_start in range(0, candidate_count, BATCH_SIZE):
            batch = candidates[batch_start : batch_start + BATCH_SIZE]
            texts = [embedding_text(title, description) for _, title, description in batch]
            try:
                with concurrent.futures.ThreadPoolExecutor(max_workers=1) as ex:
                    future = ex.submit(
                        w.serving_endpoints.query, name=EMBEDDING_MODEL, input=texts
                    )
                    response = future.result(timeout=QUERY_TIMEOUT_SECONDS)
                vectors = [item.embedding for item in response.data]
            except Exception as e:
                failed_count += len(batch)
                print(f"WARNING: embedding batch starting at {batch_start} failed: {e}")
                continue

            for (posting_id, _, _), vector in zip(batch, vectors):
                cur.execute(
                    UPDATE_SQL,
                    {"embedding": to_pgvector_literal(vector), "posting_id": posting_id},
                )
                embedded_count += 1
            conn.commit()
            print(f"progress: {embedded_count}/{candidate_count} embedded so far")

print(
    f"candidates={candidate_count} embedded={embedded_count} failed={failed_count}"
)
