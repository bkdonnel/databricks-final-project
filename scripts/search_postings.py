"""
Local verification harness for Phase 3 semantic retrieval. Embeds a query
(optionally combined with a user's profile context) via the same Foundation
Model embeddings endpoint used by notebooks/embed_job_postings.py, then ranks
job_postings by pgvector cosine similarity.

Requires PG_HOST, PG_USER, PG_DATABASE, ENDPOINT_NAME env vars -- the same
non-secret connection identifiers passed to the Databricks Job (see
databricks.yml) -- and a Databricks CLI auth profile/env config recognized by
WorkspaceClient() (e.g. DATABRICKS_CONFIG_PROFILE).

Usage:
    PG_HOST=... PG_USER=... PG_DATABASE=... ENDPOINT_NAME=... \
        python scripts/search_postings.py "remote backend roles" --profile-id 1 --top-n 5
"""
import argparse
import os

import psycopg
from databricks.sdk import WorkspaceClient

EMBEDDING_MODEL = "databricks-gte-large-en"

w = WorkspaceClient()


def env_or_die(name):
    value = os.environ.get(name)
    if not value:
        raise SystemExit(f"missing required env var: {name}")
    return value


PG_HOST = env_or_die("PG_HOST")
PG_USER = env_or_die("PG_USER")
PG_DATABASE = env_or_die("PG_DATABASE")
ENDPOINT_NAME = env_or_die("ENDPOINT_NAME")


class OAuthConnection(psycopg.Connection):
    """psycopg connection that fetches a fresh Databricks OAuth token on
    every new connection instead of using a static password."""

    @classmethod
    def connect(cls, conninfo="", **kwargs):
        credential = w.postgres.generate_database_credential(endpoint=ENDPOINT_NAME)
        kwargs["password"] = credential.token
        return super().connect(conninfo, **kwargs)


PG_CONNINFO = (
    f"dbname={PG_DATABASE} user={PG_USER} host={PG_HOST} port=5432 sslmode=require"
)


def embed(text):
    response = w.serving_endpoints.query(name=EMBEDDING_MODEL, input=[text])
    return response.data[0].embedding


def to_pgvector_literal(vector):
    return "[" + ",".join(repr(float(x)) for x in vector) + "]"


def profile_context(cur, profile_id):
    cur.execute(
        """
        SELECT target_roles, location_preference, remote_preference,
               resume_summary, notes
        FROM profiles WHERE profile_id = %(profile_id)s
        """,
        {"profile_id": profile_id},
    )
    row = cur.fetchone()
    if not row:
        raise SystemExit(f"no profile with profile_id={profile_id}")
    target_roles, location_preference, remote_preference, resume_summary, notes = row

    cur.execute(
        "SELECT skill_name FROM skills WHERE profile_id = %(profile_id)s",
        {"profile_id": profile_id},
    )
    skills = [r[0] for r in cur.fetchall()]

    parts = [
        target_roles,
        f"remote preference: {remote_preference}",
        location_preference,
        resume_summary,
        notes,
        "skills: " + ", ".join(skills) if skills else None,
    ]
    return ". ".join(p for p in parts if p)


SEARCH_SQL = """
    SELECT posting_id, title, company, location, remote_flag,
           salary_min, salary_max, url,
           1 - (embedding <=> %(query_embedding)s::vector) AS similarity
    FROM job_postings
    WHERE embedding IS NOT NULL
    ORDER BY embedding <=> %(query_embedding)s::vector
    LIMIT %(top_n)s
"""


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("query", help="natural-language search query")
    parser.add_argument("--profile-id", type=int, default=None)
    parser.add_argument("--top-n", type=int, default=5)
    args = parser.parse_args()

    with OAuthConnection.connect(PG_CONNINFO) as conn:
        with conn.cursor() as cur:
            query_text = args.query
            if args.profile_id is not None:
                context = profile_context(cur, args.profile_id)
                query_text = f"{args.query}. {context}"
                print(f"combined query text: {query_text}\n")

            query_embedding = embed(query_text)

            cur.execute(
                SEARCH_SQL,
                {
                    "query_embedding": to_pgvector_literal(query_embedding),
                    "top_n": args.top_n,
                },
            )
            rows = cur.fetchall()

    if not rows:
        print("no embedded postings found -- has embed_job_postings run yet?")
        return

    for (
        posting_id,
        title,
        company,
        location,
        remote_flag,
        salary_min,
        salary_max,
        url,
        similarity,
    ) in rows:
        print(
            f"[{similarity:.3f}] #{posting_id} {title} @ {company or '?'} "
            f"({location or '?'}, remote={remote_flag})"
        )
        if salary_min or salary_max:
            print(f"    salary: {salary_min}-{salary_max}")
        print(f"    {url}")


if __name__ == "__main__":
    main()
