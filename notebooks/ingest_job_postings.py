# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest Job Postings
# MAGIC
# MAGIC Fetches job postings from Adzuna, USAJobs, and RemoteOK, normalizes them
# MAGIC into the `job_postings` schema, dedupes with a Spark DataFrame, and
# MAGIC upserts into Lakebase Postgres (`job_postings`, keyed on
# MAGIC `(external_source, external_id)`).
# MAGIC
# MAGIC Runs as the scheduled `ingest_job_postings_job` (every 6 hours) defined in
# MAGIC `resources/ingest_job_postings_job.yml`. Self-contained — no imports from
# MAGIC other repo files (Databricks Repos cross-file notebook imports are
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
dbutils.widgets.text("secret_scope", "job-copilot")
dbutils.widgets.text(
    "search_keywords", "software engineer,data engineer,backend developer"
)
dbutils.widgets.text("results_limit_per_source", "50")

PG_HOST = dbutils.widgets.get("pg_host")
PG_USER = dbutils.widgets.get("pg_user")
PG_DATABASE = dbutils.widgets.get("pg_database")
ENDPOINT_NAME = dbutils.widgets.get("endpoint_name")
SECRET_SCOPE = dbutils.widgets.get("secret_scope")
SEARCH_KEYWORDS = [
    k.strip() for k in dbutils.widgets.get("search_keywords").split(",") if k.strip()
]
RESULTS_LIMIT_PER_SOURCE = int(dbutils.widgets.get("results_limit_per_source"))

PG_PORT = "5432"
PG_SSLMODE = "require"

assert PG_HOST and PG_USER and PG_DATABASE and ENDPOINT_NAME, (
    "pg_host, pg_user, pg_database, and endpoint_name widgets are required"
)

# COMMAND ----------

from datetime import datetime, timezone

import requests

import psycopg
from databricks.sdk import WorkspaceClient
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    BooleanType,
    IntegerType,
    TimestampType,
)

# COMMAND ----------

ADZUNA_APP_ID = dbutils.secrets.get(SECRET_SCOPE, "adzuna-app-id")
ADZUNA_APP_KEY = dbutils.secrets.get(SECRET_SCOPE, "adzuna-app-key")
USAJOBS_API_KEY = dbutils.secrets.get(SECRET_SCOPE, "usajobs-api-key")
USAJOBS_EMAIL = dbutils.secrets.get(SECRET_SCOPE, "usajobs-email")

# ---------------------------------------------------------------------------
# Lakebase connection (OAuth token rotation pattern per CLAUDE.md, matching
# lakebase-support-app/app.py:16-46 — never a static Postgres password).
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
# Fetch functions — each wrapped in its own try/except so one flaky free
# public API doesn't kill the whole run.
# ---------------------------------------------------------------------------
def fetch_adzuna(keyword, limit):
    try:
        resp = requests.get(
            "https://api.adzuna.com/v1/api/jobs/us/search/1",
            params={
                "app_id": ADZUNA_APP_ID,
                "app_key": ADZUNA_APP_KEY,
                "results_per_page": limit,
                "what": keyword,
                "content-type": "application/json",
            },
            timeout=15,
        )
        resp.raise_for_status()
        return resp.json()["results"]
    except Exception as e:
        print(f"WARNING: Adzuna fetch failed for keyword={keyword!r}: {e}")
        return []


def fetch_usajobs(keyword, limit):
    try:
        resp = requests.get(
            "https://data.usajobs.gov/api/search",
            params={"Keyword": keyword, "ResultsPerPage": limit},
            headers={
                "Host": "data.usajobs.gov",
                "User-Agent": USAJOBS_EMAIL,
                "Authorization-Key": USAJOBS_API_KEY,
            },
            timeout=15,
        )
        resp.raise_for_status()
        return resp.json()["SearchResult"]["SearchResultItems"]
    except Exception as e:
        print(f"WARNING: USAJobs fetch failed for keyword={keyword!r}: {e}")
        return []


def fetch_remoteok():
    try:
        resp = requests.get(
            "https://remoteok.com/api",
            headers={"User-Agent": "job-search-copilot-bootcamp (ingestion job)"},
            timeout=15,
        )
        resp.raise_for_status()
        return resp.json()[1:]  # element 0 is a legal/metadata notice
    except Exception as e:
        print(f"WARNING: RemoteOK fetch failed: {e}")
        return []


# ---------------------------------------------------------------------------
# Normalize functions -> dict matching job_postings columns, or None to
# drop the record (missing external_id or title).
# ---------------------------------------------------------------------------
def normalize_posted_at(value):
    """Parse a source's raw posted-date into a real datetime instead of
    passing the raw value through to Postgres's implicit string cast.
    RemoteOK's `date` field is missing on some records, falling back to
    `epoch` (a Unix-timestamp string) -- without this, that fallback value
    hits Postgres as a bare digit string, which a `timestamp` column
    rejects outright. Returns None for anything unparseable so one bad
    record doesn't fail the whole batch."""
    if value is None or value == "":
        return None
    if isinstance(value, (int, float)):
        try:
            return datetime.fromtimestamp(value, tz=timezone.utc)
        except (ValueError, OSError):
            return None
    value = str(value)
    if value.isdigit():
        try:
            return datetime.fromtimestamp(int(value), tz=timezone.utc)
        except (ValueError, OSError):
            return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


def normalize_adzuna(raw):
    external_id = raw.get("id")
    title = raw.get("title")
    if not external_id or not title:
        return None
    location = (raw.get("location") or {}).get("display_name", "")
    return {
        "external_source": "adzuna",
        "external_id": str(external_id),
        "title": title,
        "company": (raw.get("company") or {}).get("display_name"),
        "location": location,
        "remote_flag": "remote" in (title + location).lower(),
        "salary_min": int(raw["salary_min"]) if raw.get("salary_min") else None,
        "salary_max": int(raw["salary_max"]) if raw.get("salary_max") else None,
        "description": raw.get("description"),
        "url": raw.get("redirect_url"),
        "posted_at": normalize_posted_at(raw.get("created")),
    }


def normalize_usajobs(raw):
    d = raw["MatchedObjectDescriptor"]
    external_id = d.get("PositionID")
    title = d.get("PositionTitle")
    if not external_id or not title:
        return None
    locations = d.get("PositionLocation") or []
    remuneration = d.get("PositionRemuneration") or []
    user_area_details = (d.get("UserArea") or {}).get("Details") or {}
    return {
        "external_source": "usajobs",
        "external_id": str(external_id),
        "title": title,
        "company": d.get("OrganizationName") or d.get("DepartmentName"),
        "location": locations[0].get("LocationName") if locations else None,
        "remote_flag": bool(user_area_details.get("TeleworkEligible", False)),
        "salary_min": (
            int(float(remuneration[0]["MinimumRange"])) if remuneration else None
        ),
        "salary_max": (
            int(float(remuneration[0]["MaximumRange"])) if remuneration else None
        ),
        "description": user_area_details.get("JobSummary", ""),
        "url": d.get("PositionURI"),
        "posted_at": normalize_posted_at(d.get("PublicationStartDate")),
    }


def normalize_remoteok(raw):
    external_id = raw.get("id")
    title = raw.get("position")
    if not external_id or not title:
        return None
    return {
        "external_source": "remoteok",
        "external_id": str(external_id),
        "title": title,
        "company": raw.get("company"),
        "location": raw.get("location") or "Remote",
        "remote_flag": True,
        "salary_min": raw.get("salary_min"),
        "salary_max": raw.get("salary_max"),
        "description": raw.get("description"),
        "url": raw.get("url"),
        "posted_at": normalize_posted_at(raw.get("date") or raw.get("epoch")),
    }


# COMMAND ----------

# ---------------------------------------------------------------------------
# Orchestration: fetch + normalize from all 3 sources.
# ---------------------------------------------------------------------------
records = []

for keyword in SEARCH_KEYWORDS:
    for raw in fetch_adzuna(keyword, RESULTS_LIMIT_PER_SOURCE):
        normalized = normalize_adzuna(raw)
        if normalized:
            records.append(normalized)
    for raw in fetch_usajobs(keyword, RESULTS_LIMIT_PER_SOURCE):
        normalized = normalize_usajobs(raw)
        if normalized:
            records.append(normalized)

for raw in fetch_remoteok():
    normalized = normalize_remoteok(raw)
    if normalized:
        records.append(normalized)

fetched_count = len(records)
print(f"fetched={fetched_count} raw normalized records across 3 sources")

if fetched_count == 0:
    raise RuntimeError(
        "All 3 job posting sources returned zero rows this run — failing "
        "the job so email_notifications.on_failure fires."
    )

# COMMAND ----------

# ---------------------------------------------------------------------------
# Spark dedupe — real DataFrame ops, explicit schema (small/sparse batches
# risk NullType/wrong-dtype inference on nullable columns like salary_min).
# ---------------------------------------------------------------------------
job_postings_schema = StructType(
    [
        StructField("external_source", StringType(), False),
        StructField("external_id", StringType(), False),
        StructField("title", StringType(), False),
        StructField("company", StringType(), True),
        StructField("location", StringType(), True),
        StructField("remote_flag", BooleanType(), False),
        StructField("salary_min", IntegerType(), True),
        StructField("salary_max", IntegerType(), True),
        StructField("description", StringType(), True),
        StructField("url", StringType(), True),
        StructField("posted_at", TimestampType(), True),
    ]
)

postings_df = spark.createDataFrame(records, schema=job_postings_schema)
deduped_df = postings_df.dropDuplicates(["external_source", "external_id"])
deduped_count = deduped_df.count()
print(f"deduped={deduped_count} unique (external_source, external_id) rows")

# COMMAND ----------

# ---------------------------------------------------------------------------
# Upsert into Lakebase via a single OAuth-rotated psycopg connection.
# ---------------------------------------------------------------------------
UPSERT_SQL = """
    INSERT INTO job_postings (
        external_source, external_id, title, company, location,
        remote_flag, salary_min, salary_max, description, url, posted_at,
        fetched_at
    )
    VALUES (
        %(external_source)s, %(external_id)s, %(title)s, %(company)s,
        %(location)s, %(remote_flag)s, %(salary_min)s, %(salary_max)s,
        %(description)s, %(url)s, %(posted_at)s, now()
    )
    ON CONFLICT (external_source, external_id) DO UPDATE SET
        title = EXCLUDED.title,
        company = EXCLUDED.company,
        location = EXCLUDED.location,
        remote_flag = EXCLUDED.remote_flag,
        salary_min = EXCLUDED.salary_min,
        salary_max = EXCLUDED.salary_max,
        description = EXCLUDED.description,
        url = EXCLUDED.url,
        posted_at = EXCLUDED.posted_at,
        fetched_at = now()
    RETURNING (xmax = 0) AS inserted
"""

inserted_count = 0
updated_count = 0

with OAuthConnection.connect(PG_CONNINFO) as conn:
    with conn.cursor() as cur:
        for row in deduped_df.collect():
            cur.execute(UPSERT_SQL, row.asDict())
            if cur.fetchone()[0]:
                inserted_count += 1
            else:
                updated_count += 1
    conn.commit()

print(
    f"fetched={fetched_count} deduped={deduped_count} "
    f"inserted={inserted_count} updated={updated_count}"
)
