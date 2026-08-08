import os

from flask import Flask, render_template, request, redirect, url_for, flash
from databricks.sdk import WorkspaceClient
import psycopg
from psycopg_pool import ConnectionPool
from psycopg.rows import dict_row

app = Flask(__name__)
app.secret_key = os.environ.get("FLASK_SECRET_KEY", "dev-secret-change-me")

# Single-user personal copilot for now -- the schema supports multiple
# users, but there's no login system (matches lakebase-support-app, which
# also has no auth) and only one seeded user exists.
DEFAULT_USER_ID = 1

VALID_REMOTE_PREFERENCES = ["remote", "hybrid", "onsite", "any"]
VALID_PROFICIENCIES = ["beginner", "intermediate", "advanced", "expert"]
VALID_STAGES = ["saved", "applied", "interviewing", "rejected", "offer"]

EMBEDDING_MODEL = os.environ.get("EMBEDDING_MODEL", "databricks-gte-large-en")
SEARCH_TOP_N = 20
BROWSE_LIMIT = 30

# ---------------------------------------------------------------------------
# Lakebase connection (OAuth token rotation pattern per CLAUDE.md, matching
# lakebase-support-app/app.py -- never a static Postgres password).
# ---------------------------------------------------------------------------
w = WorkspaceClient()


class OAuthConnection(psycopg.Connection):
    """psycopg connection that fetches a fresh Databricks OAuth token on
    every new connection instead of using a static password."""

    @classmethod
    def connect(cls, conninfo="", **kwargs):
        endpoint_name = os.environ["ENDPOINT_NAME"]
        credential = w.postgres.generate_database_credential(endpoint=endpoint_name)
        kwargs["password"] = credential.token
        return super().connect(conninfo, **kwargs)


PGUSER = os.environ["PGUSER"]
PGHOST = os.environ["PGHOST"]
PGPORT = os.environ.get("PGPORT", "5432")
PGDATABASE = os.environ["PGDATABASE"]
PGSSLMODE = os.environ.get("PGSSLMODE", "require")

pool = ConnectionPool(
    conninfo=f"dbname={PGDATABASE} user={PGUSER} host={PGHOST} port={PGPORT} sslmode={PGSSLMODE}",
    connection_class=OAuthConnection,
    min_size=1,
    max_size=10,
    open=True,
)


def get_conn():
    return pool.connection()


# ---------------------------------------------------------------------------
# Semantic retrieval helpers (Phase 3's pgvector approach, adapted from
# scripts/search_postings.py for a live request instead of a CLI run).
# ---------------------------------------------------------------------------
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
        return None

    cur.execute(
        "SELECT skill_name FROM skills WHERE profile_id = %(profile_id)s",
        {"profile_id": profile_id},
    )
    skills = [r["skill_name"] for r in cur.fetchall()]

    parts = [
        row["target_roles"],
        f"remote preference: {row['remote_preference']}",
        row["location_preference"],
        row["resume_summary"],
        row["notes"],
        "skills: " + ", ".join(skills) if skills else None,
    ]
    return ". ".join(p for p in parts if p)


# ---------------------------------------------------------------------------
# Data access helpers
# ---------------------------------------------------------------------------
def fetch_user_display_name(user_id):
    with get_conn() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute("SELECT display_name FROM users WHERE user_id = %s", (user_id,))
            row = cur.fetchone()
            return row["display_name"] if row else "Me"


def fetch_profile(user_id):
    with get_conn() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute("SELECT * FROM profiles WHERE user_id = %s", (user_id,))
            profile = cur.fetchone()
            skills = []
            if profile:
                cur.execute(
                    "SELECT * FROM skills WHERE profile_id = %s ORDER BY skill_id",
                    (profile["profile_id"],),
                )
                skills = cur.fetchall()
    return profile, skills


def fetch_application_for_posting(user_id, posting_id):
    with get_conn() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                """SELECT * FROM applications
                   WHERE user_id = %s AND job_posting_id = %s""",
                (user_id, posting_id),
            )
            return cur.fetchone()


def fetch_posting(posting_id):
    with get_conn() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute("SELECT * FROM job_postings WHERE posting_id = %s", (posting_id,))
            return cur.fetchone()


SEARCH_SQL = """
    SELECT jp.*, a.application_id, a.stage,
           1 - (jp.embedding <=> %(query_embedding)s::vector) AS similarity
    FROM job_postings jp
    LEFT JOIN applications a
        ON a.job_posting_id = jp.posting_id AND a.user_id = %(user_id)s
    WHERE jp.embedding IS NOT NULL
    {remote_clause}
    ORDER BY jp.embedding <=> %(query_embedding)s::vector
    LIMIT %(limit)s
"""

BROWSE_SQL = """
    SELECT jp.*, a.application_id, a.stage, NULL AS similarity
    FROM job_postings jp
    LEFT JOIN applications a
        ON a.job_posting_id = jp.posting_id AND a.user_id = %(user_id)s
    WHERE 1=1
    {remote_clause}
    ORDER BY jp.posted_at DESC NULLS LAST, jp.fetched_at DESC
    LIMIT %(limit)s
"""


def search_postings(query_text, remote_only, limit=SEARCH_TOP_N):
    remote_clause = "AND jp.remote_flag = TRUE" if remote_only else ""
    query_embedding = embed(query_text)
    with get_conn() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                SEARCH_SQL.format(remote_clause=remote_clause),
                {
                    "query_embedding": to_pgvector_literal(query_embedding),
                    "user_id": DEFAULT_USER_ID,
                    "limit": limit,
                },
            )
            return cur.fetchall()


def browse_postings(remote_only, limit=BROWSE_LIMIT):
    remote_clause = "AND jp.remote_flag = TRUE" if remote_only else ""
    with get_conn() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                BROWSE_SQL.format(remote_clause=remote_clause),
                {"user_id": DEFAULT_USER_ID, "limit": limit},
            )
            return cur.fetchall()


# ---------------------------------------------------------------------------
# Routes: profile
# ---------------------------------------------------------------------------
@app.route("/")
def index():
    return redirect(url_for("postings"))


@app.route("/profile")
def profile():
    profile_row, skills = fetch_profile(DEFAULT_USER_ID)
    return render_template(
        "profile.html",
        profile=profile_row,
        skills=skills,
        valid_remote_preferences=VALID_REMOTE_PREFERENCES,
        valid_proficiencies=VALID_PROFICIENCIES,
    )


@app.route("/profile", methods=["POST"])
def update_profile():
    target_roles = request.form.get("target_roles", "").strip()[:200] or None
    location_preference = request.form.get("location_preference", "").strip()[:200] or None
    remote_preference = request.form.get("remote_preference", "any")
    resume_summary = request.form.get("resume_summary", "").strip()[:4000] or None
    notes = request.form.get("notes", "").strip()[:4000] or None

    errors = []
    if remote_preference not in VALID_REMOTE_PREFERENCES:
        errors.append("Invalid remote preference selected.")

    if errors:
        for e in errors:
            flash(e, "error")
        return redirect(url_for("profile"))

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO profiles
                    (user_id, target_roles, location_preference, remote_preference,
                     resume_summary, notes)
                VALUES (%(user_id)s, %(target_roles)s, %(location_preference)s,
                        %(remote_preference)s, %(resume_summary)s, %(notes)s)
                ON CONFLICT (user_id) DO UPDATE SET
                    target_roles = EXCLUDED.target_roles,
                    location_preference = EXCLUDED.location_preference,
                    remote_preference = EXCLUDED.remote_preference,
                    resume_summary = EXCLUDED.resume_summary,
                    notes = EXCLUDED.notes,
                    updated_at = CURRENT_TIMESTAMP
                """,
                {
                    "user_id": DEFAULT_USER_ID,
                    "target_roles": target_roles,
                    "location_preference": location_preference,
                    "remote_preference": remote_preference,
                    "resume_summary": resume_summary,
                    "notes": notes,
                },
            )
        conn.commit()

    flash("Profile saved.", "success")
    return redirect(url_for("profile"))


@app.route("/profile/skills", methods=["POST"])
def add_skill():
    skill_name = request.form.get("skill_name", "").strip()[:100]
    proficiency = request.form.get("proficiency", "intermediate")
    years_raw = request.form.get("years_experience", "").strip()

    errors = []
    if not skill_name:
        errors.append("Skill name is required.")
    if proficiency not in VALID_PROFICIENCIES:
        errors.append("Invalid proficiency level.")
    years_experience = None
    if years_raw:
        try:
            years_experience = float(years_raw)
            if not (0 <= years_experience <= 60):
                errors.append("Years of experience must be between 0 and 60.")
        except ValueError:
            errors.append("Years of experience must be a number.")

    profile_row, _ = fetch_profile(DEFAULT_USER_ID)
    if not profile_row:
        errors.append("Save your profile before adding skills.")

    if errors:
        for e in errors:
            flash(e, "error")
        return redirect(url_for("profile"))

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """INSERT INTO skills (profile_id, skill_name, proficiency, years_experience)
                   VALUES (%s, %s, %s, %s)""",
                (profile_row["profile_id"], skill_name, proficiency, years_experience),
            )
        conn.commit()

    flash(f"Added skill: {skill_name}.", "success")
    return redirect(url_for("profile"))


@app.route("/profile/skills/<int:skill_id>/delete", methods=["POST"])
def delete_skill(skill_id):
    profile_row, _ = fetch_profile(DEFAULT_USER_ID)
    if not profile_row:
        flash("No profile found.", "error")
        return redirect(url_for("profile"))

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM skills WHERE skill_id = %s AND profile_id = %s",
                (skill_id, profile_row["profile_id"]),
            )
        conn.commit()

    flash("Skill removed.", "success")
    return redirect(url_for("profile"))


# ---------------------------------------------------------------------------
# Routes: job search / browse
# ---------------------------------------------------------------------------
@app.route("/postings")
def postings():
    query_text = request.args.get("q", "").strip()
    remote_only = request.args.get("remote_only") == "1"
    use_profile = request.args.get("use_profile", "1") == "1"

    results = []
    if query_text:
        search_text = query_text
        if use_profile:
            profile_row, _ = fetch_profile(DEFAULT_USER_ID)
            if profile_row:
                with get_conn() as conn:
                    with conn.cursor(row_factory=dict_row) as cur:
                        context = profile_context(cur, profile_row["profile_id"])
                if context:
                    search_text = f"{query_text}. {context}"
        results = search_postings(search_text, remote_only)
    else:
        results = browse_postings(remote_only)

    return render_template(
        "postings.html",
        results=results,
        query_text=query_text,
        remote_only=remote_only,
        use_profile=use_profile,
    )


@app.route("/postings/<int:posting_id>/save", methods=["POST"])
def save_posting(posting_id):
    if not fetch_posting(posting_id):
        flash(f"Posting #{posting_id} was not found.", "error")
        return redirect(url_for("postings"))

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """INSERT INTO applications (user_id, job_posting_id, stage)
                   VALUES (%s, %s, 'saved')
                   ON CONFLICT (user_id, job_posting_id) DO NOTHING""",
                (DEFAULT_USER_ID, posting_id),
            )
        conn.commit()

    flash("Saved to your pipeline.", "success")
    return redirect(request.referrer or url_for("postings"))


# ---------------------------------------------------------------------------
# Routes: posting detail
# ---------------------------------------------------------------------------
@app.route("/postings/<int:posting_id>")
def posting_detail(posting_id):
    posting = fetch_posting(posting_id)
    if not posting:
        flash(f"Posting #{posting_id} was not found.", "error")
        return redirect(url_for("postings"))

    application = fetch_application_for_posting(DEFAULT_USER_ID, posting_id)
    notes, contacts = [], []
    if application:
        with get_conn() as conn:
            with conn.cursor(row_factory=dict_row) as cur:
                cur.execute(
                    """SELECT * FROM interview_notes
                       WHERE application_id = %s ORDER BY created_at ASC""",
                    (application["application_id"],),
                )
                notes = cur.fetchall()
                cur.execute(
                    """SELECT * FROM contacts
                       WHERE application_id = %s ORDER BY created_at ASC""",
                    (application["application_id"],),
                )
                contacts = cur.fetchall()

    return render_template(
        "posting_detail.html",
        posting=posting,
        application=application,
        notes=notes,
        contacts=contacts,
        valid_stages=VALID_STAGES,
    )


@app.route("/postings/<int:posting_id>/stage", methods=["POST"])
def update_stage(posting_id):
    new_stage = request.form.get("stage", "")
    if new_stage not in VALID_STAGES:
        flash("Invalid stage selected.", "error")
        return redirect(url_for("posting_detail", posting_id=posting_id))

    if not fetch_posting(posting_id):
        flash(f"Posting #{posting_id} was not found.", "error")
        return redirect(url_for("postings"))

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO applications (user_id, job_posting_id, stage, applied_at)
                VALUES (%(user_id)s, %(posting_id)s, %(stage)s,
                        CASE WHEN %(stage)s = 'applied' THEN CURRENT_TIMESTAMP END)
                ON CONFLICT (user_id, job_posting_id) DO UPDATE SET
                    stage = EXCLUDED.stage,
                    applied_at = COALESCE(applications.applied_at, EXCLUDED.applied_at),
                    last_updated_at = CURRENT_TIMESTAMP
                """,
                {"user_id": DEFAULT_USER_ID, "posting_id": posting_id, "stage": new_stage},
            )
        conn.commit()

    flash(f"Stage updated to '{new_stage}'.", "success")
    return redirect(url_for("posting_detail", posting_id=posting_id))


@app.route("/postings/<int:posting_id>/notes", methods=["POST"])
def add_note(posting_id):
    note_text = request.form.get("note_text", "").strip()[:2000]
    follow_up_date = request.form.get("follow_up_date", "").strip() or None

    application = fetch_application_for_posting(DEFAULT_USER_ID, posting_id)
    errors = []
    if not application:
        errors.append("Save this posting to your pipeline before adding notes.")
    if not note_text:
        errors.append("Note text is required.")

    if errors:
        for e in errors:
            flash(e, "error")
        return redirect(url_for("posting_detail", posting_id=posting_id))

    author = fetch_user_display_name(DEFAULT_USER_ID)
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """INSERT INTO interview_notes
                       (application_id, note_text, follow_up_date, author)
                   VALUES (%s, %s, %s, %s)""",
                (application["application_id"], note_text, follow_up_date, author),
            )
        conn.commit()

    flash("Note added.", "success")
    return redirect(url_for("posting_detail", posting_id=posting_id))


@app.route("/postings/<int:posting_id>/contacts", methods=["POST"])
def add_contact(posting_id):
    name = request.form.get("name", "").strip()[:200]
    role = request.form.get("role", "").strip()[:200] or None
    email = request.form.get("email", "").strip()[:200] or None
    linkedin_url = request.form.get("linkedin_url", "").strip()[:500] or None
    notes_text = request.form.get("notes", "").strip()[:2000] or None

    application = fetch_application_for_posting(DEFAULT_USER_ID, posting_id)
    errors = []
    if not application:
        errors.append("Save this posting to your pipeline before adding contacts.")
    if not name:
        errors.append("Contact name is required.")

    if errors:
        for e in errors:
            flash(e, "error")
        return redirect(url_for("posting_detail", posting_id=posting_id))

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """INSERT INTO contacts
                       (application_id, name, role, email, linkedin_url, notes)
                   VALUES (%s, %s, %s, %s, %s, %s)""",
                (application["application_id"], name, role, email, linkedin_url, notes_text),
            )
        conn.commit()

    flash(f"Added contact: {name}.", "success")
    return redirect(url_for("posting_detail", posting_id=posting_id))


# ---------------------------------------------------------------------------
# Routes: pipeline board
# ---------------------------------------------------------------------------
@app.route("/pipeline")
def pipeline():
    with get_conn() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                """
                SELECT a.application_id, a.stage, a.applied_at, a.last_updated_at,
                       jp.posting_id, jp.title, jp.company, jp.location, jp.remote_flag
                FROM applications a
                JOIN job_postings jp ON jp.posting_id = a.job_posting_id
                WHERE a.user_id = %s
                ORDER BY a.last_updated_at DESC
                """,
                (DEFAULT_USER_ID,),
            )
            applications = cur.fetchall()

    board = {stage: [] for stage in VALID_STAGES}
    for a in applications:
        board[a["stage"]].append(a)

    return render_template("pipeline.html", board=board, valid_stages=VALID_STAGES)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000)
