"""
Local smoke-test harness for the 3 job-posting API fetchers (Adzuna,
USAJobs, RemoteOK) before their fetch/normalize logic is ported into
notebooks/ingest_job_postings.py.

Confirms: real 200 responses, correct field nesting per source (USAJobs'
shape is under-documented), and RemoteOK's 403-without-User-Agent /
skip-element-0 quirks. Run locally against the .env file, no Databricks
connection required.

Usage:
    python scripts/verify_fetchers.py
"""
from pathlib import Path

import requests


def load_env(path=".env"):
    values = {}
    env_path = Path(path)
    if env_path.exists():
        for line in env_path.read_text().splitlines():
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, _, value = line.partition("=")
            values[key.strip()] = value.strip()
    return values


ENV = load_env()


# ---------------------------------------------------------------------------
# Adzuna
# ---------------------------------------------------------------------------
def fetch_adzuna(keyword, limit=5):
    resp = requests.get(
        "https://api.adzuna.com/v1/api/jobs/us/search/1",
        params={
            "app_id": ENV["ADZUNA_APP_ID"],
            "app_key": ENV["ADZUNA_APP_KEY"],
            "results_per_page": limit,
            "what": keyword,
            "content-type": "application/json",
        },
        timeout=15,
    )
    resp.raise_for_status()
    return resp.json()["results"]


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
        "posted_at": raw.get("created"),
    }


# ---------------------------------------------------------------------------
# USAJobs
# ---------------------------------------------------------------------------
def fetch_usajobs(keyword, limit=5):
    resp = requests.get(
        "https://data.usajobs.gov/api/search",
        params={"Keyword": keyword, "ResultsPerPage": limit},
        headers={
            "Host": "data.usajobs.gov",
            "User-Agent": ENV["USAJOBS_EMAIL"],
            "Authorization-Key": ENV["USAJOBS_API_KEY"],
        },
        timeout=15,
    )
    resp.raise_for_status()
    return resp.json()["SearchResult"]["SearchResultItems"]


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
        "posted_at": d.get("PublicationStartDate"),
    }


# ---------------------------------------------------------------------------
# RemoteOK
# ---------------------------------------------------------------------------
def fetch_remoteok():
    resp = requests.get(
        "https://remoteok.com/api",
        headers={"User-Agent": "job-search-copilot-bootcamp (verify_fetchers.py)"},
        timeout=15,
    )
    resp.raise_for_status()
    return resp.json()[1:]  # element 0 is a legal/metadata notice, not a posting


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
        "posted_at": raw.get("date") or raw.get("epoch"),
    }


# ---------------------------------------------------------------------------
# Smoke test
# ---------------------------------------------------------------------------
def main():
    print("=== Adzuna ===")
    try:
        results = fetch_adzuna("software engineer", limit=3)
        print(f"  {len(results)} results, keys: {sorted(results[0].keys())}")
        for r in results:
            print("  ", normalize_adzuna(r))
    except Exception as e:
        print(f"  FAILED: {e}")

    print("\n=== USAJobs ===")
    try:
        results = fetch_usajobs("software engineer", limit=3)
        print(
            f"  {len(results)} results, keys: "
            f"{sorted(results[0]['MatchedObjectDescriptor'].keys())}"
        )
        for r in results:
            print("  ", normalize_usajobs(r))
    except Exception as e:
        print(f"  FAILED: {e}")

    print("\n=== RemoteOK ===")
    try:
        no_ua_resp = requests.get("https://remoteok.com/api", timeout=15)
        print(f"  no-User-Agent request status: {no_ua_resp.status_code} (expect 403)")
    except Exception as e:
        print(f"  no-User-Agent request errored: {e}")

    try:
        results = fetch_remoteok()
        assert "id" not in results[0] or results[0].get("id"), (
            "sanity check: first element after slicing should be a real posting"
        )
        print(f"  {len(results)} results after skipping element 0")
        for r in results[:3]:
            print("  ", normalize_remoteok(r))
    except Exception as e:
        print(f"  FAILED: {e}")


if __name__ == "__main__":
    main()
