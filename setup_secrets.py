"""
One-time setup script: creates the Databricks secret scope and stores the
Adzuna and USAJobs API keys used by the Spark ingestion pipeline (Phase 2).
Run this locally with the Databricks CLI configured - never commit the
resulting secret values anywhere.

Reads default values from the local .env file if present, otherwise
prompts interactively.

Usage:
    python setup_secrets.py
"""
from pathlib import Path
import getpass

from databricks.sdk import WorkspaceClient
from databricks.sdk.service import workspace

SCOPE = "job-copilot"


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


def resolve(env_values, env_key, prompt_label):
    return env_values.get(env_key) or getpass.getpass(f"Paste your {prompt_label}: ")


env_values = load_env()

w = WorkspaceClient()

w.secrets.create_scope(scope=SCOPE)

w.secrets.put_secret(
    scope=SCOPE,
    key="adzuna-app-id",
    string_value=resolve(env_values, "ADZUNA_APP_ID", "Adzuna App ID"),
)
w.secrets.put_secret(
    scope=SCOPE,
    key="adzuna-app-key",
    string_value=resolve(env_values, "ADZUNA_APP_KEY", "Adzuna App Key"),
)
w.secrets.put_secret(
    scope=SCOPE,
    key="usajobs-api-key",
    string_value=resolve(env_values, "USAJOBS_API_KEY", "USAJobs API Key"),
)
w.secrets.put_secret(
    scope=SCOPE,
    key="usajobs-email",
    string_value=resolve(env_values, "USAJOBS_EMAIL", "USAJobs registered email"),
)

w.secrets.put_acl(
    scope=SCOPE,
    principal="users",
    permission=workspace.AclPermission.READ,
)
