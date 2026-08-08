"""
Phase 5: model-facing, DB-agnostic half of the AI agent. Tool *schemas* live
here; tool *implementations* live in app.py next to the Lakebase helpers
they reuse (get_conn, embed, profile_context, etc.) -- importing those into
this module would create a circular import, and app.py already owns all
Lakebase access.

`databricks-sdk`'s typed `serving_endpoints.query()` has no `tools`/
`tool_choice` parameter (confirmed by inspecting serving.py -- not present
anywhere in the SDK), so tool-calling goes around it via a direct REST call
to the OpenAI-compatible chat-completions endpoint
(`POST {host}/serving-endpoints/{name}/invocations`), authenticated with the
same WorkspaceClient() config every other component in this repo uses.
scripts/verify_agent_tools.py confirmed databricks-meta-llama-3-3-70b-instruct
returns real tool_calls (not prose) for a trivial dummy tool.
"""
import json
import os
import time

import requests
from databricks.sdk import WorkspaceClient

AGENT_MODEL = os.environ.get("AGENT_MODEL", "databricks-meta-llama-3-3-70b-instruct")
MAX_TOOL_ITERATIONS = 6
REQUEST_TIMEOUT_SECONDS = 30  # FMAPI endpoints hang rather than error (see Phase 3 gotcha)
RATE_LIMIT_RETRY_DELAYS = (1, 2, 4)  # seconds; workspace QPS limit clears almost immediately

w = WorkspaceClient()

SYSTEM_PROMPT = """\
You are the AI Job Hunting Copilot, an assistant helping a single job \
seeker (Jane Doe, user_id=1) search live job postings, understand why a \
posting is or isn't a good match for her, and manage her application \
pipeline in Lakebase.

Guidelines:
- Use search_postings for open-ended "find me jobs like X" queries -- it \
  already ranks by semantic similarity to her profile. Use get_posting when \
  you need the full description for a specific posting_id.
- Before discussing a posting's fit, prefer looking it up rather than \
  guessing from the title alone.
- get_pipeline shows her current saved/applied/interviewing/rejected/offer \
  applications; get_notes shows interview notes and contacts for one.
- get_stale_applications finds applications that haven't moved in a while --
  offer to check this if the user asks what needs follow-up.
- save_posting adds a posting to her pipeline in the 'saved' stage.
- update_stage changes an application's stage. Moving a stage to 'rejected' \
  is treated as destructive: you must first confirm with the user in plain \
  language ("Should I mark this as rejected?") and only then call \
  update_stage again with confirm="REJECT". Never pass confirm="REJECT" \
  before the user has explicitly agreed in the conversation.
- add_interview_note logs a note (and optional follow-up date) against an \
  existing application -- the posting must already be saved first.
- draft_cover_letter_snippet returns posting + profile context for you to \
  write a short tailored cover-letter snippet or resume bullet yourself in \
  your reply. It does not save anything. If the user wants the draft kept, \
  offer to log it via add_interview_note.
- If a tool returns an "error" key, explain the problem to the user in \
  plain language rather than retrying blindly.
- Keep replies concise and concrete -- reference posting titles/companies \
  and stages by name, not internal ids, unless the user asks for an id.
"""

TOOL_SCHEMAS = [
    {
        "type": "function",
        "function": {
            "name": "search_postings",
            "description": (
                "Semantic search over live job postings, ranked by similarity to the "
                "query combined with the user's profile (skills/target roles/resume "
                "summary). Returns short snippets, not full descriptions -- call "
                "get_posting for the full text of a specific result."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "Natural-language description of the roles wanted.",
                    },
                    "remote_only": {
                        "type": "boolean",
                        "description": "Restrict to remote postings only. Default false.",
                    },
                },
                "required": ["query"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_posting",
            "description": "Get the full detail (including full description) for one job posting.",
            "parameters": {
                "type": "object",
                "properties": {
                    "posting_id": {"type": "integer"},
                },
                "required": ["posting_id"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_pipeline",
            "description": "List the user's current applications grouped by stage (saved, applied, interviewing, rejected, offer).",
            "parameters": {"type": "object", "properties": {}},
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_notes",
            "description": "Get interview notes and contacts logged against a posting's application.",
            "parameters": {
                "type": "object",
                "properties": {
                    "posting_id": {"type": "integer"},
                },
                "required": ["posting_id"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_stale_applications",
            "description": (
                "List applications that haven't been updated in at least `days` days "
                "and aren't in a terminal stage (rejected/offer) -- useful for surfacing "
                "follow-ups the user may have forgotten about."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "days": {
                        "type": "integer",
                        "description": "Minimum days since last update. Default 14.",
                    },
                },
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "save_posting",
            "description": "Save a job posting to the user's pipeline in the 'saved' stage.",
            "parameters": {
                "type": "object",
                "properties": {
                    "posting_id": {"type": "integer"},
                },
                "required": ["posting_id"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "update_stage",
            "description": (
                "Change the stage of an application (saved, applied, interviewing, "
                "rejected, offer). Moving to 'rejected' requires confirm=\"REJECT\" -- "
                "only pass that after the user has explicitly agreed in conversation."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "posting_id": {"type": "integer"},
                    "stage": {
                        "type": "string",
                        "enum": ["saved", "applied", "interviewing", "rejected", "offer"],
                    },
                    "confirm": {
                        "type": "string",
                        "description": "Must be exactly 'REJECT' when stage is 'rejected'.",
                    },
                },
                "required": ["posting_id", "stage"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "add_interview_note",
            "description": "Log an interview/follow-up note against a posting's existing application.",
            "parameters": {
                "type": "object",
                "properties": {
                    "posting_id": {"type": "integer"},
                    "note_text": {"type": "string"},
                    "follow_up_date": {
                        "type": "string",
                        "description": "Optional ISO date (YYYY-MM-DD) for a follow-up.",
                    },
                },
                "required": ["posting_id", "note_text"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "draft_cover_letter_snippet",
            "description": (
                "Get full posting text plus the user's profile context so you can write "
                "a tailored cover-letter snippet or resume bullet yourself. Does not save "
                "anything."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "posting_id": {"type": "integer"},
                },
                "required": ["posting_id"],
            },
        },
    },
]


def query_chat(messages):
    headers = w.config.authenticate()
    headers["Content-Type"] = "application/json"
    url = f"{w.config.host}/serving-endpoints/{AGENT_MODEL}/invocations"
    body = {
        "messages": messages,
        "tools": TOOL_SCHEMAS,
        "tool_choice": "auto",
        "max_tokens": 1024,
    }

    # Workspace QPS limits on the FMAPI endpoint are short bursts, not a
    # sustained quota -- a multi-tool-call turn can easily fire several
    # requests within a second or two and trip REQUEST_LIMIT_EXCEEDED, but
    # the very next request typically succeeds. Retry with a short backoff
    # instead of surfacing the 429 to the user.
    delays = (0,) + RATE_LIMIT_RETRY_DELAYS
    for attempt, delay in enumerate(delays):
        if delay:
            time.sleep(delay)
        try:
            resp = requests.post(url, headers=headers, json=body, timeout=REQUEST_TIMEOUT_SECONDS)
        except requests.Timeout:
            return {
                "role": "assistant",
                "content": "Sorry, the model took too long to respond. Please try again.",
            }

        if resp.status_code == 200:
            return resp.json()["choices"][0]["message"]

        if resp.status_code == 429 and attempt < len(delays) - 1:
            continue

        return {
            "role": "assistant",
            "content": f"Sorry, the model request failed (HTTP {resp.status_code}).",
        }


def run_agent_turn(messages, tool_dispatch):
    """Runs the tool-calling loop until the model replies without a tool
    call, or MAX_TOOL_ITERATIONS is hit. Mutates and returns `messages`."""
    for _ in range(MAX_TOOL_ITERATIONS):
        assistant_message = query_chat(messages)
        messages.append(assistant_message)

        tool_calls = assistant_message.get("tool_calls")
        if not tool_calls:
            return assistant_message.get("content") or "", messages

        for call in tool_calls:
            name = call["function"]["name"]
            try:
                args = json.loads(call["function"]["arguments"] or "{}")
            except json.JSONDecodeError:
                result = {"error": "Could not parse tool arguments."}
            else:
                fn = tool_dispatch.get(name)
                if fn is None:
                    result = {"error": f"Unknown tool: {name}"}
                else:
                    result = fn(**args)
            messages.append(
                {
                    "role": "tool",
                    "tool_call_id": call["id"],
                    "content": json.dumps(result),
                }
            )

    return (
        "Sorry, I got stuck working through that request. Could you rephrase or simplify it?",
        messages,
    )
