"""
Local smoke-test harness for Phase 5's agent tool-calling. `databricks-sdk`'s
typed `serving_endpoints.query()` has no `tools`/`tool_choice` parameter at
all (confirmed by inspecting `serving.py` -- not present anywhere), so
tool-calling has to go around it via a direct REST call to the
OpenAI-compatible chat-completions endpoint
(`POST {host}/serving-endpoints/{name}/invocations`). This script sends one
trivial dummy tool schema to each candidate chat model and confirms which
ones actually return a real `tool_calls` block instead of just answering in
prose, before agent.py commits to one.

Requires a Databricks CLI auth profile/env config recognized by
WorkspaceClient() (e.g. DATABRICKS_CONFIG_PROFILE=dbc-37fad84a-a89d).

Usage:
    DATABRICKS_CONFIG_PROFILE=dbc-37fad84a-a89d python scripts/verify_agent_tools.py
"""
import json

import requests
from databricks.sdk import WorkspaceClient

CANDIDATE_MODELS = [
    "databricks-meta-llama-3-3-70b-instruct",
    "databricks-llama-4-maverick",
    "databricks-gpt-oss-120b",
    "databricks-qwen3-next-80b-a3b-instruct",
]

DUMMY_TOOL = {
    "type": "function",
    "function": {
        "name": "get_weather",
        "description": "Get the current weather for a city.",
        "parameters": {
            "type": "object",
            "properties": {
                "city": {"type": "string", "description": "City name"},
            },
            "required": ["city"],
        },
    },
}

MESSAGES = [
    {"role": "user", "content": "What's the weather like in Seattle right now?"},
]

w = WorkspaceClient()


def try_model(model_name):
    headers = w.config.authenticate()
    headers["Content-Type"] = "application/json"
    url = f"{w.config.host}/serving-endpoints/{model_name}/invocations"
    body = {
        "messages": MESSAGES,
        "tools": [DUMMY_TOOL],
        "tool_choice": "auto",
        "max_tokens": 300,
    }
    try:
        resp = requests.post(url, headers=headers, json=body, timeout=30)
    except requests.Timeout:
        return "TIMEOUT (>30s, no response)"

    if resp.status_code != 200:
        return f"HTTP {resp.status_code}: {resp.text[:300]}"

    data = resp.json()
    message = data["choices"][0]["message"]
    tool_calls = message.get("tool_calls")
    if tool_calls:
        call = tool_calls[0]["function"]
        return f"OK -- called {call['name']}({call['arguments']})"
    return f"NO TOOL CALL -- replied in prose instead: {message.get('content', '')[:200]!r}"


def main():
    for model_name in CANDIDATE_MODELS:
        print(f"{model_name}:")
        result = try_model(model_name)
        print(f"  {result}\n")


if __name__ == "__main__":
    main()
