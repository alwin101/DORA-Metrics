"""MCP data-ingestion server for DORA metrics.

Primary source: GitLab.
Additional sources: GitHub, local git repositories, and custom JSON payloads.

Run:
    python -m src.data_ingestion_mcp_server

Environment variables:
    GITLAB_TOKEN            Personal access token for GitLab API
    GITLAB_BASE_URL         Optional, defaults to https://gitlab.com
    GITHUB_TOKEN            Optional token for GitHub API
"""

from __future__ import annotations

import json
import os
import subprocess
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib import error, parse, request

from mcp.server.fastmcp import FastMCP

mcp = FastMCP("dora-data-ingestion")


def _iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass
class DoraEvent:
    source: str
    event_type: str
    timestamp: str
    repository: str
    actor: str | None = None
    metadata: dict[str, Any] | None = None


def _http_get_json(url: str, headers: dict[str, str] | None = None) -> Any:
    req = request.Request(url=url, headers=headers or {}, method="GET")
    try:
        with request.urlopen(req, timeout=30) as resp:
            payload = resp.read().decode("utf-8")
            return json.loads(payload)
    except error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="ignore")
        raise RuntimeError(f"HTTP {exc.code} from {url}: {detail}") from exc
    except error.URLError as exc:
        raise RuntimeError(f"Failed to reach {url}: {exc.reason}") from exc


# In-memory buffer exposed as MCP resource for downstream agents.
_EVENT_BUFFER: list[DoraEvent] = []


def _store_events(events: list[DoraEvent]) -> dict[str, Any]:
    _EVENT_BUFFER.extend(events)
    return {
        "ingested_count": len(events),
        "buffer_size": len(_EVENT_BUFFER),
        "sources": sorted({event.source for event in events}),
        "at": _iso_now(),
    }


@mcp.tool(
    description="Ingest deployment + change events from GitLab API. Primary ingestion method for DORA metrics."
)
def ingest_gitlab_project(
    project_id: str,
    since_iso: str | None = None,
    until_iso: str | None = None,
    base_url: str | None = None,
    private_token: str | None = None,
) -> dict[str, Any]:
    """Fetches merge requests and deployment records from a GitLab project.

    Args:
        project_id: Numeric project id or URL-encoded path (group%2Fproject).
        since_iso: Optional ISO-8601 lower timestamp bound.
        until_iso: Optional ISO-8601 upper timestamp bound.
        base_url: Optional GitLab base URL.
        private_token: Optional token; defaults to GITLAB_TOKEN env var.
    """

    token = private_token or os.getenv("GITLAB_TOKEN")
    if not token:
        raise ValueError("No GitLab token found. Set GITLAB_TOKEN or pass private_token.")

    root = (base_url or os.getenv("GITLAB_BASE_URL") or "https://gitlab.com").rstrip("/")
    encoded_project = parse.quote(project_id, safe="") if "/" in project_id else project_id

    headers = {"PRIVATE-TOKEN": token}
    mr_url = f"{root}/api/v4/projects/{encoded_project}/merge_requests?state=merged&per_page=100"
    dep_url = f"{root}/api/v4/projects/{encoded_project}/deployments?per_page=100"

    mrs = _http_get_json(mr_url, headers=headers)
    deployments = _http_get_json(dep_url, headers=headers)

    events: list[DoraEvent] = []

    for mr in mrs:
        merged_at = mr.get("merged_at")
        if not merged_at:
            continue
        if since_iso and merged_at < since_iso:
            continue
        if until_iso and merged_at > until_iso:
            continue
        events.append(
            DoraEvent(
                source="gitlab",
                event_type="change_merged",
                timestamp=merged_at,
                repository=str(project_id),
                actor=(mr.get("merged_by") or {}).get("username"),
                metadata={
                    "mr_iid": mr.get("iid"),
                    "title": mr.get("title"),
                    "web_url": mr.get("web_url"),
                },
            )
        )

    for dep in deployments:
        finished_at = dep.get("finished_at") or dep.get("updated_at")
        if not finished_at:
            continue
        if since_iso and finished_at < since_iso:
            continue
        if until_iso and finished_at > until_iso:
            continue

        status = dep.get("status") or "unknown"
        event_type = "deployment_succeeded" if status == "success" else "deployment_finished"

        events.append(
            DoraEvent(
                source="gitlab",
                event_type=event_type,
                timestamp=finished_at,
                repository=str(project_id),
                actor=(dep.get("user") or {}).get("username"),
                metadata={
                    "deployment_id": dep.get("id"),
                    "status": status,
                    "environment": (dep.get("deployable") or {}).get("environment"),
                },
            )
        )

    return _store_events(events)


@mcp.tool(description="Ingest merged PR + deployment data from GitHub repositories.")
def ingest_github_repo(
    owner: str,
    repo: str,
    since_iso: str | None = None,
    token: str | None = None,
) -> dict[str, Any]:
    auth = token or os.getenv("GITHUB_TOKEN")
    headers = {"Accept": "application/vnd.github+json"}
    if auth:
        headers["Authorization"] = f"Bearer {auth}"

    prs_url = f"https://api.github.com/repos/{owner}/{repo}/pulls?state=closed&per_page=100"
    pulls = _http_get_json(prs_url, headers=headers)

    events: list[DoraEvent] = []
    for pr in pulls:
        merged_at = pr.get("merged_at")
        if not merged_at:
            continue
        if since_iso and merged_at < since_iso:
            continue
        events.append(
            DoraEvent(
                source="github",
                event_type="change_merged",
                timestamp=merged_at,
                repository=f"{owner}/{repo}",
                actor=(pr.get("user") or {}).get("login"),
                metadata={"number": pr.get("number"), "title": pr.get("title"), "url": pr.get("html_url")},
            )
        )

    return _store_events(events)


@mcp.tool(description="Ingest data from local git history for repositories not tied to GitLab/GitHub APIs.")
def ingest_local_git(repo_path: str, since_iso: str | None = None) -> dict[str, Any]:
    path = Path(repo_path).resolve()
    if not (path / ".git").exists():
        raise ValueError(f"{path} is not a git repository")

    cmd = [
        "git",
        "-C",
        str(path),
        "log",
        "--pretty=format:%H|%cI|%an|%s",
    ]
    if since_iso:
        cmd.append(f"--since={since_iso}")

    result = subprocess.run(cmd, capture_output=True, text=True, check=True)

    events: list[DoraEvent] = []
    for line in result.stdout.splitlines():
        sha, committed_at, author, subject = line.split("|", 3)
        events.append(
            DoraEvent(
                source="git",
                event_type="commit",
                timestamp=committed_at,
                repository=str(path),
                actor=author,
                metadata={"sha": sha, "subject": subject},
            )
        )

    return _store_events(events)


@mcp.tool(description="Ingest arbitrary DORA event payloads from external systems (CI/CD, incident tools, webhooks).")
def ingest_custom_events(events_json: str) -> dict[str, Any]:
    raw = json.loads(events_json)
    if not isinstance(raw, list):
        raise ValueError("events_json must be a JSON array")

    events: list[DoraEvent] = []
    for item in raw:
        events.append(
            DoraEvent(
                source=str(item.get("source", "custom")),
                event_type=str(item["event_type"]),
                timestamp=str(item.get("timestamp", _iso_now())),
                repository=str(item["repository"]),
                actor=item.get("actor"),
                metadata=item.get("metadata", {}),
            )
        )

    return _store_events(events)


@mcp.resource("dora://events/latest")
def latest_ingested_events() -> str:
    """Returns latest in-memory events for metrics agents.

    Agents can subscribe/read this resource after calling ingestion tools.
    """

    return json.dumps([asdict(event) for event in _EVENT_BUFFER], indent=2)


@mcp.tool(description="Get current ingestion buffer and source summary.")
def ingestion_status() -> dict[str, Any]:
    by_source: dict[str, int] = {}
    for event in _EVENT_BUFFER:
        by_source[event.source] = by_source.get(event.source, 0) + 1

    return {
        "buffer_size": len(_EVENT_BUFFER),
        "sources": by_source,
        "resource": "dora://events/latest",
        "server": "dora-data-ingestion",
    }


if __name__ == "__main__":
    mcp.run()
