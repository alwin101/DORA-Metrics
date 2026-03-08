"""MCP data-ingestion server for DORA metrics.

Primary source: GitLab.
Additional sources: GitHub, local git repositories, custom JSON payloads,
and PagerDuty incidents for reliability metrics.

Run:
    python -m src.data_ingestion_mcp_server

Environment variables:
    GITLAB_TOKEN             Personal access token for GitLab API
    GITLAB_BASE_URL          Optional, defaults to https://gitlab.com
    GITHUB_TOKEN             Optional token for GitHub API
    PAGERDUTY_TOKEN          Optional token for PagerDuty API
    PAGERDUTY_BASE_URL       Optional, defaults to https://api.pagerduty.com
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


_EVENT_BUFFER: list[DoraEvent] = []


def _store_events(events: list[DoraEvent]) -> dict[str, Any]:
    _EVENT_BUFFER.extend(events)
    return {
        "ingested_count": len(events),
        "buffer_size": len(_EVENT_BUFFER),
        "sources": sorted({event.source for event in events}),
        "at": _iso_now(),
    }


def _gitlab_pipeline_id_from_deployment(deployment: dict[str, Any]) -> int | None:
    deployable = deployment.get("deployable") or {}
    pipeline = deployable.get("pipeline") or {}
    return pipeline.get("id")


def _gitlab_sha_from_deployment(deployment: dict[str, Any]) -> str | None:
    deployable = deployment.get("deployable") or {}
    # Common shape includes deployable.commit.short_id/id; fallback to deployment.sha when present.
    commit = deployable.get("commit") or {}
    return commit.get("id") or deployment.get("sha")


@mcp.tool(
    description=(
        "Ingest deployment + merged-change events from GitLab API (primary source). "
        "Includes commit SHA / pipeline IDs to support lead-time joins."
    )
)
def ingest_gitlab_project(
    project_id: str,
    since_iso: str | None = None,
    until_iso: str | None = None,
    base_url: str | None = None,
    private_token: str | None = None,
) -> dict[str, Any]:
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

        commit_sha = mr.get("merge_commit_sha") or mr.get("squash_commit_sha") or mr.get("sha")
        pipeline = mr.get("head_pipeline") or {}
        pipeline_id = pipeline.get("id")

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
                    "commit_sha": commit_sha,
                    "pipeline_id": pipeline_id,
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
        pipeline_id = _gitlab_pipeline_id_from_deployment(dep)
        commit_sha = _gitlab_sha_from_deployment(dep)

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
                    "commit_sha": commit_sha,
                    "pipeline_id": pipeline_id,
                },
            )
        )

    return _store_events(events)


@mcp.tool(description="Ingest merged PR data from GitHub repositories.")
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
                metadata={
                    "number": pr.get("number"),
                    "title": pr.get("title"),
                    "url": pr.get("html_url"),
                    "commit_sha": pr.get("merge_commit_sha"),
                },
            )
        )

    return _store_events(events)


@mcp.tool(description="Ingest data from local git history for repositories not tied to GitLab/GitHub APIs.")
def ingest_local_git(repo_path: str, since_iso: str | None = None) -> dict[str, Any]:
    path = Path(repo_path).resolve()
    if not (path / ".git").exists():
        raise ValueError(f"{path} is not a git repository")

    cmd = ["git", "-C", str(path), "log", "--pretty=format:%H|%cI|%an|%s"]
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


@mcp.tool(description="Ingest PagerDuty incidents to support change-failure-rate and MTTR calculations.")
def ingest_pagerduty_incidents(
    since_iso: str | None = None,
    until_iso: str | None = None,
    service_ids_csv: str | None = None,
    token: str | None = None,
    base_url: str | None = None,
) -> dict[str, Any]:
    auth = token or os.getenv("PAGERDUTY_TOKEN")
    if not auth:
        raise ValueError("No PagerDuty token found. Set PAGERDUTY_TOKEN or pass token.")

    root = (base_url or os.getenv("PAGERDUTY_BASE_URL") or "https://api.pagerduty.com").rstrip("/")

    params: list[tuple[str, str]] = [
        ("statuses[]", "triggered"),
        ("statuses[]", "acknowledged"),
        ("statuses[]", "resolved"),
        ("limit", "100"),
    ]
    if since_iso:
        params.append(("since", since_iso))
    if until_iso:
        params.append(("until", until_iso))
    if service_ids_csv:
        for service_id in [item.strip() for item in service_ids_csv.split(",") if item.strip()]:
            params.append(("service_ids[]", service_id))

    query = parse.urlencode(params)
    incidents_url = f"{root}/incidents?{query}"

    headers = {
        "Accept": "application/vnd.pagerduty+json;version=2",
        "Authorization": f"Token token={auth}",
    }
    payload = _http_get_json(incidents_url, headers=headers)
    incidents = payload.get("incidents", []) if isinstance(payload, dict) else []

    events: list[DoraEvent] = []
    for incident in incidents:
        incident_id = incident.get("id")
        created_at = incident.get("created_at")
        resolved_at = incident.get("resolved_at")
        service = incident.get("service") or {}
        title = incident.get("title")

        if created_at:
            events.append(
                DoraEvent(
                    source="pagerduty",
                    event_type="incident_opened",
                    timestamp=created_at,
                    repository=service.get("summary") or "infrastructure",
                    metadata={
                        "incident_id": incident_id,
                        "title": title,
                        "service_id": service.get("id"),
                        "urgency": incident.get("urgency"),
                        "status": incident.get("status"),
                    },
                )
            )

        if resolved_at:
            events.append(
                DoraEvent(
                    source="pagerduty",
                    event_type="incident_resolved",
                    timestamp=resolved_at,
                    repository=service.get("summary") or "infrastructure",
                    metadata={
                        "incident_id": incident_id,
                        "title": title,
                        "service_id": service.get("id"),
                        "urgency": incident.get("urgency"),
                        "status": incident.get("status"),
                    },
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
    """Returns latest in-memory events for metrics agents."""

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
