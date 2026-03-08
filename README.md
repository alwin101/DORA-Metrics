# DORA Metrics MCP Data Ingestion Server

This repository provides an MCP server focused on **data ingestion** for DORA metric calculation.

## Primary source: GitLab
The server prioritizes GitLab repositories/APIs and also supports alternate ingestion paths.

## Supported ingestion options
- `ingest_gitlab_project` (primary, includes `commit_sha` and `pipeline_id` on change + deployment events)
- `ingest_github_repo`
- `ingest_local_git`
- `ingest_pagerduty_incidents` (incident opened/resolved for CFR + MTTR)
- `ingest_custom_events`

## DORA coverage model
- **Deployment Frequency**: deployment events (`deployment_succeeded`)
- **Lead Time for Changes**: join `change_merged` to deployment events by `metadata.commit_sha` or `metadata.pipeline_id`
- **Change Failure Rate**: failed deployment events + incident events
- **MTTR**: `incident_opened` to `incident_resolved`

## MCP exposure for agents
The server is exposed via MCP and defines:
- Tools for ingestion and status retrieval.
- Resource `dora://events/latest` for downstream metrics agents.

## Run
```bash
python -m src.data_ingestion_mcp_server
```

## Environment
```bash
export GITLAB_TOKEN="<gitlab-token>"
export GITLAB_BASE_URL="https://gitlab.com"          # optional
export GITHUB_TOKEN="<github-token>"                 # optional
export PAGERDUTY_TOKEN="<pagerduty-token>"           # optional unless using PagerDuty ingestion
export PAGERDUTY_BASE_URL="https://api.pagerduty.com" # optional
```

## Example MCP client configuration
```json
{
  "mcpServers": {
    "dora-data-ingestion": {
      "command": "python",
      "args": ["-m", "src.data_ingestion_mcp_server"],
      "cwd": "/workspace/DORA-Metrics",
      "env": {
        "GITLAB_TOKEN": "${GITLAB_TOKEN}",
        "GITLAB_BASE_URL": "https://gitlab.com",
        "PAGERDUTY_TOKEN": "${PAGERDUTY_TOKEN}"
      }
    }
  }
}
```
