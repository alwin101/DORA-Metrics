# DORA Metrics MCP Data Ingestion Server

This repository provides an MCP server focused on **data ingestion** for DORA metric calculation.

## Primary source: GitLab
The server is designed to prioritize GitLab repositories and APIs while also supporting other ingestion paths.

## Supported ingestion options
- `ingest_gitlab_project` (primary)
- `ingest_github_repo`
- `ingest_local_git`
- `ingest_custom_events`

## MCP exposure for agents
The server is exposed via the Model Context Protocol and defines:
- Tools for ingestion and status retrieval.
- A resource: `dora://events/latest` for downstream metrics agents.

## Run
```bash
python -m src.data_ingestion_mcp_server
```

## Environment
```bash
export GITLAB_TOKEN="<gitlab-token>"
export GITLAB_BASE_URL="https://gitlab.com"   # optional
export GITHUB_TOKEN="<github-token>"          # optional
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
        "GITLAB_BASE_URL": "https://gitlab.com"
      }
    }
  }
}
```
