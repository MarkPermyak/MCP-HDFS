# MCP AI Agent for HDFS Cluster Management

This project provides a system for managing the Hadoop Distributed File System (HDFS) through natural language using an AI agent and the Model Context Protocol (MCP).

---

## Table of Contents

- [Architecture](#architecture)
- [Requirements](#requirements)
- [Quick Start](#quick-start)
- [MCP Specification](#mcp-specification)
- [Example Scenarios](#example-scenarios)
- [Audit and Security](#audit-and-security)
- [Project Structure](#project-structure)

---

## Architecture

```
+-------------------+     +-------------------+     +-------------------+
|   AI Agent        |---->|   MCP Server      |---->|   HDFS Cluster    |
|  (OpenAI/LLM)     | SSE |  (FastMCP)        | HTTP|  (Docker)         |
+-------------------+     +-------------------+     +-------------------+
       |                         |                         |
       v                         v                         v
  Operation               Audit logs               2 DataNode
  Confirmation            (hdfs_audit.log)         + NameNode
```

### Components

| Component | Technology | Purpose |
|-----------|------------|---------|
| AI Agent | OpenAI-compatible API | Natural language request processing |
| MCP Server | FastMCP | Tool provisioning via MCP protocol |
| HDFS Cluster | Docker + Hadoop 3.2.1 | Distributed file storage |

---

## Requirements

### System Requirements

- Docker and Docker Compose
- Python 3.10+
- 4 GB free memory (for HDFS cluster)

### Python Dependencies

```bash
uv sync
```

### Environment Variables

Create a `.env` file:

```env
# OpenRouter API
OPENAI_BASE_URL=https://openrouter.ai/api/v1
OPENAI_API_KEY=sk-or-xxxxx
OPENAI_MODEL=qwen/qwen3-32b

# HDFS settings
HDFS_NAMENODE=http://localhost:9870
HDFS_USER=root

# Audit
AUDIT_LOG_FILE=hdfs_audit.log
```

---

## Quick Start

### 1. Start HDFS Cluster

```bash
docker-compose up -d
```

### 2. Start MCP Server

```bash
python server.py
```

### 3. Start AI Agent

```bash
python ai_agent_openai.py
```

---

## MCP Specification

### Transport

| Parameter | Value |
|-----------|-------|
| Type | SSE (Server-Sent Events) |
| URL | `http://127.0.0.1:8000/sse` |
| Initialization Method | `initialize()` |

### Available Tools

| Tool | Description | Parameters | Risk |
|------|-------------|------------|------|
| `hdfs_list` | List files in path | `path: str` | Low |
| `hdfs_stat` | File/directory info | `path: str` | Low |
| `hdfs_mkdir` | Create directory | `path: str` | Medium |
| `hdfs_chmod` | Change permissions | `path: str, permission: str` | **High** |
| `hdfs_chown` | Change owner | `path: str, owner: str, group: str` | **High** |
| `hdfs_upload` | Upload file to HDFS | `local_path: str, hdfs_path: str` | **High** |
| `hdfs_download` | Download file from HDFS | `hdfs_path: str, local_path: str` | Low |
| `hdfs_snapshot_create` | Create snapshot | `path: str, snapshot_name: str` | Medium |
| `hdfs_snapshot_delete` | Delete snapshot | `path: str, snapshot_name: str` | **High** |
| `hdfs_setquota` | Set quotas | `path: str, namespace_quota: int, space_quota: str` | **High** |
| `hdfs_getquota` | Get quota info | `path: str` | Low |
| `hdfs_balancer_trigger` | Start balancer | `threshold: int` | **High** |
| `hdfs_balancer_status` | Balancer status | - | Low |

### Tool Call Format

```json
{
  "name": "hdfs_upload",
  "arguments": {
    "local_path": "config.txt",
    "hdfs_path": "/data"
  }
}
```

### Response Format

```json
{
  "content": [
    {
      "type": "text",
      "text": "File uploaded: config.txt -> /data"
    }
  ]
}
```

---

## Example Scenarios

### Basic Operations

| Tool | Example User Queries |
|------|---------------------|
| `hdfs_list` | "Show files in /data" <br> "List contents of root directory" |
| `hdfs_stat` | "Get info about /data/users.txt" <br> "Show file statistics for /config/app.conf" |
| `hdfs_mkdir` | "Create directory /backup" <br> "Make new folder /data/2025" |
| `hdfs_upload` | "Upload config.txt to /data" <br> "Put local file logs.txt into HDFS /logs" |
| `hdfs_download` | "Download /data/report.csv to local" <br> "Get file /config/settings.xml" |

### Permission Management

| Tool | Example User Queries |
|------|---------------------|
| `hdfs_chmod` | "Change permissions to 755 for /data/file.txt" <br> "Set 644 on /config/app.conf" |
| `hdfs_chown` | "Change owner to admin for /data" <br> "Set owner and group to data:analytics for /logs" |

### Snapshot Operations

| Tool | Example User Queries |
|------|---------------------|
| `hdfs_snapshot_create` | "Create snapshot backup_v1 for /data" <br> "Make snapshot of /config before update" |
| `hdfs_snapshot_delete` | "Delete snapshot backup_v1 from /data" <br> "Remove old snapshot from /logs" |

### Quota Management

| Tool | Example User Queries |
|------|---------------------|
| `hdfs_setquota` | "Set quota 1000 files and 10GB for /data" <br> "Limit /logs to 5GB space" |
| `hdfs_getquota` | "Show quotas for /data" <br> "Check quota usage on /user" |

### Cluster Maintenance

| Tool | Example User Queries |
|------|---------------------|
| `hdfs_balancer_trigger` | "Start cluster balancing" <br> "Run balancer with threshold 10%" |
| `hdfs_balancer_status` | "Check balancer status" <br> "Is balancer running?" |


## Audit and Security

### Operation Logging

All operations are logged to `hdfs_audit.log` in JSON format:

```json
{
  "timestamp": "2025-02-24T12:00:00.000000Z",
  "operation": "hdfs_chmod",
  "user": "root",
  "context": {"path": "/data/config.txt", "permission": "755"},
  "permission_diff": {"path": "/data/config.txt", "before": "644", "after": "755"},
  "status": "SUCCESS"
}
```

### Audit Fields

| Field | Description |
|-------|-------------|
| `timestamp` | Operation time (ISO 8601 UTC) |
| `operation` | Tool name |
| `user` | HDFS user |
| `context` | Operation arguments |
| `permission_diff` | Permission changes (for chmod/chown/setquota) |
| `status` | Execution status (SUCCESS/ERROR) |


### Risk Levels

| Level | Tools | Confirmation |
|-------|-------|--------------|
| **High** | chmod, chown, snapshot_delete, upload, setquota, balancer_trigger | Required |
| **Medium** | mkdir, snapshot_create | Not required |
| **Low** | list, stat, download, getquota, balancer_status | Not required |

---

## Project Structure

```
MCP Project/
├── docker-compose.yml          # HDFS cluster (1 NameNode + 2 DataNode)
├── server.py                   # MCP server with tools
├── ai_agent_openai.py          # AI agent with operation confirmation
├── .env                        # Environment variables
├── uv.lock                     # Python dependencies
├── pyproject.toml              # Python dependencies
├── README.md                   # This file
├── hdfs_audit.log              # Operation audit log
└── hdfs_server.log             # Retry log
```

---

## Known Limitations

1. **Balancer** requires at least 2 DataNodes for effective operation
2. **Snapshot** works only for directories with enabled feature (`-allowSnapshot`)
3. **hdfs_upload** does not support recursive directory upload
4. **Web UI** available at http://localhost:9870 (NameNode)

