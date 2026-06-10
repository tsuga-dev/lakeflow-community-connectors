# Lakeflow Tsuga Logs Community Connector

This documentation describes how to configure and use the **Tsuga Logs** Lakeflow community connector to ingest log events from the [Tsuga](https://www.tsuga.com) public API into Databricks.

## Quick start (Databricks UI)

The repository-level [README](../../../../../../README.md) carries the validated step-by-step custom-connector walkthrough (connection JSON, root-path note, the one-line `ingest.py` edit). Summary:

1. **Add data → + Add Community Connector** — source name `tsuga_logs`, this repository's URL, branch `master`.
2. **Create connection** — Auth Type `USES_ANY_STATIC_CREDENTIAL`; put the options in **Additional Options (JSON)**: `operation_api_key`, `default_query`, optional `default_cluster_id`/`base_url`, and `externalOptionsAllowList` (full value below).
3. **Ingestion setup** — pipeline name, event-log catalog/schema, root path like `/Users/<you>/tsuga_connector/src` (the folder must already exist).
4. In the generated `ingest.py`, replace the placeholder objects with `[{"table": {"source_table": "logs"}}]` — the connection name is pre-filled.
5. **Run pipeline.**

Several differently-filtered log tables can share one connection via per-table `query` overrides, but each needs its own pipeline (the upstream framework keys table configs by source table name).

## Prerequisites

- **Tsuga account**: Access to a Tsuga organization with the logs you want to ingest.
- **Operation API key**:
  - Created in Tsuga under **Settings → Operation API keys**.
  - The key's team scoping determines which logs are visible to the connector.
- **Network access**: The environment running the connector must be able to reach `https://api.tsuga.com` (or your custom deployment URL).
- **Lakeflow / Databricks environment**: A workspace where you can register a Lakeflow community connector and run ingestion pipelines.

## Setup

### Required Connection Parameters

Provide the following **connection-level** options when configuring the connector:

| Name | Type | Required | Description | Example |
|---|---|---|---|---|
| `operation_api_key` | string | yes | Tsuga operation API key used for public API authentication. | `tsuga_op:1:...` |
| `base_url` | string | no | Base URL for the Tsuga public API. Defaults to `https://api.tsuga.com`. | `https://api.tsuga.com` |
| `default_query` | string | yes* | Tsuga query for every table that does not set its own `query` table option. *One of the two must be set — there is deliberately no fallback; use `'*'` explicitly to ingest everything. Named `default_` because the framework forbids pipelines passing an option key stored on the connection. | `context.service.name:payments` |
| `default_cluster_id` | string | no | Tsuga cluster for every table that does not set its own `cluster_id` table option. Required for multi-cluster organizations. | `1ab2-3cd4e-fg5h` |
| `externalOptionsAllowList` | string | yes | Comma-separated list of option names allowed to pass through the connection. Must include the framework options (`tableName,tableNameList,tableConfigs,isDeleteFlow`) plus any per-table source options you use. The `community-connector` CLI derives this from the spec automatically; set it manually when creating the connection in the UI. | see below |

The full recommended `externalOptionsAllowList` value is:
`tableName,tableNameList,tableConfigs,isDeleteFlow,query,cluster_id,initial_lookback_seconds,incremental_overlap_seconds,window_seconds,page_size,max_concurrency,max_records_per_batch,request_timeout_seconds,allow_truncated_seconds`

> **Note**: per-table options set via `table_configuration` in the pipeline spec must be in `externalOptionsAllowList` or they are **silently dropped** by the framework. With `query`/`cluster_id` set on the connection, the default pipeline spec needs no per-table options at all.

### Obtaining the Required Parameters

1. Sign in to Tsuga.
2. Navigate to **Settings → Operation API keys**.
3. Create a key scoped to the teams whose logs you want to ingest.
4. Copy the generated key and store it securely. Use this as the `operation_api_key` connection option.

### Create a Unity Catalog Connection

1. Follow the **Lakeflow Community Connector** UI flow from the **Add Data** page.
2. Select an existing connection for this source or create a new one with `operation_api_key` (and optionally `base_url`).
3. Set `externalOptionsAllowList` to the full option list above (required for this connector to pass table-specific options).

The connection can also be created using the standard Unity Catalog API.

## Supported Objects

The Tsuga Logs connector exposes a **static list** of tables:

- `logs`

### Object summary, primary keys, and ingestion mode

| Table | Description | Ingestion Type | Primary Key | Incremental Cursor |
|---|---|---|---|---|
| `logs` | Log events matching a Tsuga query | `cdc` | `raw_json` | `event_time` (second granularity) |

## Table Configurations

### Source & Destination

These are set directly under each `table` object in the pipeline spec:

| Option | Required | Description |
|---|---|---|
| `source_table` | Yes | Must be `logs` |
| `destination_catalog` | No | Target catalog (defaults to pipeline's default) |
| `destination_schema` | No | Target schema (defaults to pipeline's default) |
| `destination_table` | No | Target table name (defaults to `source_table`) |

### Source-specific `table_configuration` options

All of these are optional when `default_query`/`default_cluster_id` are set on the connection — the minimal pipeline spec is then just `{"table": {"source_table": "logs"}}`. Per-table `query`/`cluster_id` override the connection defaults (distinct key names, so the framework's no-override rule for connection-stored options never triggers).

| Option | Required | Default | Description |
|---|---|---|---|
| `query` | Yes, here or `default_query` on the connection | none — must be explicit (`'*'` allowed) | Tsuga query string used to filter logs (e.g. `context.service.name:payments level:ERROR`). See the [query syntax documentation](https://app.tsuga.com/documentation/explore/query-syntax). |
| `cluster_id` | No | connection-level `default_cluster_id`, else org default cluster | ID of the Tsuga cluster to target. **Required for multi-cluster organizations.** |
| `initial_lookback_seconds` | No | `3600` | Initial backfill window when no cursor exists yet. |
| `incremental_overlap_seconds` | No | `60` | How far to rewind the saved cursor at the start of each run, to pick up late-arriving events. |
| `window_seconds` | No | `300` | Size of each incremental time window. |
| `page_size` | No | `1000` | `maxResults` per API call (the public API caps this at 1000). |
| `max_concurrency` | No | `1` | Number of windows fetched concurrently in one read. |
| `max_records_per_batch` | No | unset | Caps the number of records returned in one read call. |
| `request_timeout_seconds` | No | `60` | Per-request timeout. |
| `allow_truncated_seconds` | No | `false` | When `true`, a single second whose volume exceeds the per-call cap is ingested partially (newest 1000 events) instead of failing the sync. Use for high-volume queries where occasional sub-second loss is acceptable. |

## Output Schema

The connector emits a stable schema; heterogeneous log payloads are preserved in `raw_json` so they do not force schema churn in the Delta table.

| Column | Type | Nullable | Description |
|---|---|---|---|
| `event_time` | `TIMESTAMP` | no | Log event timestamp |
| `level` | `STRING` | yes | Log level (`INFO`, `ERROR`, ...) |
| `message` | `STRING` | yes | Log message |
| `service_name` | `STRING` | yes | Extracted from `context.service.name` |
| `team` | `STRING` | yes | Extracted from `context.team` |
| `env` | `STRING` | yes | Extracted from `context.env` |
| `raw_json` | `STRING` | no | Full log event as canonical JSON (primary key) |
| `extracted_at` | `TIMESTAMP` | no | When the connector extracted the event |

## Incremental Sync and Pagination

- The public `GET /v1/logs/search` endpoint caps `maxResults` at `1000` and does not expose a cursor token. The connector exhausts a time range by recursively splitting it into smaller half-open windows `[from, to)` until each call returns fewer than the per-call cap.
- Each run rewinds the saved cursor by `incremental_overlap_seconds` to pick up late-arriving events. Because the connector uses `raw_json` as the CDC key, re-read events collapse in the destination table instead of duplicating.
- If a single one-second slice itself exceeds the per-call cap, the public endpoint cannot exhaust that range and the connector fails explicitly. Narrow the `query`, reduce per-second volume, or set `allow_truncated_seconds` to accept partial data for such seconds.
- The public API enforces a per-key rate limit (120 requests/minute). The client throttles itself to one request per second per pipeline and retries `429`/`5xx` responses with exponential backoff.

## How to Run

Configure a `pipeline_spec` that references a Unity Catalog connection using this connector and the `logs` table:

```json
{
  "pipeline_spec": {
    "connection_name": "tsuga_logs_connection",
    "object": [
      {
        "table": {
          "source_table": "logs",
          "destination_table": "tsuga_error_logs",
          "table_configuration": {
            "query": "context.service.name:payments level:ERROR",
            "initial_lookback_seconds": "86400",
            "window_seconds": "300",
            "max_concurrency": "4"
          }
        }
      }
    ]
  }
}
```

- `connection_name` must point to the UC connection configured with your `operation_api_key`.
- `query` is required; start with a narrow, low-volume query and a short lookback window to validate the end-to-end flow before increasing volume.
- On the **first run**, the connector backfills `initial_lookback_seconds` of history. On **subsequent runs**, it resumes from the stored cursor plus the overlap window.
- Schedule the pipeline with your standard Databricks orchestration (e.g. a Job with a `pipeline_task`).

## References

- [Tsuga query syntax](https://app.tsuga.com/documentation/explore/query-syntax)
- [Tsuga logs exploration](https://app.tsuga.com/documentation/explore/logs)
- [Operation API keys guide](https://app.tsuga.com/documentation/account-and-settings/guides/how-to-create-and-use-an-operation-key)
- [`tsuga_logs_api_doc.md`](tsuga_logs_api_doc.md) — source API research notes
