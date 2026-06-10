# Tsuga Public Logs API — Research Notes

Read API surface used by the `tsuga_logs` connector.

## Base URL and Versioning

- Default base URL: `https://api.tsuga.com`
- All public endpoints are versioned under `/v1/`.

## Authentication

- Header: `Authorization: Bearer <operation_api_key>`
- Operation API keys are created in Tsuga under **Settings → Operation API keys** and have the shape `tsuga_op:<version>:<org_id>:<random>`.
- The key's team scoping determines which logs are visible; the API applies team-based access control server-side.
- Keys can expire; an expired or invalid key returns `401`.

## Endpoint: Search Logs

```
GET /v1/logs/search
```

| Query param | Type | Required | Description |
|---|---|---|---|
| `from` | number (epoch seconds) | yes | Start of the time range (inclusive) |
| `to` | number (epoch seconds) | yes | End of the time range (inclusive) |
| `query` | string | no | Tsuga query syntax filter (defaults to `*`) |
| `maxResults` | integer | no | 1–1000, defaults to 100 |
| `clusterId` | string | no* | Target cluster. *Required when the organization has multiple clusters; omit to use the default cluster. |

Response:

```json
{
  "logs": [
    {
      "timestamp": 1704067200000,
      "level": "INFO",
      "message": "Request completed successfully",
      "context": {
        "team": "platform",
        "service": { "name": "api" },
        "env": "prod"
      }
    }
  ]
}
```

- `timestamp` is epoch **milliseconds**; `from`/`to` request params are epoch **seconds**.
- `context` is open-ended (`additionalProperties: true`); `message` may be any JSON value due to log processing rules.
- Results are returned newest-first, truncated at `maxResults`.

## Pagination

The endpoint exposes **no cursor or offset**: `maxResults` only truncates the response. When a call returns exactly `maxResults` rows, the time range may contain more data.

The connector therefore exhausts a range by recursive bisection: split `[from, to)` in half and re-query until every call returns fewer than `maxResults` rows. A single one-second slice that still saturates the cap cannot be exhausted through this endpoint and is surfaced as an explicit error.

## Rate Limiting

- Per-key limit: ~120 requests/minute on public telemetry endpoints.
- Exceeding it returns `429` with body `{"code": "RATE_LIMITED", ...}` and **no `Retry-After` header**.
- Connector behavior: self-throttle to 1 request/second and retry `408`/`429`/`5xx` with capped exponential backoff.

## Error Shapes

| Status | Meaning |
|---|---|
| `400` | Invalid query/params (e.g. missing `from`/`to`, `maxResults` out of range, missing `clusterId` on multi-cluster orgs) |
| `401` | Missing/invalid/expired operation API key |
| `429` | Rate limited |
| `5xx` | Transient server errors (retried) |

## Other Log Endpoints (not used by the connector)

- `GET /v1/logs/patterns` — aggregated log patterns for a query/time range.
- `GET /v1/logs/patterns/new` — newly appearing error patterns.
- `GET /v1/logs/patterns/increase` — error patterns with volume increases.
- `GET /v1/logs/attributes` — discoverable log attribute names.

These return aggregates rather than raw events and do not fit row-level ingestion.
