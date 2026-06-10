"""Serve ``GET /v1/logs/search`` against the logs corpus.

Mirrors the real endpoint's semantics that the generic spec roles can't
express: ``from``/``to`` arrive in epoch seconds while records carry epoch
milliseconds, results are returned newest-first and truncated at
``maxResults`` with no cursor.

Corpus timestamps are rebased once per process so the newest record lands
30 seconds before the first request's wall-clock time — inside the
connector's default lookback window — while preserving relative spacing.
"""

from __future__ import annotations

import json
import time
from typing import Any, Dict, List
from urllib.parse import parse_qsl, urlsplit

from requests.models import PreparedRequest, Response

from databricks.labs.community_connector.source_simulator.cassette import ResponseRecord
from databricks.labs.community_connector.source_simulator.interceptor import (
    response_from_record,
)

MAX_RESULTS_DEFAULT = 100
MAX_RESULTS_CAP = 1000

_rebase_delta_ms: List[int] = []  # set on first call, stable for the process


def _rebased_logs(corpus) -> List[Dict[str, Any]]:
    records = corpus.get("logs") or []
    if not _rebase_delta_ms:
        newest_ts = max(int(r["timestamp"]) for r in records)
        _rebase_delta_ms.append(int(time.time() * 1000) - 30_000 - newest_ts)
    delta = _rebase_delta_ms[0]
    return [{**r, "timestamp": int(r["timestamp"]) + delta} for r in records]


def search_logs(prep: PreparedRequest, spec, corpus) -> Response:  # noqa: ARG001
    query = dict(parse_qsl(urlsplit(prep.url or "").query))
    if "from" not in query or "to" not in query:
        return _json_response(
            prep, 400, {"code": "BAD_REQUEST", "message": "from and to are required"}
        )

    from_ms = int(query["from"]) * 1000
    to_exclusive_ms = (int(query["to"]) + 1) * 1000
    max_results = min(int(query.get("maxResults", MAX_RESULTS_DEFAULT)), MAX_RESULTS_CAP)

    logs = [r for r in _rebased_logs(corpus) if from_ms <= int(r["timestamp"]) < to_exclusive_ms]
    logs.sort(key=lambda r: int(r["timestamp"]), reverse=True)
    return _json_response(prep, 200, {"logs": logs[:max_results]})


def _json_response(prep: PreparedRequest, status_code: int, body: dict) -> Response:
    rec = ResponseRecord(
        status_code=status_code,
        headers={"Content-Type": "application/json"},
        body_text=json.dumps(body),
        body_b64=None,
        encoding="utf-8",
        url=prep.url,
    )
    return response_from_record(rec, prep)
