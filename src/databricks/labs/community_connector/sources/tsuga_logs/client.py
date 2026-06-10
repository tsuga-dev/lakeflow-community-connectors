import hashlib
import json
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from threading import Lock
from typing import Any, Callable, Iterable, Mapping

import requests

JsonDict = dict[str, Any]
SearchFn = Callable[[Mapping[str, Any]], JsonDict]
MAX_PUBLIC_LOGS_PER_CALL = 1000
DEFAULT_RETRY_MAX_ATTEMPTS = 20
DEFAULT_RETRY_BACKOFF_SECONDS = 1.0
DEFAULT_MAX_RETRY_BACKOFF_SECONDS = 60.0
DEFAULT_MIN_REQUEST_INTERVAL_SECONDS = 1.0


def _to_iso8601_utc(timestamp_ms: int) -> str:
    return datetime.fromtimestamp(timestamp_ms / 1000, tz=timezone.utc).isoformat()


def _parse_retry_after_seconds(value: str | None) -> float | None:
    if value is None:
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    if parsed < 0:
        return None
    return parsed


def _extract_error_message(response: requests.Response) -> str | None:
    try:
        body = response.json()
    except ValueError:
        return None
    if not isinstance(body, dict):
        return None
    error = body.get("error")
    if isinstance(error, dict) and error.get("message") is not None:
        return str(error["message"])
    if body.get("message") is not None:
        return str(body["message"])
    return None


def _extract_service_name(log: Mapping[str, Any]) -> str | None:
    context = log.get("context", {})
    if isinstance(context, dict):
        service = context.get("service")
        if isinstance(service, dict):
            name = service.get("name")
            if name is not None:
                return str(name)
        if context.get("service_name") is not None:
            return str(context["service_name"])
    if log.get("service_name") is not None:
        return str(log["service_name"])
    return None


def _extract_team(log: Mapping[str, Any]) -> str | None:
    context = log.get("context", {})
    if isinstance(context, dict) and context.get("team") is not None:
        return str(context["team"])
    return None


def _extract_env(log: Mapping[str, Any]) -> str | None:
    context = log.get("context", {})
    if isinstance(context, dict) and context.get("env") is not None:
        return str(context["env"])
    return None


@dataclass(frozen=True)
class NormalizedLogRecord:
    event_time_ms: int
    event_time_sec: int
    level: str | None
    message: str | None
    service_name: str | None
    team: str | None
    env: str | None
    raw_json: str
    extracted_at: str
    fingerprint: str

    def to_row(self) -> dict[str, Any]:
        return {
            "event_time": _to_iso8601_utc(self.event_time_ms),
            "level": self.level,
            "message": self.message,
            "service_name": self.service_name,
            "team": self.team,
            "env": self.env,
            "raw_json": self.raw_json,
            "extracted_at": self.extracted_at,
        }


def normalize_log(log: Mapping[str, Any], extracted_at: str) -> NormalizedLogRecord:
    raw_json = json.dumps(log, sort_keys=True, default=str, separators=(",", ":"))
    timestamp_ms = int(log["timestamp"])
    return NormalizedLogRecord(
        event_time_ms=timestamp_ms,
        event_time_sec=timestamp_ms // 1000,
        level=str(log["level"]) if log.get("level") is not None else None,
        message=str(log["message"]) if log.get("message") is not None else None,
        service_name=_extract_service_name(log),
        team=_extract_team(log),
        env=_extract_env(log),
        raw_json=raw_json,
        extracted_at=extracted_at,
        fingerprint=hashlib.sha256(raw_json.encode("utf-8")).hexdigest(),
    )


def iter_windows(
    start_seconds: int,
    window_seconds: int,
    max_concurrency: int,
    end_exclusive_seconds: int,
) -> list[tuple[int, int]]:
    if window_seconds <= 0:
        raise ValueError("window_seconds must be > 0")
    if max_concurrency <= 0:
        raise ValueError("max_concurrency must be > 0")

    windows: list[tuple[int, int]] = []
    cursor = start_seconds
    for _ in range(max_concurrency):
        if cursor >= end_exclusive_seconds:
            break
        end_cursor = min(cursor + window_seconds, end_exclusive_seconds)
        windows.append((cursor, end_cursor))
        cursor = end_cursor
    return windows


def filter_records_from_offset(
    records: Iterable[NormalizedLogRecord],
    cursor_seconds: int,
    seen_fingerprints_at_cursor: set[str],
) -> list[NormalizedLogRecord]:
    filtered: list[NormalizedLogRecord] = []
    for record in records:
        if record.event_time_sec < cursor_seconds:
            continue
        if (
            record.event_time_sec == cursor_seconds
            and record.fingerprint in seen_fingerprints_at_cursor
        ):
            continue
        filtered.append(record)
    return filtered


def build_resume_offset(records: list[NormalizedLogRecord]) -> dict[str, Any]:
    if not records:
        raise ValueError("build_resume_offset requires at least one record")
    last_second = records[-1].event_time_sec
    seen = sorted(
        {record.fingerprint for record in records if record.event_time_sec == last_second}
    )
    return {
        "cursor_seconds": last_second,
        "seen_fingerprints_at_cursor": seen,
    }


class TsugaPublicLogsClient:
    """Thin wrapper over Tsuga's public logs search API.

    The public API exposes `maxResults` but caps it at 1000 and does not expose
    a cursor token. To exhaust a time range, this client recursively splits the
    range into smaller half-open windows until each call returns fewer than the
    per-call cap.
    """

    def __init__(
        self,
        base_url: str,
        operation_api_key: str,
        *,
        request_timeout_seconds: float = 60.0,
        retry_max_attempts: int = DEFAULT_RETRY_MAX_ATTEMPTS,
        retry_backoff_seconds: float = DEFAULT_RETRY_BACKOFF_SECONDS,
        max_retry_backoff_seconds: float = DEFAULT_MAX_RETRY_BACKOFF_SECONDS,
        min_request_interval_seconds: float = DEFAULT_MIN_REQUEST_INTERVAL_SECONDS,
        search_fn: SearchFn | None = None,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.operation_api_key = operation_api_key
        self.request_timeout_seconds = request_timeout_seconds
        self.retry_max_attempts = max(1, retry_max_attempts)
        self.retry_backoff_seconds = max(0.0, retry_backoff_seconds)
        self.max_retry_backoff_seconds = max(0.0, max_retry_backoff_seconds)
        self.min_request_interval_seconds = max(0.0, min_request_interval_seconds)
        self._search_fn = search_fn
        self._request_lock = Lock()
        self._next_request_monotonic = 0.0

    def _search(self, params: Mapping[str, Any]) -> JsonDict:
        if self._search_fn is not None:
            return self._search_fn(params)

        attempt = 0
        while True:
            try:
                self._apply_request_rate_limit()
                response = requests.get(
                    f"{self.base_url}/v1/logs/search",
                    headers={
                        "Accept": "application/json",
                        "Authorization": f"Bearer {self.operation_api_key}",
                    },
                    params=params,
                    timeout=self.request_timeout_seconds,
                )
            except requests.RequestException:
                if not self._should_retry(attempt):
                    raise
                time.sleep(self._retry_delay_seconds(None, attempt))
                attempt += 1
                continue

            if self._should_retry(attempt) and self._is_retryable_status(response.status_code):
                time.sleep(self._retry_delay_seconds(response, attempt))
                attempt += 1
                continue

            try:
                response.raise_for_status()
            except requests.HTTPError as error:
                detail = _extract_error_message(response)
                if detail is not None:
                    raise requests.HTTPError(
                        f"{error}: {detail}", response=response
                    ) from None
                raise
            return response.json()

    @staticmethod
    def _is_retryable_status(status_code: int) -> bool:
        return status_code == 429 or status_code == 408 or 500 <= status_code < 600

    def _should_retry(self, attempt: int) -> bool:
        return attempt < self.retry_max_attempts - 1

    def _retry_delay_seconds(self, response: requests.Response | None, attempt: int) -> float:
        if response is not None:
            retry_after = _parse_retry_after_seconds(response.headers.get("Retry-After"))
            if retry_after is not None:
                return min(retry_after, self.max_retry_backoff_seconds)

        delay = self.retry_backoff_seconds * (2**attempt)
        return min(delay, self.max_retry_backoff_seconds)

    def _apply_request_rate_limit(self) -> None:
        if self.min_request_interval_seconds <= 0:
            return

        with self._request_lock:
            now = time.monotonic()
            wait_seconds = self._next_request_monotonic - now
            if wait_seconds > 0:
                time.sleep(wait_seconds)
            self._next_request_monotonic = time.monotonic() + self.min_request_interval_seconds

    @staticmethod
    def _extract_logs(body: JsonDict) -> list[JsonDict]:
        if not isinstance(body, dict):
            raise RuntimeError("Unexpected Tsuga logs response payload")

        if "logs" in body:
            logs = body["logs"]
        else:
            data = body.get("data")
            if not isinstance(data, dict) or "logs" not in data:
                raise RuntimeError("Unexpected Tsuga logs response payload")
            logs = data["logs"]

        if not isinstance(logs, list):
            raise RuntimeError("Unexpected Tsuga logs response payload")
        return logs

    def fetch_window(
        self,
        *,
        from_seconds: int,
        to_exclusive_seconds: int,
        query: str,
        page_size: int,
        extracted_at: str,
        cluster_id: str | None = None,
        allow_truncated_seconds: bool = False,
    ) -> list[NormalizedLogRecord]:
        if to_exclusive_seconds <= from_seconds:
            return []

        effective_page_size = min(page_size, MAX_PUBLIC_LOGS_PER_CALL)
        collected = self._fetch_range_recursive(
            from_seconds=from_seconds,
            to_exclusive_seconds=to_exclusive_seconds,
            query=query,
            page_size=effective_page_size,
            extracted_at=extracted_at,
            cluster_id=cluster_id,
            allow_truncated_seconds=allow_truncated_seconds,
        )

        collected.sort(key=lambda record: (record.event_time_ms, record.fingerprint))
        return collected

    def _fetch_range_recursive(
        self,
        *,
        from_seconds: int,
        to_exclusive_seconds: int,
        query: str,
        page_size: int,
        extracted_at: str,
        cluster_id: str | None,
        allow_truncated_seconds: bool,
    ) -> list[NormalizedLogRecord]:
        params: dict[str, Any] = {
            "from": from_seconds,
            "to": to_exclusive_seconds - 1,
            "query": query,
            "maxResults": page_size,
        }
        if cluster_id is not None:
            params["clusterId"] = cluster_id
        body = self._search(params)
        logs = self._extract_logs(body)

        if len(logs) < page_size:
            return [normalize_log(log, extracted_at) for log in logs]

        if to_exclusive_seconds - from_seconds <= 1:
            if allow_truncated_seconds:
                return [normalize_log(log, extracted_at) for log in logs]
            raise RuntimeError(
                "Tsuga public logs API saturated a 1-second window at the per-call "
                f"limit ({page_size}). This query cannot be fully paginated with the "
                "public endpoint. Narrow the query, reduce per-second volume, or set "
                "'allow_truncated_seconds' to accept partial data for such seconds."
            )

        midpoint = from_seconds + max(1, (to_exclusive_seconds - from_seconds) // 2)
        if midpoint >= to_exclusive_seconds:
            midpoint = to_exclusive_seconds - 1

        left = self._fetch_range_recursive(
            from_seconds=from_seconds,
            to_exclusive_seconds=midpoint,
            query=query,
            page_size=page_size,
            extracted_at=extracted_at,
            cluster_id=cluster_id,
            allow_truncated_seconds=allow_truncated_seconds,
        )
        right = self._fetch_range_recursive(
            from_seconds=midpoint,
            to_exclusive_seconds=to_exclusive_seconds,
            query=query,
            page_size=page_size,
            extracted_at=extracted_at,
            cluster_id=cluster_id,
            allow_truncated_seconds=allow_truncated_seconds,
        )
        return left + right
