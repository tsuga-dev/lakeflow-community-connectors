from __future__ import annotations

import sys
import unittest
from pathlib import Path
from unittest.mock import patch

import requests

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from databricks.labs.community_connector.sources.tsuga_logs.client import (  # noqa: E402
    TsugaPublicLogsClient,
    build_resume_offset,
    filter_records_from_offset,
    iter_windows,
    normalize_log,
)


def _log(timestamp_ms: int, *, message: str) -> dict:
    return {
        "timestamp": timestamp_ms,
        "level": "INFO",
        "message": message,
        "context": {
            "team": "platform",
            "env": "prod",
            "service": {"name": "api"},
        },
    }


class FakeSearch:
    def __init__(self, logs: list[dict]) -> None:
        self.logs = logs
        self.calls: list[dict] = []

    def __call__(self, params):
        self.calls.append(dict(params))
        max_results = int(params["maxResults"])
        from_seconds = int(params["from"])
        to_seconds = int(params["to"])
        filtered = [
            log
            for log in self.logs
            if from_seconds <= (int(log["timestamp"]) // 1000) <= to_seconds
        ]
        filtered.sort(key=lambda log: int(log["timestamp"]), reverse=True)
        return {"logs": filtered[:max_results]}


class FakeResponse:
    def __init__(
        self,
        status_code: int,
        body: dict | None = None,
        headers: dict[str, str] | None = None,
    ) -> None:
        self.status_code = status_code
        self._body = body or {}
        self.headers = headers or {}

    def json(self) -> dict:
        return self._body

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise requests.HTTPError(f"{self.status_code} error", response=self)


class TsugaLogsClientTests(unittest.TestCase):
    def test_iter_windows_batches_up_to_max_concurrency(self) -> None:
        windows = iter_windows(
            start_seconds=10,
            window_seconds=5,
            max_concurrency=3,
            end_exclusive_seconds=24,
        )
        self.assertEqual(windows, [(10, 15), (15, 20), (20, 24)])

    def test_fetch_window_recursively_splits_saturated_ranges(self) -> None:
        logs = [
            _log(5_000, message="m5"),
            _log(4_000, message="m4"),
            _log(3_000, message="m3"),
            _log(2_000, message="m2"),
            _log(1_000, message="m1"),
        ]
        fake_search = FakeSearch(logs)
        client = TsugaPublicLogsClient(
            "https://api.tsuga.com",
            "tsuga_op:test",
            search_fn=fake_search,
        )

        records = client.fetch_window(
            from_seconds=1,
            to_exclusive_seconds=6,
            query="*",
            page_size=2,
            extracted_at="2026-03-30T12:00:00+00:00",
        )

        self.assertEqual([record.message for record in records], ["m1", "m2", "m3", "m4", "m5"])
        self.assertTrue(all(call["maxResults"] == 2 for call in fake_search.calls))
        self.assertGreater(len(fake_search.calls), 1)

    def test_fetch_window_passes_cluster_id_only_when_provided(self) -> None:
        fake_search = FakeSearch([_log(1_500, message="m")])
        client = TsugaPublicLogsClient(
            "https://api.tsuga.com",
            "tsuga_op:test",
            search_fn=fake_search,
        )

        client.fetch_window(
            from_seconds=1,
            to_exclusive_seconds=2,
            query="*",
            page_size=10,
            extracted_at="2026-03-30T12:00:00+00:00",
        )
        client.fetch_window(
            from_seconds=1,
            to_exclusive_seconds=2,
            query="*",
            page_size=10,
            extracted_at="2026-03-30T12:00:00+00:00",
            cluster_id="cluster-a",
        )

        self.assertNotIn("clusterId", fake_search.calls[0])
        self.assertEqual(fake_search.calls[1]["clusterId"], "cluster-a")

    def test_fetch_window_raises_when_one_second_itself_saturates(self) -> None:
        logs = [
            _log(1_100, message="a"),
            _log(1_200, message="b"),
            _log(1_300, message="c"),
        ]
        fake_search = FakeSearch(logs)
        client = TsugaPublicLogsClient(
            "https://api.tsuga.com",
            "tsuga_op:test",
            search_fn=fake_search,
        )

        with self.assertRaises(RuntimeError):
            client.fetch_window(
                from_seconds=1,
                to_exclusive_seconds=2,
                query="*",
                page_size=2,
                extracted_at="2026-03-30T12:00:00+00:00",
            )

    def test_fetch_window_truncates_saturated_second_when_allowed(self) -> None:
        logs = [
            _log(1_100, message="a"),
            _log(1_200, message="b"),
            _log(1_300, message="c"),
        ]
        fake_search = FakeSearch(logs)
        client = TsugaPublicLogsClient(
            "https://api.tsuga.com",
            "tsuga_op:test",
            search_fn=fake_search,
        )

        records = client.fetch_window(
            from_seconds=1,
            to_exclusive_seconds=2,
            query="*",
            page_size=2,
            extracted_at="2026-03-30T12:00:00+00:00",
            allow_truncated_seconds=True,
        )

        self.assertEqual(len(records), 2)

    def test_fetch_window_preserves_boundary_second_records_exactly_once(self) -> None:
        logs = [
            _log(1_100, message="s1"),
            _log(2_100, message="s2a"),
            _log(2_900, message="s2b"),
            _log(3_100, message="s3"),
        ]
        fake_search = FakeSearch(logs)
        client = TsugaPublicLogsClient(
            "https://api.tsuga.com",
            "tsuga_op:test",
            search_fn=fake_search,
        )

        records = client.fetch_window(
            from_seconds=1,
            to_exclusive_seconds=4,
            query="*",
            page_size=3,
            extracted_at="2026-03-30T12:00:00+00:00",
        )

        self.assertEqual([record.message for record in records], ["s1", "s2a", "s2b", "s3"])
        self.assertEqual(len({record.fingerprint for record in records}), 4)

    def test_build_resume_offset_tracks_all_fingerprints_at_last_second(self) -> None:
        extracted_at = "2026-03-30T12:00:00+00:00"
        records = [
            normalize_log(_log(1_000, message="older"), extracted_at),
            normalize_log(_log(2_100, message="a"), extracted_at),
            normalize_log(_log(2_700, message="b"), extracted_at),
        ]

        offset = build_resume_offset(records)

        self.assertEqual(offset["cursor_seconds"], 2)
        self.assertEqual(len(offset["seen_fingerprints_at_cursor"]), 2)

    def test_filter_records_from_offset_skips_seen_records_at_cursor_second(self) -> None:
        extracted_at = "2026-03-30T12:00:00+00:00"
        seen = normalize_log(_log(5_100, message="seen"), extracted_at)
        keep_same_second = normalize_log(_log(5_200, message="keep"), extracted_at)
        keep_later = normalize_log(_log(6_000, message="later"), extracted_at)

        filtered = filter_records_from_offset(
            [seen, keep_same_second, keep_later],
            cursor_seconds=5,
            seen_fingerprints_at_cursor={seen.fingerprint},
        )

        self.assertEqual([record.message for record in filtered], ["keep", "later"])

    def test_search_retries_rate_limits_until_success(self) -> None:
        responses = [
            FakeResponse(429, headers={"Retry-After": "0"}),
            FakeResponse(200, body={"logs": []}),
        ]
        client = TsugaPublicLogsClient(
            "https://api.tsuga.com",
            "tsuga_op:test",
            min_request_interval_seconds=0,
        )

        with (
            patch(
                "databricks.labs.community_connector.sources.tsuga_logs.client.requests.get",
                side_effect=responses,
            ) as get_mock,
            patch(
                "databricks.labs.community_connector.sources.tsuga_logs.client.time.sleep"
            ) as sleep_mock,
        ):
            body = client._search({"from": 1, "to": 2, "query": "*", "maxResults": 1})

        self.assertEqual(body, {"logs": []})
        self.assertEqual(get_mock.call_count, 2)
        sleep_mock.assert_called_once_with(0.0)

    def test_search_surfaces_api_error_message_on_http_error(self) -> None:
        for body in (
            {"error": {"message": "clusterId must be provided"}},
            {"statusCode": 400, "error": "Bad Request", "message": "clusterId must be provided"},
        ):
            client = TsugaPublicLogsClient(
                "https://api.tsuga.com",
                "tsuga_op:test",
                min_request_interval_seconds=0,
            )

            with patch(
                "databricks.labs.community_connector.sources.tsuga_logs.client.requests.get",
                return_value=FakeResponse(400, body=body),
            ):
                with self.assertRaises(requests.HTTPError) as ctx:
                    client._search({"from": 1, "to": 2, "query": "*", "maxResults": 1})

            self.assertIn("clusterId must be provided", str(ctx.exception))

    def test_search_raises_after_exhausting_rate_limit_retries(self) -> None:
        responses = [
            FakeResponse(429, headers={"Retry-After": "0"}),
            FakeResponse(429, headers={"Retry-After": "0"}),
        ]
        client = TsugaPublicLogsClient(
            "https://api.tsuga.com",
            "tsuga_op:test",
            retry_max_attempts=2,
            min_request_interval_seconds=0,
        )

        with (
            patch(
                "databricks.labs.community_connector.sources.tsuga_logs.client.requests.get",
                side_effect=responses,
            ) as get_mock,
            patch(
                "databricks.labs.community_connector.sources.tsuga_logs.client.time.sleep"
            ) as sleep_mock,
        ):
            with self.assertRaises(requests.HTTPError):
                client._search({"from": 1, "to": 2, "query": "*", "maxResults": 1})

        self.assertEqual(get_mock.call_count, 2)
        sleep_mock.assert_called_once_with(0.0)


if __name__ == "__main__":
    unittest.main()
