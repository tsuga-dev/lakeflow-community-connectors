from __future__ import annotations

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from databricks.labs.community_connector.sources.tsuga_logs.client import (
    normalize_log,  # noqa: E402
)
from databricks.labs.community_connector.sources.tsuga_logs.incremental_read import (  # noqa: E402
    collect_windowed_records,
)


def _log(timestamp_ms: int, *, message: str) -> dict:
    return {
        "timestamp": timestamp_ms,
        "level": "INFO",
        "message": message,
        "context": {
            "team": "central",
            "env": "prod",
            "service": {"name": "cluster-health"},
        },
    }


class TsugaLogsIncrementalReadTests(unittest.TestCase):
    def test_collect_windowed_records_walks_all_window_batches_until_end(self) -> None:
        extracted_at = "2026-03-30T15:00:00+00:00"
        records_by_window = {
            (1, 3): [normalize_log(_log(1_100, message="w1"), extracted_at)],
            (3, 5): [normalize_log(_log(3_100, message="w2"), extracted_at)],
            (5, 7): [normalize_log(_log(5_100, message="w3"), extracted_at)],
        }
        fetch_calls: list[list[tuple[int, int]]] = []

        def fake_fetch_windows(windows: list[tuple[int, int]]):
            fetch_calls.append(list(windows))
            return [records_by_window[bounds] for bounds in windows]

        result = collect_windowed_records(
            start_seconds=1,
            end_exclusive_seconds=7,
            window_seconds=2,
            max_concurrency=1,
            max_events=None,
            seen_fingerprints_at_cursor=set(),
            fetch_windows=fake_fetch_windows,
        )

        self.assertEqual(fetch_calls, [[(1, 3)], [(3, 5)], [(5, 7)]])
        self.assertEqual([record.message for record in result.records], ["w1", "w2", "w3"])
        self.assertEqual(result.next_cursor_seconds, 7)
        self.assertIsNone(result.resume_offset)

    def test_collect_windowed_records_returns_resume_offset_when_truncated(self) -> None:
        extracted_at = "2026-03-30T15:00:00+00:00"
        records_by_window = {
            (1, 3): [
                normalize_log(_log(1_100, message="a"), extracted_at),
                normalize_log(_log(2_100, message="b"), extracted_at),
            ],
            (3, 5): [
                normalize_log(_log(3_100, message="c"), extracted_at),
                normalize_log(_log(4_100, message="d"), extracted_at),
            ],
        }

        def fake_fetch_windows(windows: list[tuple[int, int]]):
            return [records_by_window[bounds] for bounds in windows]

        result = collect_windowed_records(
            start_seconds=1,
            end_exclusive_seconds=5,
            window_seconds=2,
            max_concurrency=1,
            max_events=3,
            seen_fingerprints_at_cursor=set(),
            fetch_windows=fake_fetch_windows,
        )

        self.assertEqual([record.message for record in result.records], ["a", "b", "c"])
        self.assertIsNotNone(result.resume_offset)
        assert result.resume_offset is not None
        self.assertEqual(result.resume_offset["cursor_seconds"], 3)


if __name__ == "__main__":
    unittest.main()
