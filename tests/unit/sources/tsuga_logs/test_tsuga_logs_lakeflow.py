from __future__ import annotations

import pickle
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from databricks.labs.community_connector.sources.tsuga_logs.tsuga_logs import (  # noqa: E402
    TsugaLogsLakeflowConnect,
)


def _connector() -> TsugaLogsLakeflowConnect:
    return TsugaLogsLakeflowConnect(
        {
            "operation_api_key": "tsuga_op:test",
            "base_url": "https://api.tsuga.com",
        }
    )


class TsugaLogsLakeflowConnectTests(unittest.TestCase):
    def test_connector_instance_is_pickleable(self) -> None:
        connector = _connector()

        serialized = pickle.dumps(connector)
        restored = pickle.loads(serialized)

        self.assertEqual(restored._base_url, "https://api.tsuga.com")
        self.assertEqual(restored._operation_api_key, "tsuga_op:test")

    def test_read_table_terminates_without_refetch_when_caught_up(self) -> None:
        connector = _connector()
        caught_up_offset = {
            "cursor_seconds": connector._init_end_exclusive_seconds,
            "seen_fingerprints_at_cursor": [],
        }

        records, end_offset = connector.read_table("logs", caught_up_offset, {"query": "*"})

        self.assertEqual(list(records), [])
        self.assertEqual(end_offset, caught_up_offset)

    def test_get_start_seconds_rewinds_only_across_runs(self) -> None:
        connector = _connector()
        previous_run_cursor = connector._init_end_exclusive_seconds - 1000

        rewound = connector._get_start_seconds(
            {"cursor_seconds": previous_run_cursor, "seen_fingerprints_at_cursor": []},
            initial_lookback_seconds=3600,
            incremental_overlap_seconds=60,
        )
        self.assertEqual(rewound, previous_run_cursor - 60)

        mid_run_resume = connector._get_start_seconds(
            {"cursor_seconds": previous_run_cursor, "seen_fingerprints_at_cursor": ["fp"]},
            initial_lookback_seconds=3600,
            incremental_overlap_seconds=60,
        )
        self.assertEqual(mid_run_resume, previous_run_cursor)

    def test_parse_table_options_requires_query(self) -> None:
        connector = _connector()

        with self.assertRaises(ValueError):
            connector._parse_table_options({})

    def test_parse_table_options_defaults_cluster_id_to_none(self) -> None:
        connector = _connector()

        options = connector._parse_table_options({"query": "*"})
        self.assertIsNone(options.cluster_id)

        options = connector._parse_table_options({"query": "*", "cluster_id": "cluster-a"})
        self.assertEqual(options.cluster_id, "cluster-a")

    def test_connection_level_defaults_apply_unless_table_overrides(self) -> None:
        connector = TsugaLogsLakeflowConnect(
            {
                "operation_api_key": "tsuga_op:test",
                "default_query": "context.team:central",
                "default_cluster_id": "cluster-conn",
            }
        )

        defaults = connector._parse_table_options({})
        self.assertEqual(defaults.query, "context.team:central")
        self.assertEqual(defaults.cluster_id, "cluster-conn")

        overridden = connector._parse_table_options(
            {"query": "level:ERROR", "cluster_id": "cluster-table"}
        )
        self.assertEqual(overridden.query, "level:ERROR")
        self.assertEqual(overridden.cluster_id, "cluster-table")


if __name__ == "__main__":
    unittest.main()
