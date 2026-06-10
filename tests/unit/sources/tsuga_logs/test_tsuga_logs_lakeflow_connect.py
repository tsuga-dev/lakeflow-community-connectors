from databricks.labs.community_connector.sources.tsuga_logs.tsuga_logs import (
    TsugaLogsLakeflowConnect,
)
from tests.unit.sources.test_suite import LakeflowConnectTests


class TestTsugaLogsConnector(LakeflowConnectTests):
    connector_class = TsugaLogsLakeflowConnect
    # Simulate mode: spec + corpus live at ``source_simulator/specs/tsuga_logs/``.
    # The /v1/logs/search endpoint is served by a custom handler that rebases
    # corpus timestamps near wall-clock now, so the connector's
    # lookback-from-now first read returns records without live credentials.
    simulator_source = "tsuga_logs"
    replay_config = {"operation_api_key": "tsuga_op:1:org:simulator-fake-key", "default_query": "*"}
