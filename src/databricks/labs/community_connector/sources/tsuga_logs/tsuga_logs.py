from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Iterator

from pyspark.sql.types import StringType, StructField, StructType, TimestampType

from databricks.labs.community_connector.interface import LakeflowConnect

from .client import (
    NormalizedLogRecord,
    TsugaPublicLogsClient,
)
from .incremental_read import collect_windowed_records


@dataclass(frozen=True)
class TableOptions:
    query: str
    cluster_id: str | None
    initial_lookback_seconds: int
    incremental_overlap_seconds: int
    window_seconds: int
    page_size: int
    max_concurrency: int
    max_records_per_batch: int | None
    request_timeout_seconds: int
    allow_truncated_seconds: bool


class TsugaLogsLakeflowConnect(LakeflowConnect):
    """Logs-only Tsuga connector using the public operation-key API."""

    DEFAULT_BASE_URL = "https://api.tsuga.com"
    DEFAULT_INITIAL_LOOKBACK_SECONDS = 3600
    DEFAULT_INCREMENTAL_OVERLAP_SECONDS = 60
    DEFAULT_WINDOW_SECONDS = 300
    DEFAULT_PAGE_SIZE = 1000
    DEFAULT_MAX_CONCURRENCY = 1
    DEFAULT_REQUEST_TIMEOUT_SECONDS = 60

    def __init__(self, options: dict[str, str]) -> None:
        super().__init__(options)
        operation_api_key = options.get("operation_api_key")
        if not operation_api_key:
            raise ValueError("Missing required connection parameter 'operation_api_key'")

        self._base_url = options.get("base_url", self.DEFAULT_BASE_URL).rstrip("/")
        self._operation_api_key = operation_api_key
        # Connection-level defaults; table options override per table. Named
        # default_* so they can never key-collide with per-table options —
        # the framework forbids a pipeline option whose key exists on the
        # connection.
        self._default_query = options.get("default_query") or None
        self._default_cluster_id = options.get("default_cluster_id") or None
        self._init_end_exclusive_seconds = int(datetime.now(timezone.utc).timestamp()) + 1
        self._extracted_at = datetime.now(timezone.utc).isoformat()

    def list_tables(self) -> list[str]:
        return ["logs"]

    def get_table_schema(self, table_name: str, table_options: dict[str, str]) -> StructType:
        self._require_known_table(table_name)
        _ = table_options
        return StructType(
            [
                StructField("event_time", TimestampType(), False),
                StructField("level", StringType(), True),
                StructField("message", StringType(), True),
                StructField("service_name", StringType(), True),
                StructField("team", StringType(), True),
                StructField("env", StringType(), True),
                StructField("raw_json", StringType(), False),
                StructField("extracted_at", TimestampType(), False),
            ]
        )

    def read_table_metadata(self, table_name: str, table_options: dict[str, str]) -> dict[str, Any]:
        self._require_known_table(table_name)
        _ = table_options
        return {
            "primary_keys": ["raw_json"],
            "cursor_field": "event_time",
            "ingestion_type": "cdc",
        }

    def read_table(
        self, table_name: str, start_offset: dict | None, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        self._require_known_table(table_name)
        options = self._parse_table_options(table_options)
        start_seconds = self._get_start_seconds(
            start_offset,
            options.initial_lookback_seconds,
            options.incremental_overlap_seconds,
        )
        if start_seconds >= self._init_end_exclusive_seconds:
            return iter([]), start_offset or self._window_offset(start_seconds)

        result = collect_windowed_records(
            start_seconds=start_seconds,
            end_exclusive_seconds=self._init_end_exclusive_seconds,
            window_seconds=options.window_seconds,
            max_concurrency=options.max_concurrency,
            max_records=options.max_records_per_batch,
            seen_fingerprints_at_cursor=set(
                (start_offset or {}).get("seen_fingerprints_at_cursor", [])
            ),
            fetch_windows=lambda windows: self._fetch_windows(windows, options),
        )

        if result.resume_offset is not None:
            return (
                iter(record.to_row() for record in result.records),
                result.resume_offset,
            )

        end_offset = self._window_offset(result.next_cursor_seconds)
        if not result.records:
            return iter([]), end_offset

        return iter(record.to_row() for record in result.records), end_offset

    def _fetch_windows(
        self, windows: list[tuple[int, int]], options: TableOptions
    ) -> list[list[NormalizedLogRecord]]:
        client = TsugaPublicLogsClient(
            self._base_url,
            self._operation_api_key,
            request_timeout_seconds=float(options.request_timeout_seconds),
        )
        if options.max_concurrency == 1 or len(windows) == 1:
            return [
                client.fetch_window(
                    from_seconds=window_start,
                    to_exclusive_seconds=window_end,
                    query=options.query,
                    page_size=options.page_size,
                    extracted_at=self._extracted_at,
                    cluster_id=options.cluster_id,
                    allow_truncated_seconds=options.allow_truncated_seconds,
                )
                for window_start, window_end in windows
            ]

        with ThreadPoolExecutor(max_workers=options.max_concurrency) as executor:
            return list(
                executor.map(
                    lambda bounds: client.fetch_window(
                        from_seconds=bounds[0],
                        to_exclusive_seconds=bounds[1],
                        query=options.query,
                        page_size=options.page_size,
                        extracted_at=self._extracted_at,
                        cluster_id=options.cluster_id,
                        allow_truncated_seconds=options.allow_truncated_seconds,
                    ),
                    windows,
                )
            )

    def _get_start_seconds(
        self,
        start_offset: dict | None,
        initial_lookback_seconds: int,
        incremental_overlap_seconds: int,
    ) -> int:
        if not start_offset or "cursor_seconds" not in start_offset:
            return max(0, self._init_end_exclusive_seconds - initial_lookback_seconds)

        cursor_seconds = int(start_offset["cursor_seconds"])
        # Already caught up within this run: don't rewind, so the next call
        # returns the same offset and the framework stops paginating.
        if cursor_seconds >= self._init_end_exclusive_seconds:
            return cursor_seconds
        # Mid-run resume after a max_records_per_batch truncation: the saved
        # fingerprints dedup the boundary second, no overlap rewind needed.
        if start_offset.get("seen_fingerprints_at_cursor"):
            return cursor_seconds
        # New run resuming from a previous run's cursor: rewind to pick up
        # late-arriving events near the old cursor.
        return max(0, cursor_seconds - incremental_overlap_seconds)

    def _parse_table_options(self, table_options: dict[str, str]) -> TableOptions:
        query = table_options.get("query") or self._default_query
        if not query:
            raise ValueError(
                "Missing required option 'query'. Set 'default_query' on the "
                "connection or 'query' in the table's table_configuration "
                "(use '*' explicitly to ingest all logs)."
            )
        return TableOptions(
            query=query,
            cluster_id=table_options.get("cluster_id") or self._default_cluster_id,
            initial_lookback_seconds=self._parse_positive_int(
                table_options.get("initial_lookback_seconds"),
                key="initial_lookback_seconds",
                default=self.DEFAULT_INITIAL_LOOKBACK_SECONDS,
            ),
            incremental_overlap_seconds=self._parse_non_negative_int(
                table_options.get("incremental_overlap_seconds"),
                key="incremental_overlap_seconds",
                default=self.DEFAULT_INCREMENTAL_OVERLAP_SECONDS,
            ),
            window_seconds=self._parse_positive_int(
                table_options.get("window_seconds"),
                key="window_seconds",
                default=self.DEFAULT_WINDOW_SECONDS,
            ),
            page_size=self._parse_positive_int(
                table_options.get("page_size"),
                key="page_size",
                default=self.DEFAULT_PAGE_SIZE,
            ),
            max_concurrency=self._parse_positive_int(
                table_options.get("max_concurrency"),
                key="max_concurrency",
                default=self.DEFAULT_MAX_CONCURRENCY,
            ),
            max_records_per_batch=self._parse_optional_positive_int(
                table_options.get("max_records_per_batch"),
                key="max_records_per_batch",
            ),
            request_timeout_seconds=self._parse_positive_int(
                table_options.get("request_timeout_seconds"),
                key="request_timeout_seconds",
                default=self.DEFAULT_REQUEST_TIMEOUT_SECONDS,
            ),
            allow_truncated_seconds=self._parse_bool(
                table_options.get("allow_truncated_seconds"),
                key="allow_truncated_seconds",
                default=False,
            ),
        )

    def _require_known_table(self, table_name: str) -> None:
        if table_name not in self.list_tables():
            raise ValueError(f"Unknown table '{table_name}'. Supported tables: ['logs']")

    @staticmethod
    def _parse_bool(value: str | None, *, key: str, default: bool) -> bool:
        if value is None or value == "":
            return default
        normalized = value.strip().lower()
        if normalized in ("true", "1"):
            return True
        if normalized in ("false", "0"):
            return False
        raise ValueError(f"'{key}' must be 'true' or 'false'")

    @staticmethod
    def _parse_positive_int(value: str | None, *, key: str, default: int) -> int:
        if value is None or value == "":
            return default
        parsed = int(value)
        if parsed <= 0:
            raise ValueError(f"'{key}' must be > 0")
        return parsed

    @staticmethod
    def _parse_non_negative_int(value: str | None, *, key: str, default: int) -> int:
        if value is None or value == "":
            return default
        parsed = int(value)
        if parsed < 0:
            raise ValueError(f"'{key}' must be >= 0")
        return parsed

    @staticmethod
    def _parse_optional_positive_int(value: str | None, *, key: str) -> int | None:
        if value is None or value == "":
            return None
        parsed = int(value)
        if parsed <= 0:
            raise ValueError(f"'{key}' must be > 0 when provided")
        return parsed

    @staticmethod
    def _window_offset(next_cursor_seconds: int) -> dict[str, Any]:
        return {
            "cursor_seconds": next_cursor_seconds,
            "seen_fingerprints_at_cursor": [],
        }
