from dataclasses import dataclass
from typing import Any, Callable

from .client import (
    NormalizedLogRecord,
    build_resume_offset,
    filter_records_from_offset,
    iter_windows,
)

FetchWindowsFn = Callable[
    [list[tuple[int, int]]],
    list[list[NormalizedLogRecord]],
]


@dataclass(frozen=True)
class WindowedReadResult:
    records: list[NormalizedLogRecord]
    next_cursor_seconds: int
    resume_offset: dict[str, Any] | None


def collect_windowed_records(
    *,
    start_seconds: int,
    end_exclusive_seconds: int,
    window_seconds: int,
    max_concurrency: int,
    max_records: int | None,
    seen_fingerprints_at_cursor: set[str],
    fetch_windows: FetchWindowsFn,
) -> WindowedReadResult:
    collected: list[NormalizedLogRecord] = []
    remaining = max_records
    next_start_seconds = start_seconds
    first_batch = True

    while next_start_seconds < end_exclusive_seconds:
        if remaining is not None and remaining <= 0:
            break

        windows = iter_windows(
            start_seconds=next_start_seconds,
            window_seconds=window_seconds,
            max_concurrency=max_concurrency,
            end_exclusive_seconds=end_exclusive_seconds,
        )
        if not windows:
            break

        fetched_by_window = fetch_windows(windows)
        for index, records in enumerate(fetched_by_window):
            filtered = filter_records_from_offset(
                records,
                cursor_seconds=start_seconds,
                seen_fingerprints_at_cursor=(
                    seen_fingerprints_at_cursor if first_batch and index == 0 else set()
                ),
            )

            if remaining is not None and len(filtered) > remaining:
                collected.extend(filtered[:remaining])
                return WindowedReadResult(
                    records=collected,
                    next_cursor_seconds=collected[-1].event_time_sec,
                    resume_offset=build_resume_offset(collected),
                )

            collected.extend(filtered)
            if remaining is not None:
                remaining -= len(filtered)

        first_batch = False
        next_start_seconds = windows[-1][1]

    return WindowedReadResult(
        records=collected,
        next_cursor_seconds=next_start_seconds,
        resume_offset=None,
    )
