"""Tsuga logs source package.

The import is conditional so the pure-Python helper modules remain importable in
local environments where `pyspark` is not installed.
"""

try:  # pragma: no cover - exercised only in Spark-capable environments
    from .tsuga_logs import TsugaLogsLakeflowConnect

    __all__ = ["TsugaLogsLakeflowConnect"]
except ModuleNotFoundError:
    __all__ = []
