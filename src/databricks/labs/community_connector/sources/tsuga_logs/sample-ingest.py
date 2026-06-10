"""
Tsuga Logs sample ingest for Databricks Lakeflow community connectors.

Paste this over the generated `ingest.py` scaffold from the Databricks UI flow
and set `connection_name` — the only edit. The Tsuga-specific configuration
(`query`, `cluster_id`, API key) lives on the Unity Catalog connection,
entered in the UI form when it was created.

Alternatively, keep the generated scaffold (it already references your
connection) and just set its objects list to
`[{"table": {"source_table": "logs"}}]`.

Per-table overrides (different query per destination table, window sizing,
burst handling) are documented in the README.
"""

from databricks.labs.community_connector import register
from databricks.labs.community_connector.pipeline import ingest

# Enable the injection of Unity Catalog connection properties into the connector
spark.conf.set("spark.databricks.unityCatalog.connectionDfOptionInjection.enabled", "true")

connection_name = "<YOUR_CONNECTION_NAME>"  # the only line to edit

register(spark, "tsuga_logs")

ingest(spark, {
    "connection_name": connection_name,
    "objects": [{"table": {"source_table": "logs"}}],
})
