# Copyright (c) "Neo4j"
# Neo4j Sweden AB [https://neo4j.com]
__all__ = [
    "BigQueryToNeo4jGDSTemplate",
    "BigQuerySink",
    "BigQuerySource",
    "Neo4jGDSToBigQueryTemplate",
    "constants",
    "strtobool",
    "util",
    "__version__",
    "__author__",
]

from importlib import metadata

__author__ = "Neo4j"
__version__ = "dev"
try:
    __version__ = metadata.version("bigquery-connector")
except metadata.PackageNotFoundError:
    pass

from templates import constants, util
from .bq_client import BigQuerySource, BigQuerySink
from .bigquery import BigQueryToNeo4jGDSTemplate, Neo4jGDSToBigQueryTemplate
from .vendored import strtobool
