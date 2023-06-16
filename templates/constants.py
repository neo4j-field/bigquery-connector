# Copyright (c) "Neo4j"
# Neo4j Sweden AB [https://neo4j.com]
"""
Constants used as environment variables or command line arguments.
"""
from . import __version__

USER_AGENT = f"Neo4jBigQuery/{__version__} (GPN:Neo4j;)"

BQ_PROJECT = "bq_project"
BQ_DATASET = "bq_dataset"
BQ_TABLE = "bq_table"
BQ_SINK_MODE = "bq_sink_mode"

DEBUG = "debug"

NODE_TABLES = "node_tables"
EDGE_TABLES = "edge_tables"

NEO4J_GRAPH_JSON = "graph_json"
NEO4J_GRAPH_JSON_URI = "graph_uri"

NEO4J_GRAPH_NAME = "graph_name"
NEO4J_DB_NAME = "neo4j_db_name"

NEO4J_LABELS = "neo4j_labels"
NEO4J_TYPES = "neo4j_types"
NEO4J_PROPERTIES = "neo4j_properties"

NEO4J_FORCE = "neo4j_force"

NEO4J_USER = "neo4j_user"
NEO4J_PASSWORD = "neo4j_password"
NEO4J_SECRET = "neo4j_secret"
NEO4J_URI = "neo4j_uri"
NEO4J_CONCURRENCY = "neo4j_concurrency"
NEO4J_ACTION = "neo4j_action"
