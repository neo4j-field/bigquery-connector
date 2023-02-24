# Copyright (c) "Neo4j"
# Neo4j Sweden AB [https://neo4j.com]
import argparse
import itertools
import time

from google.cloud.bigquery_storage import BigQueryReadClient, types
from dataproc_templates import BaseTemplate

import pyarrow as pa
from pyspark.sql import SparkSession

import neo4j_arrow as na
from ._bq_client import BigQuerySource
from . import constants as c, util

from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple


def load_model_from_gcs(uri: str) -> Optional[na.model.Graph]:
    """
    Attempt to load a Graph model from a GCS uri. Returns None on failure.
    """
    try:
        import fsspec
        with fsspec.open(uri, "rt") as f:
            return na.model.Graph.from_json(f.read())
    except Exception as e:
        return None


def send_nodes(client: na.Neo4jArrowClient,
               model: Optional[na.model.Graph] = None,
               source_field: str = "_table") -> Callable[[Any], Tuple[int, int]]:
    """
    Wrap the given client, model, and (optional) source_field in a function that
    streams PyArrow data (Table or RecordBatch) to Neo4j as nodes.
    """
    def _send_nodes(table: Any) -> Tuple[int, int]:
        result: Tuple[int, int] = client.write_nodes(table, model, source_field)
        return result
    return _send_nodes


def send_edges(client: na.Neo4jArrowClient,
               model: Optional[na.model.Graph] = None,
               source_field: str = "_table") -> Callable[[Any], Tuple[int, int]]:
    """
    Wrap the given client, model, and (optional) source_field in a function that
    streams PyArrow data (Table or RecordBatch) to Neo4j as relationships.
    """
    def _send_nodes(table: Any) -> Tuple[int, int]:
        result: Tuple[int, int] = client.write_edges(table, model, source_field)
        return result
    return _send_nodes


def tuple_sum(a: Tuple[int, int], b: Tuple[int, int]) -> Tuple[int, int]:
    """
    Reducing function for summing tuples of integers.
    """
    return (a[0] + b[0], a[1] + b[1])


def flatten(lists: List[List[Any]],
            fn: Optional[Callable[[Any], Any]] = None) -> List[Any]:
    """
    Flatten a list of lists, applying an optional function (fn) to the initial
    list of lists.
    """
    if not fn:
        fn = lambda x: x
    return [x for y in map(fn, lists) for x in y]


class BigQueryToNeo4jGDSTemplate(BaseTemplate): # type: ignore
    """
    Build a new graph projection in Neo4j GDS / AuraDS from one or many BigQuery
    tables. Utilizes Apache Arrow and Arrow Flight to achieve high throughput
    and concurrency.
    """

    @staticmethod
    def parse_bq_args(args: Dict[str, Any]) -> Dict[str, Any]:
        return args # XXX finish me when the stored proc is available

    @staticmethod
    def parse_args(args: Optional[Sequence[str]] = None) -> Dict[str, Any]:
        parser = argparse.ArgumentParser()
        parser.add_argument(
            f"--{c.NEO4J_GRAPH_JSON}",
            type=str,
            help="JSON-based representation of the Graph model.",
        )
        parser.add_argument(
            f"--{c.NEO4J_GRAPH_JSON_URI}",
            type=str,
            help="URI to a JSON representation of the Graph model.",
        )
        parser.add_argument(
            f"--{c.NEO4J_HOST}",
            help="Hostname or IP address of Neo4j server.",
            default="localhost",
        )
        parser.add_argument(
            f"--{c.NEO4J_PORT}",
            default=8491,
            type=int,
            help="TCP Port of Neo4j Arrow Flight service.",
        )
        parser.add_argument(
            f"--{c.NEO4J_USE_TLS}",
            default="True",
            type=util.strtobool,
            help="Use TLS for encrypting Neo4j Arrow Flight connection.",
        )
        parser.add_argument(
            f"--{c.NEO4J_USER}",
            default="neo4j",
            help="Neo4j Username.",
        )
        parser.add_argument(
            f"--{c.NEO4J_PASSWORD}",
            help="Neo4j Password",
        )
        parser.add_argument(
            f"--{c.NEO4J_CONCURRENCY}",
            default=4,
            type=int,
            help="Neo4j server-side concurrency.",
        )

        # BigQuery Parameters
        parser.add_argument(
            "--node_tables",
            help="Comma-separated list of BigQuery tables for nodes.",
            type=lambda x: [y.strip() for y in str(x).split(",")],
            default=[],
        )
        parser.add_argument(
            "--edge_tables",
            help="Comma-separated list of BigQuery tables for edges.",
            type=lambda x: [y.strip() for y in str(x).split(",")],
            default=[],
        )
        parser.add_argument(
            f"--{c.BQ_PROJECT}",
            type=str,
            help="GCP project containing BigQuery tables."
        )
        parser.add_argument(
            f"--{c.BQ_DATASET}",
            type=str,
            help="BigQuery dataset containing BigQuery tables."
        )
        parser.add_argument(
            "--bq_max_stream_count",
            default=8192*2,
            type=int,
            help="Maximum number of streams to generate for a BigQuery table."
        )

        # Optional/Other Parameters
        parser.add_argument(
            "--debug",
            action="store_true",
            help="Enable verbose (debug) logging.",
        )

        ns: argparse.Namespace
        ns, _ = parser.parse_known_args()
        return vars(ns)

    def run(self, spark: SparkSession, args: Dict[str, Any]) -> None:
        logger = self.get_logger(spark=spark)
        start_time = time.time()

        # 1. Load the Graph Model.
        if args[c.NEO4J_GRAPH_JSON]:
            # Try loading a literal JSON-based model
            graph = na.model.Graph.from_json(args[c.NEO4J_GRAPH_JSON])
        elif args[c.NEO4J_GRAPH_JSON_URI]:
            # Fall back to URI
            uri = args[c.NEO4J_GRAPH_JSON_URI]
            graph = load_model_from_gcs(uri)
            if not graph:
                raise ValueError(f"failed to load graph from {uri}")
        else:
            # Give up :(
            raise ValueError("missing graph data model uri or literal JSON")

        # 1b. Override graph and/or database name.
        if c.NEO4J_GRAPH_NAME in args:
            graph = graph.named(args[c.NEO4J_GRAPH_NAME])
        if c.NEO4J_DB_NAME in args:
            graph = graph.in_db(args[c.NEO4J_DB_NAME])
        logger.info(f"using graph model {graph.to_json()}")

        # 2. Initialize our clients for source and sink.
        neo4j = na.Neo4jArrowClient(args[c.NEO4J_HOST],
                                    graph.name,
                                    port=args[c.NEO4J_PORT],
                                    tls=args[c.NEO4J_USE_TLS],
                                    database=graph.db,
                                    user=args[c.NEO4J_USER],
                                    password=args[c.NEO4J_PASSWORD],
                                    concurrency=args[c.NEO4J_CONCURRENCY])
        bq = BigQuerySource(args[c.BQ_PROJECT], args[c.BQ_DATASET])
        sc = spark.sparkContext

        # 3. Prepare our collection of streams. We do this from the Spark driver
        #    so we can more easily spread the streams across the workers.
        node_streams = flatten([bq.table("papers")]) # XXX
        edge_streams = flatten([bq.table("citations")]) # XXX
        logger.info(
            f"prepared {len(node_streams):,} node streams, "
            f"{len(edge_streams):,} edge streams"
        )

        # 4. Begin our Graph import.
        result = neo4j.start(force=True) # TODO: force should be an argument
        logger.info(f"starting import: {result}")

        # 5. Load our Nodes via PySpark workers.
        cnt, size = (
            sc
            .parallelize(node_streams)
            .map(bq.consume_stream, True) # don't shuffle
            .map(send_nodes(neo4j, graph))
            .reduce(tuple_sum)
        )
        logger.info(
            f"imported {cnt:,} nodes (streamed ~{size / (1 << 20):.2f} MiB)"
        )

        # 6. Signal we're done with Nodes before moving onto Edges.
        result = neo4j.nodes_done()
        logger.info(f"signalled nodes complete: {result}")

        # 7. Now stream Edges via the PySpark workers.
        cnt, size = (
            sc
            .parallelize(edge_streams)
            .map(bq.consume_stream, True) # don't shuffle
            .map(send_edges(neo4j, graph))
            .reduce(tuple_sum)
        )
        logger.info(
            f"imported {cnt:,} edges (streamed ~{size / (1 << 20):.2f} MiB)"
        )

        # 8. Signal we're done with Edges.
        result = neo4j.edges_done()
        logger.info(f"signalled edges complete: {result}")

        # 9. TODO: await import completion and GDS projection available
        end_time = time.time()
        logger.info(f"completed in {(end_time - start_time):.3f} seconds")
