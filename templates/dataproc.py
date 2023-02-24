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
from . import util

from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple


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
    return (a[0] + b[0], a[1] + b[1])


def flatten(lists: List[List[Any]],
            fn: Optional[Callable[[Any], Any]] = None) -> List[Any]:
    if not fn:
        fn = lambda x: x
    return [x for y in map(fn, lists) for x in y]


class BigQueryToNeo4jGDSTemplate(BaseTemplate):
    """
    Build a new graph projection in Neo4j GDS / AuraDS from one or many BigQuery
    tables. Utilizes Apache Arrow and Arrow Flight to achieve high throughput
    and concurrency.
    """

    @staticmethod
    def parse_bq_args(args: Dict[str, Any]) -> Dict[str, Any]:
        return args # XXX finish me

    @staticmethod
    def parse_args(args: Optional[Sequence[str]] = None) -> Dict[str, Any]:
        parser = argparse.ArgumentParser()
        parser.add_argument(
            "--graph_json",
            type=str,
            required=True,
            help="Path to a JSON representation of the Graph model.",
        )
        parser.add_argument(
            "--neo4j_host",
            help="Hostname or IP address of Neo4j server.",
            default="localhost",
        )
        parser.add_argument(
            "--neo4j_port",
            default=8491,
            type=int,
            help="TCP Port of Neo4j Arrow Flight service.",
        )
        parser.add_argument(
            "--neo4j_use_tls",
            default="True",
            type=util.strtobool,
            help="Use TLS for encrypting Neo4j Arrow Flight connection.",
        )
        parser.add_argument(
            "--neo4j_user",
            default="neo4j",
            help="Neo4j Username.",
        )
        parser.add_argument(
            "--neo4j_password",
            help="Neo4j Password",
        )
        parser.add_argument(
            "--neo4j_concurrency",
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
            "--bq_project",
            type=str,
            help="GCP project containing BigQuery tables."
        )
        parser.add_argument(
            "--bq_dataset",
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

        # 1. XXX Graph Model hardcoded for now...
        graph = (
            na.model.Graph(name="test", db="neo4j")
            .with_node(na.model.Node(source="paper",
                                     key_field="paper",
                                     label_field="labels",
                                     years="years"))
            .with_edge(na.model.Edge(source="citations",
                                     source_field="source",
                                     target_field="target",
                                     type_field="type"))
        )

        # 2. Initialize our clients for source and sink.
        neo4j = na.Neo4jArrowClient(args["neo4j_host"],
                                    graph.name,
                                    port=args["neo4j_port"],
                                    tls=args["neo4j_use_tls"],
                                    database=graph.db,
                                    user=args["neo4j_user"],
                                    password=args["neo4j_password"],
                                    concurrency=args["neo4j_concurrency"])
        bq = BigQuerySource(args["bq_project"], args["bq_dataset"])
        sc = spark.sparkContext

        # 3. Prepare our collection of streams. We do this from the Spark driver
        #    so we can more easily spread the streams across the workers.
        node_streams = flatten([bq.table("papers")]) # XXX
        edge_streams = flatten([bq.table("citations")]) # XXX
        logger.info(
            f"prepared {len(node_streams):,} node streams ({node_streams[:1]}),"
            f" {len(edge_streams):,} edge streams ({edge_streams[:1]})"
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
        cnd_size = (
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
