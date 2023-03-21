# Copyright (c) "Neo4j"
# Neo4j Sweden AB [https://neo4j.com]
import argparse
import itertools
import sys
import time

from google.cloud.bigquery_storage import BigQueryReadClient, types
from google.protobuf import descriptor_pb2
from dataproc_templates import BaseTemplate

import pyarrow as pa
from pyspark.sql import SparkSession

import neo4j_arrow as na

from .bq_client import BigQuerySource, BigQuerySink, BQStream
from .vendored import strtobool
from . import constants as c, util

from model import Node, Edge, arrow_to_nodes, arrow_to_edges

from typing import (
    Any, Callable, Dict, Generator, List, Optional, Sequence, Tuple, Union
)

__all__ = [
    "BigQueryToNeo4jGDSTemplate",
    "Neo4jGDSToBigQueryTemplate",
]


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


def to_stream_fn(mode: str, bq: BigQuerySource,
                 graph: na.model.Graph) -> Callable[[str], List[BQStream]]:
    """
    Create a function that generates BigQuery streams with optional field
    filtering.
    """
    def _to_stream(table_name: str) -> List[BQStream]:
        fields: List[str] = []
        if mode == "node":
            node = graph.node_for_src(table_name)
            if node:
                for key in node.properties.keys():
                    fields.append(key)
                if node.key_field:
                    fields.append(node.key_field)
                if node.label_field:
                    fields.append(node.label_field)
        elif mode == "edge":
            edge = graph.edge_for_src(table_name)
            if edge:
                for key in edge.properties.keys():
                    fields.append(key)
                if edge.source_field:
                    fields.append(edge.source_field)
                if edge.target_field:
                    fields.append(edge.target_field)
                if edge.type_field:
                    fields.append(edge.type_field)
        else:
            raise ValueError("invalid mode. expected 'node' or 'edge'.")
        return bq.table(table_name, fields=fields)
    return _to_stream


class Neo4jGDSToBigQueryTemplate(BaseTemplate): # type: ignore
    """
    Stream data from a Neo4j GDS graph into a BigQuery table using the Storage
    Write API.
    """
    @staticmethod
    def parse_args(args: Optional[Sequence[str]] = None) -> Dict[str, Any]:
        # Try pulling out any BigQuery procedure environmental args.
        bq_args = util.bq_params()
        if bq_args:
            print(f"using BigQuery args: {bq_args}")

        parser = argparse.ArgumentParser()
        parser.add_argument(
            f"--{c.NEO4J_GRAPH_NAME}",
            type=str,
            help=(
                "Name for the resulting Graph projection. (Will override what "
                "may be provided in the model.)"
            ),
        )
        parser.add_argument(
            f"--{c.NEO4J_DB_NAME}",
            type=str,
            help=(
                "Name of the database to host the graph projection. (Will "
                "override what may be provided in the model.)"
            ),
            default="neo4j",
        )

        parser.add_argument(
            f"--{c.NEO4J_SECRET}",
            help="Google Secret to use for populating other values",
            type=str,
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
            type=strtobool,
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
            f"--{c.BQ_TABLE}",
            help="BigQuery table to write the Neo4j data.",
            type=str,
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

        # Simple mode switching for now; nodes or edges
        parser.add_argument(
            f"--{c.BQ_SINK_MODE}",
            help="BigQuery Sink mode ('nodes' or 'edges')",
            type=str,
            default="nodes",
        )

        # Optional/Other Parameters
        parser.add_argument(
            f"--{c.DEBUG}",
            action="store_true",
            help="Enable verbose (debug) logging.",
        )

        ns: argparse.Namespace
        if bq_args:
            # We're most likely running as a stored proc, so use that method.
            ns, _ = parser.parse_known_args(bq_args)
        else:
            # Rely entirely on sys.argv and any provided args parameter.
            ns, _ = parser.parse_known_args(args)
        return vars(ns)


    def run(self, spark: SparkSession, args: Dict[str, Any]) -> None:
        sc = spark.sparkContext
        if args[c.DEBUG]:
            sc.setLogLevel("DEBUG")
        else:
            sc.setLogLevel("INFO")

        logger = (
            sc._jvm.org.apache.log4j.LogManager # type: ignore
            .getLogger(self.__class__.__name__)
        )
        start_time = time.time()

        logger.info(
            f"starting job for {args[c.BQ_PROJECT]}/{args[c.BQ_DATASET]}/"
            f"{args[c.BQ_TABLE]}"
            f"(server concurrency={args[c.NEO4J_CONCURRENCY]})"
        )

        # 1. Get the graph and database name.
        graph_name = args[c.NEO4J_GRAPH_NAME]
        db_name = args[c.NEO4J_DB_NAME]

        # 2. Fetch our secret if any
        if c.NEO4J_SECRET in args:
            logger.info(f"fetching secret {args[c.NEO4J_SECRET]}")
            secret = util.fetch_secret(args[c.NEO4J_SECRET])
            if not secret:
                logger.warn("failed to fetch secret, falling back to params")
            else:
                args.update(secret)

        # 3. Initialize our clients for source and sink.
        neo4j = na.Neo4jArrowClient(args[c.NEO4J_HOST],
                                    graph_name,
                                    port=args[c.NEO4J_PORT],
                                    tls=args[c.NEO4J_USE_TLS],
                                    database=db_name,
                                    user=args[c.NEO4J_USER],
                                    password=args[c.NEO4J_PASSWORD],
                                    concurrency=args[c.NEO4J_CONCURRENCY])
        logger.info(f"using neo4j client {neo4j} (tls={args[c.NEO4J_USE_TLS]})")

        ### XXX TODO: FINISH ME
        # XXX fallback to Edge for now
        descriptor = descriptor_pb2.DescriptorProto()
        if args[c.BQ_SINK_MODE].lower() == "nodes":
            Node.DESCRIPTOR.CopyToProto(descriptor)
        else:
            Edge.DESCRIPTOR.CopyToProto(descriptor)

        bq = BigQuerySink(
            args[c.BQ_PROJECT], args[c.BQ_DATASET], args[c.BQ_TABLE], descriptor
        )
        logger.info(f"created sink {bq}")

        # 1. Fetch and process rows from Neo4j
        # XXX for now, we single-thread this in the Spark driver
        # XXX hardcode for current demo and lean on generators
        converter: Optional[Any] = None # XXX
        if args[c.BQ_SINK_MODE].lower() == "nodes":
            properties = ["airport_id", "x", "y"]
            topo_filters = ["Airport"]
            rows = neo4j.read_nodes(properties, labels=topo_filters,
                                    concurrency=args[c.NEO4J_CONCURRENCY])
            converter = arrow_to_nodes
        elif args[c.BQ_SINK_MODE].lower() == "edges":
            properties = ["flightCount"]
            topo_filters = ["SENDS_TO"]
            rows = neo4j.read_edges(properties=properties,
                                    relationship_types=topo_filters,
                                    concurrency=args[c.NEO4J_CONCURRENCY])
            converter = arrow_to_edges
        else:
            raise ValueError("invalid sink mode; expected either 'nodes' or 'edges'")

        cnt = 0
        try:
            batch = []
            for row in rows:
                for node in converter(row, topo_filters):
                    batch.append(node.SerializeToString())
                    cnt += 1
                    if len(batch) > 20_000: # flush
                        bq.append_rows(batch)
                        batch = []
            if batch:
                # flush stragglers
                bq.append_rows(batch)
        except Exception as e:
            logger.error(f"failure appending data from Neo4j into BigQuery: {e}")
            raise RuntimeError("oh no!")
        logger.info(f"sent {cnt:,} {args[c.BQ_SINK_MODE]} to {bq.parent}")

        # 2. Finalize write streams
        bq.finalize_write_stream()
        logger.info(f"finalized stream for {bq.parent}")

        # 3. Wait for completion?
        bq.wait_for_completion(timeout_secs=60 * 5)
        logger.info(f"completed writes to {bq.parent}")

        # 4. Commit and make data live.
        bq.commit()
        logger.info(f"commited rows to {bq.parent}")


class BigQueryToNeo4jGDSTemplate(BaseTemplate): # type: ignore
    """
    Build a new graph projection in Neo4j GDS / AuraDS from one or many BigQuery
    tables. Utilizes Apache Arrow and Arrow Flight to achieve high throughput
    and concurrency.
    """

    @staticmethod
    def parse_args(args: Optional[Sequence[str]] = None) -> Dict[str, Any]:
        # Try pulling out any BigQuery procedure environmental args.
        bq_args = util.bq_params()
        if bq_args:
            print(f"using BigQuery args: {bq_args}")

        parser = argparse.ArgumentParser()
        parser.add_argument(
            f"--{c.NEO4J_GRAPH_NAME}",
            type=str,
            help=(
                "Name for the resulting Graph projection. (Will override what "
                "may be provided in the model.)"
            ),
        )
        parser.add_argument(
            f"--{c.NEO4J_DB_NAME}",
            type=str,
            help=(
                "Name of the database to host the graph projection. (Will "
                "override what may be provided in the model.)"
            ),
            default="neo4j",
        )

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
            f"--{c.NEO4J_SECRET}",
            help="Google Secret to use for populating other values",
            type=str,
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
            type=strtobool,
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
            f"--{c.NODE_TABLES}",
            help="Comma-separated list of BigQuery tables for nodes.",
            type=lambda x: [y.strip() for y in str(x).split(",")],
            default=[],
        )
        parser.add_argument(
            f"--{c.EDGE_TABLES}",
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

        # Optional/Other Parameters
        parser.add_argument(
            f"--{c.DEBUG}",
            action="store_true",
            help="Enable verbose (debug) logging.",
        )

        ns: argparse.Namespace
        if bq_args:
            # We're most likely running as a stored proc, so use that method.
            ns, _ = parser.parse_known_args(bq_args)
        else:
            # Rely entirely on sys.argv and any provided args parameter.
            ns, _ = parser.parse_known_args(args)
        return vars(ns)

    def run(self, spark: SparkSession, args: Dict[str, Any]) -> None:
        sc = spark.sparkContext
        if args[c.DEBUG]:
            sc.setLogLevel("DEBUG")
        else:
            sc.setLogLevel("INFO")

        logger = (
            sc._jvm.org.apache.log4j.LogManager # type: ignore
            .getLogger(self.__class__.__name__)
        )
        start_time = time.time()

        logger.info(
            f"starting job for {args[c.BQ_PROJECT]}/{args[c.BQ_DATASET]}/{{"
            f"nodes:[{','.join(args[c.NODE_TABLES])}], "
            f"edges:[{','.join(args[c.EDGE_TABLES])}]}} "
            f"(server concurrency={args[c.NEO4J_CONCURRENCY]})"
        )

        # 1. Load the Graph Model.
        if args[c.NEO4J_GRAPH_JSON]:
            # Try loading a literal JSON-based model
            json_str = args[c.NEO4J_GRAPH_JSON]
            graph = na.model.Graph.from_json(json_str)
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

        # 2a. Fetch our secret if any
        if c.NEO4J_SECRET in args:
            logger.info(f"fetching secret {args[c.NEO4J_SECRET]}")
            secret = util.fetch_secret(args[c.NEO4J_SECRET])
            if not secret:
                logger.warn("failed to fetch secret, falling back to params")
            else:
                args.update(secret)

        # 2b. Initialize our clients for source and sink.
        neo4j = na.Neo4jArrowClient(args[c.NEO4J_HOST],
                                    graph.name,
                                    port=args[c.NEO4J_PORT],
                                    tls=args[c.NEO4J_USE_TLS],
                                    database=graph.db,
                                    user=args[c.NEO4J_USER],
                                    password=args[c.NEO4J_PASSWORD],
                                    concurrency=args[c.NEO4J_CONCURRENCY])
        bq = BigQuerySource(args[c.BQ_PROJECT], args[c.BQ_DATASET])
        logger.info(f"using neo4j client {neo4j} (tls={args[c.NEO4J_USE_TLS]})")

        # 3. Prepare our collection of streams. We do this from the Spark driver
        #    so we can more easily spread the streams across the workers.
        #
        # XXX this is a bit convoluted at the moment, but the functional logic
        #     is to transform the list of table names into BQStreams optionally
        #     requesting specific field names for the streams. (Default is to
        #     request all fields in a table.)
        node_streams = flatten(
            list(map(to_stream_fn("node", bq, graph), args[c.NODE_TABLES]))
        )
        edge_streams = flatten(
            list(map(to_stream_fn("edge", bq, graph), args[c.EDGE_TABLES]))
        )
        logger.info(
            f"prepared {len(node_streams):,} node streams, "
            f"{len(edge_streams):,} edge streams"
        )

        # 4. Begin our Graph import.
        result = neo4j.start(force=True) # TODO: force should be an argument
        logger.info(f"starting import for {result.get('name', graph.name)}")

        # 5. Load our Nodes via PySpark workers.
        nodes_start = time.time()
        cnt, size = (
            sc
            .parallelize(node_streams, 32)
            .map(bq.consume_stream, True) # don't shuffle
            .map(send_nodes(neo4j, graph))
            .reduce(tuple_sum)
        )
        logger.info(
            f"streamed {cnt:,} nodes, ~{size / (1<<20):,.2f} MiB original size"
        )

        # 5b. Assert we actually got nodes
        if cnt < 1:
            logger.error(f"failed to load nodes; aborting.")
            sys.exit(1)

        # 6. Signal we're done with Nodes before moving onto Edges.
        result = neo4j.nodes_done()
        duration = time.time() - nodes_start
        total = result["node_count"]
        logger.info(
            f"signalled nodes complete, imported {total:,} nodes"
            f" in {duration:,.3f}s ({total/duration:,.2f} nodes/s)"
        )
        if cnt != total:
            logger.warn(f"sent {cnt} nodes, but imported {total}!")

        # 7. Now stream Edges via the PySpark workers.
        edges_start = time.time()
        cnt, size = (
            sc
            .parallelize(edge_streams, 64)
            .map(bq.consume_stream, True) # don't shuffle
            .map(send_edges(neo4j, graph))
            .reduce(tuple_sum)
        )
        logger.info(
            f"streamed {cnt:,} edges, ~{size / (1<<20):,.2f} MiB original size"
        )

        # 8. Signal we're done with Edges.
        result = neo4j.edges_done()
        duration = time.time() - edges_start
        total = result["relationship_count"]
        logger.info(
            f"signalled edges complete, imported {total:,} edges"
            f" in {duration:,.3f}s ({total/duration:,.2f} edges/s)"
        )
        if cnt != total:
            logger.warn(f"sent {cnt} edges, but imported {total}!")

        # 9. TODO: await import completion and GDS projection available
        duration = time.time() - start_time
        logger.info(f"completed in {duration:,.3f} seconds")
