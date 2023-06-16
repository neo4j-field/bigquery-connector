# Copyright (c) "Neo4j"
# Neo4j Sweden AB [https://neo4j.com]
import argparse
import logging
import sys

import neo4j
import neo4j.exceptions

import time

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
    cast, Any, Callable, Dict, Generator, Iterable, List, Optional, Sequence,
    Tuple, Union
)

__all__ = [
    "BigQueryToNeo4jGDSTemplate",
    "Neo4jGDSToBigQueryTemplate",
]

Arrow = Union[pa.Table, pa.RecordBatch]
ArrowStream = Generator[Arrow, None, None]


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
                if node.key_field:
                    fields.append(node.key_field)
                if node.label_field:
                    fields.append(node.label_field)
                for key in node.properties.keys():
                    fields.append(key)
        elif mode == "edge":
            edge = graph.edge_for_src(table_name)
            if edge:
                if edge.source_field:
                    fields.append(edge.source_field)
                if edge.target_field:
                    fields.append(edge.target_field)
                if edge.type_field:
                    fields.append(edge.type_field)
                for key in edge.properties.keys():
                    fields.append(key)
        else:
            raise ValueError("invalid mode. expected 'node' or 'edge'.")
        return bq.table(table_name, fields=fields)

    return _to_stream


def batch_converter(converter: Callable[[Arrow, List[str]], Generator[Union[Node, Edge], None, None]],
                    topo_filters: List[str]) -> Callable[[Arrow], List[bytes]]:
    """
    Takes column-oriented batches of Arrow buffers and turn them into lists of
    graph elements (Node or Edge protobufs).
    """

    def _batch_converter(arrow: Arrow) -> List[bytes]:
        batch: List[bytes] = []
        for graph_element in converter(arrow, topo_filters):
            batch.append(graph_element.SerializeToString())
        return batch

    return _batch_converter


def read_nodes(client: na.Neo4jArrowClient) -> \
        Callable[[Tuple[Dict[str, str], List[str]]], List[Arrow]]:
    """
    Stream nodes and node properties from Neo4j.
    """

    def _read_nodes(config: Tuple[Dict[str, str], List[str]]) -> Arrow:
        properties, topo_filters = config
        cnt, log_cnt, sz = 0, 0, 0
        logging.info(f"streaming nodes from {client}")
        rows = client.read_nodes(properties, labels=topo_filters)
        for batch in client.read_nodes(properties, labels=topo_filters):
            rows.append(batch)
            cnt += batch.num_rows
            sz += batch.nbytes
            # for now we need some logging since this is an eager job
            log_cnt += batch.num_rows
            if log_cnt > 100_000:
                logging.info(f"...read {log_cnt:,} nodes")
                log_cnt = 0
        logging.info(f"read {cnt:,} nodes, {sz:,} bytes")
        return rows

    return _read_nodes


def read_edges(client: na.Neo4jArrowClient) -> \
        Callable[[Tuple[Dict[str, str], List[str]]], List[Arrow]]:
    """
    Stream edges and edge properties from Neo4j.
    """

    def _read_edges(config: Tuple[Dict[str, str], List[str]]) -> List[Arrow]:
        properties, topo_filters = config
        rows: List[Arrow] = []
        cnt, log_cnt, sz = 0, 0, 0
        for batch in client.read_edges(properties=properties,
                                       relationship_types=topo_filters):
            rows.append(batch)
            cnt += batch.num_rows
            sz += batch.nbytes
            # for now we need some logging since this is an eager job
            log_cnt += batch.num_rows
            if log_cnt > 100_000:
                logging.info(f"...read {log_cnt:,} edges")
                log_cnt = 0
        logging.info(f"read {cnt:,} edges, {sz:,} bytes")
        return rows

    return _read_edges


def append_batch(sink: BigQuerySink) \
        -> Callable[[Iterable[List[bytes]]],
        Iterable[Tuple[str, int]]]:
    """
    Create an appender to a BigQuery table using the provided BigQuerySink.
    """

    def _append_batch(batch: Iterable[List[bytes]]) \
            -> Iterable[Tuple[str, int]]:
        cnt = 0
        for b in batch:
            sink.append_rows(b)
            cnt += len(b)
        if cnt > 0:
            # It's possible we were given an empty iterable. If so, the
            # call to finalize_write_stream will fail.
            logging.info(f"appended {cnt:,} rows")
            sink.finalize_write_stream()
            yield cast(str, sink.stream_name), cnt

    return _append_batch


def build_arrow_client(graph: na.model.Graph, neo4j_uri: str, neo4j_username: str,
                       neo4j_password: str,
                       neo4j_concurrency: int, *,
                       logger: Any = None) -> na.Neo4jArrowClient:
    neo4j_logger = logging.getLogger("neo4j")
    neo4j_logger.handlers.clear()
    neo4j_logger.addHandler(util.SparkLogHandler(logger))

    with neo4j.GraphDatabase.driver(neo4j_uri,
                                    auth=neo4j.basic_auth(neo4j_username, neo4j_password)) as driver:
        try:
            record: Optional[neo4j.Record] = driver.execute_query("CALL gds.debug.arrow()",
                                                                  result_transformer_=neo4j.Result.single)
            if record:
                enabled = bool(record["enabled"])
                if not enabled:
                    raise Exception("Please ensure that Arrow Flights Service is enabled.")
                running = bool(record["running"])
                if not running:
                    raise Exception("Please ensure that Arrow Flights Service is running.")
                advertised_listen_address = str(record["advertisedListenAddress"])
                listen_address = str(record["listenAddress"])
                batch_size = int(record["batchSize"])

                uri = advertised_listen_address
                if not uri:
                    uri = listen_address

                arrow_logger = logging.getLogger("neo4j-arrow-client")
                arrow_logger.handlers.clear()
                arrow_logger.addHandler(util.SparkLogHandler(logger))

                host, port = uri.split(":")
                return na.Neo4jArrowClient(
                    host,
                    graph.name,
                    port=int(port),
                    tls=driver.encrypted,
                    database=graph.db,
                    user=neo4j_username,
                    password=neo4j_password,
                    concurrency=neo4j_concurrency,
                    max_chunk_size=batch_size,
                    logger=arrow_logger)
        except neo4j.exceptions.ClientError as e:
            if e.code != "Neo.ClientError.Procedure.ProcedureNotFound":
                raise e

        raise Exception(
            "Please ensure that you're connecting to an AuraDS or a GDS installation "
            "with Arrow Flights Service enabled.")


class Neo4jGDSToBigQueryTemplate(BaseTemplate):  # type: ignore
    """
    Stream data from a Neo4j GDS graph into a BigQuery table using the Storage
    Write API.
    """

    @staticmethod
    def parse_args(args: Optional[Sequence[str]] = None) -> Dict[str, Any]:
        # Try pulling out any BigQuery procedure environmental args.
        bq_args = util.bq_params()
        if bq_args:
            logging.info(f"using BigQuery args: {bq_args}")

        parser = argparse.ArgumentParser()
        parser.add_argument(
            f"--{c.NEO4J_GRAPH_NAME}",
            type=str,
            help="Name of the graph projection to use.",
        )
        parser.add_argument(
            f"--{c.NEO4J_DB_NAME}",
            type=str,
            help="Name of the database that hosts the graph projection.",
            default="neo4j",
        )

        parser.add_argument(
            f"--{c.NEO4J_SECRET}",
            help="Google Secret to use for populating other values",
            type=str,
        )
        parser.add_argument(
            f"--{c.NEO4J_URI}",
            help="Neo4j URI",
            default="neo4j://localhost",
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

        parser.add_argument(
            f"--{c.NEO4J_LABELS}",
            help="Comma-separated list of labels to read from Neo4j.",
            type=lambda x: [y.strip() for y in str(x).split(",")],
            default=["*"],
        )
        parser.add_argument(
            f"--{c.NEO4J_TYPES}",
            help="Comma-separated list of relationship types to read from Neo4j.",
            type=lambda x: [y.strip() for y in str(x).split(",")],
            default=["*"],
        )
        parser.add_argument(
            f"--{c.NEO4J_PROPERTIES}",
            help="Comma-separated list of properties to read from Neo4j.",
            type=lambda x: [y.strip() for y in str(x).split(",")],
            default=[],
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
            ns, _ = parser.parse_known_args(bq_args + list(args or []))
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

        logger = self.get_logger(spark)
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
                logger.warning("failed to fetch secret, falling back to params")
            else:
                args.update(secret)

        # 3. Initialize our clients for source and sink.
        client = build_arrow_client(na.model.Graph(name=graph_name, db=db_name),
                                    str(args[c.NEO4J_URI]),
                                    str(args[c.NEO4J_USER]),
                                    str(args[c.NEO4J_PASSWORD]),
                                    int(args[c.NEO4J_CONCURRENCY]), logger=logger)
        logger.info(f"using neo4j client {client}")

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
        properties = args[c.NEO4J_PROPERTIES]
        if properties is None or properties == [""] or properties == ["null"]:
            # This most likely happens from BigQuery, so help convert to valid input.
            properties = []

        converter: Optional[Any] = None  # XXX
        if args[c.BQ_SINK_MODE].lower() == "nodes":
            topo_filters = args[c.NEO4J_LABELS]
            converter = arrow_to_nodes
            reading_fn = read_nodes(client)
            logger.info(
                f"reading nodes (labels={topo_filters}, properties={properties})"
            )
        elif args[c.BQ_SINK_MODE].lower() == "edges":
            topo_filters = args[c.NEO4J_TYPES]
            converter = arrow_to_edges
            reading_fn = read_edges(client)
            logger.info(
                f"reading edges (types={topo_filters}, properties={properties})"
            )
        else:
            raise ValueError(
                "invalid sink mode; expected either 'nodes' or 'edges'"
            )

        # Depending on the Spark environment, we may or may not be able to
        # identify the number of executor cores. For now, let's fallback to
        # the neo4j_concurrency setting.
        num_partitions = max(sc.defaultParallelism, args[c.NEO4J_CONCURRENCY])
        logger.info(f"using {num_partitions:,} partitions")
        start_time = time.time()
        results: List[Tuple[str, int]] = (
            sc
            .parallelize([(properties, topo) for topo in topo_filters])  # Seed with our config
            .flatMap(reading_fn)  # Stream the data from Neo4j. XXX this is eager!
            .repartition(num_partitions)  # Repartition to get concurrency.
            .map(batch_converter(converter, topo_filters))  # -> List[ProtoBufs]
            .mapPartitions(append_batch(bq))  # Ship 'em to BigQuery!
            .collect()
        )
        duration = time.time() - start_time

        # Crude, but let's do this for now.
        streams: List[str] = [r[0] for r in results]
        cnt: int = sum([r[1] for r in results])
        logger.info(f"sent {cnt:,} rows to BigQuery using {len(streams):,} "
                    f"stream(s) in {duration:,.3f}s ({cnt / duration:,.2f} rows/s)")

    def get_logger(self, spark: SparkSession) -> logging.Logger:
        log_4j_logger = spark.sparkContext._jvm.org.apache.log4j  # pylint: disable=protected-access
        return log_4j_logger.LogManager.getLogger(self.__class__.__name__)


class BigQueryToNeo4jGDSTemplate(BaseTemplate):  # type: ignore
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
            logging.info(f"using BigQuery args: {bq_args}")

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
            f"--{c.NEO4J_URI}",
            help="Neo4j URI",
            default="neo4j://localhost",
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
        parser.add_argument(
            f"--{c.NEO4J_ACTION}",
            default="create_graph",
            type=str,
            help="The action to perform, one of create_graph or create_database."
        )
        parser.add_argument(
            f"--{c.NEO4J_FORCE}",
            default=False,
            type=bool,
            help="Whether to force the creation of the graph, by aborting an existing import or the database, "
                 "by overwriting the existing data files."
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
        debug = args[c.DEBUG]
        sc = spark.sparkContext
        if debug:
            sc.setLogLevel("DEBUG")
        else:
            sc.setLogLevel("INFO")

        logger = self.get_logger(spark)
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

        graph.validate()

        # 2a. Fetch our secret if any
        if c.NEO4J_SECRET in args:
            logger.info(f"fetching secret {args[c.NEO4J_SECRET]}")
            secret = util.fetch_secret(args[c.NEO4J_SECRET])
            if debug:
                redacted_copy = {}
                for key in secret:
                    value = secret[key]
                    if key == c.NEO4J_PASSWORD:
                        value = "<redacted>"
                    redacted_copy[key] = value
                logger.debug(f"fetched secret with values {redacted_copy}")
            if not secret:
                logger.warning("failed to fetch secret, falling back to params")
            else:
                args.update(secret)

        # 2b. Initialize our clients for source and sink.
        client = build_arrow_client(graph,
                                    str(args[c.NEO4J_URI]),
                                    str(args[c.NEO4J_USER]),
                                    str(args[c.NEO4J_PASSWORD]),
                                    int(args[c.NEO4J_CONCURRENCY]), logger=logger)
        bq = BigQuerySource(args[c.BQ_PROJECT], args[c.BQ_DATASET])
        logger.info(f"using neo4j client {client}")

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
        force = bool(args[c.NEO4J_FORCE])
        if str(args[c.NEO4J_ACTION]).lower() == "create_database":
            result = client.start_create_database(force=force)
        else:
            result = client.start_create_graph(force=force)

        logger.info(f"starting import for {result.get('name', graph.name)}")

        # 5. Load our Nodes via PySpark workers.
        num_partitions = max(sc.defaultParallelism, args[c.NEO4J_CONCURRENCY])
        logger.info(f"using {num_partitions:,} partitions")
        nodes_start = time.time()
        cnt, size = (
            sc
            .parallelize(node_streams, num_partitions)
            .map(bq.consume_stream, True)  # don't shuffle
            .map(send_nodes(client, graph))
            .reduce(tuple_sum)
        )
        logger.info(
            f"streamed {cnt:,} nodes, ~{size / (1 << 20):,.2f} MiB original size"
        )

        # 5b. Assert we actually got nodes
        if cnt < 1:
            logger.error(f"failed to load nodes; aborting.")
            sys.exit(1)

        # 6. Signal we're done with Nodes before moving onto Edges.
        result = client.nodes_done()
        duration = time.time() - nodes_start
        total = result["node_count"]
        logger.info(
            f"signalled nodes complete, imported {total:,} nodes"
            f" in {duration:,.3f}s ({total / duration:,.2f} nodes/s)"
        )
        if cnt != total:
            logger.warning(f"sent {cnt} nodes, but imported {total}!")

        # 7. Now stream Edges via the PySpark workers.
        edges_start = time.time()
        cnt, size = (
            sc
            .parallelize(edge_streams, num_partitions)
            .map(bq.consume_stream, True)  # don't shuffle
            .map(send_edges(client, graph))
            .reduce(tuple_sum)
        )
        logger.info(
            f"streamed {cnt:,} edges, ~{size / (1 << 20):,.2f} MiB original size"
        )

        # 8. Signal we're done with Edges.
        result = client.edges_done()
        duration = time.time() - edges_start
        total = result["relationship_count"]
        logger.info(
            f"signalled edges complete, imported {total:,} edges"
            f" in {duration:,.3f}s ({total / duration:,.2f} edges/s)"
        )
        if cnt != total:
            logger.warning(f"sent {cnt} edges, but imported {total}!")

        # 9. TODO: await import completion and GDS projection available
        duration = time.time() - start_time
        logger.info(f"completed in {duration:,.3f} seconds")

    def get_logger(self, spark: SparkSession) -> logging.Logger:
        log_4j_logger = spark.sparkContext._jvm.org.apache.log4j  # pylint: disable=protected-access
        return log_4j_logger.LogManager.getLogger(self.__class__.__name__)
