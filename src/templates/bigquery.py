# Copyright (c) "Neo4j"
# Neo4j Sweden AB [https://neo4j.com]
import argparse
import logging
import sys
import time
from typing import (
    Any,
    Callable,
    Dict,
    Generator,
    Iterable,
    List,
    Optional,
    Sequence,
    Tuple,
    Union,
    cast,
)

import neo4j
import neo4j.exceptions
import pyarrow as pa
from dataproc_templates import BaseTemplate
from google.protobuf import descriptor_pb2
from graphdatascience.query_runner import gds_arrow_client
from graphdatascience.query_runner.gds_arrow_client import GdsArrowClient
from pandas import DataFrame
from pyarrow import Table, RecordBatch
from pyspark.sql import SparkSession

import pattern_parser
from model import Edge, Node, arrow_to_nodes, arrow_to_edges
from . import constants as c, util
from .bq_client import BigQuerySource, BigQuerySink, BQStream
from .mapper import node_mapper, edge_mapper, MappingFn
from .model import Graph

__all__ = [
    "BigQueryToNeo4jGDSTemplate",
    "Neo4jGDSToBigQueryTemplate",
]

Arrow = Union[pa.Table, pa.RecordBatch]
ArrowData = Union[pa.Table,  Iterable[pa.RecordBatch]]
DataStream = Generator[Arrow, None, None]


class ProgressTracker:
    def __init__(self):
        self.progress = 0

    def callback(self, progress: int) -> None:
        self.progress = progress


def load_model_from_gcs(uri: str) -> Optional[Graph]:
    """
    Attempt to load a Graph model from a GCS uri. Returns None on failure.
    """
    try:
        import fsspec

        with fsspec.open(uri, "rt") as f:
            return Graph.from_json(f.read())
    except Exception:
        return None


def flatten(
        lists: List[List[Any]], fn: Optional[Callable[[Any], Any]] = None
) -> List[Any]:
    """
    Flatten a list of lists, applying an optional function (fn) to the initial
    list of lists.
    """
    if not fn:
        def fn(x: Any) -> Any:
            return x

    return [x for y in map(fn, lists) for x in y]


def map_tables(model: Graph, entity_type: str) -> Callable[[DataStream], List[ArrowData]]:
    def map_entity(table: Arrow, mapper: MappingFn):
        if not table:
            raise Exception("empty iterable of record batches provided")
        mapped_table = mapper(table)
        if isinstance(table, RecordBatch):
            mapped_table = [mapped_table]
        return mapped_table

    def _map_arrow_tables(table: DataStream) -> List[ArrowData]:
        source_field = "_table"
        if entity_type == "NODE":
            mapper = node_mapper(model, source_field)
        else:
            mapper = edge_mapper(model, source_field)

        mapped_tables = [map_entity(t, mapper) for t in table]
        return mapped_tables

    return _map_arrow_tables


def send_nodes(
        client: GdsArrowClient,
        model: Optional[Graph] = None,
) -> Callable[[ArrowData], int]:
    """
    Wrap the given client, model, and (optional) source_field in a function that
    streams PyArrow data (Table or RecordBatch) to Neo4j as nodes.
    """

    def _send_nodes_gds(table: ArrowData) -> int:
        nodes_progress = ProgressTracker()

        client.upload_nodes(model.name, table, progress_callback=nodes_progress.callback)
        return nodes_progress.progress

    return _send_nodes_gds


def send_edges(
        client: GdsArrowClient,
        model: Optional[Graph] = None,
) -> Callable[[ArrowData], int]:
    """
    Wrap the given client, model, and (optional) source_field in a function that
    streams PyArrow data (Table or RecordBatch) to Neo4j as relationships.
    """

    def _send_edges_gds(table: ArrowData) -> int:
        edge_progress = ProgressTracker()

        client.upload_relationships(model.name, table, progress_callback=edge_progress.callback)
        return edge_progress.progress

    return _send_edges_gds


def tuple_sum(a: Tuple[int, int], b: Tuple[int, int]) -> Tuple[int, int]:
    """
    Reducing function for summing tuples of integers.
    """
    return a[0] + b[0], a[1] + b[1]


def node_to_stream_fn(
        bq: BigQuerySource, graph: Graph
) -> Callable[[str], List[BQStream]]:
    """
    Create a function that generates BigQuery streams with optional field
    filtering.
    """

    def _node_to_stream(table_name: str) -> List[BQStream]:
        fields: List[str] = []
        node = graph.node_for_src(table_name)
        if node:
            if node.key_field:
                fields.append(node.key_field)
            if node.label_field:
                fields.append(node.label_field)
            for key in node.properties.keys():
                fields.append(key)
        return bq.table(table_name, fields=fields)

    return _node_to_stream


def edge_to_stream_fn(
        bq: BigQuerySource, graph: Graph
) -> Callable[[str], List[BQStream]]:
    """
    Create a function that generates BigQuery streams with optional field
    filtering.
    """

    def _edge_to_stream(table_name: str) -> List[BQStream]:
        fields: List[str] = []
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
        return bq.table(table_name, fields=fields)

    return _edge_to_stream


def convert_batch_fun(
        converter: Callable[[Arrow], Generator[Any, None, None]]
) -> Callable[[Arrow], List[bytes]]:
    def _convert_batch(arrow: Arrow) -> List[bytes]:
        batch: List[bytes] = []
        for graph_element in converter(arrow):
            batch.append(graph_element.SerializeToString())
        return batch

    return _convert_batch


def read_nodes(
        client: GdsArrowClient, pattern: pattern_parser.NodePattern, graph_name: str, database: str
) -> pa.RecordBatch:
    labels = pattern.label
    properties = pattern.properties

    dataframe: DataFrame = client.get_node_properties(
        graph_name,
        database,
        properties if properties is not None else [],
        labels if labels is not None else ["*"],
        True,
    )
    if 'labels' not in dataframe.columns:
        dataframe['labels'] = pattern.label

    return pa.RecordBatch.from_pandas(df=dataframe)


def read_edges(
        client: GdsArrowClient, pattern: pattern_parser.EdgePattern, graph_name: str, database: str
) -> pa.RecordBatch:
    properties = pattern.properties

    relationship_types = pattern.type if pattern.type is not None else ["*"]
    if properties:
        dataframe: DataFrame = client.get_relationship_properties(
            graph_name,
            database,
            properties,
            relationship_types,
        )
    else:
        dataframe: DataFrame = client.get_relationships(
            graph_name,
            database,
            relationship_types,
        )

    if 'type' not in dataframe.columns:
        dataframe['type'] = pattern.type

    return pa.RecordBatch.from_pandas(df=dataframe)


def read_by_pattern_fun(
        client: GdsArrowClient,
        graph_name: str,
        database: str,
) -> Callable[[pattern_parser.Pattern], pa.RecordBatch]:
    def _read_by_pattern(pattern: pattern_parser.Pattern) -> pa.RecordBatch:
        if isinstance(pattern, pattern_parser.NodePattern):
            return read_nodes(client, pattern, graph_name, database)
        elif isinstance(pattern, pattern_parser.EdgePattern):
            return read_edges(client, pattern, graph_name, database)
        else:
            raise ValueError(
                f"expected an instance of Pattern, but got {pattern.__class__.__name__}"
            )

    return _read_by_pattern


def append_batch_fun(
        sink_factory: Callable[[], BigQuerySink],
) -> Callable[[Iterable[List[bytes]]], Iterable[Tuple[str, int]]]:
    def append_batch(batches: Iterable[List[bytes]]) -> Iterable[Tuple[str, int]]:
        sink = sink_factory()
        cnt = 0
        for batch in batches:
            sink.append_rows(batch)
            cnt += len(batch)
        if cnt > 0:
            logging.info(f"appended {cnt:,} rows")
            sink.finalize_write_stream()
            sink.commit()
            yield cast(str, sink.stream_name), cnt

    return append_batch


def load_graph(args: Dict[str, Any]) -> Graph:
    # 1. Load the Graph Model.
    if args[c.NEO4J_GRAPH_JSON]:
        # Try loading a literal JSON-based model
        json_str = args[c.NEO4J_GRAPH_JSON]
        graph = Graph.from_json(json_str)
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

    graph.validate()

    return graph


def apply_secrets(args: Dict[str, Any], debug: bool, logger: logging.Logger) -> None:
    # 2a. Fetch our secret if any
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


def build_gds_arrow_client(
        neo4j_uri: str,
        neo4j_username: str,
        neo4j_password: str,
        *,
        logger: Any = None,
) -> GdsArrowClient:
    neo4j_logger = logging.getLogger("neo4j")
    neo4j_logger.handlers.clear()
    neo4j_logger.addHandler(util.SparkLogHandler(logger))

    with neo4j.GraphDatabase.driver(
            neo4j_uri, auth=neo4j.basic_auth(neo4j_username, neo4j_password)
    ) as driver:
        try:
            # deprecated
            record: Optional[neo4j.Record] = driver.execute_query(
                "CALL gds.debug.arrow()", result_transformer_=neo4j.Result.single
            )
            if record:
                enabled = bool(record["enabled"])
                if not enabled:
                    raise Exception(
                        "Please ensure that Arrow Flights Service is enabled."
                    )
                running = bool(record["running"])
                if not running:
                    raise Exception(
                        "Please ensure that Arrow Flights Service is running."
                    )
                advertised_listen_address = str(record["advertisedListenAddress"])
                listen_address = str(record["listenAddress"])

                uri = advertised_listen_address
                if not uri:
                    uri = listen_address

                arrow_logger = logging.getLogger("neo4j-arrow-client")
                arrow_logger.handlers.clear()
                arrow_logger.addHandler(util.SparkLogHandler(logger))

                host, port = uri.split(":")

                gds_client = gds_arrow_client.GdsArrowClient(
                    host=host,
                    port=int(port),
                    auth=(neo4j_username, neo4j_password)
                )
                return gds_client
        except neo4j.exceptions.ClientError as e:
            if e.code != "Neo.ClientError.Procedure.ProcedureNotFound":
                raise e

        raise Exception(
            "Please ensure that you're connecting to an AuraDS or a GDS installation "
            "with Arrow Flights Service enabled."
        )


def sink_fun(
        project_id: str,
        dataset: str,
        table: str,
        descriptor: descriptor_pb2.DescriptorProto,
) -> Callable[[], BigQuerySink]:
    def sink() -> BigQuerySink:
        return BigQuerySink(project_id, dataset, table, descriptor)

    return sink


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
            f"--{c.BQ_NODE_TABLE}",
            help="BigQuery table to write the Neo4j nodes data.",
            type=str,
        )
        parser.add_argument(
            f"--{c.BQ_EDGE_TABLE}",
            help="BigQuery table to write the Neo4j edges data.",
            type=str,
        )
        parser.add_argument(
            f"--{c.BQ_PROJECT}",
            type=str,
            help="GCP project containing BigQuery tables.",
        )
        parser.add_argument(
            f"--{c.BQ_DATASET}",
            type=str,
            help="BigQuery dataset containing BigQuery tables.",
        )

        parser.add_argument(
            f"--{c.NEO4J_PATTERNS}",
            help="Comma-separated list of node/edge patterns to read from Neo4j.",
            type=lambda x: pattern_parser.parse_pattern(x),
            required=True,
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
            f"starting job for {args[c.NEO4J_PATTERNS]} into "
            f"{args[c.BQ_PROJECT]}/{args[c.BQ_DATASET]}/{args[c.BQ_NODE_TABLE]} and "
            f"{args[c.BQ_PROJECT]}/{args[c.BQ_DATASET]}/{args[c.BQ_EDGE_TABLE]} "
            f"(server concurrency={args[c.NEO4J_CONCURRENCY]})"
        )

        # 1. Get the graph and database name.
        graph_name = args[c.NEO4J_GRAPH_NAME]
        db_name = args[c.NEO4J_DB_NAME]

        # 2. Fetch our secret if any
        if args[c.NEO4J_SECRET]:
            apply_secrets(args, args[c.DEBUG], logger)

        # 3. Initialize our clients for source and sink.
        # replace graph

        client = build_gds_arrow_client(
            str(args[c.NEO4J_URI]),
            str(args[c.NEO4J_USER]),
            str(args[c.NEO4J_PASSWORD]),
            logger=logger
        )

        logger.info(f"using neo4j client {client}")

        patterns: list[pattern_parser.Pattern] = args[c.NEO4J_PATTERNS]

        # Depending on the Spark environment, we may or may not be able to
        # identify the number of executor cores. For now, let's fallback to
        # the neo4j_concurrency setting.
        num_partitions = max(sc.defaultParallelism, args[c.NEO4J_CONCURRENCY])
        logger.info(f"using {num_partitions:,} streams")
        start_time = time.time()

        def process_patterns(
                filter_fun: Callable[[Any], bool],
                converter: Callable[[Arrow], Generator[Any, None, None]],
                sink_factory: Callable[[], BigQuerySink],
        ) -> Tuple[int, int]:
            results: List[Tuple[str, int]] = (
                sc.parallelize([p for p in filter(filter_fun, patterns)])
                .map(read_by_pattern_fun(client, graph_name, db_name))
                .map(convert_batch_fun(converter))
                .mapPartitions(append_batch_fun(sink_factory))
                .collect()
            )

            return len(results), sum([r[1] for r in results])

        node_descriptor = descriptor_pb2.DescriptorProto()
        Node.DESCRIPTOR.CopyToProto(node_descriptor)
        node_streams, node_rows = process_patterns(
            pattern_parser.Pattern.is_node,
            arrow_to_nodes,
            sink_fun(
                args[c.BQ_PROJECT],
                args[c.BQ_DATASET],
                args[c.BQ_NODE_TABLE],
                node_descriptor,
            ),
        )

        edge_descriptor = descriptor_pb2.DescriptorProto()
        Edge.DESCRIPTOR.CopyToProto(edge_descriptor)
        edge_streams, edge_rows = process_patterns(
            pattern_parser.Pattern.is_edge,
            arrow_to_edges,
            sink_fun(
                args[c.BQ_PROJECT],
                args[c.BQ_DATASET],
                args[c.BQ_EDGE_TABLE],
                edge_descriptor,
            ),
        )

        num_streams, num_rows = node_streams + edge_streams, node_rows + edge_rows
        duration = time.time() - start_time
        logger.info(
            f"sent {num_rows:,} rows to BigQuery using {num_streams:,} "
            f"stream(s) in {duration:,.3f}s ({num_rows / duration:,.2f} rows/s)"
        )

    def get_logger(self, spark: SparkSession) -> logging.Logger:
        log_4j_logger = (
            spark.sparkContext._jvm.org.apache.log4j  # type: ignore
        )  # pylint: disable=protected-access
        return cast(
            logging.Logger, log_4j_logger.LogManager.getLogger(self.__class__.__name__)
        )


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
            help="The action to perform, one of create_graph or create_database.",
        )
        parser.add_argument(
            f"--{c.NEO4J_FORCE}",
            default=False,
            type=bool,
            help="Whether to force the creation of the graph, by aborting an existing import or the database, "
                 "by overwriting the existing data files.",
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
            help="GCP project containing BigQuery tables.",
        )
        parser.add_argument(
            f"--{c.BQ_DATASET}",
            type=str,
            help="BigQuery dataset containing BigQuery tables.",
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

        graph = load_graph(args)
        logger.info(f"using graph ${graph.to_json()}")

        if args[c.NEO4J_SECRET]:
            apply_secrets(args, debug, logger)

        # 2b. Initialize our clients for source and sink.

        graph_name = args[c.NEO4J_GRAPH_NAME]
        db_name = args[c.NEO4J_DB_NAME]

        client = build_gds_arrow_client(
            str(args[c.NEO4J_URI]),
            str(args[c.NEO4J_USER]),
            str(args[c.NEO4J_PASSWORD]),
            logger=logger,
        )
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
            list(map(node_to_stream_fn(bq, graph), args[c.NODE_TABLES]))
        )
        edge_streams = flatten(
            list(map(edge_to_stream_fn(bq, graph), args[c.EDGE_TABLES]))
        )
        logger.info(
            f"prepared {len(node_streams):,} node streams, "
            f"{len(edge_streams):,} edge streams"
        )

        # 4. Begin our Graph import.
        force = bool(args[c.NEO4J_FORCE])
        if str(args[c.NEO4J_ACTION]).lower() == "create_database":
            client.create_database(database=db_name, force=force)
        else:
            client.create_graph(graph_name=graph_name, database=db_name)

        logger.info(f"starting import for {graph_name}")

        # 5. Load our Nodes via PySpark workers.
        num_partitions = max(sc.defaultParallelism, args[c.NEO4J_CONCURRENCY])
        logger.info(f"using {num_partitions:,} partitions")
        nodes_start = time.time()

        nodes_count = (sc.parallelize(node_streams, num_partitions)
                       .map(bq.consume_stream, True)
                       .flatMap(map_tables(graph, "NODE"))
                       .repartition(num_partitions)
                       .map(send_nodes(client, graph))
                       .sum()
                       )

        logger.info(
            f"streamed {nodes_count} nodes"
        )

        # # 5b. Assert we actually got nodes
        if nodes_count < 1:
            logger.error("failed to load nodes; aborting.")
            sys.exit(1)

        # 6. Signal we're done with Nodes before moving onto Edges.
        result = client.node_load_done(graph.name)
        duration = time.time() - nodes_start
        total = result.node_count
        logger.info(
            f"signalled nodes complete, imported {total:,} nodes"
            f" in {duration:,.3f}s ({total / duration:,.2f} nodes/s)"
        )
        if nodes_count != total:
            logger.warning(f"sent {nodes_count} nodes, but imported {total}!")

        # 7. Now stream Edges via the PySpark workers.
        edges_start = time.time()
        edges_count = (
            sc.parallelize(edge_streams, num_partitions)
            .map(bq.consume_stream, True)  # don't shuffle
            .flatMap(map_tables(graph, "EDGE"))
            .repartition(num_partitions)
            .map(send_edges(client, graph))
            .sum()
        )

        logger.info(
            f"streamed {edges_count} edges"
        )

        # 8. Signal we're done with Edges.
        result = client.relationship_load_done(graph.name)
        duration = time.time() - edges_start
        total = result.relationship_count
        logger.info(
            f"signalled edges complete, imported {total:,} edges"
            f" in {duration:,.3f}s ({total / duration:,.2f} edges/s)"
        )
        if edges_count != total:
            logger.warning(f"sent {edges_count} edges, but imported {total}!")

        # 9. TODO: await import completion and GDS projection available

        duration = time.time() - start_time
        logger.info(f"completed in {duration:,.3f} seconds")

    def get_logger(self, spark: SparkSession) -> logging.Logger:
        log_4j_logger = (
            spark.sparkContext._jvm.org.apache.log4j  # type: ignore
        )  # pylint: disable=protected-access
        return cast(
            logging.Logger, log_4j_logger.LogManager.getLogger(self.__class__.__name__)
        )
