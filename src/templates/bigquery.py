# Copyright (c) "Neo4j"
# Neo4j Sweden AB [https://neo4j.com]
import argparse
import concurrent
import itertools
import logging
import sys
from concurrent.futures import ThreadPoolExecutor

import neo4j
import neo4j.exceptions

import time

import pyarrow.parquet
from dataproc_templates import BaseTemplate

import pyarrow as pa
from pyspark.sql import SparkSession

import neo4j_arrow as na

import pattern_parser
from .bq_client import BigQuerySource, BigQuerySink, BQStream
from . import constants as c, util

from model import Node, Edge, arrow_to_nodes, arrow_to_edges

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
    except Exception:
        return None


def send_nodes(
    client: na.Neo4jArrowClient,
    model: Optional[na.model.Graph] = None,
    source_field: str = "_table",
) -> Callable[[Any], Tuple[int, int]]:
    """
    Wrap the given client, model, and (optional) source_field in a function that
    streams PyArrow data (Table or RecordBatch) to Neo4j as nodes.
    """

    def _send_nodes(table: Any) -> Tuple[int, int]:
        result: Tuple[int, int] = client.write_nodes(table, model, source_field)
        return result

    return _send_nodes


def send_edges(
    client: na.Neo4jArrowClient,
    model: Optional[na.model.Graph] = None,
    source_field: str = "_table",
) -> Callable[[Any], Tuple[int, int]]:
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


def node_to_stream_fn(
    bq: BigQuerySource, graph: na.model.Graph
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
    bq: BigQuerySource, graph: na.model.Graph
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


def convert_batch(
    arrow: Arrow, converter: Callable[[Arrow], Generator[Any, None, None]]
) -> List[bytes]:
    batch: List[bytes] = []
    for graph_element in converter(arrow):
        batch.append(graph_element.SerializeToString())
    return batch


def read_nodes(
    client: na.Neo4jArrowClient, pattern: pattern_parser.NodePattern
) -> pa.Table:
    batches = client.read_nodes(pattern.properties, labels=[pattern.label])
    tb = pa.Table.from_batches(batches)
    # tb = tb.append_column('label', pa.array([pattern.label] * len(tb), pa.string()))
    return tb


def read_edges(
    client: na.Neo4jArrowClient, pattern: pattern_parser.EdgePattern
) -> pa.Table:
    batches = client.read_edges(
        properties=pattern.properties, relationship_types=[pattern.type]
    )
    tb = pa.Table.from_batches(batches)
    tb = tb.append_column("type", pa.array([pattern.type] * len(tb), pa.string()))
    return tb


def read_by_pattern(
    client: na.Neo4jArrowClient, pattern: pattern_parser.Pattern
) -> pa.Table:
    if isinstance(pattern, pattern_parser.NodePattern):
        return read_nodes(client, pattern)
    elif isinstance(pattern, pattern_parser.EdgePattern):
        return read_edges(client, pattern)
    else:
        raise ValueError(
            f"expected an instance of Pattern, but got {pattern.__class__.__name__}"
        )


def concat_tables(tables: list[pa.Table]) -> pa.Table:
    # see if all tables are in the same schema, if so we can use a zero-copy concatenation
    promote = False
    prev_schema = tables[0].schema
    for schema in [tb.schema for tb in tables[1:]]:
        if not prev_schema.equals(schema):
            promote = True
            break

    result = pa.concat_tables(tables, promote=promote)

    # check if the tables are for nodes
    # if 'label' in result.schema.names:
    #     field_mappings = {}
    #     for sch in [tb.schema for tb in tables]:
    #         for name in sch.names:
    #             if name not in field_mappings and name != "nodeId":
    #                 field_mappings[name] = "one"
    #     field_mappings["label"] = "list"

    #         # group all rows by nodeId and collect labels into a single column of list
    #         aggregations = list(field_mappings.items())
    #         result = result.group_by(["nodeId"]).aggregate(aggregations)

    #         # aggregate returns modified column names, so let's reset our column names
    #         new_column_names = [('labels' if mapping[0] == 'label' else mapping[0]) for mapping in aggregations]
    #         new_column_names.append("nodeId")
    #         result = result.rename_columns(new_column_names)

    return result


def append_batch(batches: Iterable[List[bytes]], sink: BigQuerySink) -> None:
    cnt = 0
    for batch in batches:
        sink.append_rows(batch)
        cnt += len(batch)

    logging.info(f"appended {cnt:,} rows")


def build_arrow_client(
    graph: na.model.Graph,
    neo4j_uri: str,
    neo4j_username: str,
    neo4j_password: str,
    neo4j_concurrency: int,
    *,
    logger: Any = None,
) -> na.Neo4jArrowClient:
    neo4j_logger = logging.getLogger("neo4j")
    neo4j_logger.handlers.clear()
    neo4j_logger.addHandler(util.SparkLogHandler(logger))

    with neo4j.GraphDatabase.driver(
        neo4j_uri, auth=neo4j.basic_auth(neo4j_username, neo4j_password)
    ) as driver:
        try:
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
                    logger=arrow_logger,
                )
        except neo4j.exceptions.ClientError as e:
            if e.code != "Neo.ClientError.Procedure.ProcedureNotFound":
                raise e

        raise Exception(
            "Please ensure that you're connecting to an AuraDS or a GDS installation "
            "with Arrow Flights Service enabled."
        )


def send_batch(
    batches: Iterable[Arrow],
    converter: Callable[[Arrow], Generator[Any, None, None]],
    sink: BigQuerySink,
) -> None:
    append_batch([convert_batch(batch, converter) for batch in batches], sink)


def split_into_batches(
    iterable: Iterable[pa.RecordBatch], size: int
) -> Generator[Tuple[int, Any], None, None]:
    counter = 0
    it = iter(iterable)
    item = list(itertools.islice(it, size))
    while item:
        yield counter, item
        item = list(itertools.islice(it, size))
        counter += 1


def process_table(
    table: pyarrow.Table,
    num_partitions: int,
    converter: Callable[[Arrow], Generator[Any, None, None]],
    sink_factory: Callable[[], BigQuerySink],
    *,
    max_chunk_size: int = 10_000,
) -> Tuple[int, int]:
    bq_sinks = [sink_factory() for _ in range(0, num_partitions)]
    batches = table.to_batches(max_chunksize=max_chunk_size)

    with ThreadPoolExecutor(num_partitions) as executor:
        futures = [
            executor.submit(
                send_batch, batch, converter, bq_sinks[index % num_partitions]
            )
            for (index, batch) in split_into_batches(batches, 5)
        ]

        for future in concurrent.futures.as_completed(futures):
            if not future.exception():
                continue
            raise future.exception()  # type: ignore

    for sink in bq_sinks:
        if sink.stream_name:
            sink.finalize_write_stream()
            sink.commit()

    return num_partitions, table.num_rows


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

    # TODO: validate required table arguments based on patterns

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
        if c.NEO4J_SECRET in args:
            logger.info(f"fetching secret {args[c.NEO4J_SECRET]}")
            secret = util.fetch_secret(args[c.NEO4J_SECRET])
            if not secret:
                logger.warning("failed to fetch secret, falling back to params")
            else:
                args.update(secret)

        # 3. Initialize our clients for source and sink.
        client = build_arrow_client(
            na.model.Graph(name=graph_name, db=db_name),
            str(args[c.NEO4J_URI]),
            str(args[c.NEO4J_USER]),
            str(args[c.NEO4J_PASSWORD]),
            int(args[c.NEO4J_CONCURRENCY]),
            logger=logger,
        )
        logger.info(f"using neo4j client {client}")

        patterns: list[pattern_parser.Pattern] = args[c.NEO4J_PATTERNS]

        # Depending on the Spark environment, we may or may not be able to
        # identify the number of executor cores. For now, let's fallback to
        # the neo4j_concurrency setting.
        num_partitions = max(sc.defaultParallelism, args[c.NEO4J_CONCURRENCY])
        logger.info(f"using {num_partitions:,} streams")
        start_time = time.time()

        num_streams, num_rows = 0, 0
        if any(filter(pattern_parser.Pattern.is_node, patterns)):
            # Handle nodes
            nodes_data = [
                read_by_pattern(client, pattern)
                for pattern in filter(pattern_parser.Pattern.is_node, patterns)
            ]
            nodes_tb = concat_tables(nodes_data)
            p_streams, p_rows = process_table(
                nodes_tb,
                num_partitions,
                arrow_to_nodes,
                lambda: BigQuerySink(
                    args[c.BQ_PROJECT],
                    args[c.BQ_DATASET],
                    args[c.BQ_NODE_TABLE],
                    Node.DESCRIPTOR,
                    logger=logger,
                ),
                max_chunk_size=client.max_chunk_size,
            )
            num_streams += p_streams
            num_rows += p_rows

        if any(filter(pattern_parser.Pattern.is_edge, patterns)):
            # Handle edges
            edges_data = [
                read_by_pattern(client, pattern)
                for pattern in filter(pattern_parser.Pattern.is_edge, patterns)
            ]
            edges_tb = concat_tables(edges_data)
            p_streams, p_rows = process_table(
                edges_tb,
                num_partitions,
                arrow_to_edges,
                lambda: BigQuerySink(
                    args[c.BQ_PROJECT],
                    args[c.BQ_DATASET],
                    args[c.BQ_EDGE_TABLE],
                    Edge.DESCRIPTOR,
                    logger=logger,
                ),
                max_chunk_size=client.max_chunk_size,
            )
            num_streams += p_streams
            num_rows += p_rows

        duration = time.time() - start_time
        logger.info(
            f"sent {num_rows:,} rows to BigQuery using {num_streams:,} "
            f"stream(s) in {duration:,.3f}s ({num_rows / duration:,.2f} rows/s)"
        )

    def get_logger(self, spark: SparkSession) -> logging.Logger:
        log_4j_logger = (
            spark.sparkContext._jvm.org.apache.log4j  # type: ignore
        )  # pylint: disable=protected-access
        return logging.Logger(
            log_4j_logger.LogManager.getLogger(self.__class__.__name__)
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

        if c.NEO4J_SECRET in args:
            apply_secrets(args, debug, logger)

        # 2b. Initialize our clients for source and sink.
        client = build_arrow_client(
            graph,
            str(args[c.NEO4J_URI]),
            str(args[c.NEO4J_USER]),
            str(args[c.NEO4J_PASSWORD]),
            int(args[c.NEO4J_CONCURRENCY]),
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
            result = client.start_create_database(force=force)
        else:
            result = client.start_create_graph(force=force)

        logger.info(f"starting import for {result.get('name', graph.name)}")

        # 5. Load our Nodes via PySpark workers.
        num_partitions = max(sc.defaultParallelism, args[c.NEO4J_CONCURRENCY])
        logger.info(f"using {num_partitions:,} partitions")
        nodes_start = time.time()
        cnt, size = (
            sc.parallelize(node_streams, num_partitions)
            .map(bq.consume_stream, True)  # don't shuffle
            .map(send_nodes(client, graph))
            .reduce(tuple_sum)
        )
        logger.info(
            f"streamed {cnt:,} nodes, ~{size / (1 << 20):,.2f} MiB original size"
        )

        # 5b. Assert we actually got nodes
        if cnt < 1:
            logger.error("failed to load nodes; aborting.")
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
            sc.parallelize(edge_streams, num_partitions)
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
        log_4j_logger = (
            spark.sparkContext._jvm.org.apache.log4j  # type: ignore
        )  # pylint: disable=protected-access
        return logging.Logger(
            log_4j_logger.LogManager.getLogger(self.__class__.__name__)
        )


def load_graph(args: Dict[str, Any]) -> na.model.Graph:
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
