###
import argparse

from google.cloud.bigquery_storage import BigQueryReadClient, types
from dataproc_templates import BaseTemplate

import pyarrow as pa
from pyspark.sql import SparkSession

import neo4j_arrow as na

from . import util

from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple


def fetch_table(table_type: str) -> Callable[[str], Any]:
    def _fetch(name: str):
        client = BigQueryReadClient()
        reader = client.read_rows(name)
        table = reader.to_arrow()
        schema = table.schema.with_metadata({"_type": table_type})
        table = table.from_arrays(table.columns, schema=schema)
        return table
    return _fetch


def send_nodes(client: na.Neo4jArrowClient,
               model: Optional[na.model.Graph] = None,
               source_field: str = "_type") -> Callable[[Any], Tuple[int, int]]:
    def _send_nodes(table: Any) -> Tuple[int, int]:
        result: Tuple[int, int] = client.write_nodes(table, model, source_field)
        return result
    return _send_nodes


def send_edges(client: na.Neo4jArrowClient,
               model: Optional[na.model.Graph] = None,
               source_field: str = "_type") -> Callable[[Any], Tuple[int, int]]:
    def _send_nodes(table: Any) -> Tuple[int, int]:
        result: Tuple[int, int] = client.write_edges(table, model, source_field)
        return result
    return _send_nodes


def tuple_sum(a: Tuple[int, int], b: Tuple[int, int]) -> Tuple[int, int]:
    return (a[0] + b[0], a[1] + b[1])


def get_streams(table: str, dataset: str, project_id: str) -> List[str]:
    client = BigQueryReadClient()
    read_session = types.ReadSession(
        table=f"projects/{project_id}/datasets/{dataset}/tables/{table}",
        data_format=types.DataFormat.ARROW
    )
    # TODO filter fields
    # read_session.read_options.selected_fields=["id"]
    session = client.create_read_session(
        parent=f"projects/{project_id}",
        read_session=read_session,
    )
    return [stream.name for stream in session.streams]


class Experiment(BaseTemplate):
    """Experiment!"""

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
        logger.info("Starting...")

        # Fetch our list of streams
        # TODO: needs to be extrapolated over the node/edge tables we need


        # XXX Graph Model hardcoded for now...
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

        neo4j = na.Neo4jArrowClient(args["neo4j_host"],
                                    graph.name,
                                    port=args["neo4j_port"],
                                    tls=args["neo4j_use_tls"],
                                    database=graph.db,
                                    user=args["neo4j_user"],
                                    password=args["neo4j_password"],
                                    concurrency=args["neo4j_concurrency"])
        # PySpark time
        sc = spark.sparkContext
        result = neo4j.start(force=True) # todo: force should be an argument
        logger.info(f"starting feed: {result}")
        nodes = (
            sc
            .parallelize(get_streams("papers",
                                     args["bq_dataset"],
                                     args["bq_project"]))
            .map(fetch_table("papers"))
            .map(send_nodes(neo4j, graph))
            .reduce(tuple_sum)
        )
        logger.info(f"Nodes: {nodes}")
        result = neo4j.nodes_done()
        logger.info(f"Nodes result: {result}")

        edges = (
            sc
            .parallelize(get_streams("citations",
                                     args["bq_dataset"],
                                     args["bq_project"]))
            .map(fetch_table("citations"))
            .map(send_edges(neo4j, graph))
            .reduce(tuple_sum)
        )
        logger.info(f"Edges: {edges}")
        result = neo4j.edges_done()
        logger.info(f"Edges result: {result}")
