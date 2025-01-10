import os
import random
import string
from typing import Callable, Tuple

import neo4j
import pandas
import pyarrow
import pytest
import testcontainers.neo4j

from google.cloud import bigquery

user_count = 10
user_id_base = 1000
user_node_id_base = 0
question_count = 20
question_id_base = 2000
question_node_id_base = 100
answer_count = 40
answer_id_base = 3000
answer_node_id_base = 200


def users_table() -> pyarrow.Table:
    node_ids = [user_node_id_base + seq for seq in range(0, user_count)]
    names = ["node_id", "id", "user_name", "full_name", "label"]
    node_ids_data = pyarrow.array(list(node_ids))
    ids_data = pyarrow.array([seq + user_id_base for seq in node_ids])
    user_names_data = pyarrow.array([f"user_{seq + user_id_base}" for seq in node_ids])
    full_name_data = pyarrow.array(
        [f"full_{seq + user_id_base} name_{seq + user_id_base}" for seq in node_ids]
    )
    labels_data = pyarrow.array(["User" for _ in node_ids])

    result = pyarrow.Table.from_arrays(
        [node_ids_data, ids_data, user_names_data, full_name_data, labels_data],
        names=names,
    ).replace_schema_metadata({"_table": "users"})

    return result


def questions_table() -> pyarrow.Table:
    node_ids = [question_node_id_base + seq for seq in range(0, question_count)]
    names = [
        "node_id",
        "id",
        "text",
        "user_id",
        "user_node_id",
        "label",
        "asked_by_type",
    ]
    node_ids_data = pyarrow.array(list(node_ids))
    ids_data = pyarrow.array([seq + question_id_base for seq in node_ids])
    text_data = pyarrow.array([f"question {seq}" for seq in node_ids])
    user_id_data = pyarrow.array([seq % user_count + user_id_base for seq in node_ids])
    user_node_id_data = pyarrow.array(
        [seq % user_count + user_node_id_base for seq in node_ids]
    )
    label_data = pyarrow.array(["Question" for _ in node_ids])
    asked_by_type = pyarrow.array(["ASKED_BY" for _ in node_ids])

    result = pyarrow.Table.from_arrays(
        [
            node_ids_data,
            ids_data,
            text_data,
            user_id_data,
            user_node_id_data,
            label_data,
            asked_by_type,
        ],
        names=names,
    ).replace_schema_metadata({"_table": "questions"})

    return result


def answers_table() -> pyarrow.Table:
    node_ids = [answer_node_id_base + seq for seq in range(0, answer_count)]
    names = [
        "node_id",
        "id",
        "question_id",
        "user_id",
        "text",
        "question_node_id",
        "user_node_id",
        "label",
        "authored_by_type",
        "answer_for_type",
    ]
    node_ids_data = pyarrow.array(list(node_ids))
    ids_data = pyarrow.array([seq + answer_id_base for seq in node_ids])
    question_id_data = pyarrow.array(
        [seq % question_count + question_id_base for seq in node_ids]
    )
    user_id_data = pyarrow.array([seq % user_count + user_id_base for seq in node_ids])
    text_data = pyarrow.array(
        [
            f"answer {seq + answer_id_base} for question {seq % question_count + question_id_base}"
            for seq in node_ids
        ]
    )
    question_node_id_data = pyarrow.array(
        [seq % question_count + question_node_id_base for seq in node_ids]
    )
    user_node_id_data = pyarrow.array(
        [seq % user_count + user_node_id_base for seq in node_ids]
    )
    label_data = pyarrow.array(["Answer" for _ in node_ids])
    authored_by_type_data = pyarrow.array(["AUTHORED_BY" for _ in node_ids])
    answer_for_type_data = pyarrow.array(["ANSWER_FOR" for _ in node_ids])

    result = pyarrow.Table.from_arrays(
        [
            node_ids_data,
            ids_data,
            question_id_data,
            user_id_data,
            text_data,
            question_node_id_data,
            user_node_id_data,
            label_data,
            authored_by_type_data,
            answer_for_type_data,
        ],
        names=names,
    ).replace_schema_metadata({"_table": "answers"})

    return result


def free_port() -> int:
    import socket

    with socket.socket() as sock:
        sock.bind(("", 0))
        return int(sock.getsockname()[1])


def gds_version(driver: neo4j.Driver) -> str:
    with driver.session() as session:
        version = session.run(
            "CALL gds.debug.sysInfo() YIELD key, value WITH * WHERE key = $key RETURN value",
            {"key": "gdsVersion"},
        ).single(strict=True)[0]
        return version


@pytest.fixture(scope="module")
def neo4j():
    host_arrow_port = free_port()

    container = (
        testcontainers.neo4j.Neo4jContainer(
            os.getenv("NEO4J_IMAGE", "neo4j:5-enterprise")
        )
        .with_volume_mapping(
            os.getenv("GDS_LICENSE_FILE", "/tmp/gds.license"), "/licenses/gds.license"
        )
        .with_env("NEO4J_ACCEPT_LICENSE_AGREEMENT", "yes")
        .with_env("NEO4J_PLUGINS", '["graph-data-science"]')
        .with_env("NEO4J_gds_enterprise_license__file", "/licenses/gds.license")
        .with_env("NEO4J_dbms_security_procedures_unrestricted", "gds.*")
        .with_env("NEO4J_dbms_security_procedures_allowlist", "gds.*")
        .with_env("NEO4J_gds_arrow_enabled", "true")
        .with_env("NEO4J_gds_arrow_listen__address", "0.0.0.0")
        .with_env(
            "NEO4J_gds_arrow_advertised__listen__address",
            f"127.0.0.1:{host_arrow_port}",
        )
        .with_exposed_ports(7687, 7474, 8491)
        .with_bind_ports(8491, host_arrow_port)
    )
    container.start()

    yield container

    container.stop()


@pytest.fixture(scope="module")
def driver(neo4j):
    driver = neo4j.get_driver()

    yield driver

    driver.close()


@pytest.fixture(autouse=True)
def setup(driver):
    with driver.session() as session:
        session.run("CREATE OR REPLACE DATABASE neo4j WAIT").consume()

    yield


@pytest.fixture()
def bigquery_dataset() -> Tuple[bigquery.Client, str, str]:
    name = "test_" + "".join(
        random.choices(string.ascii_uppercase + string.digits, k=8)
    )
    client = bigquery.Client()
    dataset = client.create_dataset(f"{client.project}.{name}")

    yield client, client.project, dataset.dataset_id

    client.delete_dataset(dataset, delete_contents=True)


@pytest.fixture()
def questions_dataset(bigquery_dataset) -> Tuple[bigquery.Client, str, str]:
    client, project, dataset = bigquery_dataset

    def import_df(df: pandas.DataFrame, table: str) -> None:
        job = client.load_table_from_dataframe(df, table)
        job.result(timeout=60)

    import_df(users_table().to_pandas(), f"{project}.{dataset}.users")
    import_df(questions_table().to_pandas(), f"{project}.{dataset}.questions")
    import_df(answers_table().to_pandas(), f"{project}.{dataset}.answers")

    yield client, project, dataset


@pytest.fixture()
def graph_tables(bigquery_dataset) -> Tuple[bigquery.Client, str, str, str, str]:
    nodes_table = "out_nodes_" + "".join(
        random.choices(string.ascii_uppercase + string.digits, k=4)
    )
    edges_table = "out_edges_" + "".join(
        random.choices(string.ascii_uppercase + string.digits, k=4)
    )
    client, project, dataset = bigquery_dataset

    client.create_table(
        bigquery.Table(
            f"{project}.{dataset}.{nodes_table}",
            [
                bigquery.SchemaField("node_id", "INT64", "REQUIRED"),
                bigquery.SchemaField("labels", "STRING", "REPEATED"),
                bigquery.SchemaField("properties", "JSON"),
            ],
        )
    )
    client.create_table(
        bigquery.Table(
            f"{project}.{dataset}.{edges_table}",
            [
                bigquery.SchemaField("source_node_id", "INT64", "REQUIRED"),
                bigquery.SchemaField("target_node_id", "INT64", "REQUIRED"),
                bigquery.SchemaField("type", "STRING", "REQUIRED"),
                bigquery.SchemaField("properties", "JSON"),
            ],
        )
    )

    yield client, project, dataset, nodes_table, edges_table


@pytest.fixture
def graph_projection_with_embeddings(driver) -> Tuple[str, int, int]:
    graph = "people_" + "".join(
        random.choices(string.ascii_uppercase + string.digits, k=4)
    )
    node_count, relationship_count = 0, 0

    with driver.session() as session:
        session.run(
            """
            CREATE
                (dan:Person {name: 'Dan', age: 18}),
                (annie:Person {name: 'Annie', age: 12}),
                (matt:Person {name: 'Matt', age: 22}),
                (jeff:Person {name: 'Jeff', age: 51}),
                (brie:Person {name: 'Brie', age: 45}),
                (elsa:Person {name: 'Elsa', age: 65}),
                (john:Person {name: 'John', age: 64}),

                (dan)-[:KNOWS {weight: 1.0}]->(annie),
                (dan)-[:KNOWS {weight: 1.0}]->(matt),
                (annie)-[:KNOWS {weight: 1.0}]->(matt),
                (annie)-[:KNOWS {weight: 1.0}]->(jeff),
                (annie)-[:KNOWS {weight: 1.0}]->(brie),
                (matt)-[:KNOWS {weight: 3.5}]->(brie),
                (brie)-[:KNOWS {weight: 1.0}]->(elsa),
                (brie)-[:KNOWS {weight: 2.0}]->(jeff),
                (john)-[:KNOWS {weight: 1.0}]->(jeff)"""
        ).consume()

        result = session.run(
            f"""
            CALL gds.graph.project(
              '{graph}',
              'Person',
              {{
                KNOWS: {{
                  orientation: 'UNDIRECTED',
                  properties: 'weight'
                }}
              }},
              {{ nodeProperties: ['age'] }}
            )
        """
        ).single()
        node_count = result["nodeCount"]
        relationship_count = result["relationshipCount"]

        session.run(
            f"""
            CALL gds.fastRP.mutate(
              '{graph}',
              {{
                embeddingDimension: 8,
                mutateProperty: 'embeddings'
              }}
            )
            YIELD nodePropertiesWritten
        """
        ).consume()

    yield graph, node_count, relationship_count
