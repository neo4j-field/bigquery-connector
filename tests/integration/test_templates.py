import os

from pyspark.sql import SparkSession

import templates.bq_client
from conftest import user_count, answer_count, question_count

graph_name = "questions_and_answers"


def test_create_projection(neo4j, driver, questions_dataset):
    client, project, dataset = questions_dataset

    spark = SparkSession.builder.getOrCreate()
    template = templates.BigQueryToNeo4jGDSTemplate()
    args = template.parse_args(
        [
            f"--graph_name={graph_name}",
            f"--graph_uri={os.path.dirname(os.path.realpath(__file__))}/graph.json",
            f"--neo4j_uri={neo4j.get_connection_url()}",
            f"--neo4j_user={neo4j.NEO4J_USER}",
            f"--neo4j_password={neo4j.NEO4J_ADMIN_PASSWORD}",
            "--neo4j_db=neo4j",
            f"--bq_project={project}",
            f"--bq_dataset={dataset}",
            "--node_tables=users,questions,answers",
            "--edge_tables=questions,answers",
        ]
    )
    template.run(spark, args)

    # assert what we have
    with driver.session() as session:
        result = session.run(
            "CALL gds.graph.list() YIELD graphName, nodeCount, relationshipCount "
            + "WITH * WHERE graphName = $graph_name RETURN *",
            graph_name=graph_name,
        ).fetch(1)

        assert len(result) == 1
        assert result[0]["graphName"] == graph_name
        assert result[0]["nodeCount"] == user_count + question_count + answer_count
        assert result[0]["relationshipCount"] == question_count + answer_count


def test_write_back(neo4j, driver, questions_dataset, graph_tables):
    client, project, dataset, nodes_table, edges_table = graph_tables

    test_create_projection(neo4j, driver, questions_dataset)

    spark = SparkSession.builder.getOrCreate()
    template = templates.Neo4jGDSToBigQueryTemplate()
    args = template.parse_args(
        [
            f"--graph_name={graph_name}",
            f"--neo4j_uri={neo4j.get_connection_url()}",
            f"--neo4j_user={neo4j.NEO4J_USER}",
            f"--neo4j_password={neo4j.NEO4J_ADMIN_PASSWORD}",
            "--neo4j_db=neo4j",
            f"--bq_project={project}",
            f"--bq_dataset={dataset}",
            f"--bq_node_table={nodes_table}",
            f"--bq_edge_table={edges_table}",
            "--neo4j_patterns=(:User),(:Question),(:Answer),[:ASKED_BY],[:ANSWER_FOR]",
        ]
    )
    template.run(spark, args)

    nodes_count, expected_nodes_count = 0, user_count + question_count + answer_count
    edges_count, expected_edges_count = 0, question_count + answer_count

    def row_count(table: str) -> int:
        rows = client.query(f"SELECT COUNT(*) AS count FROM `{table}`").result()
        for row in rows:
            return row.count
        return 0

    nodes_count = row_count(f"{project}.{dataset}.{nodes_table}")
    edges_count = row_count(f"{project}.{dataset}.{edges_table}")

    assert nodes_count == expected_nodes_count
    assert edges_count == expected_edges_count


def test_write_back_with_array(
    neo4j, driver, graph_projection_with_embeddings, graph_tables
):
    (
        graph_name,
        expected_node_count,
        expected_relationship_count,
    ) = graph_projection_with_embeddings
    client, project, dataset, nodes_table, edges_table = graph_tables

    spark = SparkSession.builder.getOrCreate()
    template = templates.Neo4jGDSToBigQueryTemplate()
    args = template.parse_args(
        [
            f"--graph_name={graph_name}",
            f"--neo4j_uri={neo4j.get_connection_url()}",
            f"--neo4j_user={neo4j.NEO4J_USER}",
            f"--neo4j_password={neo4j.NEO4J_ADMIN_PASSWORD}",
            "--neo4j_db=neo4j",
            f"--bq_project={project}",
            f"--bq_dataset={dataset}",
            f"--bq_node_table={nodes_table}",
            f"--bq_edge_table={edges_table}",
            "--neo4j_patterns=(:Person {embeddings, age}),[:KNOWS{weight}]",
        ]
    )
    template.run(spark, args)

    def row_count(table: str) -> int:
        rows = client.query(f"SELECT COUNT(*) AS count FROM `{table}`").result()
        for row in rows:
            return row.count
        return 0

    assert row_count(f"{project}.{dataset}.{nodes_table}") == expected_node_count
    assert (
        row_count(f"{project}.{dataset}.{edges_table}") == expected_relationship_count
    )
