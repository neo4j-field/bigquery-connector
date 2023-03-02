CREATE OR REPLACE PROCEDURE
  `neo4j-se-team-201905.bqr_neo4j_demo.neo4j_gds_graph_project`(graph_name STRING,
    graph_uri STRING,
    neo4j_secret STRING,
    bq_project STRING,
    bq_dataset STRING,
    node_tables ARRAY<STRING>,
    edge_tables ARRAY<STRING>)
WITH CONNECTION `neo4j-se-team-201905.eu.voutila-bq-spark-eu` OPTIONS (engine='SPARK',
    runtime_version='2.0',
    container_image='eu.gcr.io/neo4j-se-team-201905/neo4j-bigquery-connector:0.3.0',
    properties=[],
    description="Project a graph from BigQuery into Neo4j AuraDS or GDS.")
  LANGUAGE python AS R"""
# Need to back out this Spark BigQuery Support crap.
import sys
print(f"original path: {sys.path}")
newpath = [p for p in sys.path if not "spark-bigquery-support" in p]
sys.path = newpath
print(f"new path: {sys.path}")

from pyspark.sql import SparkSession
from templates import BigQueryToNeo4jGDSTemplate

spark = (
      SparkSession
      .builder
      .appName("Neo4j BigQuery Connector")
      .getOrCreate()
)

template = BigQueryToNeo4jGDSTemplate()
args = template.parse_args()
template.run(spark, args)
""";
