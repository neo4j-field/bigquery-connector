CREATE OR REPLACE PROCEDURE
  `neo4j-se-team-201905.bqr_neo4j_demo.neo4j_gds_graph_project`(
	    graph_name STRING,
	    graph_json STRING,
	    neo4j_db_name STRING,
	    neo4j_user STRING,
	    neo4j_password STRING,
	    neo4j_host STRING,
	    bq_project STRING,
	    bq_dataset STRING,
	    node_tables STRING,
	    edge_tables STRING
	  )
	WITH CONNECTION `neo4j-se-team-201905.eu.voutila-bq-spark-eu`
	OPTIONS (
		    description="Project a graph from BigQuery into Neo4j AuraDS or GDS.",
		    engine="SPARK",
		    runtime_version="2.0",
		    container_image="eu.gcr.io/neo4j-se-team-201905/neo4j-bigquery-connector:0.2.0"
	)

	LANGUAGE PYTHON AS r"""
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
