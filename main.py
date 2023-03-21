# Copyright (c) "Neo4j"
# Neo4j Sweden AB [https://neo4j.com]
from pyspark.sql import SparkSession
from templates import BigQueryToNeo4jGDSTemplate


if __name__ == "__main__":
    spark = (
        SparkSession
        .builder
        .appName("BigQuery -> Neo4j Connector")
        .getOrCreate()
    )

    # XXX Hardcode a single template for now.
    template = BigQueryToNeo4jGDSTemplate()
    args = template.parse_args()
    template.run(spark, args)
