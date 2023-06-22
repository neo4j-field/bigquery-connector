# Copyright (c) "Neo4j"
# Neo4j Sweden AB [https://neo4j.com]
from pyspark.sql import SparkSession
from templates import Neo4jGDSToBigQueryTemplate


if __name__ == "__main__":
    spark = (
        SparkSession
        .builder
        .appName("Neo4j -> BigQuery Connector")
        .config("spark.executor.memory", "32G")
        .config("spark.driver.memory", "32G")
        .config("spark.driver.maxResultSize", "4G")
        .getOrCreate()
    )

    # XXX Hardcode a single template for now.
    template = Neo4jGDSToBigQueryTemplate()
    args = template.parse_args()
    template.run(spark, args)
