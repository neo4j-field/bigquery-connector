# Copyright (c) "Neo4j"
# Neo4j Sweden AB [https://neo4j.com]
from pyspark.sql import SparkSession

from templates import BigQueryToNeo4jGDSTemplate

from typing import cast, Dict, List, Tuple, Sequence


if __name__ == "__main__":
    spark = (
        SparkSession
        .builder
        .appName("Neo4j BigQuery Integration")
        .getOrCreate()
    )

    # XXX Hardcode a single template for now.
    template = BigQueryToNeo4jGDSTemplate()
    args = template.parse_args()
    template.run(spark, args)
