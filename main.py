import os
import json

from pyspark.sql import SparkSession

from templates import BigQueryToNeo4jGDSTemplate

from typing import cast, Dict, List, Tuple


BQ_PARAM_PREFIX = "BIGQUERY_PROC_PARAM"


def bq_params() -> Dict[str, str]:
    """
    BigQuery Stored Procedures for Apache Spark send inputs via the process's
    environment, encoding in JSON. Each one is prefixed with
    "BIGQUERY_PROC_PARAM.".

    Defers the JSON parsing to other functions that need to consume the values.
    """
    # This is an ugly typing dance :(
    env: Dict[str, str] = dict(os.environ)
    values: List[Tuple[str, str]] = [
        (k.replace(BQ_PARAM_PREFIX, ""), v) for (k, v) in env.items()
        if k.startswith(BQ_PARAM_PREFIX)
    ]
    return dict(values)


spark = (
    SparkSession
    .builder
    .appName("Neo4j BigQuery Integration")
    .getOrCreate()
)

# TODO: initialize log4j in spark?

# XXX Hardcode a single template for now.
template = BigQueryToNeo4jGDSTemplate()

bq_args = bq_params()
if bq_args:
    # We are being invoked by a BQ Stored Procedure
    args = template.parse_bq_args(bq_args)
else:
    # We are being run some other way on Spark
    args = template.parse_args()

template.run(spark, args)
