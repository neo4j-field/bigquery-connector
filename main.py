from pyspark.sql import SparkSession

from templates import Experiment

spark = (
    SparkSession
    .builder
    .appName("Neo4j BigQuery Integration")
    .getOrCreate()
)

# todo: initialize log4j in spark?

template = Experiment()
args = template.parse_args()
template.run(spark, args)
