from pyspark.sql import SparkSession

from templates import Experiment

spark = (
    SparkSession
    .builder
    .appName("Experiment")
    .getOrCreate()
)

template = Experiment()
template.run(spark, dict())
