###
from google.cloud.bigquery_storage import BigQueryReadClient, types
from dataproc_templates import BaseTemplate

import pyarrow as pa
from pyspark.sql import SparkSession

from typing import Any, Dict, Optional, Sequence


class Experiment(BaseTemplate):
    """Experiment!"""

    @staticmethod
    def parse_args(args: Optional[Sequence[str]] = None) -> Dict[str, Any]:
        return {}

    def run(self, spark: SparkSession, args: Dict[str, Any]) -> None:
        logger = self.get_logger(spark=spark)
        logger.info("Starting...")

        client = BigQueryReadClient()
        read_session = types.ReadSession(
            table=f"projects/neo4j-se-team-201905/datasets/stackoverflow/tables/posts_answers",
            data_format=types.DataFormat.ARROW
        )
        read_session.read_options.selected_fields=["id"]
        session = client.create_read_session(
            parent=f"projects/neo4j-se-team-201905",
            read_session=read_session,
        )
        logger.info(f"ready to consume session with expiration: {session.expire_time}")

        sc = spark.sparkContext
        junk = sc.parallelize([stream.name for stream in session.streams]).first()
        logger.info(f"junk: {junk}")
        logger.info("...Done!")
