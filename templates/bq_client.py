# Copyright (c) "Neo4j"
# Neo4j Sweden AB [https://neo4j.com]
"""
Helper classes for interacting with BigQuery via the Storage API.
"""
import logging

from google.cloud.bigquery_storage import (
    BigQueryReadClient, DataFormat, ReadSession
)
from google.api_core.gapic_v1.client_info import ClientInfo

import pyarrow as pa
import neo4j_arrow as na

from .constants import USER_AGENT

from typing import (
    cast, Any, Dict, Generator, List, NamedTuple, Optional, Union, Tuple
)

Arrow = Union[pa.Table, pa.RecordBatch]
DataStream = Generator[Arrow, None, None]


class BQStream(NamedTuple):
    """
    Represents a streamable part of a BQ Table. Used to simplify PySpark jobs.
    """
    table: str
    stream: str


class BigQuerySource:
    """
    Wrapper around a BigQuery Dataset. Uses the Storage API to generate a list
    of streams that the BigQueryReadClient can fetch.
    """
    client: Optional[BigQueryReadClient] = None
    client_info: ClientInfo = ClientInfo(user_agent=USER_AGENT) # type: ignore

    def __init__(self, project_id: str, dataset: str, *,
                 data_format: int = DataFormat.ARROW,
                 max_stream_count: int = 1_000):
        self.project_id = project_id
        self.dataset = dataset
        self.basepath = f"projects/{self.project_id}/datasets/{self.dataset}"
        if max_stream_count < 1:
            raise ValueError("max_stream_count must be greater than 0")
        if data_format != DataFormat.ARROW and data_format != DataFormat.AVRO:
            raise ValueError("invalid data format")
        self.data_format = data_format
        self.max_stream_count = min(1_000, max_stream_count)

    def __str__(self) -> str:
        return f"BigQuerySource{{{self.basepath}}}"

    def __getstate__(self) -> Dict[str, Any]:
        state = self.__dict__.copy()
        if "client" in state:
            del state["client"]
        return state

    def copy(self) -> "BigQuerySource":
        source = BigQuerySource(self.project_id, self.dataset,
                                max_stream_count=self.max_stream_count)
        return source

    def table(self, table: str, *, fields: List[str] = []) -> List[BQStream]:
        """
        Get one or many streams for a given BigQuery table, returning a Tuple
        of the table name and the list of its streams.
        """
        if self.client is None:
            self.client = BigQueryReadClient(client_info=self.client_info)

        read_session = ReadSession(
            table=f"{self.basepath}/tables/{table}",
            data_format=self.data_format
        )
        if fields:
            read_session.read_options.selected_fields=fields

        session = self.client.create_read_session(
            parent=f"projects/{self.project_id}",
            read_session=read_session,
            max_stream_count=self.max_stream_count,
        )
        return [BQStream(table=table, stream=s.name) for s in session.streams]

    def consume_stream(self, bq_stream: BQStream) -> DataStream:
        """
        Generate a stream of structured data (Arrow or Avro) from a BigQuery
        table using the Storate Read API.
        """
        table, stream = bq_stream
        if getattr(self, "client", None) is None:
            self.client = BigQueryReadClient(client_info=self.client_info)

        rows = (
            cast(BigQueryReadClient, self.client)
            .read_rows(stream) # type: ignore
            .rows()
        )
        if self.data_format == DataFormat.ARROW:
            for page in rows.pages:
                arrow = page.to_arrow()
                schema = arrow.schema.with_metadata({"_table": table})
                yield arrow.from_arrays(arrow.columns, schema=schema)
        elif self.data_format == DataFormat.AVRO:
            raise RuntimeError("AVRO support unfinished")
            #for page in rows.pages:
                # TODO: schema updates to specify the source table
                #yield page.to_dataframe()
        else:
            raise ValueError("invalid data format")
