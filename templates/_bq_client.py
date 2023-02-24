# Copyright (c) "Neo4j"
# Neo4j Sweden AB [https://neo4j.com]
"""
Helper classes for interacting with BigQuery via the Storage API.
"""
import logging

from google.cloud.bigquery_storage import (
    BigQueryReadClient, DataFormat, ReadSession
)

import pandas as pd
import pyarrow as pa
import neo4j_arrow as na

from typing import cast, Any, Dict, Generator, List, Optional, Union, Tuple


Arrow = Union[pa.Table, pa.RecordBatch]
DataStream = Generator[Union[Arrow, pd.DataFrame], None, None]


class BigQuerySource:
    """
    Wrapper around a BigQuery Dataset. Uses the Storage API to generate a list
    of streams that the BigQueryReadClient can fetch.
    """

    def __init__(self, project_id: str, dataset: str, *,
                 data_format: int = DataFormat.ARROW,
                 max_stream_count: int = 1_000):
        self.project_id = project_id
        self.dataset = dataset
        self.client: Optional[BigQueryReadClient] = None
        self.basepath = f"projects/{self.project_id}/datasets/{self.dataset}"
        if max_stream_count < 1:
            raise ValueError("max_stream_count must be greater than 0")
        if data_format != DataFormat.ARROW and data_format != DataFormat.AVRO:
            raise ValueError("invalid data format")
        self.data_format = data_format
        self.max_stream_count = min(1_000, max_stream_count)

    def __str__(self):
        return f"BigQuerySource{{{self.basepath}}}"

    def __getstate__(self):
        state = self.__dict__.copy()
        if "client" in state:
            del state["client"]
        return state

    def copy(self) -> "BigQuerySource":
        source = BigQuerySource(self.project_id, self.dataset,
                                max_stream_count=self.max_stream_count)
        return source

    def table(self, table: str, *, fields: List[str] = []) -> List[str]:
        """Get one or many streams for a given BigQuery table."""
        if self.client is None:
            self.client = BigQueryReadClient()

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
        return [stream.name for stream in session.streams]

    def consume_stream(self, stream: str) -> DataStream:
        """Apply consumer to a stream in the form of a generator"""
        if getattr(self, "client", None) is None:
            self.client = BigQueryReadClient()

        reader = cast(BigQueryReadClient, self.client).read_rows(stream)
        rows = reader.rows()
        if self.data_format == DataFormat.ARROW:
            for page in rows.pages:
                yield page.to_arrow()
        elif self.data_format == DataFormat.AVRO:
            for page in rows.pages:
                yield page.to_dataframe()
        else:
            raise ValueError("invalid data format")
