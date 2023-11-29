# Copyright (c) "Neo4j"
# Neo4j Sweden AB [https://neo4j.com]
"""
Helper classes for interacting with BigQuery via the Storage API.
"""
import logging

from google.cloud.bigquery_storage import (
    BigQueryReadClient,
    BigQueryWriteClient,
    DataFormat,
    ReadSession,
)
from google.cloud.bigquery_storage_v1 import types, writer
from google.protobuf import descriptor_pb2, wrappers_pb2

from google.api_core.gapic_v1.client_info import ClientInfo
from google.api_core.future import Future

import pyarrow as pa

from .constants import USER_AGENT

from typing import (
    cast,
    Any,
    Callable,
    Dict,
    Generator,
    List,
    NamedTuple,
    Optional,
    Union,
    Tuple,
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
    client_info: ClientInfo = ClientInfo(user_agent=USER_AGENT)

    def __init__(
        self,
        project_id: str,
        dataset: str,
        *,
        data_format: int = DataFormat.ARROW,
        max_stream_count: int = 1_000,
    ):
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
        source = BigQuerySource(
            self.project_id, self.dataset, max_stream_count=self.max_stream_count
        )
        return source

    def table(self, table: str, *, fields: List[str] = []) -> List[BQStream]:
        """
        Get one or many streams for a given BigQuery table, returning a Tuple
        of the table name and the list of its streams.
        """
        if self.client is None:
            self.client = BigQueryReadClient(client_info=self.client_info)

        read_session = ReadSession(
            table=f"{self.basepath}/tables/{table}", data_format=self.data_format
        )
        if fields:
            read_session.read_options.selected_fields = fields

        session = self.client.create_read_session(
            parent=f"projects/{self.project_id}",
            read_session=read_session,
            max_stream_count=self.max_stream_count,
        )
        return [BQStream(table=table, stream=s.name) for s in session.streams]

    def consume_stream(self, bq_stream: BQStream) -> DataStream:
        """
        Generate a stream of structured data (Arrow or Avro) from a BigQuery
        table using the Storage Read API.
        """
        table_name, stream_name = bq_stream
        if getattr(self, "client", None) is None:
            self.client = BigQueryReadClient(client_info=self.client_info)

        rows_stream = cast(BigQueryReadClient, self.client).read_rows(stream_name)

        if self.data_format == DataFormat.ARROW:
            rows = rows_stream.rows()
            for page in rows.pages:
                arrow = page.to_arrow()
                schema = arrow.schema.with_metadata({"_table": table_name})
                yield arrow.from_arrays(arrow.columns, schema=schema)
        elif self.data_format == DataFormat.AVRO:
            rows = rows_stream.rows()
            for page in rows.pages:
                df = page.to_dataframe()
                table = pa.Table.from_pandas(df)
                schema = table.schema.with_metadata({"_table": table})
                for batch in table.to_batches():
                    yield pa.RecordBatch.from_arrays(batch.columns, schema=schema)
        else:
            raise ValueError("invalid data format")


class BigQuerySink:
    """
    Wrapper around a BigQuery table. Uses the Storage API to write data.
    """

    client: Optional[BigQueryWriteClient] = None
    client_info: ClientInfo = ClientInfo(user_agent=USER_AGENT)

    def __init__(
        self,
        project_id: str,
        dataset: str,
        table: str,
        descriptor: descriptor_pb2.DescriptorProto,
        *,
        logger: Optional[logging.Logger] = None,
    ):
        self.project_id = project_id
        self.dataset = dataset
        self.table = table
        self.parent = BigQueryWriteClient.table_path(project_id, dataset, table)

        # state tracking
        self.offset: int = 0
        self.stream: Optional[Any] = None
        self.stream_name: Optional[str] = None

        if not logger:
            logger = logging.getLogger("BigQuerySink")
        self.logger = logger

        self.serialized_proto_data: bytes = descriptor.SerializeToString()

        self.proto_data: Optional[Any] = None

    def __str__(self) -> str:
        return f"BigQuerySink({self.parent})"

    def __getstate__(self) -> Dict[str, Any]:
        state = self.__dict__.copy()
        # Remove the FlightClient and CallOpts as they're not serializable
        if "logger" in state:
            del state["logger"]
        return state

    def _latent_init(self) -> None:
        """
        Latent initialization to support pickling.
        """
        # XXX maybe move this?
        proto_schema = types.ProtoSchema()
        descriptor = descriptor_pb2.DescriptorProto()
        descriptor.ParseFromString(self.serialized_proto_data)
        proto_schema.proto_descriptor = descriptor
        self.proto_data = types.AppendRowsRequest.ProtoData()
        self.proto_data.writer_schema = proto_schema

        self.logger = logging.getLogger("BigQuerySink")

    @staticmethod
    def print_completion(f: Future) -> None:
        """Print the future to stdout."""
        logger = logging.getLogger("BigQuerySink")
        logger.info(f"completed {f}")

    def append_rows(self, rows: List[bytes]) -> Tuple[str, int]:
        """
        Submit a series of BigQuery rows (already serialized ProtoBufs),
        creating a write stream if required.

        Returns the stream name appended to and the number of rows appended.
        """
        if self.client is None:
            # Latent client creation to support serialization.
            self.client = BigQueryWriteClient(client_info=self.client_info)
            self._latent_init()

        if self.stream is None:
            #
            # Each sink will work with 1 stream. It should be able to append
            # repeatedly to the same stream.
            #
            write_stream_type = types.WriteStream()
            write_stream_type.type_ = cast(
                types.WriteStream.Type, types.WriteStream.Type.PENDING  # XXX
            )
            write_stream = self.client.create_write_stream(
                parent=self.parent,
                write_stream=write_stream_type,
            )
            self.stream_name = write_stream.name

            req_template = types.AppendRowsRequest()
            req_template.write_stream = self.stream_name
            if self.proto_data is None:
                raise RuntimeError("missing self.proto_data")
            req_template.proto_rows = self.proto_data

            self.stream = writer.AppendRowsStream(self.client, req_template)
            self.logger.info(f"created write stream {self.stream_name}")

        # Process our batch.
        # XXX For now, we ignore any API limits and hope for the best.
        proto_rows = types.ProtoRows()
        self.logger.info(f"appending {len(rows):,} rows to {self.stream_name}")
        for row in rows:
            proto_rows.serialized_rows.append(row)

        request = types.AppendRowsRequest()
        request.offset = cast(wrappers_pb2.Int64Value, self.offset)
        proto_data = types.AppendRowsRequest.ProtoData()
        proto_data.rows = proto_rows
        request.proto_rows = proto_data

        future = self.stream.send(request)
        future.result(timeout=None)
        self.offset += len(rows)
        return cast(str, self.stream_name), len(rows)  # XXX ignoring offset for now

    def finalize_write_stream(self) -> Tuple[str, int]:
        """
        Finalize any pending write stream. If no client or stream, this is a
        no-op and just warns.

        Returns the stream name finalized and the final offset.
        """
        if self.client is None:
            self.client = BigQueryWriteClient(client_info=self.client_info)
            self._latent_init()

        if self.stream_name:
            self.client.finalize_write_stream(name=self.stream_name)
        else:
            raise RuntimeError("no valid stream name")

        return self.stream_name, self.offset  # type: ignore # should be non-None at this point

    def commit(self) -> None:
        """
        Commit pending write streams. If no streams are provided, uses the
        existing stream named by self.stream_name.
        """
        if self.client is None:
            self.client = BigQueryWriteClient(client_info=self.client_info)
            self._latent_init()

        commit_req = types.BatchCommitWriteStreamsRequest()
        commit_req.parent = self.parent
        if self.stream_name:
            commit_req.write_streams = [self.stream_name]
        else:
            raise RuntimeError("no valid stream name(s)")

        self.client.batch_commit_write_streams(commit_req)
