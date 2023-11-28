import json
import logging

import pyarrow as pa

from . import Node, Edge  # , Property, PropertyValueType

from typing import Generator, List, Optional, Union

Arrow = Union[pa.Table, pa.RecordBatch]


def arrow_to_nodes(arrow: Arrow) -> Generator[Node, None, None]:
    """
    Generate rows of Nodes (protobuf format) from an Apache Arrow-based record.
    """
    # TODO assert schema
    # TODO schema nonsense for properties?
    rows = arrow.num_rows
    node_ids = arrow.column("nodeId")
    labels = arrow.column("labels")

    # XXX naive approach using field names for now
    # N.b. We need to rely on the schema if using RecordBatches.
    props = [n for n in arrow.schema.names if n not in ["nodeId", "labels"]]

    for row in range(rows):
        node = Node()
        node.node_id = node_ids[row].as_py()
        for label in list(labels[row].as_py()):
            node.labels.append(str(label))
        items = filter(
            lambda kv: kv[1] is not None,
            [(key, arrow.column(key)[row].as_py()) for key in props],
        )
        if items:
            node.properties = json.dumps(dict(items))
        else:
            node.properties = "{}"  # "empty" JSON Object
        yield node


def arrow_to_edges(arrow: Arrow) -> Generator[Edge, None, None]:
    """
    Generate rows of Edges (protobuf format) from an Apache Arrow-based record.

    XXX types is unused currently and exists to match the signature of
        arrow_to_nodes
    """
    rows = arrow.num_rows
    source_node_ids = arrow.column("sourceNodeId")
    target_node_ids = arrow.column("targetNodeId")
    types = arrow.column("relationshipType")

    # XXX naive approach using field names for now
    # N.b. We need to rely on the schema if using RecordBatches.
    props = [
        n
        for n in arrow.schema.names
        if n not in ["sourceNodeId", "targetNodeId", "relationshipType"]
    ]

    for row in range(rows):
        edge = Edge()
        edge.source_node_id = source_node_ids[row].as_py()
        edge.target_node_id = target_node_ids[row].as_py()
        edge.type = types[row].as_py()
        items = filter(
            lambda kv: kv[1] is not None,
            [(key, arrow.column(key)[row].as_py()) for key in props],
        )
        if items:
            edge.properties = json.dumps(dict(items))
        else:
            edge.properties = "{}"  # "empty" JSON Object
        yield edge
