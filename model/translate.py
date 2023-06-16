import json
import logging

import pyarrow as pa

from . import Node, Edge #, Property, PropertyValueType

from typing import Generator, List, Optional, Union

Arrow = Union[pa.Table, pa.RecordBatch]


def arrow_to_nodes(arrow: Arrow,
                   labels: List[str] = []) -> Generator[Node, None, None]:
    """
    Generate rows of Nodes (protobuf format) from an Apache Arrow-based record.

    An optional "labels" list can be provided to hardcode a node labels in the
    resulting output.
    """
    # TODO assert schema
    # TODO schema nonsense for properties?
    rows, cols = arrow.num_rows, arrow.num_columns
    node_ids = arrow.column("nodeId")
    print(f"received {rows} rows with {cols} columns")

    _labels: Union[List[str], pa.lib.ListArray]
    if labels:
        # XXX naive approach
        # ignore '*' wildcard labels
        if "*" in labels:
            labels.remove("*") # XXX should we be using a set(), not list()?
        _labels = [labels for _ in range(rows)]
    else:
        _labels = arrow.column("labels")

    # XXX naive approach using field names for now
    # N.b. We need to rely on the schema if using RecordBatches.
    props = [n for n in arrow.schema.names if n not in ["nodeId", "labels"]]

    for row in range(rows):
        node = Node()
        node.node_id = node_ids[row].as_py()
        for l in list(_labels[row]):
            node.labels.append(str(l))
        items = [(key, arrow.column(key)[row].as_py()) for key in props]
        if items:
            node.properties = json.dumps(dict(items))
        else:
            node.properties = "{}" # "empty" JSON Object
        yield node


def arrow_to_edges(arrow: Arrow,
                   _types: List[str] = []) -> Generator[Edge, None, None]:
    """
    Generate rows of Edges (protobuf format) from an Apache Arrow-based record.

    XXX types is unused currently and exists to match the signature of
        arrow_to_nodes
    """
    rows, cols = arrow.num_rows, arrow.num_columns
    source_node_ids = arrow.column("sourceNodeId")
    target_node_ids = arrow.column("targetNodeId")
    types = arrow.column("relationshipType")

    # XXX naieve approach using field names for now
    # N.b. We need to rely on the schema if using RecordBatches.
    props = [
        n for n in arrow.schema.names
        if n not in ["sourceNodeId", "targetNodeId", "relationshipType"]
    ]

    for row in range(rows):
        edge = Edge()
        edge.source_node_id = source_node_ids[row].as_py()
        edge.target_node_id = target_node_ids[row].as_py()
        edge.type = types[row].as_py()
        items = [(key, arrow.column(key)[row].as_py()) for key in props]
        if items:
            edge.properties = json.dumps(dict(items))
        else:
            edge.properties = "{}" # "empty" JSON Object
        yield edge
