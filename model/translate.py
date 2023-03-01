from . import Node, Edge, Property, PropertyValueType
import pyarrow as pa

from typing import Generator, List, Optional, Union

Arrow = Union[pa.Table, pa.RecordBatch]


def arrow_to_nodes(arrow: Arrow,
                   label: Optional[str] = None) -> Generator[Node, None, None]:
    """
    Generate rows of Nodes (protobuf format) from an Apache Arrow-based record.

    An optional "label" can be provided to hardcode a single node label in the
    resulting output.
    """
    # TODO assert schema
    # TODO schema nonsense for properties?
    rows, cols = arrow.num_rows, arrow.num_columns
    node_ids = arrow.column("nodeId")

    labels: Union[List[str], pa.lib.ListArray]
    if label:
        # XXX naive approach
        labels = [label for _ in range(rows)]
    else:
        labels = arrow.column("labels")

    for row in range(rows):
        node = Node()
        node.node_id = node_ids[row].as_py()
        for l in list(labels[row]):
            node.labels.append(str(l))
        # TODO properties
        yield node


def arrow_to_edges(arrow: Arrow) -> Generator[Edge, None, None]:
    """
    Generate rows of Edges (protobuf format) from an Apache Arrow-based record.
    """
    rows, cols = arrow.num_rows, arrow.num_columns
    source_node_ids = arrow.column("sourceNodeId")
    target_node_ids = arrow.column("targetNodeId")
    types = arrow.column("relationshipType")

    for row in range(rows):
        edge = Edge()
        edge.source_node_id = source_node_ids[row].as_py()
        edge.target_node_id = target_node_ids[row].as_py()
        edge.type = types[row].as_py()
        # TODO properties
        yield edge
