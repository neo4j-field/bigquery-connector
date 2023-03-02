#
"""
Tests for the Protobuf data.
"""
from . import Node, arrow_to_nodes, arrow_to_edges

import pyarrow as pa


def test_arrow_to_nodes() -> None:
    """
    Test converting Apache Arrow formats into Node protobufs.

    XXX Needs node property testing
    """
    dim = 10
    table = pa.Table.from_pydict({
        "nodeId": list(range(dim)),
        "labels": [["Node"] for _ in range(dim)],
        "name": [f"Person {x}" for x in range(dim)],
        "age": [x for x in range(dim)],
        "height": [1.1 * x for x in range(dim)],
    })
    batch = table.to_batches()[0]

    g = arrow_to_nodes(table)
    for i in range(dim):
        node = next(g)
        assert node.node_id == table.column("nodeId")[i].as_py()
        assert list(node.labels) == table.column("labels")[i].as_py()

    g = arrow_to_nodes(batch)
    for i in range(dim):
        node = next(g)
        assert node.node_id == table.column("nodeId")[i].as_py()
        assert list(node.labels) == table.column("labels")[i].as_py()


def test_arrow_to_edges() -> None:
    """
    Test converting Apache Arrow formats into Edge protobufs.

    XXX Needs edge property testing
    """
    dim = 10
    table = pa.Table.from_pydict({
        "sourceNodeId": list(range(dim)),
        "targetNodeId": [x + 100 for x in range(dim)],
        "relationshipType": ["AnEdge" for _ in range(dim)],
        "name": [f"something {x}" for x in range(dim)],
        "age": [x for x in range(dim)],
        "weight": [1.1 * x for x in range(dim)],
    })
    batch = table.to_batches()[0]

    g = arrow_to_edges(table)
    for i in range(dim):
        edge = next(g)
        assert edge.source_node_id == table.column("sourceNodeId")[i].as_py()
        assert edge.target_node_id == table.column("targetNodeId")[i].as_py()
        assert edge.type == table.column("relationshipType")[i].as_py()

    g = arrow_to_edges(batch)
    for i in range(dim):
        edge = next(g)
        assert edge.source_node_id == table.column("sourceNodeId")[i].as_py()
        assert edge.target_node_id == table.column("targetNodeId")[i].as_py()
        assert edge.type == table.column("relationshipType")[i].as_py()