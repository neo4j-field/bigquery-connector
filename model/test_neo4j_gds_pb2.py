#
"""
Tests for the Protobuf data.
"""
from . import Node, arrow_to_nodes

import pyarrow as pa


def test_converting_arrow_to_protobuf():
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
