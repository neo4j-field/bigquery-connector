from templates.mapper import node_mapper, nop_mapper, edge_mapper

import pyarrow as pa

from templates.model import Graph, Node, Edge


def test_nop_mapper():
    mapper = nop_mapper
    t = pa.table({"labels": ["Junk"], "nodeId": ["junk_id"]})
    result = mapper(t)
    assert result is not None
    assert "nodeId" in result.schema.names
    assert "labels" in result.schema.names


def test_node_mapper():
    SRC_KEY = "gcs_source"
    g = Graph(
        name="junk",
        nodes=[
            Node(
                source="gs://.*/junk.*parquet",
                label_field="my_labels",
                key_field="my_key",
            )
        ],
    )
    mapper = node_mapper(g, SRC_KEY)

    src = "gs://bucket/folder/junk_0001.parquet"
    t = pa.table({"my_labels": ["Junk"], "my_key": ["junk_id"]})
    schema = t.schema.with_metadata({SRC_KEY: src})
    table = pa.Table.from_arrays(t.columns, schema=schema)

    result = mapper(table)
    assert result is not None
    assert "nodeId" in result.schema.names
    assert "labels" in result.schema.names
    assert "my_labels" not in result.schema.names
    assert "my_key" not in result.schema.names


def test_edge_mapper():
    SRC_KEY = "gcs_source"
    g = Graph(
        name="junk",
        edges=[
            Edge(
                source="gs://.*/junk.*parquet",
                type_field="my_type",
                source_field="my_src",
                target_field="my_tgt",
            )
        ],
    )
    mapper = edge_mapper(g, SRC_KEY)

    src = "gs://bucket/folder/junk_0001.parquet"
    t = pa.table({"my_type": ["JUNK"], "my_src": ["junk_id"], "my_tgt": ["junk_id"]})
    schema = t.schema.with_metadata({SRC_KEY: src})
    table = pa.Table.from_arrays(t.columns, schema=schema)

    result = mapper(table)
    assert result is not None
    assert "relationshipType" in result.schema.names
    assert "sourceNodeId" in result.schema.names
    assert "targetNodeId" in result.schema.names
    assert "my_type" not in result.schema.names
    assert "my_src" not in result.schema.names
    assert "my_tgt" not in result.schema.names
