from typing import Optional, Callable, Union, List, Tuple
import pyarrow as pa

from templates.model import Graph

Arrow = Union[pa.Table, pa.RecordBatch]
MappingFn = Callable[[Arrow], Arrow]


def node_mapper(model: Graph, source_field: Optional[str] = None) -> MappingFn:
    """
    Generate a mapping function for a Node.
    """

    def _map(data: Arrow) -> Arrow:
        schema = data.schema
        if source_field:
            src = schema.metadata.get(source_field.encode("utf8"))
            node = model.node_for_src(src.decode("utf8"))
        else:  # guess at labels
            my_label = data["labels"][0].as_py()
            node = model.node_by_label(my_label)
        if not node:
            raise Exception("cannot find matching node in model given " f"{data.schema}")

        columns, fields = _rename_and_add_column([], [], data, node.key_field, "nodeId")
        if node.label:
            columns.append(pa.array([node.label] * len(data), pa.string()))
            fields.append(pa.field("labels", pa.string()))
        if node.label_field:
            columns, fields = _rename_and_add_column(columns, fields, data, node.label_field, "labels")
        for name in node.properties:
            columns, fields = _rename_and_add_column(columns, fields, data, name, node.properties[name])

        return data.from_arrays(columns, schema=pa.schema(fields))

    return _map


def edge_mapper( model: Graph, source_field: Optional[str] = None) -> MappingFn:
    """
    Generate a mapping function for an Edge.
    """

    def _map(data: Arrow) -> Arrow:
        schema = data.schema
        if source_field:
            src = schema.metadata.get(source_field.encode("utf8"))
            edge = model.edge_for_src(src.decode("utf8"))
        else:  # guess at type
            my_type = data["type"][0].as_py()
            edge = model.edge_by_type(my_type)
        if not edge:
            raise Exception("cannot find matching edge in model given " f"{data.schema}")

        columns, fields = _rename_and_add_column([], [], data, edge.source_field, "sourceNodeId")
        columns, fields = _rename_and_add_column(columns, fields, data, edge.target_field, "targetNodeId")
        if edge.type:
            columns.append(pa.array([edge.type] * len(data), pa.string()))
            fields.append(pa.field("relationshipType", pa.string()))
        if edge.type_field:
            columns, fields = _rename_and_add_column(columns, fields, data, edge.type_field, "relationshipType")
        for name in edge.properties:
            columns, fields = _rename_and_add_column(columns, fields, data, name, edge.properties[name])

        return data.from_arrays(columns, schema=pa.schema(fields))

    return _map


def nop_mapper(data: Arrow) -> Arrow:
        """
        Used as a no-op mapping function.
        """
        return data

def _rename_and_add_column(
        columns: List[Union[pa.Array, pa.ChunkedArray]],
        fields: List[pa.Field],
        data: Arrow,
        current_name: str,
        new_name: str,
) -> Tuple[List[Union[pa.Array, pa.ChunkedArray]], List[pa.Field]]:
    idx = data.schema.get_field_index(current_name)
    columns.append(data.columns[idx])
    fields.append(data.schema.field(idx).with_name(new_name))
    return columns, fields
