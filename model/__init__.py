#
"""
Protobuf Integration with Neo4j GDS
"""
__all__ = [
    "Node",
    "Edge",
    "arrow_to_nodes",
    "arrow_to_edges",
]

from .neo4j_gds_pb2 import Node, Edge # type: ignore
from .translate import arrow_to_nodes, arrow_to_edges
