# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: neo4j-gds.proto
# Protobuf Python Version: 4.25.1
"""Generated protocol buffer code."""
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import symbol_database as _symbol_database
from google.protobuf.internal import builder as _builder

# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()


DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(
    b'\n\x0fneo4j-gds.proto\x12\tneo4j_gds";\n\x04Node\x12\x0f\n\x07node_id\x18\x01 \x02(\x03\x12\x0e\n\x06labels\x18\x02 \x03(\t\x12\x12\n\nproperties\x18\x03 \x01(\t"X\n\x04\x45\x64ge\x12\x16\n\x0esource_node_id\x18\x01 \x02(\x03\x12\x16\n\x0etarget_node_id\x18\x02 \x02(\x03\x12\x0c\n\x04type\x18\x03 \x02(\t\x12\x12\n\nproperties\x18\x04 \x01(\t'
)

_globals = globals()
_builder.BuildMessageAndEnumDescriptors(DESCRIPTOR, _globals)
_builder.BuildTopDescriptorsAndMessages(DESCRIPTOR, "neo4j_gds_pb2", _globals)
if _descriptor._USE_C_DESCRIPTORS == False:
    DESCRIPTOR._options = None
    _globals["_NODE"]._serialized_start = 30
    _globals["_NODE"]._serialized_end = 89
    _globals["_EDGE"]._serialized_start = 91
    _globals["_EDGE"]._serialized_end = 179
# @@protoc_insertion_point(module_scope)
