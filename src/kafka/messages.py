"""
Kafka message serialisation/deserialisation.

Messages are JSON-encoded. Each topic has a known key and value schema.

feature-updates topic:
  key:   feature_view:entity_id
  value: FeatureUpdateMessage JSON

schema-changes topic:
  key:   feature_view
  value: SchemaChangeMessage JSON
"""

import json
from src.models.schemas import FeatureUpdateMessage, SchemaChangeMessage


def encode_feature_update(msg: FeatureUpdateMessage) -> tuple[bytes, bytes]:
    key = f"{msg.feature_view}:{msg.entity_id}".encode()
    value = msg.model_dump_json().encode()
    return key, value


def decode_feature_update(key_bytes: bytes, value_bytes: bytes) -> FeatureUpdateMessage:
    return FeatureUpdateMessage.model_validate_json(value_bytes)


def encode_schema_change(msg: SchemaChangeMessage) -> tuple[bytes, bytes]:
    key = msg.feature_view.encode()
    value = msg.model_dump_json().encode()
    return key, value


def decode_schema_change(key_bytes: bytes, value_bytes: bytes) -> SchemaChangeMessage:
    return SchemaChangeMessage.model_validate_json(value_bytes)
