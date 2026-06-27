"""
aiokafka async producer.

Validates the payload against the schema registry before producing to ensure
producers cannot push type-incompatible data downstream.
"""

import json
from datetime import datetime
from typing import Any

from aiokafka import AIOKafkaProducer
from src.config import get_settings
from src.models.schemas import FeatureUpdateMessage, SchemaChangeMessage, FeatureRow
from src.kafka.messages import encode_feature_update, encode_schema_change
from src.schema_registry.registry import get_latest_version_number, get_schema

_producer: AIOKafkaProducer | None = None


async def get_producer() -> AIOKafkaProducer:
    global _producer
    if _producer is None:
        s = get_settings()
        _producer = AIOKafkaProducer(
            bootstrap_servers=s.kafka_bootstrap_servers,
            compression_type="gzip",
            acks="all",            # wait for all replicas to acknowledge
            enable_idempotence=True,
        )
        await _producer.start()
    return _producer


async def close_producer() -> None:
    global _producer
    if _producer:
        await _producer.stop()
        _producer = None


async def produce_feature_updates(
    feature_view: str,
    rows: list[FeatureRow],
    producer_id: str = "api",
) -> int:
    """
    Publish feature updates to Kafka after schema validation.
    Returns the number of messages produced.
    """
    s = get_settings()
    version = await get_latest_version_number(feature_view)
    if version is None:
        raise ValueError(f"No schema registered for feature view '{feature_view}'")

    schema_ver = await get_schema(feature_view, version)
    if schema_ver is None:
        raise ValueError(f"Schema v{version} not found for '{feature_view}'")

    # Validate all rows against the schema
    expected_features = {f.name for f in schema_ver.spec.features}
    for row in rows:
        unknown = set(row.features.keys()) - expected_features
        if unknown:
            raise ValueError(
                f"Features {unknown} not in schema v{version} for '{feature_view}'"
            )

    producer = await get_producer()
    for row in rows:
        msg = FeatureUpdateMessage(
            feature_view=feature_view,
            entity_id=row.entity_id,
            features=row.features,
            event_timestamp=row.event_timestamp,
            schema_version=version,
            producer_id=producer_id,
        )
        key, value = encode_feature_update(msg)
        await producer.send(s.kafka_feature_updates_topic, key=key, value=value)

    await producer.flush()
    return len(rows)


async def produce_schema_change(
    feature_view: str,
    new_version: int,
    old_version: int | None,
    change_type: str,
) -> None:
    s = get_settings()
    msg = SchemaChangeMessage(
        feature_view=feature_view,
        old_version=old_version,
        new_version=new_version,
        change_type=change_type,
    )
    producer = await get_producer()
    key, value = encode_schema_change(msg)
    await producer.send(s.kafka_schema_changes_topic, key=key, value=value)
    await producer.flush()
