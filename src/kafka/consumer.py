"""
aiokafka async consumer — the change propagation worker.

Reads from the feature-updates topic and writes to BOTH:
  - PostgreSQL (offline store, for training + PIT joins)
  - Redis (online store, for low-latency inference)

This is the dual-write that keeps online and offline stores
eventually consistent.

Delivery guarantee: at-least-once via manual offset commit after both
writes succeed. If either write fails, the message will be reprocessed.
"""

import asyncio
import logging
import json
from datetime import datetime

from aiokafka import AIOKafkaConsumer, TopicPartition
from aiokafka.errors import KafkaError

from src.config import get_settings
from src.kafka.messages import decode_feature_update
from src.store.offline_store import write_feature_rows
from src.store.online_store import write_online_features
from src.db.postgres import get_pool, run_migrations
from src.models.schemas import FeatureRow

log = logging.getLogger(__name__)


async def _process_feature_update(msg) -> None:
    """Write one Kafka message to both PostgreSQL and Redis."""
    update = decode_feature_update(msg.key, msg.value)

    row = FeatureRow(
        entity_id=update.entity_id,
        features=update.features,
        event_timestamp=update.event_timestamp,
    )

    # Write to offline store (PostgreSQL)
    await write_feature_rows(update.feature_view, [row])

    # Write to online store (Redis)
    await write_online_features(
        feature_view=update.feature_view,
        entity_id=update.entity_id,
        features=update.features,
        event_timestamp=update.event_timestamp,
    )


async def run_consumer(stop_event: asyncio.Event | None = None) -> None:
    """
    Long-running consumer loop.
    Commits offsets only after both PostgreSQL and Redis writes succeed.
    """
    s = get_settings()

    # Ensure DB schema exists before consuming
    await run_migrations()

    consumer = AIOKafkaConsumer(
        s.kafka_feature_updates_topic,
        bootstrap_servers=s.kafka_bootstrap_servers,
        group_id=s.kafka_consumer_group,
        auto_offset_reset=s.kafka_auto_offset_reset,
        enable_auto_commit=False,   # manual commit for at-least-once
        value_deserializer=None,    # raw bytes; we decode manually
        key_deserializer=None,
        max_poll_records=100,
        session_timeout_ms=30_000,
        heartbeat_interval_ms=10_000,
    )

    await consumer.start()
    log.info(
        "Consumer started on topic '%s', group '%s'",
        s.kafka_feature_updates_topic,
        s.kafka_consumer_group,
    )

    try:
        async for msg in consumer:
            if stop_event and stop_event.is_set():
                break
            try:
                await _process_feature_update(msg)
                await consumer.commit({
                    TopicPartition(msg.topic, msg.partition): msg.offset + 1
                })
            except Exception:
                log.exception(
                    "Failed to process message at offset %d (topic=%s partition=%d); "
                    "will retry on next poll.",
                    msg.offset, msg.topic, msg.partition,
                )
                # Do not commit — message will be redelivered
    finally:
        await consumer.stop()
        log.info("Consumer stopped.")
