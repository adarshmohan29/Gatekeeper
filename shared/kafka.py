from __future__ import annotations

import uuid

from confluent_kafka import Consumer


def tail_consumer(bootstrap: str, topic: str, name: str) -> Consumer:
    """Consumer that reads only new messages, in a fresh group so no stale members delay it."""
    consumer = Consumer(
        {
            "bootstrap.servers": bootstrap,
            "group.id": f"{name}-{uuid.uuid4()}",
            "auto.offset.reset": "latest",
        }
    )
    consumer.subscribe([topic])
    return consumer
