from __future__ import annotations

import time
import uuid
from collections.abc import Iterable, Iterator

from confluent_kafka import Consumer, KafkaException, Message, Producer


def producer(bootstrap: str) -> Producer:
    # Idempotent: broker-side dedup, so producer retries never duplicate records.
    return Producer({"bootstrap.servers": bootstrap, "linger.ms": 50, "enable.idempotence": True})


def group_consumer(bootstrap: str, topic: str, group: str) -> Consumer:
    """Consumer-group member for a service. Offsets are committed manually, after processing."""
    consumer = Consumer(
        {
            "bootstrap.servers": bootstrap,
            "group.id": group,
            "auto.offset.reset": "latest",
            "enable.auto.commit": False,
            # A member that dies without leaving the group is evicted after this long; its
            # partitions sit unconsumed until then. Clean shutdowns leave immediately (close()).
            "session.timeout.ms": 10_000,
        }
    )
    consumer.subscribe([topic])
    return consumer


def tail_consumer(bootstrap: str, topic: str, name: str) -> Consumer:
    """Reads only new messages, in a fresh group so no stale members delay it."""
    return group_consumer(bootstrap, topic, f"{name}-{uuid.uuid4()}")


def _value(msg: Message) -> bytes | None:
    """The message's value, or None for a transient error (librdkafka retries those itself)."""
    err = msg.error()
    if err is not None:
        if err.fatal():
            raise KafkaException(err)
        return None
    return msg.value()


def batch_values(msgs: Iterable[Message]) -> list[bytes]:
    return [v for v in map(_value, msgs) if v is not None]


def stream(consumer: Consumer, seconds: float) -> Iterator[tuple[bytes | None, bytes]]:
    """Yield (key, value) of each message until `seconds` have passed."""
    deadline = time.monotonic() + seconds
    while (left := deadline - time.monotonic()) > 0:
        msg = consumer.poll(min(left, 1.0))
        if msg is not None and (value := _value(msg)) is not None:
            yield msg.key(), value
