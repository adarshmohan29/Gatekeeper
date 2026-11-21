from __future__ import annotations

import time
import uuid
from collections.abc import Iterable, Iterator, Sequence
from typing import NamedTuple, Protocol, Self, TypeVar

from confluent_kafka import Consumer, KafkaError, KafkaException, Message, Producer
from prometheus_client import Counter
from pydantic import ValidationError

DEAD_LETTERS = Counter(
    "gatekeeper_dead_letters_total", "Malformed messages sent to a dead-letter topic", ["topic"]
)


class Delivered(NamedTuple):
    partition: int
    offset: int
    key: bytes | None
    value: bytes


# A broker that dies abruptly sends no TCP reset: requests in flight to it hang until this
# timeout (librdkafka's default is 60 s, which stalled its partitions for a minute in the chaos
# tests). Fetches long-poll for at most 0.5 s, so 10 s is still generous for a live broker.
SOCKET_TIMEOUT_MS = 10_000


def producer(bootstrap: str) -> Producer:
    # Idempotent: broker-side dedup, so producer retries never duplicate records.
    return Producer(
        {
            "bootstrap.servers": bootstrap,
            "linger.ms": 50,
            "enable.idempotence": True,
            "socket.timeout.ms": SOCKET_TIMEOUT_MS,
        }
    )


def group_consumer(bootstrap: str, topic: str, group: str) -> Consumer:
    """Consumer-group member for a service. Offsets are committed manually, after processing."""
    consumer = Consumer(
        {
            "bootstrap.servers": bootstrap,
            "group.id": group,
            "auto.offset.reset": "latest",
            "enable.auto.commit": False,
            # A member that dies without leaving the group is evicted after this long; its
            # partitions sit unconsumed until then (clean shutdowns leave at once, via close()).
            # 6 s is the broker's minimum. Heartbeats come from librdkafka's own thread, so a
            # busy Python loop doesn't miss them; only a truly dead process gets evicted.
            "session.timeout.ms": 6_000,
            "heartbeat.interval.ms": 2_000,
            "socket.timeout.ms": SOCKET_TIMEOUT_MS,
        }
    )
    consumer.subscribe([topic])
    return consumer


def tail_consumer(bootstrap: str, topic: str, name: str) -> Consumer:
    """Reads only new messages, in a fresh group so no stale members delay it."""
    return group_consumer(bootstrap, topic, f"{name}-{uuid.uuid4()}")


# Commit failures that only mean "the group moved under us": the coordinator broker restarted
# or a rebalance is in progress. The next commit covers the same offsets.
TRANSIENT_COMMIT_ERRORS = frozenset(
    {
        KafkaError.NOT_COORDINATOR,
        KafkaError.COORDINATOR_NOT_AVAILABLE,
        KafkaError.REBALANCE_IN_PROGRESS,
        KafkaError.ILLEGAL_GENERATION,
        KafkaError.UNKNOWN_MEMBER_ID,
        KafkaError._TRANSPORT,
    }
)


def commit(consumer: Consumer) -> None:
    """Commit consumed offsets, skipping transient failures instead of crashing.

    Crashing on them (as the first chaos run did) adds a pod restart with backoff to every
    broker restart. Skipping is safe: processing is idempotent, so a replay changes nothing.
    """
    try:
        consumer.commit(asynchronous=False)
    except KafkaException as e:
        if e.args[0].code() not in TRANSIENT_COMMIT_ERRORS:
            raise


def _delivered(msg: Message) -> Delivered | None:
    """The message as a record, or None for a transient error (librdkafka retries those itself)."""
    err = msg.error()
    if err is not None:
        if err.fatal():
            raise KafkaException(err)
        return None
    value = msg.value()
    if value is None:
        return None
    # partition/offset are typed Optional but always set on a data message.
    return Delivered(msg.partition() or 0, msg.offset() or 0, msg.key(), value)


def batch(msgs: Iterable[Message]) -> list[Delivered]:
    return [d for d in map(_delivered, msgs) if d is not None]


def stream(consumer: Consumer, seconds: float) -> Iterator[Delivered]:
    """Yield each message until `seconds` have passed."""
    deadline = time.monotonic() + seconds
    while (left := deadline - time.monotonic()) > 0:
        msg = consumer.poll(min(left, 1.0))
        if msg is not None and (d := _delivered(msg)) is not None:
            yield d


class Parseable(Protocol):
    @classmethod
    def from_bytes(cls, data: bytes) -> Self: ...


R = TypeVar("R", bound=Parseable)


def parse_or_dead_letter(
    msgs: Sequence[Delivered], record: type[R], out: Producer, topic: str
) -> list[tuple[Delivered, R]]:
    """Parse each message; send any that don't parse to `<topic>-dlq` instead of crashing.

    A poison message would otherwise crash the consumer, which restarts and reads it again.
    """
    # Create the series at 0 on the first (normal) batch: a labelled counter that first appears
    # at 1 hides that increase from Prometheus, so the first dead letter would never alert.
    dead_letters = DEAD_LETTERS.labels(topic)
    parsed = []
    for m in msgs:
        try:
            parsed.append((m, record.from_bytes(m.value)))
        except ValidationError as e:
            out.produce(f"{topic}-dlq", key=m.key, value=m.value, headers={"error": str(e)[:1000]})
            dead_letters.inc()
    return parsed
