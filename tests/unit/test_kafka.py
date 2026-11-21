from __future__ import annotations

from dataclasses import dataclass
from typing import Any, cast

import pytest
from confluent_kafka import KafkaError, KafkaException

from shared.kafka import batch_values, producer, stream, tail_consumer


@dataclass
class FakeMsg:
    val: bytes | None = b"v"
    err: KafkaError | None = None

    def error(self) -> KafkaError | None:
        return self.err

    def value(self) -> bytes | None:
        return self.val

    def key(self) -> bytes:
        return b"k"


TRANSIENT = KafkaError(KafkaError._TRANSPORT)
FATAL = KafkaError(KafkaError._FATAL, fatal=True)


class FakeConsumer:
    def __init__(self, msgs: list[FakeMsg | None]) -> None:
        self.msgs = msgs

    def poll(self, _timeout: float) -> FakeMsg | None:
        return self.msgs.pop(0) if self.msgs else None


def test_batch_values_skips_transient_errors_and_empty_values() -> None:
    msgs = [FakeMsg(b"a"), FakeMsg(err=TRANSIENT), FakeMsg(None), FakeMsg(b"b")]
    assert batch_values(cast(Any, msgs)) == [b"a", b"b"]


def test_fatal_error_raises() -> None:
    with pytest.raises(KafkaException):
        batch_values(cast(Any, [FakeMsg(err=FATAL)]))


def test_stream_yields_until_deadline() -> None:
    consumer = FakeConsumer([FakeMsg(b"a"), None, FakeMsg(err=TRANSIENT), FakeMsg(b"b")])
    assert list(stream(cast(Any, consumer), seconds=0.2)) == [(b"k", b"a"), (b"k", b"b")]


def test_clients_construct_without_a_broker() -> None:
    # Construction and subscribe are local; nothing connects until the first poll.
    tail_consumer("127.0.0.1:1", "flow-events", "test").close()
    producer("127.0.0.1:1")
