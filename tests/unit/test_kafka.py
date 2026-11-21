from __future__ import annotations

from dataclasses import dataclass
from typing import Any, cast

import pytest
from confluent_kafka import KafkaError, KafkaException

from shared.events import Score
from shared.kafka import (
    DEAD_LETTERS,
    Delivered,
    batch,
    commit,
    parse_or_dead_letter,
    producer,
    stream,
    tail_consumer,
)


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

    def partition(self) -> int:
        return 3

    def offset(self) -> int:
        return 42


TRANSIENT = KafkaError(KafkaError._TRANSPORT)
FATAL = KafkaError(KafkaError._FATAL, fatal=True)


class FakeConsumer:
    def __init__(self, msgs: list[FakeMsg | None]) -> None:
        self.msgs = msgs

    def poll(self, _timeout: float) -> FakeMsg | None:
        return self.msgs.pop(0) if self.msgs else None


def test_batch_skips_transient_errors_and_empty_values() -> None:
    msgs = [FakeMsg(b"a"), FakeMsg(err=TRANSIENT), FakeMsg(None), FakeMsg(b"b")]
    assert batch(cast(Any, msgs)) == [Delivered(3, 42, b"k", b"a"), Delivered(3, 42, b"k", b"b")]


def test_fatal_error_raises() -> None:
    with pytest.raises(KafkaException):
        batch(cast(Any, [FakeMsg(err=FATAL)]))


def test_stream_yields_until_deadline() -> None:
    consumer = FakeConsumer([FakeMsg(b"a"), None, FakeMsg(err=TRANSIENT), FakeMsg(b"b")])
    assert [m.value for m in stream(cast(Any, consumer), seconds=0.2)] == [b"a", b"b"]


def test_clients_construct_without_a_broker() -> None:
    # Construction and subscribe are local; nothing connects until the first poll.
    tail_consumer("127.0.0.1:1", "flow-events", "test").close()
    producer("127.0.0.1:1")


class FakeProducer:
    def __init__(self) -> None:
        self.sent: list[tuple[str, bytes | None, bytes, dict[str, str]]] = []

    def produce(self, topic: str, key: bytes | None, value: bytes, headers: dict[str, str]) -> None:
        self.sent.append((topic, key, value, headers))


def test_malformed_messages_go_to_the_dead_letter_topic() -> None:
    good = Score.model_validate(
        {"src_ip": "10.0.0.5", "dst_ip": "10.0.0.2", "window_end": 1, "detector": "t", "score": 0.3}
    )
    msgs = [Delivered(0, 1, b"k", good.to_bytes()), Delivered(0, 2, b"k", b"not json")]
    out = FakeProducer()
    before = DEAD_LETTERS.labels("scores")._value.get()

    parsed = parse_or_dead_letter(msgs, Score, cast(Any, out), "scores")

    assert parsed == [(msgs[0], good)]
    ((topic, key, value, headers),) = out.sent
    assert (topic, key, value) == ("scores-dlq", b"k", b"not json")
    assert "Invalid JSON" in headers["error"]
    assert DEAD_LETTERS.labels("scores")._value.get() == before + 1


class CommitFails:
    def __init__(self, code: int) -> None:
        self.code = code

    def commit(self, **_: bool) -> None:
        raise KafkaException(KafkaError(self.code))


@pytest.mark.parametrize("code", [KafkaError.NOT_COORDINATOR, KafkaError._ASSIGNMENT_LOST])
def test_transient_commit_failures_are_skipped(code: int) -> None:
    commit(cast(Any, CommitFails(code)))  # coordinator moved / session lost: no crash


def test_other_commit_failures_still_raise() -> None:
    with pytest.raises(KafkaException):
        commit(cast(Any, CommitFails(KafkaError.TOPIC_AUTHORIZATION_FAILED)))
