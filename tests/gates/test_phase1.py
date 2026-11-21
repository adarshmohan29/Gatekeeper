"""Gate 1: flow events from live traffic reach Kafka, keyed by source IP.

Requires the stack to be running (`make up`); Locust provides the traffic.
"""

from itertools import islice

from shared.config import Settings
from shared.events import FlowEvent
from shared.kafka import stream, tail_consumer

WANTED = 20
TIMEOUT_S = 20


def test_flow_events_reach_kafka() -> None:
    settings = Settings.from_env()
    consumer = tail_consumer(settings.kafka_bootstrap, settings.flow_topic, "gate1")
    try:
        msgs = list(islice(stream(consumer, TIMEOUT_S), WANTED))
    finally:
        consumer.close()

    assert len(msgs) == WANTED, f"only {len(msgs)} events in {TIMEOUT_S}s"
    events = [FlowEvent.from_bytes(value) for _, value in msgs]
    assert all(key == e.partition_key for (key, _), e in zip(msgs, events, strict=True))
    # Only inbound HTTP: proves outgoing packets and our own Kafka traffic are filtered out.
    assert all(e.dst_port == 80 for e in events)
