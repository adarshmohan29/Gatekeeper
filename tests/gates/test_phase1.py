"""Gate 1: flow events from live traffic reach Kafka, keyed by source IP.

Requires the stack to be running (`make up`); Locust provides the traffic.
"""

import time

from shared.config import Settings
from shared.events import FlowEvent
from shared.kafka import tail_consumer

WANTED = 20
TIMEOUT_S = 20


def test_flow_events_reach_kafka() -> None:
    settings = Settings.from_env()
    consumer = tail_consumer(settings.kafka_bootstrap, settings.flow_topic, "gate1")
    events: list[FlowEvent] = []
    deadline = time.monotonic() + TIMEOUT_S
    try:
        while len(events) < WANTED and time.monotonic() < deadline:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            value = msg.value()
            assert msg.error() is None and value is not None, msg.error()
            event = FlowEvent.from_bytes(value)
            assert msg.key() == event.partition_key
            events.append(event)
    finally:
        consumer.close()

    assert len(events) == WANTED, f"only {len(events)} events in {TIMEOUT_S}s"
    # Only inbound HTTP: proves outgoing packets and our own Kafka traffic are filtered out.
    assert all(e.dst_port == 80 for e in events)
