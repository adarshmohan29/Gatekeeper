"""Consume flow events, keep windows in Redis, emit one feature vector per active source per second.

Kafka offsets are committed after the Redis write; replays of an uncommitted batch are
skipped by offset, so each packet is counted exactly once (see windows.py).
"""

from __future__ import annotations

import signal
import sys
import time

from prometheus_client import Counter, start_http_server
from redis import Redis

from services.feature_engine.windows import apply, read_features
from shared.config import Settings
from shared.events import FlowEvent
from shared.kafka import batch, commit, group_consumer, parse_or_dead_letter, producer

TTL_SLACK_S = 5  # keep buckets a little longer than the window so late reads still see them
COUNTED = Counter("gatekeeper_events_counted_total", "Flow events counted into Redis windows")
REPLAYED = Counter("gatekeeper_events_replayed_total", "Replayed flow events skipped by offset")
EMITTED = Counter("gatekeeper_vectors_emitted_total", "Feature vectors published")


def main() -> None:
    settings = Settings.from_env()
    r = Redis.from_url(settings.redis_url, decode_responses=True)
    consumer = group_consumer(settings.kafka_bootstrap, settings.flow_topic, "feature-engine")
    out = producer(settings.kafka_bootstrap)
    start_http_server(settings.metrics_port)
    # Sources seen since the last tick: the only in-process state. Losing it costs one tick.
    active: set[str] = set()
    next_tick = int(time.time()) + 1
    # SIGTERM -> SystemExit, so `finally` delivers pending vectors and leaves the group at once.
    signal.signal(signal.SIGTERM, lambda *_: sys.exit(0))

    try:
        while True:
            msgs = batch(consumer.consume(1000, timeout=0.2))
            if msgs:
                parsed = parse_or_dead_letter(msgs, FlowEvent, out, settings.flow_topic)
                events = apply(r, parsed, settings.window_s + TTL_SLACK_S)
                COUNTED.inc(len(events))
                REPLAYED.inc(len(parsed) - len(events))
                active.update(str(e.src_ip) for e in events)
                commit(consumer)

            if time.time() >= next_tick:
                for fv in read_features(r, list(active), next_tick, settings.window_s):
                    out.produce(settings.features_topic, key=fv.partition_key, value=fv.to_bytes())
                    EMITTED.inc()
                out.poll(0)
                active.clear()
                next_tick += 1
    finally:
        out.flush()
        consumer.close()


if __name__ == "__main__":
    main()
