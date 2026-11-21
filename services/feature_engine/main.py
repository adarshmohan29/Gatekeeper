"""Consume flow events, keep windows in Redis, emit one feature vector per active source per second.

At-least-once: offsets are committed only after the batch is in Redis. A crash replays at most
the uncommitted batch, which double-counts those packets in their buckets (see NOTES.md).
"""

from __future__ import annotations

import logging
import signal
import sys
import time

from redis import Redis

from services.feature_engine.windows import aggregate, read_features, write
from shared.config import Settings
from shared.events import FlowEvent
from shared.kafka import batch_values, group_consumer, producer

TTL_SLACK_S = 5  # keep buckets a little longer than the window so late reads still see them


def main() -> None:
    logging.basicConfig(level=logging.INFO)
    settings = Settings.from_env()
    r = Redis.from_url(settings.redis_url, decode_responses=True)
    consumer = group_consumer(settings.kafka_bootstrap, settings.flow_topic, "feature-engine")
    out = producer(settings.kafka_bootstrap)
    # Sources seen since the last tick: the only in-process state. Losing it costs one tick.
    active: set[str] = set()
    next_tick = int(time.time()) + 1
    # SIGTERM -> SystemExit, so `finally` delivers pending vectors and leaves the group at once.
    signal.signal(signal.SIGTERM, lambda *_: sys.exit(0))

    try:
        while True:
            msgs = consumer.consume(1000, timeout=0.2)
            events = [FlowEvent.from_bytes(v) for v in batch_values(msgs)]
            if events:
                write(r, aggregate(events), settings.window_s + TTL_SLACK_S)
                active.update(str(e.src_ip) for e in events)
            if msgs:
                consumer.commit(asynchronous=False)

            if time.time() >= next_tick:
                for src in active:
                    fv = read_features(r, src, next_tick, settings.window_s)
                    if fv is not None:
                        out.produce(
                            settings.features_topic, key=fv.partition_key, value=fv.to_bytes()
                        )
                out.poll(0)
                active.clear()
                next_tick += 1
    finally:
        out.flush()
        consumer.close()


if __name__ == "__main__":
    main()
