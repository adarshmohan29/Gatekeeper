"""Load generator: produce synthetic flow events straight into Kafka at a fixed rate.

Each source sends rate/sources events/s: keep that near normal client rates (~25/s) so the
detector doesn't flag the load itself. Run several with different --first-source values.
"""

from __future__ import annotations

import argparse
import time

from services.bench.synthetic import event_bytes, source
from shared.config import Settings
from shared.kafka import producer

TICK_S = 0.01
FLUSH_S = 30


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("rate", type=int, help="events per second")
    parser.add_argument("seconds", type=int)
    parser.add_argument("sources", type=int, help="distinct simulated clients")
    parser.add_argument("--first-source", type=int, default=0)
    args = parser.parse_args()

    settings = Settings.from_env()
    out = producer(settings.kafka_bootstrap)
    srcs = [source(args.first_source + i) for i in range(args.sources)]
    keys = [s.encode() for s in srcs]
    sent, start = 0, time.monotonic()
    while (elapsed := time.monotonic() - start) < args.seconds:
        due = int(args.rate * elapsed)  # pace to the target rate; catch up if we fell behind
        now = time.time()
        while sent < due:
            i = sent % args.sources
            try:
                out.produce(settings.flow_topic, key=keys[i], value=event_bytes(srcs[i], now))
                sent += 1
            except BufferError:
                out.poll(0.01)
        out.poll(0)
        time.sleep(TICK_S)
    undelivered = out.flush(FLUSH_S)  # bounded: an unreachable broker must not hang the test
    took = time.monotonic() - start
    print(f"offered {args.rate}/s, produced {sent} events in {took:.1f}s = {sent / took:,.0f}/s")
    if undelivered:
        raise SystemExit(f"{undelivered} events not delivered: is KAFKA_BOOTSTRAP right?")


if __name__ == "__main__":
    main()
