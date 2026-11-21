"""Sniff inbound packets on the victim's interface and publish them to Kafka, keyed by source IP.

Runs inside the victim's network namespace (needs NET_RAW).
"""

from __future__ import annotations

import signal
import socket
import sys
import time
from urllib.parse import urlsplit

from prometheus_client import Counter, start_http_server

from services.collector.parse import parse_frame
from shared.config import Settings
from shared.kafka import producer

ETH_P_ALL = 0x0003
PUBLISHED = Counter("gatekeeper_events_published_total", "Flow events published to Kafka")
DROPPED = Counter("gatekeeper_events_dropped_total", "Flow events dropped: producer queue full")


def main() -> None:
    settings = Settings.from_env()
    # Control-plane traffic shares this interface; it isn't traffic to the victim.
    control_ports = {
        int(settings.kafka_bootstrap.rsplit(":", 1)[1]),
        urlsplit(settings.enforcer_url).port,
        settings.metrics_port,
    }
    out = producer(settings.kafka_bootstrap)
    sock = socket.socket(socket.AF_PACKET, socket.SOCK_RAW, socket.ntohs(ETH_P_ALL))
    sock.bind((settings.capture_iface, 0))
    start_http_server(settings.metrics_port)
    signal.signal(signal.SIGTERM, lambda *_: sys.exit(0))  # so `finally` runs on docker stop

    try:
        while True:
            frame, addr = sock.recvfrom(65535)
            # Only frames addressed to the victim. Not PACKET_OUTGOING (its own sends), and not
            # PACKET_OTHERHOST: when a peer's container is gone, the bridge floods the victim's
            # own retransmissions back to it, which looked like traffic *from* the victim.
            if addr[2] != socket.PACKET_HOST:
                continue
            event = parse_frame(frame, time.time())
            if event is None or not control_ports.isdisjoint((event.src_port, event.dst_port)):
                continue
            try:
                out.produce(settings.flow_topic, key=event.partition_key, value=event.to_bytes())
                PUBLISHED.inc()
            except BufferError:  # producer queue full: shed load instead of crashing
                DROPPED.inc()
            out.poll(0)
    finally:
        out.flush()  # deliver events already queued


if __name__ == "__main__":
    main()
