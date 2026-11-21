"""Sniff inbound packets on the victim's interface and publish them to Kafka, keyed by source IP.

Runs inside the victim's network namespace (needs NET_RAW).
"""

from __future__ import annotations

import logging
import signal
import socket
import sys
import time
from urllib.parse import urlsplit

from services.collector.parse import parse_frame
from shared.config import Settings
from shared.kafka import producer

ETH_P_ALL = 0x0003
DROP_LOG_EVERY = 10_000

log = logging.getLogger("collector")


def main() -> None:
    logging.basicConfig(level=logging.INFO)
    settings = Settings.from_env()
    # Control-plane traffic shares this interface; it isn't traffic to the victim.
    control_ports = {
        int(settings.kafka_bootstrap.rsplit(":", 1)[1]),
        urlsplit(settings.enforcer_url).port,
    }
    out = producer(settings.kafka_bootstrap)
    sock = socket.socket(socket.AF_PACKET, socket.SOCK_RAW, socket.ntohs(ETH_P_ALL))
    sock.bind((settings.capture_iface, 0))
    dropped = 0
    signal.signal(signal.SIGTERM, lambda *_: sys.exit(0))  # so `finally` runs on docker stop

    try:
        while True:
            frame, addr = sock.recvfrom(65535)
            if addr[2] == socket.PACKET_OUTGOING:
                continue
            event = parse_frame(frame, time.time())
            if event is None or not control_ports.isdisjoint((event.src_port, event.dst_port)):
                continue
            try:
                out.produce(settings.flow_topic, key=event.partition_key, value=event.to_bytes())
            except BufferError:  # producer queue full: shed load instead of crashing
                dropped += 1
                if dropped % DROP_LOG_EVERY == 1:
                    log.warning("producer queue full, %d events dropped so far", dropped)
            out.poll(0)

    finally:
        out.flush()  # deliver events already queued


if __name__ == "__main__":
    main()
