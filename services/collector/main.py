"""Sniff inbound packets on the victim's interface and publish them to Kafka, keyed by source IP.

Runs inside the victim's network namespace (needs NET_RAW).
"""

from __future__ import annotations

import socket
import time

from confluent_kafka import Producer

from services.collector.parse import parse_frame
from shared.config import Settings

ETH_P_ALL = 0x0003


def main() -> None:
    settings = Settings.from_env()
    kafka_port = int(settings.kafka_bootstrap.rsplit(":", 1)[1])
    producer = Producer(
        {
            "bootstrap.servers": settings.kafka_bootstrap,
            "linger.ms": 50,
            "enable.idempotence": True,  # retries never duplicate events
        }
    )
    sock = socket.socket(socket.AF_PACKET, socket.SOCK_RAW, socket.ntohs(ETH_P_ALL))
    sock.bind((settings.capture_iface, 0))

    while True:
        frame, addr = sock.recvfrom(65535)
        if addr[2] == socket.PACKET_OUTGOING:
            continue
        event = parse_frame(frame, time.time())
        # Skip our own Kafka traffic, which shares this interface.
        if event is None or kafka_port in (event.src_port, event.dst_port):
            continue
        producer.produce(settings.flow_topic, key=event.partition_key, value=event.to_bytes())
        producer.poll(0)


if __name__ == "__main__":
    main()
