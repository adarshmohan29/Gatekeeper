"""Synthetic flow events for load tests, built as JSON directly: pydantic would cost more
per event than the pipeline being measured. Shape is checked by the unit test."""

from __future__ import annotations

DST = "10.250.255.254"  # a destination nothing else uses


def source(i: int) -> str:
    """The i-th simulated client: 10.250.0.0/16, so they never collide with real pods."""
    return f"10.250.{i // 250 % 250}.{i % 250 + 1}"


def event_bytes(src: str, ts: float) -> bytes:
    return (
        f'{{"src_ip":"{src}","dst_ip":"{DST}","ts":{ts},"src_port":40000,"dst_port":80,'
        f'"proto":6,"tcp_flags":16,"pkt_len":100}}'
    ).encode()
