"""Turn a raw Ethernet frame into a FlowEvent (IPv4 TCP/UDP only)."""

from __future__ import annotations

import struct
from ipaddress import IPv4Address

from shared.events import FlowEvent, Proto

ETH_HDR_LEN = 14
ETHERTYPE_IPV4 = b"\x08\x00"
MIN_IP_HDR_LEN = 20
TCP_FLAGS_OFFSET = 13
UDP_PORTS_LEN = 4


def parse_frame(frame: bytes, ts: float) -> FlowEvent | None:
    if len(frame) < ETH_HDR_LEN + MIN_IP_HDR_LEN or frame[12:14] != ETHERTYPE_IPV4:
        return None
    ip = frame[ETH_HDR_LEN:]
    try:
        proto = Proto(ip[9])
    except ValueError:
        return None

    l4 = ip[(ip[0] & 0x0F) * 4 :]  # IHL is in 32-bit words; skips IP options
    if len(l4) < (TCP_FLAGS_OFFSET + 1 if proto is Proto.TCP else UDP_PORTS_LEN):
        return None

    src_port, dst_port = struct.unpack_from("!HH", l4)
    return FlowEvent(
        ts=ts,
        src_ip=IPv4Address(ip[12:16]),
        dst_ip=IPv4Address(ip[16:20]),
        src_port=src_port,
        dst_port=dst_port,
        proto=proto,
        tcp_flags=l4[TCP_FLAGS_OFFSET] if proto is Proto.TCP else 0,
        pkt_len=struct.unpack_from("!H", ip, 2)[0],  # IP total length
    )
