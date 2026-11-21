import struct

from services.collector.parse import parse_frame
from shared.events import Proto, TcpFlags

ETH_IPV4 = b"\x00" * 12 + b"\x08\x00"
SRC, DST = bytes([10, 0, 0, 5]), bytes([10, 0, 0, 2])


def ip_header(proto: int, total_len: int, options: bytes = b"") -> bytes:
    ihl = 5 + len(options) // 4
    return (
        struct.pack("!BBHHHBBH4s4s", 0x40 | ihl, 0, total_len, 0, 0, 64, proto, 0, SRC, DST)
        + options
    )


def tcp_header(flags: int) -> bytes:
    return struct.pack("!HHIIBBHHH", 40000, 80, 0, 0, 0x50, flags, 0, 0, 0)


def test_tcp_syn() -> None:
    ev = parse_frame(ETH_IPV4 + ip_header(6, 40) + tcp_header(TcpFlags.SYN), ts=1.5)
    assert ev is not None
    assert (str(ev.src_ip), str(ev.dst_ip)) == ("10.0.0.5", "10.0.0.2")
    assert (ev.src_port, ev.dst_port) == (40000, 80)
    assert (ev.proto, ev.pkt_len, ev.ts) == (Proto.TCP, 40, 1.5)
    assert ev.has_flag(TcpFlags.SYN)
    assert not ev.has_flag(TcpFlags.ACK)


def test_udp() -> None:
    udp = struct.pack("!HHHH", 5353, 9999, 8, 0)
    ev = parse_frame(ETH_IPV4 + ip_header(17, 28) + udp, ts=0.0)
    assert ev is not None
    assert (ev.proto, ev.src_port, ev.dst_port, ev.tcp_flags) == (Proto.UDP, 5353, 9999, 0)


def test_skips_ip_options() -> None:
    frame = ETH_IPV4 + ip_header(6, 44, options=b"\x01" * 4) + tcp_header(TcpFlags.ACK)
    ev = parse_frame(frame, ts=0.0)
    assert ev is not None
    assert ev.dst_port == 80
    assert ev.has_flag(TcpFlags.ACK)


def test_rejects_non_ipv4() -> None:
    ipv6 = b"\x00" * 12 + b"\x86\xdd"
    assert parse_frame(ipv6 + ip_header(6, 40) + tcp_header(0), ts=0.0) is None


def test_rejects_other_protocols() -> None:
    icmp = 1
    assert parse_frame(ETH_IPV4 + ip_header(icmp, 28) + b"\x00" * 8, ts=0.0) is None


def test_rejects_truncated() -> None:
    assert parse_frame(ETH_IPV4 + b"\x45", ts=0.0) is None
    assert parse_frame(ETH_IPV4 + ip_header(6, 40) + tcp_header(0)[:10], ts=0.0) is None
