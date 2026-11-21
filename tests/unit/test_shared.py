import pytest
from pydantic import ValidationError

from shared.config import Settings
from shared.events import FlowEvent, Proto, TcpFlags


def _event(**overrides: object) -> FlowEvent:
    fields: dict[str, object] = {
        "ts": 1700000000.5,
        "src_ip": "10.0.0.5",
        "dst_ip": "10.0.0.2",
        "src_port": 40000,
        "dst_port": 80,
        "proto": Proto.TCP,
        "tcp_flags": TcpFlags.SYN,
        "pkt_len": 60,
    }
    fields.update(overrides)
    return FlowEvent.model_validate(fields)


def test_round_trip() -> None:
    ev = _event()
    assert FlowEvent.from_bytes(ev.to_bytes()) == ev


def test_partition_key_is_src_ip() -> None:
    assert _event(src_ip="10.0.0.9").partition_key == b"10.0.0.9"


def test_flags() -> None:
    ev = _event(tcp_flags=TcpFlags.SYN | TcpFlags.ACK)
    assert ev.has_flag(TcpFlags.SYN)
    assert ev.has_flag(TcpFlags.ACK)
    assert not _event().has_flag(TcpFlags.ACK)


@pytest.mark.parametrize("bad", [{"src_port": 70000}, {"src_ip": "not-an-ip"}, {"pkt_len": -1}])
def test_rejects_invalid(bad: dict[str, object]) -> None:
    with pytest.raises(ValidationError):
        _event(**bad)


def test_settings_from_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("FLOW_TOPIC", "custom")
    s = Settings.from_env()
    assert s.flow_topic == "custom"
    assert s.kafka_bootstrap == "127.0.0.1:19092"
    assert s.capture_iface == "eth0"
    assert s.allowlist == ()


def test_allowlist_parses_networks(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ALLOWLIST", "172.30.0.201/32,10.0.0.0/8")
    assert [str(n) for n in Settings.from_env().allowlist] == ["172.30.0.201/32", "10.0.0.0/8"]
