import math

import fakeredis
import pytest

from services.feature_engine.windows import aggregate, entropy, read_features, write
from shared.events import FlowEvent, Proto, TcpFlags

VICTIM = "10.0.0.2"
WINDOW = 10
END = 1000  # window covers seconds 990..999


def ev(
    src: str, sec: float, flags: int = TcpFlags.ACK, port: int = 80, size: int = 100
) -> FlowEvent:
    return FlowEvent.model_validate(
        {
            "ts": sec,
            "src_ip": src,
            "dst_ip": VICTIM,
            "src_port": 40000,
            "dst_port": port,
            "proto": Proto.TCP,
            "tcp_flags": flags,
            "pkt_len": size,
        }
    )


@pytest.fixture
def r() -> fakeredis.FakeRedis:
    return fakeredis.FakeRedis(decode_responses=True)


def test_aggregate_sums_a_batch() -> None:
    inc = aggregate([ev("10.0.0.5", 990.2, TcpFlags.SYN), ev("10.0.0.5", 990.7, size=50)])
    assert inc["s:10.0.0.5:990"] == {"p": 2, "b": 150, "syn": 1, "ack": 1, f"to:{VICTIM}": 2}
    assert inc[f"d:{VICTIM}:990:src"] == {"10.0.0.5": 2}
    assert inc[f"d:{VICTIM}:990:port"] == {"80": 2}


@pytest.mark.parametrize(
    ("counts", "bits"),
    [([], 0.0), ([7], 0.0), ([1, 1], 1.0), ([5, 5, 5, 5], 2.0), ([1, 0, 1], 1.0)],
)
def test_entropy(counts: list[int], bits: float) -> None:
    assert math.isclose(entropy(counts), bits, abs_tol=1e-12)


def test_features_over_window(r: fakeredis.FakeRedis) -> None:
    events = [ev("10.0.0.5", 995, TcpFlags.SYN, port=p) for p in (80, 81)]
    events += [ev("10.0.0.6", 999), ev("10.0.0.6", 989), ev("10.0.0.6", 1000)]  # last two outside
    write(r, aggregate(events), ttl_s=60)

    fv = read_features(r, "10.0.0.5", END, WINDOW)
    assert fv is not None
    assert str(fv.dst_ip) == VICTIM
    assert (fv.pkt_rate, fv.byte_rate) == (0.2, 20.0)
    assert fv.syn_ack_ratio == 2.0  # no ACKs: SYNs / (0 + 1)
    assert fv.dst_distinct_srcs == 2
    assert math.isclose(fv.dst_src_entropy, entropy([2, 1]))  # .5 sent 2 packets, .6 sent 1
    assert math.isclose(fv.dst_port_entropy, entropy([2, 1]))  # port 80 twice, 81 once

    other = read_features(r, "10.0.0.6", END, WINDOW)
    assert other is not None
    assert other.pkt_rate == 0.1  # only the packet at 999 is inside the window


def test_concurrent_writers_add_up(r: fakeredis.FakeRedis) -> None:
    # Two replicas writing the same bucket: HINCRBY makes the counts add, not overwrite.
    write(r, aggregate([ev("10.0.0.5", 995)]), ttl_s=60)
    write(r, aggregate([ev("10.0.0.5", 995)]), ttl_s=60)
    fv = read_features(r, "10.0.0.5", END, WINDOW)
    assert fv is not None
    assert fv.pkt_rate == 0.2


def test_buckets_expire(r: fakeredis.FakeRedis) -> None:
    write(r, aggregate([ev("10.0.0.5", 995)]), ttl_s=15)
    assert 0 < r.ttl("s:10.0.0.5:995") <= 15


def test_silent_source_has_no_features(r: fakeredis.FakeRedis) -> None:
    assert read_features(r, "10.0.0.9", END, WINDOW) is None
