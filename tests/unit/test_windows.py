import math

import fakeredis
import pytest

from services.feature_engine.windows import aggregate, apply, entropy, read_features
from shared.events import FlowEvent, Proto, TcpFlags
from shared.kafka import Delivered

VICTIM = "10.0.0.2"
WINDOW = 10
END = 1000  # window covers seconds 990..999
TTL = 60


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


def deliver(
    *events: FlowEvent, partition: int = 0, first_offset: int = 0
) -> list[tuple[Delivered, FlowEvent]]:
    return [
        (Delivered(partition, first_offset + i, e.partition_key, e.to_bytes()), e)
        for i, e in enumerate(events)
    ]


def rate(r: fakeredis.FakeRedis, src: str = "10.0.0.5") -> float:
    fv = read_features(r, src, END, WINDOW)
    assert fv is not None
    return fv.pkt_rate


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
    apply(r, deliver(*events), TTL)

    fv = read_features(r, "10.0.0.5", END, WINDOW)
    assert fv is not None
    assert str(fv.dst_ip) == VICTIM
    assert (fv.pkt_rate, fv.byte_rate) == (0.2, 20.0)
    assert fv.syn_ack_ratio == 2.0  # no ACKs: SYNs / (0 + 1)
    assert fv.dst_distinct_srcs == 2
    assert math.isclose(fv.dst_src_entropy, entropy([2, 1]))  # .5 sent 2 packets, .6 sent 1
    assert math.isclose(fv.dst_port_entropy, entropy([2, 1]))  # port 80 twice, 81 once
    assert rate(r, "10.0.0.6") == 0.1  # only the packet at 999 is inside the window


def test_replicas_on_different_partitions_add_up(r: fakeredis.FakeRedis) -> None:
    # Same bucket written by two replicas: HINCRBY makes the counts add, not overwrite.
    apply(r, deliver(ev("10.0.0.5", 995), partition=0), TTL)
    apply(r, deliver(ev("10.0.0.5", 995), partition=1), TTL)
    assert rate(r) == 0.2


def test_replayed_batch_is_counted_once(r: fakeredis.FakeRedis) -> None:
    # Crash after the Redis write but before the Kafka commit: the batch is delivered again.
    msgs = deliver(ev("10.0.0.5", 995), ev("10.0.0.5", 996))
    assert len(apply(r, msgs, TTL)) == 2
    assert apply(r, msgs, TTL) == []
    assert rate(r) == 0.2


def test_partly_replayed_batch_counts_only_new_messages(r: fakeredis.FakeRedis) -> None:
    apply(r, deliver(ev("10.0.0.5", 995), ev("10.0.0.5", 995)), TTL)  # offsets 0, 1
    third = ev("10.0.0.5", 996)
    new = apply(r, deliver(ev("10.0.0.5", 995), third, first_offset=1), TTL)  # offsets 1, 2
    assert new == [third]
    assert rate(r) == 0.3


def test_partitions_are_tracked_separately(r: fakeredis.FakeRedis) -> None:
    apply(r, deliver(ev("10.0.0.5", 995), partition=0, first_offset=5), TTL)
    # A lower offset on another partition is new data, not a replay.
    assert len(apply(r, deliver(ev("10.0.0.5", 995), partition=1, first_offset=2), TTL)) == 1
    assert (r.get("fe:offset:0"), r.get("fe:offset:1")) == ("5", "2")


def test_empty_batch_is_a_no_op(r: fakeredis.FakeRedis) -> None:
    assert apply(r, [], TTL) == []
    assert r.dbsize() == 0


def test_buckets_expire(r: fakeredis.FakeRedis) -> None:
    apply(r, deliver(ev("10.0.0.5", 995)), ttl_s=15)
    assert 0 < r.ttl("s:10.0.0.5:995") <= 15


def test_silent_source_has_no_features(r: fakeredis.FakeRedis) -> None:
    assert read_features(r, "10.0.0.9", END, WINDOW) is None
