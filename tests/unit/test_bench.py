from services.bench.synthetic import DST, event_bytes, source
from shared.events import FlowEvent, Proto


def test_synthetic_events_are_valid_flow_events() -> None:
    e = FlowEvent.from_bytes(event_bytes(source(0), 1000.5))
    assert (str(e.src_ip), str(e.dst_ip), e.ts, e.proto) == ("10.250.0.1", DST, 1000.5, Proto.TCP)


def test_sources_are_distinct_and_stay_in_their_range() -> None:
    ips = [source(i) for i in range(20_000)]
    assert len(set(ips)) == len(ips)
    assert all(ip.startswith("10.250.") for ip in ips)
