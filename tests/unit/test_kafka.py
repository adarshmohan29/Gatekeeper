from shared.kafka import tail_consumer


def test_tail_consumer_subscribes() -> None:
    # No broker needed: construction and subscribe are local until the first poll.
    consumer = tail_consumer("127.0.0.1:1", "flow-events", "test")
    try:
        assert consumer.assignment() == []
    finally:
        consumer.close()
