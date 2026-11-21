"""Print flow events from Kafka as they arrive. Run from the host while the stack is up."""

from shared.config import Settings
from shared.events import FlowEvent
from shared.kafka import tail_consumer


def main() -> None:
    settings = Settings.from_env()
    consumer = tail_consumer(settings.kafka_bootstrap, settings.flow_topic, "debug")
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            value = msg.value()
            if msg.error() or value is None:
                raise RuntimeError(msg.error())
            print(FlowEvent.from_bytes(value))
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
