"""Print records from a Kafka topic as they arrive. Usage: consume.py [topic]"""

import math
import sys

from shared.config import Settings
from shared.kafka import stream, tail_consumer


def main() -> None:
    settings = Settings.from_env()
    topic = sys.argv[1] if len(sys.argv) > 1 else settings.flow_topic
    consumer = tail_consumer(settings.kafka_bootstrap, topic, "debug")
    try:
        for m in stream(consumer, math.inf):
            print(m.value.decode())
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
