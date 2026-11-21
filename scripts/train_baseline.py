"""Fit the threshold detector on normal traffic. Run with the stack up and no attack running."""

import argparse
import sys
from pathlib import Path

from services.detector.detectors import ThresholdDetector
from shared.config import Settings
from shared.events import FeatureVector
from shared.kafka import stream, tail_consumer


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--seconds", type=float, default=60)
    args = parser.parse_args()

    settings = Settings.from_env()
    consumer = tail_consumer(settings.kafka_bootstrap, settings.features_topic, "train")
    try:
        normal = [FeatureVector.from_bytes(m.value) for m in stream(consumer, args.seconds)]
    finally:
        consumer.close()

    if not normal:
        sys.exit("no feature vectors received: is the stack up (`make up`)?")
    detector = ThresholdDetector.fit(normal)
    detector.save(Path(settings.models_dir) / "baseline.json")
    print(f"fitted on {len(normal)} vectors: {detector.bounds}")


if __name__ == "__main__":
    main()
