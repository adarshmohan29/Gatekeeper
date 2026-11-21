"""Gate 2: normal traffic is not flagged; a SYN flood is, within a few seconds.

Requires the stack (`make up`) and a fitted baseline (`make baseline`). Launches the attack itself.
"""

import subprocess
import time
from ipaddress import ip_address
from pathlib import Path

from shared.config import Settings
from shared.events import Score
from shared.kafka import stream, tail_consumer

COMPOSE = [
    "docker",
    "compose",
    "-f",
    str(Path(__file__).parents[2] / "infra/compose/docker-compose.yml"),
]
ATTACKER = ip_address("172.30.0.200")  # fixed in docker-compose.yml
NORMAL_S = 30
ATTACK_S = 20
OBSERVE_S = 15  # watch scores for most of the attack
MAX_DELAY_S = 10


def test_flood_is_flagged() -> None:
    settings = Settings.from_env()
    threshold = settings.score_threshold
    consumer = tail_consumer(settings.kafka_bootstrap, settings.scores_topic, "gate2")
    try:
        normal = [Score.from_bytes(m.value) for m in stream(consumer, NORMAL_S)]

        attack = subprocess.Popen(
            [*COMPOSE, "run", "--rm", "attacker", "syn", str(ATTACK_S)],
            stdout=subprocess.DEVNULL,
        )
        start = time.monotonic()
        try:
            during = [
                (time.monotonic() - start, Score.from_bytes(m.value))
                for m in stream(consumer, OBSERVE_S)
            ]
        finally:
            attack.wait(timeout=ATTACK_S + 30)
    finally:
        consumer.close()

    assert normal, "no scores at all: is the pipeline running?"
    false_alarms = [s for s in normal if s.score >= threshold]
    assert not false_alarms, f"normal traffic flagged: {false_alarms}"

    flagged = [(t, s) for t, s in during if s.score >= threshold]
    assert flagged, "flood was never flagged"
    assert all(s.src_ip == ATTACKER for _, s in flagged), "a normal source was flagged"
    delay = flagged[0][0]
    print(f"\ndetection delay (attack launch -> first flagged score): {delay:.1f}s")
    assert delay <= MAX_DELAY_S
