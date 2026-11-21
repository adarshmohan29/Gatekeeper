import random
from pathlib import Path

import pytest

from services.drift.psi import FlaggedSources, Reference, drift, load, save

rng = random.Random(0)
TRAINING = [rng.gauss(23, 4) for _ in range(1000)]  # a client's packets/s, normal hours
REF = Reference.fit(TRAINING)
ALERT = 0.25  # alert threshold in infra/monitoring/alerts.yml


def live(mean: float, sd: float = 1.0, n: int = 600) -> list[float]:
    return [rng.gauss(mean, sd) for _ in range(n)]


def test_reference() -> None:
    assert sum(REF.proportions) == pytest.approx(1.0)
    assert REF.n == 1000


def test_same_traffic_is_stable() -> None:
    assert drift(REF, live(23, sd=4)) < 0.1


def test_narrower_traffic_is_not_drift() -> None:
    # Quiet hours: live traffic covers only part of what training saw. Standard PSI calls
    # this a big shift; the models have seen all of it, so it isn't one. Concentrating in
    # one bin still adds a little (~0.1), well below the alert.
    assert drift(REF, live(23, sd=0.5)) < ALERT


@pytest.mark.parametrize("factor", [1.5, 2, 10])
def test_traffic_moving_somewhere_new_is_drift(factor: float) -> None:
    assert drift(REF, live(23 * factor)) > ALERT


def test_a_short_attack_in_the_window_is_not_drift() -> None:
    window = live(23, sd=4, n=580) + [2000.0] * 20  # 20 s of one attacker among normal sources
    assert drift(REF, window) < ALERT


def test_round_trip(tmp_path: Path) -> None:
    save({"pkt_rate": REF}, tmp_path / "ref.json")
    assert load(tmp_path / "ref.json") == {"pkt_rate": REF}


def test_flagged_sources_are_held_out_for_a_while() -> None:
    flagged = FlaggedSources(hold_s=60)
    flagged.flag("172.30.0.200", now=1000)
    assert flagged.recent("172.30.0.200", now=1059)
    assert not flagged.recent("172.30.0.200", now=1060)
    assert not flagged.recent("172.30.0.6", now=1000)  # never flagged
    flagged.flag("172.30.0.201", now=1050)
    flagged.prune(now=1070)
    assert flagged.last == {"172.30.0.201": 1050}
