from pathlib import Path

import pytest

from services.detector.detectors import ThresholdDetector, load_detector
from shared.events import FeatureVector


def fv(
    pkt_rate: float = 10.0, byte_rate: float = 1000.0, syn_ack_ratio: float = 0.0
) -> FeatureVector:
    return FeatureVector.model_validate(
        {
            "src_ip": "10.0.0.5",
            "dst_ip": "10.0.0.2",
            "window_end": 1000,
            "pkt_rate": pkt_rate,
            "byte_rate": byte_rate,
            "syn_ack_ratio": syn_ack_ratio,
            "dst_distinct_srcs": 3,
            "dst_src_entropy": 1.5,
            "dst_port_entropy": 0.0,
        }
    )


def test_fit_takes_p99_times_margin_with_floor() -> None:
    normal = [fv(pkt_rate=float(r)) for r in range(1, 101)]  # p99 of 1..100 is 99
    d = ThresholdDetector.fit(normal, margin=2.0)
    assert d.bounds == {"pkt_rate": 198.0, "byte_rate": 2000.0, "syn_ack_ratio": 2.0}


def test_score_is_half_at_the_bound_and_capped() -> None:
    d = ThresholdDetector({"pkt_rate": 100.0, "byte_rate": 1e9, "syn_ack_ratio": 1e9})
    assert d.score(fv(pkt_rate=10)) == pytest.approx(0.05)
    assert d.score(fv(pkt_rate=100)) == 0.5
    assert d.score(fv(pkt_rate=10_000)) == 1.0


def test_worst_feature_decides() -> None:
    d = ThresholdDetector({"pkt_rate": 100.0, "byte_rate": 1e9, "syn_ack_ratio": 2.0})
    assert d.score(fv(pkt_rate=10, syn_ack_ratio=50)) == 1.0  # SYN flood at a modest rate


def test_save_load_round_trip(tmp_path: Path) -> None:
    ThresholdDetector({"pkt_rate": 1.0, "byte_rate": 2.0, "syn_ack_ratio": 3.0}).save(
        tmp_path / "baseline.json"
    )
    loaded = load_detector("threshold", tmp_path)
    assert isinstance(loaded, ThresholdDetector)
    assert loaded.bounds == {"pkt_rate": 1.0, "byte_rate": 2.0, "syn_ack_ratio": 3.0}
    assert loaded.name == "threshold"


def test_unknown_detector() -> None:
    with pytest.raises(ValueError, match="unknown detector"):
        load_detector("magic", Path("models"))
