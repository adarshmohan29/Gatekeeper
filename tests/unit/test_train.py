import json
import random
from pathlib import Path
from typing import Any

import numpy as np
import onnxruntime as ort
import pytest
import torch

from ml.dataset import vector
from ml.train import ALL_FEATURES, matrix, train_autoencoder, train_xgboost
from services.detector.detectors import SCORED_FEATURES, load_detector


def synthetic(n: int = 200) -> list[dict[str, Any]]:
    """Normal sources around 25 pkt/s; attackers at thousands of SYNs/s with no ACKs."""
    rng = random.Random(0)
    rows = []
    for i in range(n):
        attack = i % 4 == 0
        rate = rng.uniform(1000, 5000) if attack else rng.uniform(15, 35)
        rows.append(
            {
                "src_ip": f"10.0.{i // 250}.{i % 250 + 1}",
                "dst_ip": "10.0.9.9",
                "window_end": 1000 + i,
                "pkt_rate": rate,
                "byte_rate": rate * (60 if attack else 100),
                "syn_ack_ratio": rate if attack else rng.uniform(0, 0.2),
                "dst_distinct_srcs": 4,
                "dst_src_entropy": rng.uniform(1.0, 2.0),
                "dst_port_entropy": 0.0,
                "label": int(attack),
                "role": "attacker" if attack else "normal",
            }
        )
    return rows


@pytest.fixture(scope="module")
def data() -> list[dict[str, Any]]:
    return synthetic()


def run_onnx(onnx: bytes, x: np.ndarray, output: str) -> np.ndarray:
    (out,) = ort.InferenceSession(onnx).run([output], {"input": x})
    return np.asarray(out)


def test_xgboost_onnx_matches_native(data: list[dict[str, Any]]) -> None:
    model, onnx, meta = train_xgboost(data)
    x = matrix(data, ALL_FEATURES)
    np.testing.assert_allclose(
        run_onnx(onnx, x, "probabilities"), model.predict_proba(x), atol=1e-5
    )
    assert meta == {"features": list(ALL_FEATURES), "output": "probabilities", "column": 1}


def test_autoencoder_onnx_matches_native(data: list[dict[str, Any]]) -> None:
    model, onnx, meta = train_autoencoder(data, epochs=50)
    x = matrix(data, SCORED_FEATURES)
    with torch.no_grad():
        native = model(torch.from_numpy(x)).numpy()
    np.testing.assert_allclose(run_onnx(onnx, x, "error"), native, rtol=1e-4, atol=1e-6)
    assert meta["features"] == list(SCORED_FEATURES) and meta["threshold"] > 0


@pytest.mark.parametrize("name", ["xgboost", "autoencoder"])
def test_served_models_separate_attacks(
    name: str, data: list[dict[str, Any]], tmp_path: Path
) -> None:
    _, onnx, meta = train_xgboost(data) if name == "xgboost" else train_autoencoder(data)
    (tmp_path / f"{name}.onnx").write_bytes(onnx)
    (tmp_path / f"{name}.json").write_text(json.dumps(meta))
    detector = load_detector(name, tmp_path)
    scores = {r["label"]: detector.score(vector(r)) for r in data[:8]}
    assert detector.name == name
    assert scores[1] >= 0.5 > scores[0]
    assert all(0.0 <= s <= 1.0 for s in scores.values())
