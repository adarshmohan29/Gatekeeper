"""Train the ML detectors and export them to ONNX (model bytes + metadata for OnnxDetector).

- xgboost: supervised, on all six features. Trees are scale-invariant: no preprocessing.
- autoencoder: trained on normal traffic only, on the per-source features (destination features
  are shared by every source talking to an attacked victim, so they can't attribute an attack).
  log1p + standardization live inside the exported graph, so serving can't drift from training.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

import numpy as np
import torch
from numpy.typing import NDArray
from onnxmltools import convert_xgboost
from onnxmltools.convert.common.data_types import FloatTensorType
from torch import nn
from xgboost import XGBClassifier

from services.detector.detectors import SCORED_FEATURES, p99

ALL_FEATURES = (
    "pkt_rate",
    "byte_rate",
    "syn_ack_ratio",
    "dst_distinct_srcs",
    "dst_src_entropy",
    "dst_port_entropy",
)
MIN_STD = 0.1  # in log1p units: keeps a feature that's ~constant in normal traffic from exploding
MARGIN = 2.0  # same rule as the threshold baseline: alarm at 2x the normal p99

Rows = Sequence[dict[str, Any]]


def matrix(rows: Rows, features: Sequence[str]) -> NDArray[np.float32]:
    return np.array([[r[f] for f in features] for r in rows], dtype=np.float32)


def train_xgboost(rows: Rows) -> tuple[XGBClassifier, bytes, dict[str, Any]]:
    """Returns (native model, ONNX bytes, metadata)."""
    model = XGBClassifier(n_estimators=100, max_depth=4, learning_rate=0.1, random_state=7)
    model.fit(matrix(rows, ALL_FEATURES), [r["label"] for r in rows])
    onx = convert_xgboost(
        model, initial_types=[("input", FloatTensorType([None, len(ALL_FEATURES)]))]
    )
    meta = {"features": list(ALL_FEATURES), "output": "probabilities", "column": 1}
    return model, onx.SerializeToString(), meta


class Autoencoder(nn.Module):
    """Outputs each row's reconstruction error (mean squared, in standardized log space)."""

    mean: torch.Tensor  # registered buffers: saved and exported with the model
    std: torch.Tensor

    def __init__(self, mean: torch.Tensor, std: torch.Tensor) -> None:
        super().__init__()
        self.register_buffer("mean", mean)
        self.register_buffer("std", std)
        n = len(mean)
        self.net = nn.Sequential(
            nn.Linear(n, 8),
            nn.Tanh(),
            nn.Linear(8, 2),
            nn.Tanh(),
            nn.Linear(2, 8),
            nn.Tanh(),
            nn.Linear(8, n),
        )

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        z = (torch.log1p(x) - self.mean) / self.std
        error: torch.Tensor = ((self.net(z) - z) ** 2).mean(dim=1)
        return error


def train_autoencoder(rows: Rows, epochs: int = 300) -> tuple[Autoencoder, bytes, dict[str, Any]]:
    """Returns (native model, ONNX bytes, metadata)."""
    torch.manual_seed(7)
    # One thread: the net is tiny, and torch's OpenMP threads deadlock against XGBoost's
    # OpenMP runtime in the same process on macOS (see ml/__init__.py).
    torch.set_num_threads(1)
    x = torch.from_numpy(matrix([r for r in rows if r["role"] == "normal"], SCORED_FEATURES))
    logx = torch.log1p(x)
    model = Autoencoder(logx.mean(dim=0), logx.std(dim=0).clamp(min=MIN_STD))
    opt = torch.optim.Adam(model.parameters(), lr=1e-2)
    for _ in range(epochs):
        opt.zero_grad()
        loss = model(x).mean()
        loss.backward()
        opt.step()

    model.eval()
    with torch.no_grad():
        threshold = MARGIN * p99(model(x).tolist())
    program = torch.onnx.export(
        model,
        (x[:1],),
        input_names=["input"],
        output_names=["error"],
        dynamic_shapes=({0: torch.export.Dim("n")},),  # any batch size
        dynamo=True,
        verbose=False,
    )
    assert program is not None  # always returned when no file path is given
    meta = {"features": list(SCORED_FEATURES), "output": "error", "threshold": threshold}
    return model, program.model_proto.SerializeToString(), meta
