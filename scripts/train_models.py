"""Train all three detectors on the training split of data/dataset.parquet; save to models/."""

import json
from pathlib import Path

from ml.dataset import load, split, vector
from ml.train import train_autoencoder, train_xgboost
from services.detector.detectors import ThresholdDetector

ROOT = Path(__file__).parents[1]
MODELS = ROOT / "models"


def main() -> None:
    train, test = split(load(ROOT / "data/dataset.parquet"))
    print(
        f"train: {len({r['session_id'] for r in train})} sessions, {len(train)} rows; "
        f"test: {len({r['session_id'] for r in test})} sessions, {len(test)} rows"
    )

    ThresholdDetector.fit([vector(r) for r in train if r["role"] == "normal"]).save(
        MODELS / "baseline.json"
    )
    for name, fit in (("xgboost", train_xgboost), ("autoencoder", train_autoencoder)):
        _, onnx, meta = fit(train)
        (MODELS / f"{name}.onnx").write_bytes(onnx)
        (MODELS / f"{name}.json").write_text(json.dumps(meta, indent=2) + "\n")
    print(f"wrote baseline.json, xgboost.onnx, autoencoder.onnx to {MODELS}")


if __name__ == "__main__":
    main()
