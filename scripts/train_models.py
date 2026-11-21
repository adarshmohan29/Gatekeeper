"""Train all three detectors on the training split of data/dataset.parquet; save to models/."""

import json
from pathlib import Path

from ml.dataset import load, split, vector
from ml.train import train_autoencoder, train_xgboost
from services.detector.detectors import SCORED_FEATURES, ThresholdDetector
from services.drift.psi import Reference, save

ROOT = Path(__file__).parents[1]
MODELS = ROOT / "models"


def main() -> None:
    train, test = split(load(ROOT / "data/dataset.parquet"))
    print(
        f"train: {len({r['session_id'] for r in train})} sessions, {len(train)} rows; "
        f"test: {len({r['session_id'] for r in test})} sessions, {len(test)} rows"
    )

    normal = [r for r in train if r["role"] == "normal"]
    ThresholdDetector.fit([vector(r) for r in normal]).save(MODELS / "baseline.json")
    # The drift monitor compares live traffic against normal training traffic, on per-source
    # features only: every session has an attack in it, so destination features in training
    # describe an attacked victim, and quiet live traffic would always look "drifted".
    refs = {f: Reference.fit([r[f] for r in normal]) for f in SCORED_FEATURES}
    save(refs, MODELS / "drift_reference.json")
    for name, fit in (("xgboost", train_xgboost), ("autoencoder", train_autoencoder)):
        _, onnx, meta = fit(train)
        (MODELS / f"{name}.onnx").write_bytes(onnx)
        (MODELS / f"{name}.json").write_text(json.dumps(meta, indent=2) + "\n")
    print(f"wrote baseline, drift reference, xgboost and autoencoder to {MODELS}")


if __name__ == "__main__":
    main()
