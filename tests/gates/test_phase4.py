"""Gate 4: a results table for all three detectors, from a leak-free split.

Requires `make dataset`, `make train` and `make evaluate` (no running stack needed).
"""

from pathlib import Path

from ml.dataset import HELD_OUT, load, split, vector
from services.detector.detectors import load_detector

ROOT = Path(__file__).parents[2]
DETECTORS = ("threshold", "xgboost", "autoencoder")
HARD_CASES = ("flash crowd", "slow ramp")


def test_split_has_no_session_leakage() -> None:
    train, test = split(load(ROOT / "data/dataset.parquet"))
    assert not {r["session_id"] for r in train} & {r["session_id"] for r in test}
    assert not {r["kind"] for r in train} & HELD_OUT
    assert {r["kind"] for r in test} >= HELD_OUT


def test_all_detectors_load_by_name_and_score_the_same_replay() -> None:
    _, test = split(load(ROOT / "data/dataset.parquet"))
    replay = [vector(r) for r in test[:200]]
    for name in DETECTORS:
        detector = load_detector(name, ROOT / "models")
        scores = [detector.score(v) for v in replay]
        assert all(0.0 <= s <= 1.0 for s in scores), name


def test_results_table_is_complete() -> None:
    table = [
        line for line in (ROOT / "docs/results.md").read_text().splitlines() if line.startswith("|")
    ]
    header, rows = table[0], table[2:]
    assert [c.strip() for c in header.strip("|").split("|")][1:] == list(DETECTORS)
    for row in rows:
        cells = [c.strip() for c in row.strip("|").split("|")]
        assert len(cells) == 1 + len(DETECTORS) and all(cells), row
    assert all(any(case in row for row in rows) for case in HARD_CASES)
