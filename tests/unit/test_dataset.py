from collections import Counter
from pathlib import Path
from typing import Any

from ml.dataset import HELD_OUT, SCHEDULE, Session, load, make_sessions, row, save, split, vector
from shared.events import FeatureVector

SESSION = Session(id=4, kind="syn", sources=("172.30.0.200",), duration_s=10, pps=500, size=64)
FLASH = Session(id=5, kind="flash", sources=("172.30.0.210",), duration_s=10, pps=40, size=0)


def fv(src: str) -> FeatureVector:
    return FeatureVector.model_validate(
        {
            "src_ip": src,
            "dst_ip": "172.30.0.3",
            "window_end": 1000,
            "pkt_rate": 1.0,
            "byte_rate": 1.0,
            "syn_ack_ratio": 0.0,
            "dst_distinct_srcs": 3,
            "dst_src_entropy": 1.5,
            "dst_port_entropy": 0.0,
        }
    )


def test_schedule_is_deterministic_and_varied() -> None:
    sessions = make_sessions(seed=7)
    assert sessions == make_sessions(seed=7)
    assert Counter(s.kind for s in sessions) == Counter(SCHEDULE)
    assert all(len(s.sources) == 2 for s in sessions if s.kind == "mixed")
    floods = [s for s in sessions if s.kind != "flash"]
    assert len({s.pps for s in floods}) > len(floods) // 2  # rates really vary
    assert all(s.sources[0].startswith("172.30.0.21") for s in sessions if s.kind == "flash")


def test_labels_come_from_the_schedule() -> None:
    attack = row(fv("172.30.0.200"), SESSION)
    assert (attack["role"], attack["label"], attack["session_id"]) == ("attacker", 1, 4)
    assert row(fv("172.30.0.6"), SESSION)["label"] == 0  # a normal source during the attack
    crowd = row(fv("172.30.0.210"), FLASH)
    assert (crowd["role"], crowd["label"]) == ("flash", 0)  # legitimate, however loud


def rows_for(kinds: dict[int, str]) -> list[dict[str, Any]]:
    return [{"session_id": s, "kind": k, "i": i} for s, k in kinds.items() for i in range(3)]


def test_split_is_by_session_and_holds_out_kinds() -> None:
    kinds = {i: k for i, k in enumerate(["syn"] * 10 + ["http"] * 4 + sorted(HELD_OUT))}
    train, test = split(rows_for(kinds))
    train_s, test_s = {r["session_id"] for r in train}, {r["session_id"] for r in test}
    assert not train_s & test_s
    assert train_s | test_s == set(kinds)
    assert not {r["kind"] for r in train} & HELD_OUT
    assert len(test_s - {s for s, k in kinds.items() if k in HELD_OUT}) == round(14 * 0.3)


def test_parquet_round_trip(tmp_path: Path) -> None:
    rows = [row(fv("172.30.0.200"), SESSION), row(fv("172.30.0.6"), SESSION)]
    save(rows, tmp_path / "d" / "dataset.parquet")
    loaded = load(tmp_path / "d" / "dataset.parquet")
    assert loaded == rows
    assert vector(loaded[0]) == fv("172.30.0.200")
