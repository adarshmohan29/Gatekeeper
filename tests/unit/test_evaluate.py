from typing import Any

import pytest

from ml.evaluate import metrics

K = 3


def r(role: str, w: int, kind: str = "syn", session: int = 1, src: str = "a") -> dict[str, Any]:
    return {"role": role, "window_end": w, "kind": kind, "session_id": session, "src_ip": src}


def test_false_positive_rates() -> None:
    scored = [(r("normal", 1), 0.1), (r("normal", 2), 0.9), (r("flash", 1), 0.7)]
    m = metrics(scored, 0.5, K)
    assert m["False-positive rate, normal traffic"] == 0.5
    assert m["False-positive rate, flash crowd"] == 1.0


def test_delay_and_time_to_mitigation() -> None:
    # Flagged at windows 102-106 after appearing at 100: delay 2, K=3 in a row reached at 104.
    scored = [(r("attacker", w), 0.9 if w >= 102 else 0.1) for w in range(100, 107)]
    m = metrics(scored, 0.5, K)
    assert m["Detection rate, seen attacks (SYN/HTTP/mixed)"] == 1.0
    assert m["Median detection delay, seen attacks (s)"] == 2
    assert m[f"Median time to mitigation, all attacks ({K} windows in a row, s)"] == 4


def test_groups_and_misses() -> None:
    scored = [
        (r("attacker", 10, kind="udp", src="u"), 0.9),
        (r("attacker", 10, kind="ramp", src="slow"), 0.1),  # never flagged
        (r("attacker", 11, kind="ramp", src="slow"), 0.2),
    ]
    m = metrics(scored, 0.5, K)
    assert (
        m["Detection rate, unseen attack type (UDP)"],
        m["Median detection delay, UDP (s)"],
    ) == (1.0, 0)
    assert m["Detection rate, slow ramp"] == 0.0
    assert m["Median detection delay, slow ramp (s)"] is None
    assert m["Detection rate, seen attacks (SYN/HTTP/mixed)"] is None  # none in this set
    assert m[f"Median time to mitigation, all attacks ({K} windows in a row, s)"] is None


@pytest.mark.parametrize("gap", [True, False])
def test_mitigation_needs_consecutive_flags(gap: bool) -> None:
    flagged = {100, 101, 103, 104} if gap else {100, 101, 102}
    scored = [(r("attacker", w), 0.9 if w in flagged else 0.1) for w in range(100, 106)]
    ttm = metrics(scored, 0.5, K)[
        f"Median time to mitigation, all attacks ({K} windows in a row, s)"
    ]
    assert ttm == (None if gap else 2)
