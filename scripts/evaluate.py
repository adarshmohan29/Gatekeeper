"""Replay the test split through each detector and write docs/results.md.

Each detector is evaluated in its own process, so its peak memory is its own.
"""

import json
import resource
import subprocess
import sys
import time
from datetime import date
from pathlib import Path
from typing import Any

from ml.dataset import HELD_OUT, load, split, vector
from ml.evaluate import metrics
from services.detector.detectors import load_detector
from shared.config import Settings

ROOT = Path(__file__).parents[1]
DETECTORS = ("threshold", "xgboost", "autoencoder")


def model_file(name: str) -> str:
    return "baseline" if name == "threshold" else name


BENCH_S = 2.0  # score the test set repeatedly for at least this long
NO_DECIMALS_FROM = 100


def peak_rss_mb() -> float:
    peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss  # bytes on macOS, KiB on Linux
    return peak / (1e6 if sys.platform == "darwin" else 1e3)


def evaluate_one(name: str) -> dict[str, float | None]:
    settings = Settings.from_env()
    _, test = split(load(ROOT / "data/dataset.parquet"))
    before = peak_rss_mb()  # harness and libraries already loaded: same for every detector
    detector = load_detector(name, ROOT / "models")
    vectors = [vector(r) for r in test]
    result = metrics(
        [(r, detector.score(v)) for r, v in zip(test, vectors, strict=True)],
        settings.score_threshold,
        settings.consecutive_windows,
    )

    n, wall, cpu = 0, time.perf_counter(), time.process_time()
    while time.perf_counter() - wall < BENCH_S:
        for v in vectors:
            detector.score(v)
        n += len(vectors)
    wall, cpu = time.perf_counter() - wall, time.process_time() - cpu
    return result | {
        "Throughput (windows/s, 1 core)": n / wall,
        "CPU per window (µs)": 1e6 * cpu / n,
        "Memory added by loading and running the detector (MB)": peak_rss_mb() - before,
        "Model size on disk (KB)": sum(
            p.stat().st_size for p in (ROOT / "models").glob(f"{model_file(name)}*")
        )
        / 1e3,
    }


def fmt(metric: str, value: float | None) -> str:
    if value is None:
        return "never detected" if "delay" in metric or "mitigation" in metric else "n/a"
    if "rate," in metric:
        return f"{value:.1%}"
    return f"{value:,.0f}" if value >= NO_DECIMALS_FROM else f"{value:.1f}"


def report(results: dict[str, dict[str, Any]], train: list[Any], test: list[Any]) -> str:
    n_train, n_test = (len({r["session_id"] for r in rows}) for rows in (train, test))
    header = "| Metric | " + " | ".join(DETECTORS) + " |\n|---|" + "---|" * len(DETECTORS)
    lines = [
        f"| {m} | " + " | ".join(fmt(m, results[d][m]) for d in DETECTORS) + " |"
        for m in results[DETECTORS[0]]
    ]
    held_out = ", ".join(sorted(HELD_OUT))
    return "\n".join(
        [
            "# Detector comparison",
            "",
            f"Generated {date.today()} by `make evaluate` from `data/dataset.parquet`"
            " (`make dataset`).",
            "",
            f"- **Data:** {n_train} training sessions ({len(train)} vectors), {n_test} test"
            f" sessions ({len(test)} vectors), split by session. Never in training: {held_out}"
            " (UDP is an unseen attack type; flash crowds and slow ramps are the hard cases).",
            "- **Same input for all:** each detector scores the same recorded test vectors;"
            " flagged = score >= 0.5.",
            "- **Delays** are in 1 s windows from an attacker's first vector; the live pipeline"
            " adds about 1 s of capture and Kafka latency (Gates 2-3).",
            "",
            header,
            *lines,
            "",
        ]
    )


def main() -> None:
    if sys.argv[1:2] == ["--one"]:
        print(json.dumps(evaluate_one(sys.argv[2])))
        return
    results = {
        name: json.loads(
            subprocess.run(
                [sys.executable, __file__, "--one", name],
                check=True,
                capture_output=True,
                text=True,
            ).stdout
        )
        for name in DETECTORS
    }
    train, test = split(load(ROOT / "data/dataset.parquet"))
    out = ROOT / "docs/results.md"
    out.write_text(report(results, train, test))
    print(out.read_text())


if __name__ == "__main__":
    main()
