"""Gate 0: repo layout and tooling are in place."""

from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]

REQUIRED = [
    "docs/PROPOSAL_AND_PLAN.md",
    "PLAN.md",
    "CLAUDE.md",
    "NOTES.md",
    "Makefile",
    "pyproject.toml",
    ".gitignore",
    "shared/__init__.py",
    *(
        f"services/{s}"
        for s in ["collector", "feature_engine", "detector", "mitigation", "loadgen", "victim"]
    ),
    *(f"infra/{d}" for d in ["compose", "k8s", "monitoring"]),
    "models",
    "notebooks",
    "tests/unit",
    "tests/gates",
]


def test_layout() -> None:
    missing = [p for p in REQUIRED if not (ROOT / p).exists()]
    assert not missing, f"missing: {missing}"
