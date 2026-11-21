"""Gate 5c: one command demos an attack and its mitigation.

Runs `make demo` exactly as a user would (it brings the stack up first). The demo exits
non-zero unless the attacker is blocked and normal traffic stays under 1% failures.
"""

import subprocess
from pathlib import Path


def test_make_demo_blocks_the_attack_and_spares_normal_traffic() -> None:
    demo = subprocess.run(
        ["make", "demo"],
        cwd=Path(__file__).parents[2],
        capture_output=True,
        text=True,
        timeout=900,
        check=False,  # print the output first, then assert
    )
    print(demo.stdout[-2000:], demo.stderr[-2000:])
    assert demo.returncode == 0
    assert "PASS: attacker blocked" in demo.stdout
