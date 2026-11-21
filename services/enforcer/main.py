"""Enforcer: applies mitigation rules with nftables. Runs in the victim's netns (NET_ADMIN).

The table is created once and kept across enforcer restarts, so active rules survive them.
"""

from __future__ import annotations

import secrets
import subprocess
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI, Header, HTTPException

from services.enforcer.nft import TABLE, add_rule_cmd, ruleset
from shared.config import Settings
from shared.events import Rule

settings = Settings.from_env()


@asynccontextmanager
async def lifespan(_: FastAPI) -> AsyncIterator[None]:
    exists = subprocess.run(["nft", "list", "table", *TABLE], capture_output=True, check=False)
    if exists.returncode != 0:
        subprocess.run(
            ["nft", "-f", "-"], input=ruleset(settings.rate_limit_pps), text=True, check=True
        )
    yield


app = FastAPI(lifespan=lifespan)


@app.get("/healthz")
def healthz() -> dict[str, str]:
    return {"status": "ok"}


@app.post("/rules", status_code=204)
def apply(rule: Rule, authorization: str = Header(default="")) -> None:
    # The API is reachable from the lab network, attackers included: without this check,
    # anyone could get legitimate users blocked. An empty token rejects everything.
    expected = f"Bearer {settings.enforcer_token}"
    if not settings.enforcer_token or not secrets.compare_digest(authorization, expected):
        raise HTTPException(status_code=401)
    subprocess.run(add_rule_cmd(rule), check=True)
