PY      ?= .venv/bin/python
COMPOSE ?= docker compose -f infra/compose/docker-compose.yml

.PHONY: venv up down logs test lint gate baseline

venv:
	python3 -m venv .venv
	$(PY) -m pip install -e '.[dev]'

up:
	$(COMPOSE) up -d --build
	$(COMPOSE) build attacker

down:
	$(COMPOSE) down -v

logs:
	$(COMPOSE) logs -f

test:
	$(PY) -m pytest tests/unit --cov --cov-report=term-missing

lint:
	$(PY) -m ruff check .
	$(PY) -m ruff format --check .
	$(PY) -m mypy

# Usage: make gate N=1
gate:
	@test -n "$(N)" || (echo "usage: make gate N=<phase>"; exit 1)
	$(PY) -m pytest -s tests/gates/test_phase$(N).py

# Fit the detector baseline on normal traffic (stack up, no attack running).
baseline:
	$(PY) scripts/train_baseline.py --seconds 60
