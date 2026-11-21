PY      ?= .venv/bin/python
COMPOSE ?= docker compose -f infra/compose/docker-compose.yml

.PHONY: venv up down logs test lint gate baseline dataset train evaluate k8s-up k8s-down load-test

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

# Phase 4: labelled dataset from the live stack (~20 min), train all detectors, compare them.
dataset:
	$(PY) scripts/generate_dataset.py

train:
	$(PY) scripts/train_models.py

evaluate:
	$(PY) scripts/evaluate.py

# Phase 5b: the same system on a local Kubernetes cluster (kind). Stop Compose first
# (`make down`): the Docker VM has 4 GB, not enough for both.
KUBECTL = kubectl --context kind-gatekeeper
CALICO = https://raw.githubusercontent.com/projectcalico/calico/v3.30.3/manifests/calico.yaml

k8s-up:
	kind get clusters | grep -qx gatekeeper || kind create cluster --config infra/k8s/kind.yaml
	$(KUBECTL) apply -f $(CALICO)
	$(KUBECTL) -n kube-system rollout status ds/calico-node --timeout=300s
	docker build -t gatekeeper:dev -f services/Dockerfile .
	docker build -t gatekeeper-attacker:dev services/attacker
	kind load docker-image gatekeeper:dev gatekeeper-attacker:dev --name gatekeeper
	kubectl kustomize --load-restrictor LoadRestrictionsNone infra/k8s | $(KUBECTL) apply -f -
	helm repo add kedacore https://kedacore.github.io/charts --force-update
	helm --kube-context kind-gatekeeper upgrade --install keda kedacore/keda --version 2.21.1 \
		--namespace keda --create-namespace --wait
	kubectl kustomize infra/k8s/autoscaling | $(KUBECTL) apply -f -
	$(KUBECTL) -n gatekeeper wait --for=condition=complete job/redpanda-init --timeout=300s
	$(KUBECTL) -n gatekeeper wait --for=condition=available deploy --all --timeout=300s

k8s-down:
	kind delete cluster --name gatekeeper

load-test:
	$(PY) scripts/load_test.py
