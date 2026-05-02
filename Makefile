SHELL ?= /bin/sh
PYTHON ?= .venv/bin/python
ENV_FILE ?= .env

ifneq (,$(wildcard $(ENV_FILE)))
include $(ENV_FILE)
export
endif

K8S_NAMESPACE ?= banking-lab5
K8S_BASE ?= k8s/base
FACADE_IMAGE ?= computer-system-architecture-facade:lab5
LOGGING_IMAGE ?= computer-system-architecture-logging:lab5
COUNTER_IMAGE ?= computer-system-architecture-counter:lab5

.PHONY: k8s-build k8s-up k8s-down k8s-status k8s-forward \
	k8s-logs-facade k8s-logs-logging k8s-logs-counter \
	send-test-transactions test-performance test-unit

k8s-build:
	docker build -f services/facade_service/Dockerfile -t $(FACADE_IMAGE) .
	docker build -f services/logging_service/Dockerfile -t $(LOGGING_IMAGE) .
	docker build -f services/counter_service/Dockerfile -t $(COUNTER_IMAGE) .

k8s-up:
	kubectl apply -k $(K8S_BASE)
	kubectl -n $(K8S_NAMESPACE) rollout status statefulset/postgres --timeout=180s
	kubectl -n $(K8S_NAMESPACE) rollout status statefulset/kafka --timeout=180s
	kubectl -n $(K8S_NAMESPACE) wait --for=condition=complete job/kafka-topic-init --timeout=180s
	kubectl -n $(K8S_NAMESPACE) rollout status statefulset/hazelcast --timeout=180s
	kubectl -n $(K8S_NAMESPACE) rollout status deployment/logging-service --timeout=180s
	kubectl -n $(K8S_NAMESPACE) rollout status deployment/counter-service --timeout=180s
	kubectl -n $(K8S_NAMESPACE) rollout status deployment/facade-service --timeout=180s

k8s-down:
	kubectl delete -k $(K8S_BASE) --ignore-not-found=true

k8s-status:
	kubectl -n $(K8S_NAMESPACE) get pods,svc,configmap,secret

k8s-forward:
	kubectl -n $(K8S_NAMESPACE) port-forward svc/facade-service 9000:8000

k8s-logs-facade:
	kubectl -n $(K8S_NAMESPACE) logs -f deployment/facade-service

k8s-logs-logging:
	kubectl -n $(K8S_NAMESPACE) logs -f deployment/logging-service

k8s-logs-counter:
	kubectl -n $(K8S_NAMESPACE) logs -f deployment/counter-service

send-test-transactions:
	$(PYTHON) scripts/send_test_transactions.py

test-performance:
	uv run pytest -m performance -s tests/performance/test_facade_performance.py

test-unit:
	uv run pytest -q tests/unit
